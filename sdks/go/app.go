package kernable

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strings"
	"sync"
	"time"
)

// AppOptions describe an app: one service made of whole domains, for Client.App. Version,
// Description and Metadata go into the membership's metadata and the app's overview.
type AppOptions struct {
	// The service's name. Default: this process's service (Options.Service); one is required.
	Service string
	// The app's version, e.g. 1.0.0.
	Version string
	// One line about the app.
	Description string
	// More metadata.
	Metadata map[string]string
	// Every aggregate, read model and reactor defined on them runs in this app; their processes
	// are registered.
	Domains []*Domain
	// When the app's deployment takes the service over from another one that serves it. Default:
	// KERNABLE_SWITCH, else CaughtUp.
	Switch Switch
}

// App is a service made of whole domains, ready to run: one worker for all their aggregates,
// one ListenerHandle per read model and reactor, and their business processes, registered once
// the worker and listeners are connected. Create it with Client.App, start it with Run.
type App struct {
	c         *Client
	manifest  Manifest
	domains   []*Domain
	worker    *Worker
	listeners []*ListenerHandle
	processes []*ProcessBuilder

	readyCh   chan struct{}
	serving   chan struct{}
	servingMu sync.Once
	mu        sync.Mutex
	// The app's processes as the kernel registered them; filled once Ready.
	registered []ProcessVersion
	readyErr   error
}

// App is an app made of whole domains: Run applies the service's membership (its domains'
// aggregates as workers, their read models and reactors as listeners, those that check requests
// as middlewares, live ones not listed; and their processes), then runs all of them and, once
// they are connected, registers the processes. One service is one app.
func (c *Client) App(o AppOptions) (*App, error) {
	service := firstOf(o.Service, c.service)
	if service == "" {
		return nil, errors.New("an app needs a service: set Service in the app or the options")
	}
	sw := o.Switch
	if sw == "" {
		switch v := os.Getenv("KERNABLE_SWITCH"); v {
		case "", "caught-up":
			sw = CaughtUp
		case "now", "never":
			sw = Switch(v)
		default:
			return nil, fmt.Errorf("KERNABLE_SWITCH=%s: caught-up, now or never", v)
		}
	}
	a := &App{c: c, domains: o.Domains, readyCh: make(chan struct{}), serving: make(chan struct{})}
	var aggregates []aggregateDef
	metadata := map[string]string{}
	if o.Version != "" {
		metadata["version"] = o.Version
	}
	if o.Description != "" {
		metadata["description"] = o.Description
	}
	for k, v := range o.Metadata {
		metadata[k] = v
	}
	m := Manifest{Service: service, Metadata: metadata, Deployment: c.deployment, Switch: sw}
	for _, d := range o.Domains {
		for _, agg := range d.aggregates {
			aggregates = append(aggregates, agg)
			m.Workers = append(m.Workers, agg)
		}
		for _, l := range d.listeners {
			a.listeners = append(a.listeners, newListenerHandle(c, l, service))
			if l.options.Live {
				continue
			}
			if len(l.middlewares) > 0 {
				m.Middlewares = append(m.Middlewares, l)
			} else {
				m.Listeners = append(m.Listeners, l)
			}
		}
		for _, p := range d.processes {
			a.processes = append(a.processes, p)
			m.Processes = append(m.Processes, p.Qualified())
		}
	}
	if len(aggregates) > 0 {
		a.worker = newWorker(c, aggregates, service)
	}
	a.manifest = m
	return a, nil
}

// Manifest is the membership Run applies.
func (a *App) Manifest() Manifest { return a.manifest }

// Ready is closed once every worker and listener of the app is connected, its deployment serves
// the service and its processes are registered (or registering failed: see Run's error).
func (a *App) Ready() <-chan struct{} { return a.readyCh }

// Serving is closed once the app's membership is applied: its deployment serves the service.
func (a *App) Serving() <-chan struct{} { return a.serving }

// Registered is the app's processes as the kernel registered them, once Ready.
func (a *App) Registered() []ProcessVersion {
	a.mu.Lock()
	defer a.mu.Unlock()
	return slices.Clone(a.registered)
}

// Run applies the membership (the key needs manage for the service), then runs the worker and
// the listeners until ctx ends, reconnecting when a connection drops. If another deployment
// serves the service, the kernel switches to this one only as AppOptions.Switch says: until then
// the app runs beside it and applies its membership again every second. Once all are connected
// and it serves, it registers the domains' processes and prints an overview of the app on
// stderr. It returns an error if the kernel refuses the membership for another reason than the
// switch, or if a process cannot be registered (it stops the app then, so the deploy fails);
// nil once ctx ends.
func (a *App) Run(ctx context.Context, options ...WorkerOptions) error {
	started := time.Now()
	applied := true
	if err := a.c.Apply(ctx, a.manifest); err != nil {
		if !WaitsForSwitch(err) {
			return err
		}
		applied = false
		a.c.log.print(levelInfo, "app", fmt.Sprintf("deployment %s runs beside the one that serves (%v)", a.manifest.Deployment, err))
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if a.worker != nil {
		a.worker.announce = false
	}
	for _, l := range a.listeners {
		l.announce = false
	}
	var wg sync.WaitGroup
	if a.worker != nil {
		wg.Add(1)
		go func() { defer wg.Done(); _ = a.worker.Run(ctx, options...) }()
	}
	for _, l := range a.listeners {
		wg.Add(1)
		go func() { defer wg.Done(); _ = l.Run(ctx) }()
	}
	var runErr error
	wg.Add(1)
	go func() {
		defer wg.Done()
		if applied {
			a.servingMu.Do(func() { close(a.serving) })
		} else if !a.switchLater(ctx) {
			return
		} else {
			a.servingMu.Do(func() { close(a.serving) })
		}
		// Once all are connected and it serves; not at all if the app stops before.
		if a.worker != nil {
			select {
			case <-a.worker.Ready():
			case <-ctx.Done():
				return
			}
		}
		for _, l := range a.listeners {
			select {
			case <-l.Ready():
			case <-ctx.Done():
				return
			}
		}
		err := a.register(ctx)
		a.mu.Lock()
		a.readyErr = err
		a.mu.Unlock()
		close(a.readyCh)
		if err != nil {
			runErr = err
			cancel()
			return
		}
		a.overview(time.Since(started))
	}()
	wg.Wait()
	return runErr
}

// switchLater applies the membership every second until the kernel takes it; false if the app
// stops first.
func (a *App) switchLater(ctx context.Context) bool {
	for {
		sleep(ctx, time.Second)
		if ctx.Err() != nil {
			return false
		}
		err := a.c.Apply(ctx, a.manifest)
		if err == nil {
			a.c.log.print(levelInfo, "app", fmt.Sprintf("deployment %s serves %s now", a.manifest.Deployment, a.manifest.Service))
			return true
		}
		if !WaitsForSwitch(err) {
			a.c.log.print(levelWarn, "app", fmt.Sprintf("applying the membership failed: %v", err))
		}
	}
}

// register registers every process of the app's domains, in order.
func (a *App) register(ctx context.Context) error {
	var out []ProcessVersion
	for _, p := range a.processes {
		v, err := a.c.RegisterProcess(ctx, p)
		if err != nil {
			return err
		}
		out = append(out, v)
	}
	a.mu.Lock()
	a.registered = out
	a.mu.Unlock()
	return nil
}

func (a *App) overview(took time.Duration) {
	log := a.c.log
	if !log.enabled(levelInfo) {
		return
	}
	m := a.manifest
	names := map[string]bool{}
	for _, d := range a.domains {
		names[d.Name] = true
	}
	// A single domain's own names go without it; with several, every name is qualified.
	short := func(domain, name string) string {
		if len(names) == 1 && names[domain] {
			return name
		}
		return domain + "." + name
	}
	relative := func(t string) string {
		domain, name, ok := strings.Cut(t, ".")
		if ok && len(names) == 1 && names[domain] {
			return name
		}
		return t
	}
	count := func(n int, what string) string {
		if n == 1 {
			return fmt.Sprintf("%d %s", n, what)
		}
		return fmt.Sprintf("%d %ss", n, what)
	}
	listed := []string{count(len(m.Workers), "worker"), count(len(m.Listeners), "listener")}
	if len(m.Middlewares) > 0 {
		listed = append(listed, count(len(m.Middlewares), "middleware"))
	}
	if n := len(m.Processes); n > 0 {
		if n == 1 {
			listed = append(listed, "1 process")
		} else {
			listed = append(listed, fmt.Sprintf("%d processes", n))
		}
	}
	type row struct {
		kind, name string
		details    []string
	}
	var rows []row
	for _, w := range m.Workers {
		var types []string
		types = append(types, w.commandTypes()...)
		slices.Sort(types)
		rows = append(rows, row{"worker", short(w.domain().Name, w.typeName()), []string{"runs " + strings.Join(types, ", ")}})
	}
	for _, h := range a.listeners {
		kind := "listener"
		if h.listener.options.Live {
			kind = "live"
		}
		h.mu.Lock()
		at := h.startedAt
		h.mu.Unlock()
		rows = append(rows, row{kind, short(h.listener.Domain.Name, h.listener.Name), h.listener.details(at, relative)})
	}
	for _, r := range a.Registered() {
		domain, name, _ := strings.Cut(r.Name, ".")
		v := fmt.Sprintf("version %d", r.Version)
		if r.New {
			v += " (new)"
		}
		rows = append(rows, row{"process", short(domain, name), []string{v}})
	}
	width := 0
	for _, r := range rows {
		width = max(width, len(r.name))
	}
	var title []string
	title = append(title, "kernable")
	head := m.Service
	if v := m.Metadata["version"]; v != "" {
		head += " " + v
	}
	title = append(title, head)
	if d := m.Metadata["description"]; d != "" {
		title = append(title, d)
	}
	lines := []string{
		log.style(1, strings.Join(title, " · ")),
		fmt.Sprintf("  %-10s  %s", "kernel", log.peer),
		fmt.Sprintf("  %-10s  applied: %s", "membership", strings.Join(listed, ", ")),
		"",
	}
	for _, r := range rows {
		for i, d := range r.details {
			if i == 0 {
				lines = append(lines, fmt.Sprintf("  %-10s  %-*s  %s", r.kind, width, r.name, d))
			} else {
				lines = append(lines, fmt.Sprintf("  %-10s  %-*s  %s", "", width, "", d))
			}
		}
	}
	lines = append(lines, "", "  "+log.style(2, fmt.Sprintf("ready in %d ms", took.Milliseconds())), "")
	log.mu.Lock()
	defer log.mu.Unlock()
	fmt.Fprintln(os.Stderr, strings.Join(lines, "\n"))
}
