package kernable

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"slices"
	"testing"

	"github.com/kernable/kernable/sdks/go/internal/pb"
)

// The signing test vectors (proto/test-vectors/signing.json): every message and signature must
// come out byte for byte.

type vector struct {
	Input     json.RawMessage `json:"input"`
	Message   string          `json:"message"`
	Signature string          `json:"signature"`
	Hash      string          `json:"hash"`
	IDs       []string        `json:"ids"`
	Refs      []string        `json:"refs"`
	Seed      string          `json:"seed"`
	KeyID     string          `json:"key_id"`
}

type vContext struct {
	SenderTenant        string  `json:"sender_tenant"`
	SenderIdentity      string  `json:"sender_identity"`
	SenderAccount       string  `json:"sender_account"`
	SenderSession       string  `json:"sender_session"`
	OnBehalfOf          string  `json:"on_behalf_of"`
	ExpectedVersion     *uint64 `json:"expected_version"`
	CorrelationID       string  `json:"correlation_id"`
	CausationID         string  `json:"causation_id"`
	TraceID             string  `json:"trace_id"`
	BypassAuthorization bool    `json:"bypass_authorization"`
	Attributes          []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"attributes"`
}

func (c *vContext) pb() *pb.RequestContext {
	if c == nil {
		return nil
	}
	r := &pb.RequestContext{
		SenderTenant: c.SenderTenant, SenderIdentity: c.SenderIdentity, SenderAccount: c.SenderAccount,
		SenderSession: c.SenderSession, OnBehalfOf: c.OnBehalfOf, ExpectedVersion: c.ExpectedVersion,
		CorrelationId: c.CorrelationID, CausationId: c.CausationID, TraceId: c.TraceID,
		BypassAuthorization: c.BypassAuthorization,
	}
	for _, a := range c.Attributes {
		r.Attributes = append(r.Attributes, &pb.Attribute{Key: a.Key, Value: a.Value})
	}
	return r
}

type vCommand struct {
	CommandID     string    `json:"command_id"`
	TenantID      string    `json:"tenant_id"`
	Domain        string    `json:"domain"`
	AggregateType string    `json:"aggregate_type"`
	AggregateID   string    `json:"aggregate_id"`
	CommandType   string    `json:"command_type"`
	PayloadHex    string    `json:"payload_hex"`
	Context       *vContext `json:"context"`
	DeliverAt     int64     `json:"deliver_at"`
	DeliverUntil  int64     `json:"deliver_until"`
}

func (c vCommand) pb() *pb.Command {
	return &pb.Command{
		CommandId: c.CommandID, TenantId: c.TenantID, Domain: c.Domain, AggregateType: c.AggregateType,
		AggregateId: c.AggregateID, CommandType: c.CommandType, Payload: unhex(c.PayloadHex), Context: c.Context.pb(),
		DeliverAt: c.DeliverAt, DeliverUntil: c.DeliverUntil,
	}
}

func unhex(s string) []byte {
	b, err := hex.DecodeString(s)
	if err != nil {
		panic(err)
	}
	return b
}

func vectors(t *testing.T) (map[string]vector, *Key) {
	t.Helper()
	raw, err := os.ReadFile("../../proto/test-vectors/signing.json")
	if err != nil {
		t.Fatal(err)
	}
	all := map[string]json.RawMessage{}
	if err := json.Unmarshal(raw, &all); err != nil {
		t.Fatal(err)
	}
	v := map[string]vector{}
	for name, r := range all {
		var x vector
		if json.Unmarshal(r, &x) == nil {
			v[name] = x
		}
	}
	key, err := KeyFromHex(v["key"].Seed)
	if err != nil {
		t.Fatal(err)
	}
	return v, key
}

func input[T any](t *testing.T, v vector) T {
	t.Helper()
	var x T
	if err := json.Unmarshal(v.Input, &x); err != nil {
		t.Fatal(err)
	}
	return x
}

func check(t *testing.T, v map[string]vector, key *Key, name string, message []byte) {
	t.Helper()
	if got := hex.EncodeToString(message); got != v[name].Message {
		t.Errorf("%s message:\n got %s\nwant %s", name, got, v[name].Message)
	}
	if got := hex.EncodeToString(key.sign(message).Sig); got != v[name].Signature {
		t.Errorf("%s signature: got %s, want %s", name, got, v[name].Signature)
	}
}

func TestKeyID(t *testing.T) {
	v, key := vectors(t)
	if key.ID != v["key"].KeyID {
		t.Fatalf("key ID %s, want %s", key.ID, v["key"].KeyID)
	}
	if _, err := KeyFromHex("zz"); err == nil {
		t.Fatal("a key that is not hex is refused")
	}
}

type vEvent struct {
	EventType  string `json:"event_type"`
	Schema     uint32 `json:"schema"`
	PayloadHex string `json:"payload_hex"`
}

type vResult struct {
	Command         vCommand `json:"command"`
	ExpectedVersion uint64   `json:"expected_version"`
	Events          []vEvent `json:"events"`
}

func (r vResult) events() []*pb.NewEvent {
	var out []*pb.NewEvent
	for _, e := range r.Events {
		out = append(out, &pb.NewEvent{EventType: e.EventType, Schema: e.Schema, Payload: unhex(e.PayloadHex)})
	}
	return out
}

type vQuery struct {
	QueryID     string    `json:"query_id"`
	TenantID    string    `json:"tenant_id"`
	Domain      string    `json:"domain"`
	QueryType   string    `json:"query_type"`
	PayloadHex  string    `json:"payload_hex"`
	Context     *vContext `json:"context"`
	MinPosition uint64    `json:"min_position"`
	IssuedAt    int64     `json:"issued_at"`
	WaitOnly    bool      `json:"wait_only"`
	Target      *struct {
		AggregateType string `json:"aggregate_type"`
		AggregateID   string `json:"aggregate_id"`
	} `json:"target"`
}

func (q vQuery) pb() *pb.QueryRequest {
	r := &pb.QueryRequest{
		QueryId: q.QueryID, TenantId: q.TenantID, Domain: q.Domain, QueryType: q.QueryType, Payload: unhex(q.PayloadHex),
		Context: q.Context.pb(), MinPosition: q.MinPosition, IssuedAt: q.IssuedAt, WaitOnly: q.WaitOnly,
	}
	if q.Target != nil {
		r.Target = &pb.Target{AggregateType: q.Target.AggregateType, AggregateId: q.Target.AggregateID}
	}
	return r
}

func TestMessages(t *testing.T) {
	v, key := vectors(t)
	check(t, v, key, "command", commandMessage(input[vCommand](t, v["command"]).pb()))
	check(t, v, key, "command_scheduled", commandMessage(input[vCommand](t, v["command_scheduled"]).pb()))
	r := input[vResult](t, v["result"])
	check(t, v, key, "result", resultMessage(streamOf(r.Command.pb()), r.ExpectedVersion, r.events()))
	rej := input[struct {
		CommandID string `json:"command_id"`
		Code      string `json:"code"`
		Message   string `json:"message"`
	}](t, v["rejection"])
	check(t, v, key, "rejection", rejectionMessage(rej.CommandID, rej.Code, rej.Message))
	ch := input[struct {
		NonceHex string `json:"nonce_hex"`
	}](t, v["challenge"])
	check(t, v, key, "challenge", challengeMessage(unhex(ch.NonceHex)))
	for _, name := range []string{"query", "query_wait_only", "query_target"} {
		check(t, v, key, name, queryMessage(input[vQuery](t, v[name]).pb()))
	}
	cancel := input[struct {
		CommandID string    `json:"command_id"`
		TenantID  string    `json:"tenant_id"`
		Context   *vContext `json:"context"`
		IssuedAt  int64     `json:"issued_at"`
	}](t, v["cancel"])
	check(t, v, key, "cancel", cancelMessage(&pb.CancelRequest{CommandId: cancel.CommandID, TenantId: cancel.TenantID, Context: cancel.Context.pb(), IssuedAt: cancel.IssuedAt}))
	s := input[struct {
		TenantID      string `json:"tenant_id"`
		Domain        string `json:"domain"`
		AggregateType string `json:"aggregate_type"`
		AggregateID   string `json:"aggregate_id"`
		Version       uint64 `json:"version"`
		Schema        string `json:"schema"`
		StateHex      string `json:"state_hex"`
	}](t, v["snapshot"])
	check(t, v, key, "snapshot", snapshotMessage(s.TenantID, s.Domain, s.AggregateType, s.AggregateID, s.Version, s.Schema, unhex(s.StateHex)))
	sub := input[struct {
		AfterPosition uint64   `json:"after_position"`
		EventTypes    []string `json:"event_types"`
		IssuedAt      int64    `json:"issued_at"`
	}](t, v["subscribe"])
	check(t, v, key, "subscribe", subscribeMessage(sub.AfterPosition, sub.EventTypes, sub.IssuedAt))
	cp := input[struct {
		GlobalPosition uint64 `json:"global_position"`
		ChainHashHex   string `json:"chain_hash_hex"`
		CreatedAt      int64  `json:"created_at"`
	}](t, v["checkpoint"])
	check(t, v, key, "checkpoint", checkpointMessage(cp.GlobalPosition, unhex(cp.ChainHashHex), cp.CreatedAt))
}

func TestDataAndCache(t *testing.T) {
	v, key := vectors(t)
	d := input[struct {
		Op          string    `json:"op"`
		TenantID    string    `json:"tenant_id"`
		Domain      string    `json:"domain"`
		Key         string    `json:"key"`
		ContentType string    `json:"content_type"`
		DataHex     string    `json:"data_hex"`
		IssuedAt    int64     `json:"issued_at"`
		Context     *vContext `json:"context"`
	}](t, v["data_put"])
	check(t, v, key, "data_put", dataMessage(d.Op, &pb.DataRequest{TenantId: d.TenantID, Domain: d.Domain, Key: d.Key, ContentType: d.ContentType, Data: unhex(d.DataHex), IssuedAt: d.IssuedAt, Context: d.Context.pb()}))
	c := input[struct {
		Op       string    `json:"op"`
		TenantID string    `json:"tenant_id"`
		Domain   string    `json:"domain"`
		Key      string    `json:"key"`
		ValueHex string    `json:"value_hex"`
		TTLMs    uint64    `json:"ttl_ms"`
		IssuedAt int64     `json:"issued_at"`
		Context  *vContext `json:"context"`
	}](t, v["cache_set"])
	if c.Context != nil {
		t.Fatal("the cache vector has no context: it encodes like an empty one")
	}
	check(t, v, key, "cache_set", cacheMessage(c.Op, &pb.CacheRequest{TenantId: c.TenantID, Domain: c.Domain, Key: c.Key, Value: unhex(c.ValueHex), TtlMs: c.TTLMs, IssuedAt: c.IssuedAt}))
}

type vMembership struct {
	Service     string   `json:"service"`
	Workers     []string `json:"workers"`
	Listeners   []string `json:"listeners"`
	Middlewares []string `json:"middlewares"`
	Metadata    []struct {
		Key   string `json:"key"`
		Value string `json:"value"`
	} `json:"metadata"`
	IssuedAt   int64    `json:"issued_at"`
	Processes  []string `json:"processes"`
	Deployment string   `json:"deployment"`
}

func TestMemberships(t *testing.T) {
	v, key := vectors(t)
	for _, name := range []string{"membership", "membership_processes", "membership_deployment"} {
		m := input[vMembership](t, v[name])
		p := &pb.Membership{Service: m.Service, Workers: m.Workers, Listeners: m.Listeners, Middlewares: m.Middlewares, IssuedAt: m.IssuedAt, Processes: m.Processes, Deployment: m.Deployment}
		for _, a := range m.Metadata {
			p.Metadata = append(p.Metadata, &pb.Attribute{Key: a.Key, Value: a.Value})
		}
		check(t, v, key, name, membershipMessage(p))
	}
}

func TestProcesses(t *testing.T) {
	v, key := vectors(t)
	p := input[struct {
		ModelHex string `json:"model_hex"`
		IssuedAt int64  `json:"issued_at"`
	}](t, v["process"])
	check(t, v, key, "process", processMessage(unhex(p.ModelHex), p.IssuedAt))
	a := input[struct {
		Name     string    `json:"name"`
		Context  *vContext `json:"context"`
		IssuedAt int64     `json:"issued_at"`
	}](t, v["process_act"])
	check(t, v, key, "process_act", processActMessage(a.Name, a.Context.pb(), a.IssuedAt))
	rm := input[struct {
		Name     string `json:"name"`
		IssuedAt int64  `json:"issued_at"`
	}](t, v["process_remove"])
	check(t, v, key, "process_remove", processRemoveMessage(rm.Name, rm.IssuedAt))
	rs := input[struct {
		Process  string `json:"process"`
		CaseID   string `json:"case_id"`
		TenantID string `json:"tenant_id"`
		Action   string `json:"action"`
		IssuedAt int64  `json:"issued_at"`
	}](t, v["process_resolve"])
	check(t, v, key, "process_resolve", processResolveMessage(rs.Process, rs.CaseID, rs.TenantID, rs.Action, rs.IssuedAt))
}

type vEventHash struct {
	PrevHex        string    `json:"prev_hex"`
	GlobalPosition uint64    `json:"global_position"`
	TenantID       string    `json:"tenant_id"`
	Domain         string    `json:"domain"`
	AggregateType  string    `json:"aggregate_type"`
	AggregateID    string    `json:"aggregate_id"`
	Version        uint64    `json:"version"`
	EventType      string    `json:"event_type"`
	Schema         uint32    `json:"schema"`
	PayloadHex     string    `json:"payload_hex"`
	CommandID      string    `json:"command_id"`
	CreatedAt      int64     `json:"created_at"`
	ProducerKeyID  string    `json:"producer_key_id"`
	ProducerSigHex string    `json:"producer_sig_hex"`
	Context        *vContext `json:"context"`
}

func (e vEventHash) pb() *pb.Event {
	return &pb.Event{
		GlobalPosition: e.GlobalPosition, TenantId: e.TenantID, Domain: e.Domain, AggregateType: e.AggregateType,
		AggregateId: e.AggregateID, Version: e.Version, EventType: e.EventType, Schema: e.Schema, Payload: unhex(e.PayloadHex),
		CommandId: e.CommandID, CreatedAt: e.CreatedAt, ProducerSig: &pb.Signature{KeyId: e.ProducerKeyID, Sig: unhex(e.ProducerSigHex)},
		Context: e.Context.pb(),
	}
}

func TestVault(t *testing.T) {
	v, key := vectors(t)
	in := input[[]struct {
		SaltHex  string `json:"salt_hex"`
		Subject  string `json:"subject"`
		Secret   bool   `json:"secret"`
		ValueHex string `json:"value_hex"`
	}](t, v["vault_ref"])
	var refs []string
	for _, i := range in {
		refs = append(refs, vaultRef(unhex(i.SaltHex), i.Subject, i.Secret, unhex(i.ValueHex)))
	}
	if !slices.Equal(refs, v["vault_ref"].Refs) {
		t.Fatalf("vault refs %v, want %v", refs, v["vault_ref"].Refs)
	}
	cv := input[struct {
		Command   vCommand `json:"command"`
		VaultRefs []string `json:"vault_refs"`
	}](t, v["command_vault"])
	c := cv.Command.pb()
	c.VaultRefs = cv.VaultRefs
	check(t, v, key, "command_vault", commandMessage(c))
	r := input[vResult](t, v["result"])
	rv := input[struct {
		VaultRefs [][]string `json:"vault_refs"`
		Erases    []string   `json:"erases"`
	}](t, v["result_vault"])
	events := r.events()
	for i, e := range events {
		e.VaultRefs, e.Erases = rv.VaultRefs[i], rv.Erases[i]
	}
	check(t, v, key, "result_vault", resultMessage(streamOf(r.Command.pb()), r.ExpectedVersion, events))
}

func TestEventHash(t *testing.T) {
	v, _ := vectors(t)
	in := input[vEventHash](t, v["event_hash"])
	prev := unhex(in.PrevHex)
	hash := func(change func(*pb.Event)) string {
		e := in.pb()
		change(e)
		return hex.EncodeToString(eventHash(prev, e))
	}
	vault := input[struct {
		VaultRefs []string `json:"vault_refs"`
		Erases    string   `json:"erases"`
	}](t, v["event_hash_vault"])
	origin := input[struct {
		OriginPartition string `json:"origin_partition"`
		OriginPosition  uint64 `json:"origin_position"`
	}](t, v["event_hash_origin"])
	deployment := input[struct {
		Deployment string `json:"deployment"`
	}](t, v["event_hash_deployment"])
	for name, change := range map[string]func(*pb.Event){
		"event_hash":            func(*pb.Event) {},
		"event_hash_vault":      func(e *pb.Event) { e.VaultRefs, e.Erases = vault.VaultRefs, vault.Erases },
		"event_hash_origin":     func(e *pb.Event) { e.OriginPartition, e.OriginPosition = origin.OriginPartition, origin.OriginPosition },
		"event_hash_deployment": func(e *pb.Event) { e.Deployment = deployment.Deployment },
	} {
		if got := hash(change); got != v[name].Hash {
			t.Errorf("%s: %s, want %s", name, got, v[name].Hash)
		}
	}
}

func TestDerivedIDs(t *testing.T) {
	v, _ := vectors(t)
	in := input[struct {
		CommandID string   `json:"command_id"`
		N         []uint64 `json:"n"`
	}](t, v["derived_id"])
	var ids []string
	for _, n := range in.N {
		ids = append(ids, derivedID(in.CommandID, n))
	}
	if !slices.Equal(ids, v["derived_id"].IDs) {
		t.Fatalf("derived IDs %v, want %v", ids, v["derived_id"].IDs)
	}
}

func TestIndex(t *testing.T) {
	v, key := vectors(t)
	type vKey struct {
		Name         string `json:"name"`
		Global       bool   `json:"global"`
		ValueHashHex string `json:"value_hash_hex"`
		Subject      string `json:"subject"`
	}
	in := input[struct {
		Command         vCommand `json:"command"`
		ExpectedVersion uint64   `json:"expected_version"`
		Keys            []vKey   `json:"keys"`
		Facts           []string `json:"facts"`
		Requires        []struct {
			Domain        string `json:"domain"`
			AggregateType string `json:"aggregate_type"`
			AggregateID   string `json:"aggregate_id"`
			Fact          string `json:"fact"`
			Key           string `json:"key"`
			ValueHashHex  string `json:"value_hash_hex"`
			Absent        bool   `json:"absent"`
		} `json:"requires"`
	}](t, v["result_index"])
	var keys []*pb.UniqueKey
	for _, k := range in.Keys {
		keys = append(keys, &pb.UniqueKey{Name: k.Name, Global: k.Global, ValueHash: unhex(k.ValueHashHex), Subject: k.Subject})
	}
	var requires []*pb.Requirement
	for _, r := range in.Requires {
		requires = append(requires, &pb.Requirement{Domain: r.Domain, AggregateType: r.AggregateType, AggregateId: r.AggregateID, Fact: r.Fact, Key: r.Key, ValueHash: unhex(r.ValueHashHex), Absent: r.Absent})
	}
	check(t, v, key, "result_index", indexMessage(streamOf(in.Command.pb()), in.ExpectedVersion, keys, in.Facts, requires))
	// The values behind the vectors' hashes.
	if hex.EncodeToString(valueHash("A-1001")) != in.Keys[0].ValueHashHex || hex.EncodeToString(valueHash("max@example.com")) != in.Keys[1].ValueHashHex {
		t.Fatal("value hashes")
	}
	u := input[struct {
		TenantID      string `json:"tenant_id"`
		Domain        string `json:"domain"`
		AggregateType string `json:"aggregate_type"`
		Keys          []vKey `json:"keys"`
	}](t, v["unique_key_id"])
	var ids []string
	for _, k := range u.Keys {
		ids = append(ids, hex.EncodeToString(uniqueKeyID(u.TenantID, u.Domain, u.AggregateType, &pb.UniqueKey{Name: k.Name, Global: k.Global, ValueHash: unhex(k.ValueHashHex), Subject: k.Subject})))
	}
	if !slices.Equal(ids, v["unique_key_id"].IDs) {
		t.Fatalf("unique key IDs %v, want %v", ids, v["unique_key_id"].IDs)
	}
}
