package kit

import "slices"

// ordered is a map that keeps the order its keys came in (as the other kits' read models list
// rows in the order they were created).
type ordered[V any] struct {
	keys []string
	m    map[string]V
}

func newOrdered[V any]() *ordered[V] { return &ordered[V]{m: map[string]V{}} }

func (o *ordered[V]) get(k string) (V, bool) { v, ok := o.m[k]; return v, ok }

func (o *ordered[V]) set(k string, v V) {
	if _, ok := o.m[k]; !ok {
		o.keys = append(o.keys, k)
	}
	o.m[k] = v
}

// remove keeps the order of the others.
func (o *ordered[V]) remove(k string) {
	if _, ok := o.m[k]; !ok {
		return
	}
	delete(o.m, k)
	o.keys = slices.DeleteFunc(o.keys, func(x string) bool { return x == k })
}

// retain keeps the entries keep says yes to, in their order.
func (o *ordered[V]) retain(keep func(string, V) bool) {
	var keys []string
	for _, k := range o.keys {
		if keep(k, o.m[k]) {
			keys = append(keys, k)
		} else {
			delete(o.m, k)
		}
	}
	o.keys = keys
}

// values are the values in order.
func (o *ordered[V]) values() []V {
	out := make([]V, 0, len(o.keys))
	for _, k := range o.keys {
		out = append(out, o.m[k])
	}
	return out
}
