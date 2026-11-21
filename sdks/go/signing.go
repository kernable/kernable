package kernable

// Signed messages and the event hash chain, as specified in proto/SIGNING.md.

import (
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"os"
	"strings"

	"github.com/kernable/kernable/sdks/go/internal/pb"
)

// Key is an Ed25519 signing key. Its ID is the lowercase hex public key.
type Key struct {
	ID   string
	priv ed25519.PrivateKey
}

// KeyFromHex reads a hex-encoded 32-byte seed (the content of a `.key` file).
func KeyFromHex(s string) (*Key, error) {
	s = strings.TrimSpace(s)
	seed, err := hex.DecodeString(s)
	if err != nil || len(seed) != ed25519.SeedSize {
		return nil, fmt.Errorf("not a 32-byte key in hex")
	}
	priv := ed25519.NewKeyFromSeed(seed)
	return &Key{ID: hex.EncodeToString(priv.Public().(ed25519.PublicKey)), priv: priv}, nil
}

// KeyFromFile reads a `.key` file.
func KeyFromFile(path string) (*Key, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("key %s: %w", path, err)
	}
	k, err := KeyFromHex(string(b))
	if err != nil {
		return nil, fmt.Errorf("key %s: %w", path, err)
	}
	return k, nil
}

func (k *Key) sign(message []byte) *pb.Signature {
	return &pb.Signature{KeyId: k.ID, Sig: ed25519.Sign(k.priv, message)}
}

// msg builds a signed message: length-prefixed fields (`u32 BE length || bytes`), integers as
// 8 bytes big-endian.
type msg struct{ b []byte }

func newMsg(tag string) *msg { m := &msg{}; return m.str(tag) }

func (m *msg) bytes(b []byte) *msg {
	m.b = binary.BigEndian.AppendUint32(m.b, uint32(len(b)))
	m.b = append(m.b, b...)
	return m
}

func (m *msg) str(s string) *msg { return m.bytes([]byte(s)) }

func (m *msg) u64(n uint64) *msg { m.b = binary.BigEndian.AppendUint64(m.b, n); return m }

func (m *msg) i64(n int64) *msg { return m.u64(uint64(n)) }

func (m *msg) flag(b bool) *msg {
	if b {
		return m.u64(1)
	}
	return m.u64(0)
}

// context encodes a request context; an absent one like an empty one.
func (m *msg) context(c *pb.RequestContext) *msg {
	if c == nil {
		c = &pb.RequestContext{}
	}
	m.str(c.SenderTenant).str(c.SenderIdentity).str(c.SenderAccount).str(c.SenderSession).str(c.OnBehalfOf)
	if c.ExpectedVersion != nil {
		m.u64(1).u64(*c.ExpectedVersion)
	} else {
		m.u64(0)
	}
	m.str(c.CorrelationId).str(c.CausationId).str(c.TraceId).flag(c.BypassAuthorization).u64(uint64(len(c.Attributes)))
	for _, a := range c.Attributes {
		m.str(a.Key).str(a.Value)
	}
	return m
}

// refs appends vault references, only when there are any.
func (m *msg) refs(refs []string) *msg {
	if len(refs) == 0 {
		return m
	}
	m.u64(uint64(len(refs)))
	for _, r := range refs {
		m.str(r)
	}
	return m
}

func (m *msg) list(items []string) *msg {
	m.u64(uint64(len(items)))
	for _, s := range items {
		m.str(s)
	}
	return m
}

func sum(b []byte) []byte { h := sha256.Sum256(b); return h[:] }

// stream is what identifies a command's stream in a signed message.
type stream struct{ commandID, tenantID, domain, aggregateType, aggregateID string }

func streamOf(c *pb.Command) stream {
	return stream{c.CommandId, c.TenantId, c.Domain, c.AggregateType, c.AggregateId}
}

func (m *msg) stream(s stream) *msg {
	return m.str(s.commandID).str(s.tenantID).str(s.domain).str(s.aggregateType).str(s.aggregateID)
}

func commandMessage(c *pb.Command) []byte {
	m := newMsg("kernable.v1.command").stream(streamOf(c)).str(c.CommandType).bytes(c.Payload).context(c.Context).refs(c.VaultRefs)
	// Optional fields, each appended only when set.
	if c.DeliverAt != 0 {
		m.str("deliver_at").i64(c.DeliverAt)
	}
	if c.DeliverUntil != 0 {
		m.str("deliver_until").i64(c.DeliverUntil)
	}
	return m.b
}

func cancelMessage(c *pb.CancelRequest) []byte {
	return newMsg("kernable.v1.cancel").str(c.CommandId).str(c.TenantId).context(c.Context).i64(c.IssuedAt).b
}

// vaultRef is the reference of a value kept in the vault: it commits to salt, subject, kind and
// value.
func vaultRef(salt []byte, subject string, secret bool, value []byte) string {
	return hex.EncodeToString(sum(newMsg("kernable.v1.vault").bytes(salt).str(subject).flag(secret).bytes(value).b))
}

func queryMessage(q *pb.QueryRequest) []byte {
	m := newMsg("kernable.v1.query").str(q.QueryId).str(q.TenantId).str(q.Domain).str(q.QueryType).bytes(q.Payload).
		context(q.Context).u64(q.MinPosition).i64(q.IssuedAt)
	// Optional fields, each appended only when set.
	if q.WaitOnly {
		m.str("wait_only")
	}
	if q.Target != nil {
		m.str("target").str(q.Target.AggregateType).str(q.Target.AggregateId)
	}
	return m.b
}

func resultMessage(s stream, expectedVersion uint64, events []*pb.NewEvent) []byte {
	m := newMsg("kernable.v1.result").stream(s).u64(expectedVersion).u64(uint64(len(events)))
	vault := false
	for _, e := range events {
		m.str(e.EventType).u64(uint64(e.Schema)).bytes(e.Payload)
		vault = vault || len(e.VaultRefs) > 0 || e.Erases != ""
	}
	// Vault fields, appended only when an event has any.
	if vault {
		for _, e := range events {
			m.list(e.VaultRefs).str(e.Erases)
		}
	}
	return m.b
}

// indexMessage is what a worker signs for the unique keys, facts and requirements of its
// result: a message of its own, because every event stores the result signature and `kernable
// verify` checks it against the stored events alone.
func indexMessage(s stream, expectedVersion uint64, keys []*pb.UniqueKey, facts []string, requires []*pb.Requirement) []byte {
	m := newMsg("kernable.v1.index").stream(s).u64(expectedVersion).u64(uint64(len(keys)))
	for _, k := range keys {
		m.str(k.Name).flag(k.Global).bytes(k.ValueHash).str(k.Subject)
	}
	m.list(facts).u64(uint64(len(requires)))
	for _, r := range requires {
		m.str(r.Domain).str(r.AggregateType).str(r.AggregateId).str(r.Fact).str(r.Key).bytes(r.ValueHash).flag(r.Absent)
	}
	return m.b
}

// valueHash is a unique key's `value_hash`: the SHA-256 of the value's UTF-8 bytes.
func valueHash(value string) []byte { return sum([]byte(value)) }

// uniqueKeyID is the identity of a unique key in the kernel's index. The kernel computes it;
// workers send only the value's hash. Here for the test vectors.
func uniqueKeyID(tenant, domain, aggregateType string, k *pb.UniqueKey) []byte {
	if k.Global {
		tenant = ""
	}
	return sum(newMsg("kernable.v1.key").str(tenant).str(domain).str(aggregateType).str(k.Name).bytes(k.ValueHash).b)
}

func snapshotMessage(tenantID, domain, aggregateType, aggregateID string, version uint64, schema string, state []byte) []byte {
	return newMsg("kernable.v1.snapshot").str(tenantID).str(domain).str(aggregateType).str(aggregateID).u64(version).str(schema).bytes(state).b
}

// dataMessage is what a client signs to put, get, delete or list files: the data's SHA-256 hash,
// not the data, the request context, and for a listing its page.
func dataMessage(op string, r *pb.DataRequest) []byte {
	m := newMsg("kernable.v1.data." + op).str(r.TenantId).str(r.Domain).str(r.Key).str(r.ContentType).bytes(sum(r.Data)).
		i64(r.IssuedAt).context(r.Context)
	if op == "list" {
		m.str(r.After)
	}
	return m.b
}

// cacheMessage is what a client signs to set, get or delete a cache value, with the value's
// SHA-256 hash and the request context.
func cacheMessage(op string, r *pb.CacheRequest) []byte {
	return newMsg("kernable.v1.cache." + op).str(r.TenantId).str(r.Domain).str(r.Key).bytes(sum(r.Value)).u64(r.TtlMs).
		i64(r.IssuedAt).context(r.Context).b
}

func rejectionMessage(commandID, code, message string) []byte {
	return newMsg("kernable.v1.rejection").str(commandID).str(code).str(message).b
}

func challengeMessage(nonce []byte) []byte { return newMsg("kernable.v1.challenge").bytes(nonce).b }

func subscribeMessage(afterPosition uint64, eventTypes []string, issuedAt int64) []byte {
	return newMsg("kernable.v1.subscribe").u64(afterPosition).i64(issuedAt).list(eventTypes).b
}

// membershipMessage is what a key granted `manage` signs to apply a service's membership.
func membershipMessage(ms *pb.Membership) []byte {
	m := newMsg("kernable.v1.membership").str(ms.Service).list(ms.Workers).list(ms.Listeners).list(ms.Middlewares).
		u64(uint64(len(ms.Metadata)))
	for _, a := range ms.Metadata {
		m.str(a.Key).str(a.Value)
	}
	m.i64(ms.IssuedAt)
	// Optional: appended only when set.
	if len(ms.Processes) > 0 {
		m.str("processes").list(ms.Processes)
	}
	if ms.Deployment != "" {
		m.str("deployment").str(ms.Deployment)
	}
	return m.b
}

// processActMessage is what a key granted `process` signs to set as whom a process acts.
func processActMessage(name string, c *pb.RequestContext, issuedAt int64) []byte {
	return newMsg("kernable.v1.process_act").str(name).context(c).i64(issuedAt).b
}

// processMessage is what a key granted `process` signs to register a process model: the
// model's JSON as sent.
func processMessage(model []byte, issuedAt int64) []byte {
	return newMsg("kernable.v1.process").bytes(model).i64(issuedAt).b
}

// processRemoveMessage is what a key granted `process` signs to remove a process model.
func processRemoveMessage(name string, issuedAt int64) []byte {
	return newMsg("kernable.v1.process_remove").str(name).i64(issuedAt).b
}

// processResolveMessage is what a key granted `process` signs to retry or drop a stuck case.
func processResolveMessage(process, caseID, tenantID, action string, issuedAt int64) []byte {
	return newMsg("kernable.v1.process_resolve").str(process).str(caseID).str(tenantID).str(action).i64(issuedAt).b
}

// checkpointMessage is what the kernel signs for a checkpoint of its log. Here for the test
// vectors.
func checkpointMessage(position uint64, chainHash []byte, createdAt int64) []byte {
	return newMsg("kernable.v1.checkpoint").u64(position).bytes(chainHash).i64(createdAt).b
}

// derivedID is the n-th ID derived for a command (`ctx.ID()`): a version 8 UUID.
func derivedID(commandID string, n uint64) string {
	b := sum(newMsg("kernable.v1.id").str(commandID).u64(n).b)[:16]
	b[6] = (b[6] & 0x0f) | 0x80
	b[8] = (b[8] & 0x3f) | 0x80
	h := hex.EncodeToString(b)
	return h[0:8] + "-" + h[8:12] + "-" + h[12:16] + "-" + h[16:20] + "-" + h[20:]
}

// eventHash is the hash chaining a stored event to the one before it. Here for the test
// vectors.
func eventHash(prev []byte, e *pb.Event) []byte {
	sig := e.ProducerSig
	if sig == nil {
		sig = &pb.Signature{}
	}
	m := newMsg("kernable.v1.event").bytes(prev).u64(e.GlobalPosition).str(e.TenantId).str(e.Domain).str(e.AggregateType).
		str(e.AggregateId).u64(e.Version).str(e.EventType).u64(uint64(e.Schema)).bytes(e.Payload).str(e.CommandId).
		i64(e.CreatedAt).str(sig.KeyId).bytes(sig.Sig).context(e.Context)
	// Each appended only when set.
	if len(e.VaultRefs) > 0 || e.Erases != "" {
		m.list(e.VaultRefs).str(e.Erases)
	}
	if e.OriginPartition != "" {
		m.str(e.OriginPartition).u64(e.OriginPosition)
	}
	if e.Deployment != "" {
		m.str("deployment").str(e.Deployment)
	}
	return sum(m.b)
}
