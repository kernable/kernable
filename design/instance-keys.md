# One key per instance, trust from the root, the copy key at join

Status: decided 2026-11-27; steps 1 to 4 built the same day. Builds on [a writer from its copy](writer-from-copy.md),
and replaces its "copy key by hand only" and its separate project key.

Today a small setup (one writer, two readers) needs a dozen key files and many public files
copied by hand. This design brings an instance down to one secret file of its own, with
everything else either derived from it, signed by the project's root key, or handed over sealed.
No secret ever leaves the machine it was made on except the copy key, and that only sealed to an
instance the root key granted it.

## 1. One instance key

`kernable keys new <name>` writes `<name>.key` as today. Started with `--instance-key <file>`, a
kernel takes every key it needs from that one file:

| Purpose | Key | How |
|---|---|---|
| Checkpoints, following, pushing, calls at a relay | Ed25519 | the file's 32 bytes as the seed, so the instance's ID is the file's `.pub` |
| The vault | 32 secret bytes | HKDF-SHA256(seed, info `kernable.v1.instance.vault`) |
| Receiving a copy key | X25519 | HKDF-SHA256(seed, info `kernable.v1.instance.exchange`) |

One public ID names the instance everywhere: in grants, in a relay's access list, in a writer
approval, in the console. Every signature it makes already carries its own context
(`kernable.v1.checkpoint`, `kernable.v1.relay-stream`, …, proto/SIGNING.md), so one key serves
them all without one signature passing for another.

- `--kernel-key`, `--vault-key`, `--follow-key` and the pusher's `--key` go away. An existing
  `kernel.key` is a valid instance key with the same ID; its vault moves to the derived vault key
  with `kernable vault rekey`.
- A relay's role `push` includes reading the pushed copy, so a reader becomes the writer with the
  key it follows with (`--become-writer` takes no key file any more).
- Replacing an instance's key replaces all of them: a new instance key, a new grant, and the vault
  re-encrypted. A compromised instance is replaced as a whole anyway.

## 2. One root per project: grants and writer approvals

The root key (`root.key`, called the **project root key**) signs grants as today, and now also
writer approvals (`kernable.v1.writer-approval`, as built with the project key). There is no
separate project key: its trust would come from the root anyway, and a root leak would need both
replaced. `--project-key` goes away; readers check approvals against `--root`, and replacing the
root moves the trust in grants and approvals together.

A reader trusts as its writer: the key given with `--primary-kernel-key`, or any key the root
approved for its copy. An instance that takes its writer from somewhere it does not trust (a
managed node, below) only takes an approved one, the first writer included.

## 3. A joined instance pulls what is public or signed

`kernable node join <url> <token> --root <root>` makes the kernel's one key on the machine,
joins with its public half and pins the root it is given (the console and the CLI print the whole
command). The node then pulls from the control point, besides its settings
(`GET /v1/nodes/<id>/signed`):

- the grants for its key, signed by the root: the newest written beside its key;
- the writer approvals of its project, signed by the root: written to `approvals/<copy>.approval`.

It checks every signature against the pinned root before it writes a file; its kernel checks the
approvals again (`--approvals`): a reader takes the writer approved for its copy beside any it
was given, and needs none given; `--become-writer` takes its approval from there. A changed grant
or approval fails its signature: the control point can deliver but cannot forge.

The member who holds the root signs on their own machine and hands the signed file to the Cloud
(`kernable cloud projects grant …`, `… approve …`); the account service checks it against the
project's root to catch mistakes early and the control point stores it, newest per key and copy.

## 4. The copy key, sealed to a granted instance

A grant may carry `copy=<copy>`: the root lets this instance have the copy key. A new instance
whose `--reader-copy-key` file does not exist asks the copy's pusher for it through the relay
(`RelayService.CopyKey`), with its grant:

1. The relay lets only a key it lets read the copy ask, and passes the ask down the pusher's
   `Calls` stream.
2. The pusher, started with the roots it trusts (`--root`; a node passes the kernel's), checks the
   grant against them, `copy=<copy>` in it, and that neither the key nor its grant's signer is
   revoked (it notes revocations as it pushes them). Then it seals the copy key to the asking key
   (X25519 from its Ed25519 key, an ephemeral key pair, XChaCha20-Poly1305 keyed by the SHA-256 of
   a label, both public halves and the shared secret, bound to the copy, the recipient and the copy
   key's ID) and signs the sealed bytes with its own key, the writer's
   (`kernable.v1.copy-key-handover`).
3. The relay carries the answer. The instance takes it only signed by a writer key it trusts,
   opens it, checks the key's ID, writes the file (mode 0600) and starts.

Nothing in this lets the relay, or whoever runs it, read a copy: it never holds a key that opens
the envelope, and it cannot make a grant. Whoever holds the root can, by joining an instance of
their own: the root is the project's single anchor, and is kept offline accordingly.

Without `--root` (`[push] copy-key-handover = false`) the pusher hands it to nobody. While no
pusher is connected the instance says so and asks again; only the writer's pusher hands it over.

## 5. Rotating a copy key

Only by command (`kernable relay rotate`, or the Cloud's CLI): the pusher makes a new copy key,
seals it to every instance whose grant still carries `copy=<copy>` and that is active at the
relay, waits until each reports the new key's fingerprint, and then starts the copy anew under it
(a new copy key keeps the copy's name, as decided before). An instance whose key was revoked gets
nothing.

## Order of building

1. Instance key (`--instance-key`, derivation, old flags removed, `vault rekey` to the derived key).
2. Writer approvals by the root; the project key removed.
3. Grants with `copy=`, exchange keys, envelopes through the relay, the hand-over.
4. Joined nodes pull grants, approvals, the relay and the copy.
5. Fingerprints reported, rotation by command.
