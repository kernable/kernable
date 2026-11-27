# One key per instance, trust from the root, the copy key at join

Status: decided 2026-11-27, not built. Builds on [a writer from its copy](writer-from-copy.md),
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

A node joined to a control point (`kernable node join`, [managed nodes](../docs/reference/managed-node.md))
already pulls its settings. It now also pulls, and checks before using:

- the grants for its key, signed by the root;
- the writer approvals for the copies it follows, signed by the root;
- the relay's address and its copy's name.

It pins `root.pub` at join (the join prints the root's ID to compare). A changed grant or
approval fails its signature; the control point can deliver but cannot forge. Joining makes the
instance key on the machine and sends only its ID: the control point lists it as waiting until
someone with the root key signs its grant (`kernable grant`, uploaded with the CLI).

## 4. The copy key, sealed to a granted instance

A grant may carry `copy=<copy>`: the root lets this instance have the copy key. An instance
holding the key (the pusher first, else a reader) seals it to such an instance through the relay:

1. The relay tells a connected holder which keys of the copy wait for its key, with their grants
   and their exchange keys (each signed by the instance's own key).
2. The holder checks the grant against its root, `copy=<copy>` in it, and the exchange key's
   signature. Then it seals the copy key to the exchange key (HPKE, RFC 9180: X25519,
   HKDF-SHA256, ChaCha20-Poly1305), with the copy, the recipient, the key's fingerprint and the
   grant's hash as associated data, and signs the envelope with its own key.
3. The relay keeps the envelope, opaque to it, and hands it to the recipient when it connects.
   The recipient checks the sender's signature (a key its root granted, or its writer), opens the
   envelope, and checks the fingerprint against the records it can now unseal.

Nothing in this lets the relay, or whoever runs it, read a copy: it never holds a key that opens
the envelope, and it cannot make a grant. Whoever holds the root can, by joining an instance of
their own: the root is the project's single anchor, and is kept offline accordingly.

`[push] copy-key-handover = off` keeps a copy's key by hand only. A relay without a holder
connected keeps the request until one comes.

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
