# Compliance

kernable does not make an organization compliant or certified: ISO 27001, SOC 2 and the GDPR cover your whole organization, its risks, policies and people. What kernable does is implement a set of **technical controls** in the software built on it, so you don't build them yourself and can show them to an auditor. This page maps them, and says where kernable stops.

The control numbers refer to ISO/IEC 27001:2022 (Annex A), the SOC 2 Trust Services Criteria (2017, revised 2022), PCI DSS 4.0 and the regulations as published. Check them against the version your audit uses.

## What kernable covers

| kernable | How you show it | Supports |
|---|---|---|
| **A complete, tamper-evident history.** Every change is an event; every event is signed by the worker that decided it and hash-chained to the one before; the kernel signs checkpoints of the chain. ([Trust](./trust#the-hash-chain)) | `kernable verify` recomputes the chain and every signature, and reports any changed, removed or reordered event | ISO 27001 A.8.15 logging, A.5.33 protection of records · SOC 2 CC7.2, processing integrity · PCI DSS Req. 10 · HIPAA §164.312(b) audit controls, §164.312(c)(1) integrity · GoBD traceability and immutability · FDA 21 CFR Part 11 §11.10(e) audit trails |
| **Who did what, for whom.** Every command carries a request context signed by its client (`senderIdentity`, `onBehalfOf`, correlation and causation IDs), stored with every event it caused. ([Queries and middleware](./queries-and-middleware)) | The history of any aggregate, in the UI or the store | ISO 27001 A.8.15 · SOC 2 CC6.1 · GDPR Art. 5(2) accountability |
| **Personal data that can be erased.** Fields marked `personal` go to an encrypted vault, not the log; an erasure is a signed event that deletes a person's values within the tenant and leaves a proof without the person. ([Data over time](./data#personal-data-the-vault)) | The erasing event and its proof; the chain still verifies | GDPR Art. 17 right to erasure, Art. 25 data protection by design, Art. 32 · ISO 27001 A.8.10 information deletion, A.5.34 privacy and protection of PII |
| **Access control that fails closed.** Grants signed by a root key limit what each process may do; middleware checks every command, query, file and cache request, and a missing middleware refuses everything it covers. ([Trust](./trust#keys-and-grants), [Queries and middleware](./queries-and-middleware)) | The grants and the signed membership of each service | ISO 27001 A.5.15 access control, A.8.3 information access restriction, A.8.2 privileged access rights · SOC 2 CC6.1, CC6.3 · NIS2 Art. 21(2) |
| **Cryptography and key management.** Ed25519 signatures on every message, mutual TLS between processes, AES-256-GCM in the vault; keys rotate and are revoked without a restart, and revocations are stored signed and never undone. ([Trust](./trust#revoking-and-changing-keys), [Mutual TLS](../how-to/tls)) | `kernable verify` lists everything a revoked key signed since its revocation time | ISO 27001 A.8.24 use of cryptography · GDPR Art. 32 · HIPAA §164.312(a)(2)(iv), §164.312(e) |
| **Monitoring, backup and recovery.** Metrics and a readiness check, a documented backup and restore, crash tests at every point where the kernel has stored something. ([Monitor](../how-to/monitor), [Back up and restore](../how-to/backup), [Failures](./failures)) | The metrics, your backup runs, the [guarantees](./failures) | ISO 27001 A.8.13 information backup, A.8.16 monitoring activities · DORA Art. 9, Art. 12 |

## Where kernable stops

These are yours to cover, with other tools or in your application:

- **Reads are not recorded.** The log holds changes. Queries are checked by middleware but not stored; if you must log who read what, do it in an [after-middleware](./queries-and-middleware), or in your API.
- **Manual failover.** A [standby](../how-to/standby) holds every acknowledged write (no data loss on failover), but an operator promotes it; availability requirements (SOC 2 availability, DORA, NIS2) still need your runbook for that, and backups.
- **The event store is not encrypted as a whole.** Only vault values are. Encrypt the disk or volume the store lives on.
- **The request context is stored in clear** and is not erased with a person. Use IDs that do not identify a person by themselves ([What stays](../how-to/erasure#what-stays)).
- **Backups keep erased data.** Restoring an older vault brings erased values back until you delete them again ([Backups and erasure](../how-to/backup#backups-and-erasure)).
- **Events after the last checkpoint** are protected by the chain only. Keep `--checkpoint-interval-ms` short.
- **People, not processes.** kernable authenticates processes by their keys. Signing in people (passwords, SSO, sessions) is your application's part.
- **Retention periods.** Events are never deleted, and kernable has no automatic retention: personal data leaves through erasures, which your application triggers when a period ends.
- **The organization.** Risk assessment, policies, training, suppliers, incident handling: no software covers these.
