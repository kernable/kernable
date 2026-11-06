// The Session aggregate: one per sign-in. Commands are in ../command/.

import { accountDomain, type Session, type SessionEnd, type SessionOpened } from "../domain.ts";

export const sessionAggregate = accountDomain
  .aggregate<Session>("session", { status: "none", factors: [], generation: 0, openedAt: 0, lastRefreshAt: 0 })
  .apply("SessionOpened", (s, d: SessionOpened) => {
    s.status = "active";
    s.accountId = d.accountId;
    s.method = d.method;
    s.factors = d.factors;
    s.generation = 1;
    s.refreshTokenHash = d.refreshTokenHash;
    s.openedAt = d.at;
    s.lastRefreshAt = d.at;
  })
  .apply("SessionRefreshed", (s, d: { generation: number; refreshTokenHash: string; at: number }) => {
    s.generation = d.generation;
    s.refreshTokenHash = d.refreshTokenHash;
    s.lastRefreshAt = d.at;
  })
  .apply("SessionFactorAdded", (s, d: { factor: string }) => void s.factors.push(d.factor))
  .apply("SessionEnded", (s, d: { reason: SessionEnd }) => {
    s.status = "ended";
    s.endReason = d.reason;
  });
