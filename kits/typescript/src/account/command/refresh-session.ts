import { Rejected } from "@kernable/sdk";

import { DAY, SESSION_IDLE_DAYS, SESSION_MAX_DAYS } from "../../shared/constants.ts";
import { parseToken, sha256 } from "../../shared/secrets.ts";
import { sessionAggregate } from "../aggregate/session.ts";

// Public: the refresh token is the proof. It names its session and generation; an older
// generation is a reuse and ends the session.
sessionAggregate.command("RefreshSession", (s, d: { refreshToken: string; newRefreshToken: string; ip?: string }, cmd, ctx) => {
  const at = ctx.now().getTime();
  const parts = parseToken(d.refreshToken);
  const next = parseToken(d.newRefreshToken);
  if (s.status !== "active" || !parts || parts[0] !== cmd.aggregateId) throw new Rejected("Not a valid session", "invalid_token");
  const generation = Number(parts[1]);
  if (generation < s.generation) return [{ type: "SessionEnded", data: { reason: "reuse" } }];
  if (generation !== s.generation || sha256(d.refreshToken) !== s.refreshTokenHash) throw new Rejected("Not a valid session", "invalid_token");
  if (at > s.lastRefreshAt + SESSION_IDLE_DAYS * DAY) return [{ type: "SessionEnded", data: { reason: "idle" } }];
  if (at > s.openedAt + SESSION_MAX_DAYS * DAY) return [{ type: "SessionEnded", data: { reason: "expired" } }];
  if (!next || next[0] !== cmd.aggregateId || Number(next[1]) !== s.generation + 1) throw new Rejected("A malformed refresh token", "invalid_token");
  return [{
    type: "SessionRefreshed",
    data: { generation: s.generation + 1, refreshTokenHash: sha256(d.newRefreshToken), ip: d.ip ?? null, accountId: s.accountId, at },
  }];
});
