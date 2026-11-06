// Registers every part of the account domain (a part not imported here does not exist) and
// exports what other domains and clients use.

export { accountDomain } from "./domain.ts";
export type * from "./domain.ts";
export * from "./events.ts";
export { accountAggregate, lockedUntil, type Account, sessionAggregate, type Session } from "./aggregates.ts";
export { RegisterAccount, RequestVerification, VerifyEmail, SignIn, OpenSession, RefreshSession, AddSessionFactor, EndSession } from "./commands.ts";
export { accountsReadModel, type AccountRow } from "./readmodels.ts";
export { FindAccountByEmail, GetAccount } from "./queries.ts";
export { sessionsReactor, mailReactor } from "./reactors.ts";
