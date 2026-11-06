// Registers every part of the account domain (a part not imported here does not exist) and
// exports what other domains and clients use.

export { accountDomain } from "./domain.ts";
export type * from "./domain.ts";
export * from "./events.ts";
export { accountAggregate, lockedUntil, type Account, sessionAggregate, type Session, ssoAggregate, type Sso } from "./aggregates.ts";
export { RegisterAccount, RequestVerification, VerifyEmail, SignIn, OpenSession, RefreshSession, AddSessionFactor, EndSession, SuspendAccount, ReactivateAccount, DeleteAccount, ChangePassword, RequestPasswordReset, ResetPassword, RequestEmailChange, ConfirmEmailChange, NoteRegistrationAttempt, StartTotp, ConfirmTotp, DisableTotp, RegenerateRecoveryCodes, ProveSecondFactor, AddPasskey, RemovePasskey, SignInWithPasskey, ProvePasskey, SignInWithProvider, LinkProvider, UnlinkProvider, CreateSso, UpdateSso, AddSsoDomain, VerifySsoDomain, RemoveSsoDomain, EnforceSso, DisableSso, EnableSso, DeleteSso } from "./commands.ts";
export { accountsReadModel, ssosReadModel, type AccountRow, type SessionRow, type SsoRow } from "./readmodels.ts";
export { FindAccountByEmail, GetAccount, ListSessions, FindAccountByLink, FindSsoByDomain, GetSsoSecret, GetSso } from "./queries.ts";
export { sessionsReactor, mailReactor, endingReactor } from "./reactors.ts";
