// Registers every part of the invitation domain (a part not imported here does not exist) and
// exports what other domains and clients use.

export { invitationDomain } from "./domain.ts";
export type * from "./domain.ts";
export { linkToken, parseLinkToken } from "./domain.ts";
export * from "./events.ts";
export { invitationAggregate, linkAggregate, type Invitation, type InvitationLink } from "./aggregates.ts";
export {
  CreateInvitation, ResendInvitation, RevokeInvitation, DeclineInvitation, AcceptInvitation, RecordInvitationMailSent, EraseInvitation,
  CreateInvitationLink, RevokeInvitationLink, JoinWithInvitationLink,
} from "./commands.ts";
export { invitationsReadModel, type InvitationRow, type LinkRow } from "./readmodels.ts";
export { ListInvitations, PeekInvitation, ListInvitationLinks, PeekInvitationLink } from "./queries.ts";
export { invitationMailReactor, ownerInvitationReactor, invitationCleanupReactor } from "./reactors.ts";
