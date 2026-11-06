// The invitation commands: each class with its handler.

import { Command, Rejected } from "@kernable/sdk";

import { DAY, INVITATION_DAYS } from "../shared/constants.ts";
import { equalSecrets, normalEmail, parseInvitationToken, randomSecret, validEmail } from "../shared/secrets.ts";
import { type Invitation, invitationAggregate } from "./aggregates.ts";
import type { InvitedGroup } from "./domain.ts";
import {
  InvitationAccepted, InvitationCreated, InvitationDeclined, InvitationErased, InvitationMailSent, InvitationResent, InvitationRevoked,
} from "./events.ts";

function groups(value: unknown): InvitedGroup[] {
  if (value === undefined) return [];
  if (!Array.isArray(value)) throw new Rejected("groups is a list", "invalid_groups");
  return value.map((g: InvitedGroup) => {
    if (!g || typeof g.group !== "string" || !g.group) throw new Rejected("Each group needs its ID", "invalid_groups");
    const scope = g.scope && typeof g.scope.type === "string" && typeof g.scope.id === "string" ? { type: g.scope.type, id: g.scope.id } : null;
    return { group: g.group, scope };
  });
}

/** The token, checked against the invitation: open, this tenant's, not expired, the secret right. */
function proven(s: Invitation, token: unknown, tenant: string, aggregateId: string, now: number): string {
  const t = parseInvitationToken(token);
  if (s.status !== "open" || !t || t.tenant !== tenant || t.invitationId !== aggregateId || t.tokenId !== s.tokenId || !s.token || !equalSecrets(s.token, t.secret)) {
    throw new Rejected("The invitation is not valid", "invalid_token");
  }
  if (now > s.expiresAt) throw new Rejected("The invitation has expired", "expired_token");
  return t.tokenId;
}

/**
 * Invites `email` to the tenant, into `groups`. The middleware lets the sender attach only groups
 * it could grant, and owner invitations only to owners and the platform.
 */
export class CreateInvitation extends Command {
  email!: string;
  groups?: InvitedGroup[];
  owner?: boolean;
}

invitationAggregate.command(CreateInvitation, (s, d, _cmd, ctx) => {
  if (s.status !== "none") throw new Rejected("The invitation exists", "exists");
  const email = normalEmail(d.email);
  if (!validEmail(email)) throw new Rejected("Not an email address", "invalid_email");
  return [InvitationCreated.new({
    email, groups: groups(d.groups), owner: d.owner === true, tokenId: randomSecret(9), token: randomSecret(),
    expiresAt: ctx.now().getTime() + INVITATION_DAYS * DAY,
  })];
}, {
  // Every group must be active in this tenant at commit.
  requires: (d) => groups(d.groups).map((g) => ({ domain: "group", aggregateType: "group", aggregateId: g.group, fact: "active" })),
});

export class ResendInvitation extends Command {}

invitationAggregate.command(ResendInvitation, (s, _d, _cmd, ctx) => {
  if (s.status !== "open") throw new Rejected("Only an open invitation is resent", "invalid_state");
  return [InvitationResent.new({
    previousTokenId: s.tokenId!, tokenId: randomSecret(9), token: randomSecret(), expiresAt: ctx.now().getTime() + INVITATION_DAYS * DAY,
  })];
});

export class RevokeInvitation extends Command {}

invitationAggregate.command(RevokeInvitation, (s) => {
  if (s.status !== "open") throw new Rejected("Only an open invitation is revoked", "invalid_state");
  return [InvitationRevoked.new({ tokenId: s.tokenId! })];
});

/** Public: the token is the proof. */
export class DeclineInvitation extends Command {
  static secret: (keyof DeclineInvitation)[] = ["token"];
  token!: string;
}

invitationAggregate.command(DeclineInvitation, (s, d, cmd, ctx) => {
  const tokenId = proven(s, d.token, cmd.tenant, cmd.aggregateId, ctx.now().getTime());
  return [InvitationDeclined.new({ tokenId })];
});

/**
 * The API's: it says which account accepts, and its email, which must be the invited one.
 * For a new person it picks the account ID and registers the account right after.
 */
export class AcceptInvitation extends Command {
  static secret: (keyof AcceptInvitation)[] = ["token"];
  token!: string;
  accountId!: string;
  email!: string;
  newAccount!: boolean;
}

invitationAggregate.command(AcceptInvitation, (s, d, cmd, ctx) => {
  const tokenId = proven(s, d.token, cmd.tenant, cmd.aggregateId, ctx.now().getTime());
  if (normalEmail(d.email) !== s.email) throw new Rejected("The invitation is for another email address", "wrong_email");
  if (typeof d.accountId !== "string" || !d.accountId) throw new Rejected("Which account?", "invalid_account");
  return [InvitationAccepted.new({ tokenId, accountId: d.accountId, newAccount: d.newAccount === true, groups: s.groups, owner: s.owner })];
});

/** Internal: the mail reactor, once the mail is out. */
export class RecordInvitationMailSent extends Command {
  tokenId!: string;
}

invitationAggregate.command(RecordInvitationMailSent, (_s, d) => [InvitationMailSent.new({ tokenId: d.tokenId })]);

/** Erases the invited email; an open invitation is revoked first. */
export class EraseInvitation extends Command {}

invitationAggregate.command(EraseInvitation, (s) => {
  if (s.status === "none" || s.erased) throw new Rejected("Nothing to erase", "not_found");
  return s.status === "open" ? [InvitationRevoked.new({ tokenId: s.tokenId! }), InvitationErased.new()] : [InvitationErased.new()];
});
