// The invitation commands: each class with its handler.

import { Command, Rejected } from "@kernable/sdk";

import { DAY, INVITATION_DAYS } from "../shared/constants.ts";
import { equalSecrets, normalEmail, parseInvitationToken, randomSecret, sha256, validEmail } from "../shared/secrets.ts";
import { type Invitation, invitationAggregate, linkAggregate } from "./aggregates.ts";
import { type InvitedGroup, linkToken, parseLinkToken } from "./domain.ts";
import {
  InvitationAccepted, InvitationCreated, InvitationDeclined, InvitationErased, InvitationLinkCreated, InvitationLinkRevoked,
  InvitationLinkUsed, InvitationMailSent, InvitationResent, InvitationRevoked,
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

/**
 * A reusable invitation link into `groups` (never as owner). The API generates the secret and
 * shows the token once; the event keeps its hash. The middleware lets the sender attach only
 * groups it could grant.
 */
export class CreateInvitationLink extends Command {
  static secret: (keyof CreateInvitationLink)[] = ["secret"];
  secret!: string;
  groups?: InvitedGroup[];
  label?: string;
  /** Until when it works (ms since epoch); null: until revoked. */
  expiresAt?: number | null;
  /** How many people may join through it; null: no limit. */
  maxUses?: number | null;
}

linkAggregate.command(CreateInvitationLink, (s, d, cmd) => {
  if (s.status !== "none") throw new Rejected("The link exists", "exists");
  if (typeof d.secret !== "string" || !/^[A-Za-z0-9_-]{32,}$/.test(d.secret)) throw new Rejected("A secret of at least 32 URL-safe characters", "invalid_secret");
  const maxUses = d.maxUses ?? null;
  if (maxUses !== null && !(Number.isInteger(maxUses) && maxUses > 0)) throw new Rejected("maxUses is a positive whole number", "invalid_max_uses");
  const expiresAt = d.expiresAt ?? null;
  if (expiresAt !== null && typeof expiresAt !== "number") throw new Rejected("expiresAt is a time in ms", "invalid_expiry");
  return [InvitationLinkCreated.new({
    groups: groups(d.groups), hash: sha256(linkToken(cmd.tenant, cmd.aggregateId, d.secret)), label: typeof d.label === "string" ? d.label : "",
    expiresAt, maxUses,
  })];
}, {
  requires: (d) => groups(d.groups).map((g) => ({ domain: "group", aggregateType: "group", aggregateId: g.group, fact: "active" })),
});

export class RevokeInvitationLink extends Command {}

linkAggregate.command(RevokeInvitationLink, (s) => {
  if (s.status !== "open") throw new Rejected("Only an open link is revoked", "invalid_state");
  return [InvitationLinkRevoked.new()];
});

/**
 * The API's: the signed-in account `accountId` joins the tenant through the link. Someone without
 * an account registers and verifies first: a link, unlike an emailed invitation, proves no email.
 */
export class JoinWithInvitationLink extends Command {
  static secret: (keyof JoinWithInvitationLink)[] = ["token"];
  token!: string;
  accountId!: string;
}

linkAggregate.command(JoinWithInvitationLink, (s, d, cmd, ctx) => {
  const t = parseLinkToken(d.token);
  if (s.status !== "open" || !t || t.tenant !== cmd.tenant || t.linkId !== cmd.aggregateId || !s.hash || !equalSecrets(s.hash, t.hash)) {
    throw new Rejected("The link is not valid", "invalid_token");
  }
  if (s.expiresAt !== null && ctx.now().getTime() > s.expiresAt) throw new Rejected("The link has expired", "expired_token");
  if (s.maxUses !== null && s.accounts.length >= s.maxUses) throw new Rejected("The link is used up", "used_up");
  if (typeof d.accountId !== "string" || !d.accountId) throw new Rejected("Which account?", "invalid_account");
  if (s.accounts.includes(d.accountId)) throw new Rejected("Joined through this link already", "exists");
  return [InvitationLinkUsed.new({ accountId: d.accountId, groups: s.groups })];
});
