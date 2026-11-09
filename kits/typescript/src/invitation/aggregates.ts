// The invitation aggregates: state, how each event changes it, unique keys and facts.

import { invitationDomain, type InvitationStatus, type InvitedGroup, type LinkStatus } from "./domain.ts";
import {
  InvitationAccepted, InvitationCreated, InvitationDeclined, InvitationErased, InvitationLinkCreated, InvitationLinkRevoked,
  InvitationLinkUsed, InvitationResent, InvitationRevoked,
} from "./events.ts";

export interface Invitation {
  status: InvitationStatus;
  erased: boolean;
  email?: string | null;
  groups: InvitedGroup[];
  owner: boolean;
  tokenId?: string;
  /** The token while open; in the vault, so null once used or erased. */
  token: string | null;
  expiresAt: number;
  accountId?: string;
}

export const invitationAggregate = invitationDomain
  .aggregate<Invitation>("invitation", { status: "none", erased: false, groups: [], owner: false, token: null, expiresAt: 0 })
  .apply(InvitationCreated, (s, d) => {
    s.status = "open";
    s.email = d.email;
    s.groups = d.groups;
    s.owner = d.owner;
    s.tokenId = d.tokenId;
    s.token = d.token;
    s.expiresAt = d.expiresAt;
  })
  .apply(InvitationResent, (s, d) => {
    s.tokenId = d.tokenId;
    s.token = d.token;
    s.expiresAt = d.expiresAt;
  })
  .apply(InvitationRevoked, (s) => {
    s.status = "revoked";
    s.token = null;
  })
  .apply(InvitationDeclined, (s) => {
    s.status = "declined";
    s.token = null;
  })
  .apply(InvitationAccepted, (s, d) => {
    s.status = "accepted";
    s.token = null;
    s.accountId = d.accountId;
  })
  .apply(InvitationErased, (s) => {
    s.erased = true;
    s.email = null;
  })
  // One open invitation per email and tenant.
  .unique("email", (s) => (s.status === "open" && s.email ? s.email : null));

export interface InvitationLink {
  status: LinkStatus;
  groups: InvitedGroup[];
  /** The token's hash: the token itself is shown once, by the API. */
  hash?: string;
  label: string;
  expiresAt: number | null;
  maxUses: number | null;
  /** The accounts that joined through it: each once. */
  accounts: string[];
}

export const linkAggregate = invitationDomain
  .aggregate<InvitationLink>("link", { status: "none", groups: [], label: "", expiresAt: null, maxUses: null, accounts: [] })
  .apply(InvitationLinkCreated, (s, d) => {
    s.status = "open";
    s.groups = d.groups;
    s.hash = d.hash;
    s.label = d.label;
    s.expiresAt = d.expiresAt;
    s.maxUses = d.maxUses;
  })
  .apply(InvitationLinkRevoked, (s) => void (s.status = "revoked"))
  .apply(InvitationLinkUsed, (s, d) => void s.accounts.push(d.accountId));
