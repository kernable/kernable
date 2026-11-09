// The invitation events: what happened to invitations, the vocabulary other domains read.

import { Event, type PersonalFields } from "@kernable/sdk";

import { invitationDomain, type InvitedGroup } from "./domain.ts";

/** The email belongs to the invitation (its own subject); the token sits in the vault until used. */
export class InvitationCreated extends Event {
  static personal: PersonalFields<InvitationCreated>[] = [
    { fields: ["email"], subject: (_d, id) => `invitation:${id}` },
    { fields: ["token"], subject: (d) => `token:${d.tokenId}` },
  ];
  email!: string | null;
  groups!: InvitedGroup[];
  /** An owner invitation: accepting makes the person an owner. */
  owner!: boolean;
  tokenId!: string;
  token!: string | null;
  expiresAt!: number;
}

/** A new token; erases the old one. */
export class InvitationResent extends Event {
  static personal: PersonalFields<InvitationResent>[] = [{ fields: ["token"], subject: (d) => `token:${d.tokenId}` }];
  static erases = (d: { previousTokenId: string }) => `token:${d.previousTokenId}`;
  previousTokenId!: string;
  tokenId!: string;
  token!: string | null;
  expiresAt!: number;
}

const usedToken = (d: { tokenId: string }) => `token:${d.tokenId}`;

export class InvitationRevoked extends Event {
  static erases = usedToken;
  tokenId!: string;
}

export class InvitationDeclined extends Event {
  static erases = usedToken;
  tokenId!: string;
}

/** The identity reactor creates the identity with these groups (and the owner) from it. */
export class InvitationAccepted extends Event {
  static erases = usedToken;
  tokenId!: string;
  accountId!: string;
  newAccount!: boolean;
  groups!: InvitedGroup[];
  owner!: boolean;
}

export class InvitationMailSent extends Event {
  tokenId!: string;
}

/** Erases the invited email: when the tenant is deleted, or the person asks. */
export class InvitationErased extends Event {
  static erases = (_d: unknown, id: string) => `invitation:${id}`;
}

/**
 * A reusable invitation: anyone signed in who has its token may join the tenant with these groups,
 * until it is revoked, expires or is used up. Only the token's hash is kept: the API shows the
 * token once.
 */
export class InvitationLinkCreated extends Event {
  groups!: InvitedGroup[];
  hash!: string;
  label!: string;
  expiresAt!: number | null;
  maxUses!: number | null;
}

export class InvitationLinkRevoked extends Event {}

/** Someone joined through the link: the identity reactor creates their identity with these groups. */
export class InvitationLinkUsed extends Event {
  accountId!: string;
  groups!: InvitedGroup[];
}

// Declared on the domain, so a process that only reads them knows them too.
invitationDomain.event(
  InvitationCreated, InvitationResent, InvitationRevoked, InvitationDeclined, InvitationAccepted, InvitationMailSent, InvitationErased,
  InvitationLinkCreated, InvitationLinkRevoked, InvitationLinkUsed,
);
