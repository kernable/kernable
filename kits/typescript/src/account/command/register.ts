import { Rejected } from "@kernable/sdk";

import { HOUR, VERIFICATION_HOURS } from "../../shared/constants.ts";
import { hashPassword, normalEmail, randomSecret, validEmail, validPassword } from "../../shared/secrets.ts";
import { accountAggregate } from "../aggregate/account.ts";

export interface RegisterAccount {
  email: string;
  name: string;
  password?: string;
  /** Set by the API when an accepted invitation proved the email: active at once. */
  verified?: boolean;
}

/** A new verification token, kept in the vault under its own subject until used. */
export function verificationRequested(email: string, at: number) {
  return {
    type: "VerificationRequested",
    data: { tokenId: randomSecret(9), token: randomSecret(), email, expiresAt: at + VERIFICATION_HOURS * HOUR },
  };
}

accountAggregate.command("RegisterAccount", async (s, d: RegisterAccount, _cmd, ctx) => {
  if (s.status !== "none") throw new Rejected("The account exists", "exists");
  const email = normalEmail(d.email);
  if (!validEmail(email)) throw new Rejected("Not an email address", "invalid_email");
  if (typeof d.name !== "string" || !d.name.trim() || d.name.length > 200) throw new Rejected("A name is needed", "invalid_name");
  if (d.password !== undefined && !validPassword(d.password)) throw new Rejected("A password has 10 to 1024 characters", "weak_password");
  const at = ctx.now().getTime();
  const passwordHash = d.password === undefined ? null : await hashPassword(d.password);
  const verified = d.verified === true;
  const registered = { type: "AccountRegistered", data: { email, name: d.name.trim(), passwordHash, verified, at } };
  return verified ? [registered] : [registered, verificationRequested(email, at)];
});
