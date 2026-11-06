// Sends the emailed tokens: it reads them from the vault (its key is granted vault_read).

import { ORDERS } from "../../shared/constants.ts";
import { currentMailer } from "../../shared/mail.ts";
import { token } from "../../shared/secrets.ts";
import { accountDomain, type VerificationRequested } from "../domain.ts";

export const mailReactor = accountDomain
  .reactor("mail", { order: ORDERS.reactors })
  .on("VerificationRequested", async (d: VerificationRequested, evt) => {
    // Erased already (the account verified, or was deleted): nothing to send.
    if (!d.token || !d.email) return;
    const link = token(evt.aggregateId, d.tokenId, d.token);
    await currentMailer().send({
      to: d.email, purpose: "verify", token: link, subject: "Confirm your email address",
      text: `Confirm your email address with this code: ${link}`,
    });
  });
