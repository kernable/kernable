// Registers every part of the account domain: a part that is not imported here does not exist.

import "./aggregate/account.ts";
import "./aggregate/session.ts";
import "./command/register.ts";
import "./command/request-verification.ts";
import "./command/verify-email.ts";
import "./command/sign-in.ts";
import "./command/open-session.ts";
import "./command/refresh-session.ts";
import "./command/add-session-factor.ts";
import "./command/end-session.ts";
import "./readmodel/accounts.ts";
import "./query/find-by-email.ts";
import "./query/get.ts";
import "./reactor/sessions.ts";
import "./reactor/mail.ts";

export { accountDomain } from "./domain.ts";
export type * from "./domain.ts";
