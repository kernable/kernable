// Tokens, hashes and passwords.

import { createHash, createHmac, randomBytes, timingSafeEqual } from "node:crypto";

import { hash, verify } from "@node-rs/argon2";

/** A new random secret: 32 bytes, base64url. */
export function randomSecret(bytes = 32): string {
  return randomBytes(bytes).toString("base64url");
}

/** SHA-256 in hex: how high-entropy tokens are stored (refresh tokens, API keys, recovery codes). */
export function sha256(value: string): string {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

/** Whether two strings are equal, in time that does not depend on where they differ. */
export function equalSecrets(a: string, b: string): boolean {
  const x = Buffer.from(a, "utf8");
  const y = Buffer.from(b, "utf8");
  return x.length === y.length && timingSafeEqual(x, y);
}

/**
 * A token of three dot-separated parts: `<aggregateId>.<tokenId>.<secret>` for emailed tokens,
 * `<sessionId>.<generation>.<secret>` for refresh tokens, so the token names its aggregate.
 */
export function token(...parts: [string, string | number, string]): string {
  return parts.join(".");
}

/** The three parts of a token, or null if it has another shape. */
export function parseToken(value: unknown): [string, string, string] | null {
  if (typeof value !== "string") return null;
  const parts = value.split(".");
  return parts.length === 3 && parts.every((p) => p.length > 0) ? [parts[0], parts[1], parts[2]] : null;
}

/** argon2id with OWASP's parameters (19 MiB, 2 iterations, 1 lane). */
export function hashPassword(password: string): Promise<string> {
  return hash(password, { memoryCost: 19 * 1024, timeCost: 2, parallelism: 1 });
}

/**
 * An argon2id hash of a password nobody uses, with the same parameters: checked instead when there
 * is no hash to check, so an unknown address or an account without a password takes as long as a
 * wrong password. The same in every language's kit.
 */
export const DUMMY_PASSWORD_HASH = "$argon2id$v=19$m=19456,t=2,p=1$xZHjBkZVyzUUEIsFk5QnZg$ATIGagX8bkGblnc8NAAmXvUMTDX3izgmV74SUrCIjZg";

/** Whether `password` is the one `stored` hashes. Without a hash, a check against `DUMMY_PASSWORD_HASH` that fails, taking as long. */
export async function checkPassword(stored: string | null | undefined, password: unknown): Promise<boolean> {
  if (!stored || typeof password !== "string") {
    await verify(DUMMY_PASSWORD_HASH, typeof password === "string" ? password : "").catch(() => false);
    return false;
  }
  try {
    return await verify(stored, password);
  } catch {
    return false;
  }
}

/**
 * The characters the kits take for space: controls, spaces, line and paragraph separators and the
 * byte order mark. One explicit set, the same in every language's kit (each language's own idea of
 * whitespace differs).
 */
const SPACE = "\\x00-\\x20\\x7f\\x85\\xa0\\u1680\\u2000-\\u200a\\u2028\\u2029\\u202f\\u205f\\u3000\\ufeff";
const SPACE_ANY = new RegExp(`[${SPACE}]`);
const SPACE_EDGES = new RegExp(`^[${SPACE}]+|[${SPACE}]+$`, "g");
/** An unpaired surrogate: no character at all (with the `u` flag, paired ones are one code point). */
const LONE_SURROGATE = /\p{Cs}/u;

/** Whether `value` holds one of the kits' space characters. */
export function hasSpace(value: string): boolean {
  return SPACE_ANY.test(value);
}

/** `value` without the kits' space characters at either end. */
export function trimSpace(value: string): string {
  return value.replace(SPACE_EDGES, "");
}

/** The length of `value` in Unicode code points: how every kit counts the length of names and secrets. */
export function chars(value: string): number {
  return [...value].length;
}

/** Lowercased and trimmed: how emails are compared and held as unique keys. */
export function normalEmail(email: unknown): string {
  return typeof email === "string" ? trimSpace(email).toLowerCase() : "";
}

/** The domain of an address, lowercased: what follows its `@`. */
export function emailDomain(email: unknown): string {
  const e = normalEmail(email);
  return e.slice(e.lastIndexOf("@") + 1);
}

/**
 * An address: one `@`, a local part, a domain of at least two labels, none of them empty, none of
 * the kits' space characters (see `SPACE`), no unpaired surrogates, at most 254 bytes in UTF-8. The
 * same rule in every language's kit.
 */
export function validEmail(email: string): boolean {
  const at = email.indexOf("@");
  if (at < 1 || email.indexOf("@", at + 1) !== -1 || SPACE_ANY.test(email) || LONE_SURROGATE.test(email)) return false;
  if (Buffer.byteLength(email, "utf8") > 254) return false;
  const labels = email.slice(at + 1).split(".");
  return labels.length >= 2 && labels.every((l) => l.length > 0);
}

export function validPassword(password: unknown): password is string {
  return typeof password === "string" && chars(password) >= 10 && chars(password) <= 1024;
}

/** An invitation's token names its tenant too, so the API knows where to accept it: four parts. */
export function invitationToken(tenant: string, invitationId: string, tokenId: string, secret: string): string {
  return [tenant, invitationId, tokenId, secret].join(".");
}

export function parseInvitationToken(value: unknown): { tenant: string; invitationId: string; tokenId: string; secret: string } | null {
  if (typeof value !== "string") return null;
  const p = value.split(".");
  return p.length === 4 && p.every((x) => x.length > 0) ? { tenant: p[0], invitationId: p[1], tokenId: p[2], secret: p[3] } : null;
}

// --- TOTP (RFC 6238): HMAC-SHA1, 30 s steps, 6 digits, base32 secrets -----------------------------

const BASE32 = "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567";

/** A new TOTP secret: 20 random bytes in base32, as authenticator apps take it. */
export function totpSecret(): string {
  let bits = "";
  for (const b of randomBytes(20)) bits += b.toString(2).padStart(8, "0");
  return bits.match(/.{1,5}/g)!.map((c) => BASE32[parseInt(c.padEnd(5, "0"), 2)]).join("");
}

function base32(secret: string): Buffer {
  let bits = "";
  for (const c of secret.replace(/=+$/, "").toUpperCase()) {
    const v = BASE32.indexOf(c);
    if (v < 0) throw new Error("not base32");
    bits += v.toString(2).padStart(5, "0");
  }
  return Buffer.from((bits.match(/.{8}/g) ?? []).map((b) => parseInt(b, 2)));
}

/** The 6-digit code of `secret` for the 30-second step `step`. */
export function totpCode(secret: string, step: number): string {
  const counter = Buffer.alloc(8);
  counter.writeBigUInt64BE(BigInt(step));
  const mac = createHmac("sha1", base32(secret)).update(counter).digest();
  const offset = mac[mac.length - 1] & 0xf;
  return String((mac.readUInt32BE(offset) & 0x7fffffff) % 1_000_000).padStart(6, "0");
}

/** The step `code` belongs to, at `now` give or take one step (clock drift), or null. */
export function totpStep(secret: string, code: unknown, now: number): number | null {
  if (typeof code !== "string" || !/^\d{6}$/.test(code)) return null;
  const step = Math.floor(now / 30_000);
  for (const s of [step, step - 1, step + 1]) if (equalSecrets(totpCode(secret, s), code)) return s;
  return null;
}
