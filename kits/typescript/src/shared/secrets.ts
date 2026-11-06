// Tokens, hashes and passwords.

import { createHash, randomBytes, timingSafeEqual } from "node:crypto";

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

export async function checkPassword(stored: string | null | undefined, password: unknown): Promise<boolean> {
  if (!stored || typeof password !== "string") return false;
  try {
    return await verify(stored, password);
  } catch {
    return false;
  }
}

/** Lowercased and trimmed: how emails are compared and held as unique keys. */
export function normalEmail(email: unknown): string {
  return typeof email === "string" ? email.trim().toLowerCase() : "";
}

export function validEmail(email: string): boolean {
  return /^[^\s@]+@[^\s@]+\.[^\s@]+$/.test(email) && email.length <= 254;
}

export function validPassword(password: unknown): password is string {
  return typeof password === "string" && password.length >= 10 && password.length <= 1024;
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
