// Access tokens: short-lived, signed with Ed25519 (JWT, EdDSA), checked by the API
// itself without asking the kernel. The middleware still refuses an ended session at once.

import { createPrivateKey, createPublicKey, generateKeyPairSync, sign, verify, type KeyObject } from "node:crypto";
import { readFileSync } from "node:fs";

import { ACCESS_TOKEN_MINUTES, MINUTE } from "../shared/constants.ts";

export interface Claims {
  /** The account. */
  sub: string;
  /** The session. */
  sid: string;
  /** The factors the session had when the token was issued. */
  fac: string[];
  iat: number;
  exp: number;
}

export interface SigningKeys {
  privateKey: KeyObject;
  publicKey: KeyObject;
}

/** New keys, e.g. for development and tests. */
export function newSigningKeys(): SigningKeys {
  return generateKeyPairSync("ed25519");
}

/** Keys from a PEM file holding the private key (PKCS #8). */
export function signingKeysFrom(path: string): SigningKeys {
  const privateKey = createPrivateKey(readFileSync(path));
  return { privateKey, publicKey: createPublicKey(privateKey) };
}

const b64 = (value: unknown) => Buffer.from(JSON.stringify(value)).toString("base64url");
const HEADER = b64({ alg: "EdDSA", typ: "JWT" });

export function issueAccessToken(keys: SigningKeys, claims: Omit<Claims, "iat" | "exp">, now = Date.now()): string {
  const iat = Math.floor(now / 1000);
  const body = b64({ ...claims, iat, exp: iat + ACCESS_TOKEN_MINUTES * (MINUTE / 1000) });
  const signature = sign(null, Buffer.from(`${HEADER}.${body}`), keys.privateKey).toString("base64url");
  return `${HEADER}.${body}.${signature}`;
}

/** The claims of a valid, unexpired token, else null. */
export function checkAccessToken(keys: Pick<SigningKeys, "publicKey">, token: string, now = Date.now()): Claims | null {
  const parts = typeof token === "string" ? token.split(".") : [];
  if (parts.length !== 3 || parts[0] !== HEADER) return null;
  if (!verify(null, Buffer.from(`${parts[0]}.${parts[1]}`), keys.publicKey, Buffer.from(parts[2], "base64url"))) return null;
  try {
    const claims = JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8")) as Claims;
    if (typeof claims.sub !== "string" || typeof claims.sid !== "string" || !(claims.exp * 1000 > now)) return null;
    return claims;
  } catch {
    return null;
  }
}
