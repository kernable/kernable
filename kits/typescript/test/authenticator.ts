// A software authenticator for tests: it answers WebAuthn the way a browser with a platform
// authenticator does (P-256, attestation "none", user verified), without a browser.

import { createHash, generateKeyPairSync, randomBytes, sign, type KeyObject } from "node:crypto";

import { isoCBOR } from "@simplewebauthn/server/helpers";

const b64 = (b: Uint8Array) => Buffer.from(b).toString("base64url");
const sha256 = (b: Uint8Array | string) => createHash("sha256").update(b).digest();

export class Authenticator {
  readonly credentialId = randomBytes(16);
  private readonly privateKey: KeyObject;
  private readonly cose: Uint8Array;
  private counter = 0;

  private readonly rpId: string;
  private readonly origin: string;
  private readonly userId: string;

  constructor(rpId: string, origin: string, userId: string) {
    this.rpId = rpId;
    this.origin = origin;
    this.userId = userId;
    const { privateKey, publicKey } = generateKeyPairSync("ec", { namedCurve: "P-256" });
    this.privateKey = privateKey;
    const jwk = publicKey.export({ format: "jwk" });
    // COSE_Key: kty EC2 (2), alg ES256 (-7), crv P-256 (1), x, y.
    this.cose = isoCBOR.encode(new Map<number, number | Uint8Array>([
      [1, 2], [3, -7], [-1, 1], [-2, Buffer.from(jwk.x!, "base64url")], [-3, Buffer.from(jwk.y!, "base64url")],
    ]));
  }

  private authData(flags: number, attested?: Uint8Array): Buffer {
    const count = Buffer.alloc(4);
    count.writeUInt32BE(this.counter);
    return Buffer.concat([sha256(this.rpId), Buffer.from([flags]), count, ...(attested ? [attested] : [])]);
  }

  private clientData(type: string, challenge: string, origin = this.origin): Buffer {
    return Buffer.from(JSON.stringify({ type, challenge, origin, crossOrigin: false }));
  }

  /** The browser's answer to registration options with this challenge. */
  register(challenge: string) {
    const idLength = Buffer.alloc(2);
    idLength.writeUInt16BE(this.credentialId.length);
    const attested = Buffer.concat([Buffer.alloc(16), idLength, this.credentialId, this.cose]);
    // Flags: user present (0x01), user verified (0x04), attested credential data (0x40).
    const attestationObject = isoCBOR.encode(new Map<string, unknown>([["fmt", "none"], ["attStmt", new Map()], ["authData", this.authData(0x45, attested)]]) as never);
    return {
      id: b64(this.credentialId), rawId: b64(this.credentialId), type: "public-key" as const, clientExtensionResults: {},
      response: { clientDataJSON: b64(this.clientData("webauthn.create", challenge)), attestationObject: b64(attestationObject), transports: ["internal" as const] },
    };
  }

  /** The browser's answer to sign-in options with this challenge, from `origin` (default the right one). */
  assert(challenge: string, origin?: string) {
    this.counter += 1;
    const authenticatorData = this.authData(0x05);
    const clientDataJSON = this.clientData("webauthn.get", challenge, origin);
    const signature = sign("sha256", Buffer.concat([authenticatorData, sha256(clientDataJSON)]), this.privateKey);
    return {
      id: b64(this.credentialId), rawId: b64(this.credentialId), type: "public-key" as const, clientExtensionResults: {},
      response: {
        clientDataJSON: b64(clientDataJSON), authenticatorData: b64(authenticatorData), signature: b64(signature),
        userHandle: b64(Buffer.from(this.userId)),
      },
    };
  }
}
