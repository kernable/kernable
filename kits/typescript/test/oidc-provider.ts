// A minimal OpenID Connect provider for tests: discovery, JWKS, a token endpoint with PKCE and an
// RS256 ID token. `login` plays the user who signs in at the provider and is sent back with a code.

import { createHash, generateKeyPairSync, randomBytes, sign, type KeyObject } from "node:crypto";
import { createServer, type Server } from "node:http";

export interface User { sub: string; email: string; email_verified: boolean }

const b64 = (v: unknown) => Buffer.from(typeof v === "string" ? v : JSON.stringify(v)).toString("base64url");

export class OidcProvider {
  readonly clientId = "kit-client";
  readonly clientSecret = randomBytes(16).toString("hex");
  issuer = "";
  private server?: Server;
  private readonly key: KeyObject;
  private readonly jwk: Record<string, unknown>;
  private readonly codes = new Map<string, { user: User; challenge: string; nonce: string; redirectUri: string }>();

  constructor() {
    const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
    this.key = privateKey;
    this.jwk = { ...publicKey.export({ format: "jwk" }), kid: "k1", alg: "RS256", use: "sig" };
  }

  async start(): Promise<void> {
    this.server = createServer((req, res) => {
      const url = new URL(req.url ?? "/", this.issuer);
      const json = (status: number, body: unknown) => {
        res.writeHead(status, { "content-type": "application/json" });
        res.end(JSON.stringify(body));
      };
      if (url.pathname === "/.well-known/openid-configuration") {
        return json(200, {
          issuer: this.issuer, authorization_endpoint: `${this.issuer}/authorize`, token_endpoint: `${this.issuer}/token`, jwks_uri: `${this.issuer}/jwks`,
          response_types_supported: ["code"], subject_types_supported: ["public"], id_token_signing_alg_values_supported: ["RS256"],
          code_challenge_methods_supported: ["S256"], token_endpoint_auth_methods_supported: ["client_secret_post", "client_secret_basic"],
        });
      }
      if (url.pathname === "/jwks") return json(200, { keys: [this.jwk] });
      if (url.pathname === "/token" && req.method === "POST") {
        let body = "";
        req.on("data", (c) => (body += c));
        req.on("end", () => {
          const form = new URLSearchParams(body);
          const basic = (req.headers.authorization ?? "").replace(/^Basic /, "");
          const [id, secret] = basic ? Buffer.from(basic, "base64").toString().split(":").map(decodeURIComponent) : [form.get("client_id"), form.get("client_secret")];
          const grant = this.codes.get(form.get("code") ?? "");
          this.codes.delete(form.get("code") ?? "");
          const verifier = form.get("code_verifier") ?? "";
          if (id !== this.clientId || secret !== this.clientSecret) return json(401, { error: "invalid_client" });
          if (!grant || grant.redirectUri !== form.get("redirect_uri") || createHash("sha256").update(verifier).digest("base64url") !== grant.challenge) {
            return json(400, { error: "invalid_grant" });
          }
          const now = Math.floor(Date.now() / 1000);
          const header = b64({ alg: "RS256", kid: "k1", typ: "JWT" });
          const payload = b64({ iss: this.issuer, aud: this.clientId, iat: now, exp: now + 300, nonce: grant.nonce, ...grant.user });
          const idToken = `${header}.${payload}.${sign("sha256", Buffer.from(`${header}.${payload}`), this.key).toString("base64url")}`;
          json(200, { access_token: randomBytes(16).toString("hex"), token_type: "Bearer", expires_in: 300, id_token: idToken });
        });
        return;
      }
      json(404, { error: "not_found" });
    });
    await new Promise<void>((resolve) => this.server!.listen(0, "127.0.0.1", resolve));
    const address = this.server.address() as { port: number };
    this.issuer = `http://127.0.0.1:${address.port}`;
  }

  /** The user signs in at the provider for this authorization URL: the URL the browser comes back to. */
  login(authorizationUrl: string, user: User): string {
    const url = new URL(authorizationUrl);
    const code = randomBytes(16).toString("hex");
    this.codes.set(code, {
      user, challenge: url.searchParams.get("code_challenge") ?? "", nonce: url.searchParams.get("nonce") ?? "", redirectUri: url.searchParams.get("redirect_uri") ?? "",
    });
    const back = new URL(url.searchParams.get("redirect_uri")!);
    back.searchParams.set("code", code);
    back.searchParams.set("state", url.searchParams.get("state") ?? "");
    return back.href;
  }

  stop(): void {
    this.server?.close();
  }
}
