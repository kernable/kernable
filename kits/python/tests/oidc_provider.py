"""A minimal OpenID Connect provider for tests: discovery, JWKS, a token endpoint with PKCE and an
RS256 ID token. `login` plays the user who signs in at the provider and is sent back with a code."""

import base64
import hashlib
import json
import secrets
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, unquote, urlencode, urlparse

from cryptography.hazmat.primitives import hashes
from cryptography.hazmat.primitives.asymmetric import padding, rsa


def _b64(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).rstrip(b"=").decode()


def _int(n: int) -> str:
    return _b64(n.to_bytes((n.bit_length() + 7) // 8, "big"))


class OidcProvider:
    def __init__(self) -> None:
        self.client_id = "kit-client"
        self.client_secret = secrets.token_hex(16)
        self.key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
        n = self.key.public_key().public_numbers()
        self.jwk = {"kty": "RSA", "n": _int(n.n), "e": _int(n.e), "kid": "k1", "alg": "RS256", "use": "sig"}
        self.codes: dict[str, dict] = {}
        provider = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, *args) -> None:
                pass

            def json(self, status: int, body: dict) -> None:
                raw = json.dumps(body).encode()
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(raw)))
                self.end_headers()
                self.wfile.write(raw)

            def do_GET(self) -> None:  # noqa: N802
                path = urlparse(self.path).path
                if path == "/.well-known/openid-configuration":
                    i = provider.issuer
                    return self.json(200, {"issuer": i, "authorization_endpoint": f"{i}/authorize", "token_endpoint": f"{i}/token",
                                           "jwks_uri": f"{i}/jwks", "response_types_supported": ["code"],
                                           "id_token_signing_alg_values_supported": ["RS256"], "code_challenge_methods_supported": ["S256"]})
                if path == "/jwks":
                    return self.json(200, {"keys": [provider.jwk]})
                self.json(404, {"error": "not_found"})

            def do_POST(self) -> None:  # noqa: N802
                form = {k: v[0] for k, v in parse_qs(self.rfile.read(int(self.headers.get("content-length") or 0)).decode()).items()}
                basic = (self.headers.get("authorization") or "").removeprefix("Basic ")
                if basic:
                    id_, _, secret = base64.b64decode(basic).decode().partition(":")
                    id_, secret = unquote(id_), unquote(secret)
                else:
                    id_, secret = form.get("client_id"), form.get("client_secret")
                grant = provider.codes.pop(form.get("code", ""), None)
                if id_ != provider.client_id or secret != provider.client_secret:
                    return self.json(401, {"error": "invalid_client"})
                challenge = _b64(hashlib.sha256(form.get("code_verifier", "").encode()).digest())
                if not grant or grant["redirect_uri"] != form.get("redirect_uri") or challenge != grant["challenge"]:
                    return self.json(400, {"error": "invalid_grant"})
                now = int(time.time())
                header = _b64(json.dumps({"alg": "RS256", "kid": "k1", "typ": "JWT"}).encode())
                payload = _b64(json.dumps({"iss": provider.issuer, "aud": provider.client_id, "iat": now, "exp": now + 300,
                                           "nonce": grant["nonce"], **grant["user"]}).encode())
                signature = provider.key.sign(f"{header}.{payload}".encode(), padding.PKCS1v15(), hashes.SHA256())
                self.json(200, {"access_token": secrets.token_hex(16), "token_type": "Bearer", "expires_in": 300,
                                "id_token": f"{header}.{payload}.{_b64(signature)}"})

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.issuer = f"http://127.0.0.1:{self.server.server_address[1]}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def login(self, authorization_url: str, user: dict) -> str:
        """The user signs in at the provider for this authorization URL: the URL the browser comes back to."""
        q = {k: v[0] for k, v in parse_qs(urlparse(authorization_url).query).items()}
        code = secrets.token_hex(16)
        self.codes[code] = {"user": user, "challenge": q.get("code_challenge", ""), "nonce": q.get("nonce", ""), "redirect_uri": q.get("redirect_uri", "")}
        return f"{q['redirect_uri']}?{urlencode({'code': code, 'state': q.get('state', '')})}"

    def stop(self) -> None:
        self.server.shutdown()
        self.server.server_close()
