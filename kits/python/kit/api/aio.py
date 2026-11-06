"""`KitApi` for asyncio frameworks (FastAPI, Starlette, aiohttp): the same methods, awaited. Each
runs the blocking method in a worker thread (`asyncio.to_thread`), so a request waiting for the
kernel never blocks the event loop. `check` stays plain: it needs no kernel.

    api = AsyncKitApi(KitApi(kernable, keys, service))
    signed_in = await api.sign_in(email, password, ip=ip)
"""

import asyncio

from kernable import RequestContext

from .api import KitApi
from .tokens import Claims


class AsyncKitApi:
    def __init__(self, api: KitApi) -> None:
        #: The blocking API underneath, e.g. for `service_context`.
        self.api = api

    def check(self, access_token: str) -> Claims | None:
        return self.api.check(access_token)

    async def register(self, email: str, name: str, password: str | None = None) -> dict:
        return await asyncio.to_thread(self.api.register, email, name, password)

    async def verify_email(self, email_token: str) -> bool:
        return await asyncio.to_thread(self.api.verify_email, email_token)

    async def sign_in(self, email: str, password: str, ip: str | None = None, user_agent: str | None = None) -> dict:
        return await asyncio.to_thread(self.api.sign_in, email, password, ip, user_agent)

    async def context_for(self, claims: Claims, tenant: str | None = None) -> RequestContext:
        return await asyncio.to_thread(self.api.context_for, claims, tenant)

    async def refresh(self, refresh_token: str, ip: str | None = None) -> dict:
        return await asyncio.to_thread(self.api.refresh, refresh_token, ip)

    async def prove_second_factor(self, claims: Claims, code: str) -> dict:
        return await asyncio.to_thread(self.api.prove_second_factor, claims, code)

    async def start_totp(self, claims: Claims, issuer: str, label: str) -> dict:
        return await asyncio.to_thread(self.api.start_totp, claims, issuer, label)

    async def confirm_totp(self, claims: Claims, code: str) -> dict:
        return await asyncio.to_thread(self.api.confirm_totp, claims, code)

    async def create_api_key(self, context: RequestContext, tenant: str, identity_id: str, label: str = "", expires_at: int | None = None) -> dict:
        return await asyncio.to_thread(self.api.create_api_key, context, tenant, identity_id, label, expires_at)

    async def check_api_key(self, key: str) -> RequestContext | None:
        return await asyncio.to_thread(self.api.check_api_key, key)

    async def passkey_registration_options(self, claims: Claims, user_name: str) -> dict:
        return await asyncio.to_thread(self.api.passkey_registration_options, claims, user_name)

    async def add_passkey(self, claims: Claims, response: dict, label: str | None = None) -> dict:
        return await asyncio.to_thread(self.api.add_passkey, claims, response, label)

    async def passkey_sign_in_options(self) -> dict:
        return await asyncio.to_thread(self.api.passkey_sign_in_options)

    async def sign_in_with_passkey(self, response: dict, ip: str | None = None) -> dict:
        return await asyncio.to_thread(self.api.sign_in_with_passkey, response, ip)

    async def prove_passkey(self, claims: Claims, response: dict) -> dict:
        return await asyncio.to_thread(self.api.prove_passkey, claims, response)

    async def oidc_start(self, *, provider: str | None = None, email: str | None = None, link: Claims | None = None) -> dict:
        return await asyncio.to_thread(lambda: self.api.oidc_start(provider=provider, email=email, link=link))

    async def oidc_finish(self, callback: str, ip: str | None = None) -> dict:
        return await asyncio.to_thread(self.api.oidc_finish, callback, ip)

    async def verify_sso_domain(self, tenant: str, sso_id: str, domain: str) -> dict:
        return await asyncio.to_thread(self.api.verify_sso_domain, tenant, sso_id, domain)

    async def change_password(self, claims: Claims, current: str | None, new: str) -> dict:
        return await asyncio.to_thread(self.api.change_password, claims, current, new)

    async def request_password_reset(self, email: str) -> None:
        await asyncio.to_thread(self.api.request_password_reset, email)

    async def reset_password(self, reset_token: str, new: str) -> dict:
        return await asyncio.to_thread(self.api.reset_password, reset_token, new)

    async def request_email_change(self, claims: Claims, email: str) -> dict:
        return await asyncio.to_thread(self.api.request_email_change, claims, email)

    async def confirm_email_change(self, change_token: str) -> dict:
        return await asyncio.to_thread(self.api.confirm_email_change, change_token)

    async def peek_invitation(self, invitation_token: str) -> dict | None:
        return await asyncio.to_thread(self.api.peek_invitation, invitation_token)

    async def accept_invitation(self, invitation_token: str, *, claims: Claims | None = None, name: str | None = None,
                                password: str | None = None) -> dict:
        return await asyncio.to_thread(lambda: self.api.accept_invitation(invitation_token, claims=claims, name=name, password=password))

    async def sign_out(self, claims: Claims) -> None:
        await asyncio.to_thread(self.api.sign_out, claims)
