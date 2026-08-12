from __future__ import annotations

import uuid
from dataclasses import dataclass
from typing import Any, Optional

from fastapi import Request, WebSocket, WebSocketDisconnect
from xcore.kernel.api.auth import AuthPayload
from xcore.kernel.api.rbac import get_current_user
from xcore.services.base import BaseService, ServiceStatus

from .ws import WebSocketManager


@dataclass
class WebSocketConf:
    channel: list[str]

    @classmethod
    def from_dict(cls, data: dict) -> "WebSocketConf":
        return cls(channel=data.get("channel", []))


class WsManager(BaseService):
    name = "websocket"

    def __init__(self, config: dict) -> None:
        self._status = ServiceStatus.INITIALIZING
        self.config = config

        self.client_ids: dict[str, dict[str, str]] = {}  # <-- initialisé
        self.ws: Optional[WebSocketManager] = None
        self.configuration: Optional[WebSocketConf] = None

    async def init(self) -> None:
        self.configuration = WebSocketConf.from_dict(data=self.config)
        self.ws = WebSocketManager()
        self._status = ServiceStatus.READY

    async def shutdown(self) -> None:
        # Ici shutdown est sync dans ton code original
        self.ws = None
        self._status = ServiceStatus.STOPPED

    async def ws_endpoint(self, ws: WebSocket, request: Request, channel: str):
        response: AuthPayload = await get_current_user(request)

        # Sécurité
        if not self.configuration or channel not in self.configuration.channel:
            print(
                f"[WS SECURITY] User {response.get('sub')} denied access to channel "
                f"'{channel}' (missing module {channel})"
            )
            await ws.accept()
            await ws.close(code=4003)
            return

        client_id = str(uuid.uuid4())

        inf: dict[str, str] = {
            "sub": str(response.get("sub")),
            "channel": str(channel),
        }
        self.client_ids[client_id] = inf

        if self.ws is None:
            # Normalement init() a déjà créé self.ws, mais on garde safe.
            await ws.close(code=1011)
            self.client_ids.pop(client_id, None)
            return

        await self.ws.connect(
            channel=channel, client_id=client_id, ws=ws, user_id=response.get("sub")
        )

        try:
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            await self.ws.disconnect(channel=channel, client_id=client_id)
            self.client_ids.pop(client_id, None)
        except Exception as e:
            print(f"[WS] Deconnexion inattendue {client_id[:8]} sur {channel}: {e}")
            await self.ws.disconnect(channel=channel, client_id=client_id)
            self.client_ids.pop(client_id, None)

    async def broadcast(self, channel: str, event: str, data: Any):
        if self.ws is not None:
            await self.ws.broadcast(channel, event, data)

    async def health_check(
        self,
    ):

        if self.ws:
            return True, "Service running"
        return False, "Service stopped"

    async def status(
        self,
    ):

        return {}

    async def info(
        self,
    ):
        return {
            "client_count": len(self.client_ids),
            "channels": list(self.configuration.channel) if self.configuration else [],
            "client_ids": self.client_ids,
        }
