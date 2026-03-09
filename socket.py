
import asyncio
import contextlib
import traceback
from dataclasses import dataclass
from typing import Any, Optional
from urllib.parse import unquote

import orjson
from fastapi import APIRouter, Cookie, WebSocket, WebSocketDisconnect, status
from fastapi.exceptions import HTTPException
from starlette.websockets import WebSocketState

from app.helpers.codecHelpers import prep_incoming_payload
from app.helpers.taskContext import TaskContext
from app.logs.logging import log

router = APIRouter(prefix="/ws")

def get_messaging_service():
    from app.server import get_mm
    try:
        return get_mm()
    except Exception as e:
        raise HTTPException(status_code=503, detail="Messaging Service Not Available") from e


@dataclass
class Identity:
    fp: str
    sfp: Optional[str]
    data: dict[str, Any]


def _client_meta(ws: WebSocket) -> dict[str, Any]:
    if ws.client:
        return {"client_ip": ws.client.host, "client_port": ws.client.port}
    return {"client_ip": None, "client_port": None}


def _parse_identity_cookie(user_identity: str, ws: WebSocket) -> Identity:
    try:
        payload = orjson.loads(unquote(user_identity))
    except orjson.JSONDecodeError as e:
        raise ValueError("Invalid user_identity cookie JSON") from e

    fp = payload.get("fp")
    sfp = payload.get("sfp")

    if not isinstance(fp, str) or len(fp) < 5:
        raise ValueError(f"Invalid fingerprint in cookie: {fp!r}")

    payload.update(_client_meta(ws))
    return Identity(fp=fp, sfp=sfp, data=payload)


async def _safe_close(ws: WebSocket, code: int = status.WS_1000_NORMAL_CLOSURE) -> None:
    try:
        if ws.client_state != WebSocketState.CONNECTED: return
        await ws.close(code=code)
    except Exception:
        return


@router.websocket("/")
async def websocket_endpoint(
        websocket: WebSocket,
        user_identity: Optional[str] = Cookie(None, alias="user_identity"),
):
    manager = get_messaging_service()
    ctx = TaskContext()
    identity: Optional[Identity] = None

    close_code: int = status.WS_1000_NORMAL_CLOSURE

    # Accept exactly once
    try:
        await websocket.accept()
    except Exception:
        await log.error("Failed to accept websocket connection.")
        close_code = status.WS_1006_ABNORMAL_CLOSURE
        return

    try:
        # --- Identity extraction / validation ---
        if not user_identity:
            await log.error("Closing connection: Missing user_identity cookie.")
            close_code = status.WS_1008_POLICY_VIOLATION
            return

        try:
            identity = _parse_identity_cookie(user_identity, websocket)
        except ValueError as e:
            await log.error(f"Closing connection: {e}")
            close_code = status.WS_1008_POLICY_VIOLATION
            return
        except Exception as e:
            await log.error(f"Closing connection: Error processing user_identity cookie: {e}")
            close_code = status.WS_1008_POLICY_VIOLATION
            return

        # --- Connect (bookkeeping only) ---
        try:
            await manager.connect(websocket, identity.data)
        except Exception:
            await log.error("Failed to connect websocket.")
            close_code: int = status.WS_1006_ABNORMAL_CLOSURE
            return

        # --- Main receive loop ---
        _msg_semaphore = asyncio.Semaphore(64)  # limit concurrent handler tasks per connection
        try:
            while websocket.client_state == WebSocketState.CONNECTED:
                try:
                    raw_data = await websocket.receive_bytes()
                except WebSocketDisconnect as e:
                    code = e.code.value if hasattr(e.code, "value") else e.code
                    reason = e.code.name if hasattr(e.code, "name") else e.reason

                    reason = reason if not (reason is None or reason == "") else "Unknown"
                    code = code if not (code is None or code == 0 or code == "") else "?"
                    msg = f" {reason} ({code})" if code != 1005 else ''

                    ip_str = f"({identity.data.get('client_ip')}:{identity.data.get('client_port')})"
                    await log.websocket(f"WebSocket closed: {ip_str}{msg}")
                    break
                except RuntimeError as e:
                    await log.error(f"Client error while receiving data: {e}")
                    break


                message = prep_incoming_payload(raw_data)

                try:
                    await _msg_semaphore.acquire()
                    msg, action = await manager.handle_message(websocket, message)
                    async def _guarded(coro, sem):
                        try:
                            await coro
                        finally:
                            sem.release()
                    ctx.spawn(_guarded(msg, _msg_semaphore), name=action)
                except asyncio.CancelledError:
                    break
                except asyncio.TimeoutError as e:
                    await log.websocket(f"Timeout handling message: {e}")
                except Exception as e:
                    await log.websocket(f"Error handling message: {e}")
        except Exception as e:
            await log.error(f"Socket loop error: {e}")
            close_code: int = status.WS_1006_ABNORMAL_CLOSURE
            raise

    except Exception as e:
        # Most "disconnect" cases are already handled above.
        meta = identity.data if identity else _client_meta(websocket)
        await log.error(
            f"Unhandled error in WebSocket loop "
            f"({meta.get('client_ip')}:{meta.get('client_port')}): {e}\n{traceback.format_exc()}"
        )
        close_code = status.WS_1011_INTERNAL_ERROR

    finally:
        # Cleanup only: must NOT close inside manager.disconnect()
        try:
            await asyncio.shield(manager.disconnect(websocket, identity.data if identity else None))
        except Exception:
            pass

        try:
            await asyncio.shield(ctx.close())
        except Exception:
            pass

        # Close exactly once, at the very end.
        await _safe_close(websocket, code=close_code)

