import contextlib

from rethinkdb import r

__all__ = ["r", "connect"]

r.set_loop_type("asyncio")

@contextlib.asynccontextmanager
async def connect():
    conn = await r.connect()
    try:
        yield conn
    finally:
        try:
            await conn.close()
        except Exception:
            pass
