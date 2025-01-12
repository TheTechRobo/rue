import contextlib, os

from rethinkdb import r

__all__ = ["r", "connect"]

r.set_loop_type("asyncio")

HOST = os.getenv("RUE_DB_HOST", "localhost")
PORT = os.getenv("RUE_DB_PORT", "28015")
USER = os.getenv("RUE_DB_USER", "admin")
PASSWORD = os.getenv("RUE_DB_PASSWORD", "")
SSL = os.getenv("RUE_DB_SSL")

@contextlib.asynccontextmanager
async def connect():
    options = dict(
        host = HOST,
        port = PORT,
        user = USER,
        password = PASSWORD
    )
    if SSL:
        options |= dict(
            ssl = {"ca_certs": SSL}
        )
    conn = await r.connect(**options)
    try:
        yield conn
    finally:
        try:
            await conn.close()
        except Exception:
            pass
