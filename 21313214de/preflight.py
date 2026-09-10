"""Hosting checks and one polling process per SQLite database."""
import asyncio
import os
import re
from pathlib import Path


def validate_settings():
    token = os.getenv('BOT_TOKEN', '').strip()
    admins = os.getenv('ADMIN_IDS', '').strip()
    if not re.fullmatch(r'\d+:[A-Za-z0-9_-]{30,}', token):
        raise ValueError('Set BOT_TOKEN in .env (Telegram BotFather token).')
    if not admins or any(not x.strip().isdigit() or int(x) <= 0 for x in admins.split(',')):
        raise ValueError('Set ADMIN_IDS in .env to your numeric Telegram user ID(s).')
    if os.getenv('LICENSE_API_ENABLED', '0') != '0':
        raise ValueError('Use LICENSE_API_ENABLED=0: licenses are hosted on Cloudflare.')
    if os.getenv('LICENSE_ALLOW_LOCAL_HTTP', '0') != '0':
        raise ValueError('Use LICENSE_ALLOW_LOCAL_HTTP=0 for this hosting package.')
    if bool(os.getenv('PLATEGA_MERCHANT_ID', '').strip()) != bool(os.getenv('PLATEGA_SECRET', '').strip()):
        raise ValueError('Set both PLATEGA_MERCHANT_ID and PLATEGA_SECRET, or leave both empty.')


async def run_check(bot, online=False):
    validate_settings()
    bot.store.client.validate_config()
    try:
        await bot.init_db()
        await asyncio.to_thread(bot.prepare_jar, bot.MOD_FILE_PATH)
        if not Path(bot.START_IMAGE_PATH).is_file():
            raise ValueError('START_IMAGE_PATH does not exist.')
        Path(bot.RELEASES_DIR).mkdir(parents=True, exist_ok=True)
        if online:
            if not await asyncio.to_thread(bot.store.client.health):
                raise ValueError('Cloudflare DBR-v1 health check failed.')
            # Read-only admin request; a missing license is a valid authenticated response.
            await asyncio.to_thread(bot.store.client.get, next(iter(bot.ADMIN_IDS)))
        print('OK: configuration, database migration, banner and integrated JAR.' +
              (' Cloudflare health and admin authentication OK.' if online else ' No external services contacted.'))
    finally:
        await bot.engine.dispose()


class SingleInstance:
    def __init__(self, db_path):
        self.path = Path(db_path).resolve().with_suffix('.lock')
        self.file = None

    def __enter__(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.file = self.path.open('a+b')
        if self.path.stat().st_size == 0:
            self.file.write(b'0')
            self.file.flush()
        self.file.seek(0)
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(self.file.fileno(), msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(self.file.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            self.file.close()
            raise RuntimeError('Another bot process already uses this database. Stop it first.') from None
        return self

    def __exit__(self, *args):
        self.file.close()
