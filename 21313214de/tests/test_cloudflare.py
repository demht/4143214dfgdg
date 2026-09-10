"""Offline tests: fake DBR-v1 Worker, no live credentials or Telegram messages."""
import asyncio
from contextlib import closing
from io import BytesIO
import json
import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch, AsyncMock
import urllib.error

os.environ['PYTHON_DOTENV_DISABLED'] = '1'
from cloudflare_store import CloudflareStore
from cloudflare_license_client import CloudflareLicenseClient, CloudflareLicenseError, DEFAULT_LICENSE_URL
from subscription_store import SubscriptionStore, epoch, stamp


class FakeCloud:
    def __init__(self, clock):
        self.clock, self.rows, self.calls = clock, {}, []
        self.offline, self.lose_response = False, None

    def get(self, user):
        if self.offline:
            raise CloudflareLicenseError('cloudflare_unavailable')
        row = self.rows.get(user)
        if row:
            row = dict(row)
            row['status'] = 'BLOCKED' if row['blocked'] else ('ACTIVE' if row['expires_at'] > self.clock() else 'EXPIRED')
        return row

    def result(self, method, user):
        self.calls.append(method)
        if self.lose_response == method:
            self.lose_response = None
            raise CloudflareLicenseError('cloudflare_unavailable')
        return self.get(user)

    def create(self, user, seconds, debris_id=None):
        if self.offline:
            raise CloudflareLicenseError('cloudflare_unavailable')
        if user not in self.rows:
            self.rows[user] = dict(user_id=user, debris_id=debris_id or 'DBR-ABCD-EFGH-JKLM',
                expires_at=int(self.clock()) + seconds, blocked=False, legacy_lifetime=False,
                device_bound=False, binding_version=0, created_at=int(self.clock()),
                activated_at=None, last_check_at=None, reset_at=None)
        return self.result('create', user)

    def set_duration(self, user, seconds):
        self.rows[user]['expires_at'] = int(self.clock()) + seconds
        return self.result('set', user)

    def extend(self, user, seconds):
        self.rows[user]['expires_at'] = max(int(self.clock()), self.rows[user]['expires_at']) + seconds
        return self.result('extend', user)

    def set_blocked(self, user, blocked):
        self.rows[user]['blocked'] = blocked
        return self.result('block', user)

    def reset_device(self, user):
        self.rows[user]['device_bound'] = False
        self.rows[user]['binding_version'] += 1
        return self.result('reset', user)


class CloudStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.now = 1_800_000_000
        self.cloud = FakeCloud(lambda: self.now)
        self.store = CloudflareStore(Path(self.tmp.name) / 'bot.db', client=self.cloud, clock=lambda: self.now)
        self.store.migrate()

    def tearDown(self):
        self.tmp.cleanup()

    def pay(self, oid=1, seconds=120):
        with self.store.transaction() as c:
            c.execute('''INSERT INTO orders(id,user_id,payment_method,amount,currency,status,created_at,duration_seconds)
                VALUES (?,10,'stars',145,'XTR','INVOICE',?,?)''', (oid, stamp(self.now), seconds))
        self.store.confirm(oid, 'stars', f'test-charge-{oid}', 145, 'XTR', 10)

    def deliver(self, oid=1):
        order, row, claim = self.store.claim_delivery(oid)
        self.store.finish_delivery(oid, claim)
        return row

    def restart(self):
        self.store = CloudflareStore(self.store.path, client=self.cloud, clock=lambda: self.now)
        self.store.migrate()

    def test_payment_is_persisted_before_cloudflare_and_issues_real_id(self):
        self.pay()
        self.assertIsNone(self.store.get(10))
        self.assertEqual(self.cloud.calls, [])
        row = self.deliver()
        self.assertEqual(row['license_id'], self.cloud.get(10)['debris_id'])
        self.assertEqual(epoch(row['expires_at']), self.now + 120)
        self.assertIsNone(self.store.claim_delivery(1))

    def test_cloudflare_outage_keeps_paid_order_for_retry(self):
        self.cloud.offline = True
        self.pay()
        with self.assertRaises(CloudflareLicenseError):
            self.store.claim_delivery(1)
        self.now += 21
        self.cloud.offline = False
        self.restart()
        self.assertEqual(self.store.pending_deliveries(), [1])
        self.assertEqual(epoch(self.deliver()['expires_at']), self.now + 120)

    def test_lost_create_response_retries_without_duplicate_time(self):
        self.pay()
        self.cloud.lose_response = 'create'
        with self.assertRaises(CloudflareLicenseError):
            self.store.claim_delivery(1)
        target = self.cloud.get(10)['expires_at']
        self.now += 21
        self.restart()
        self.assertEqual(epoch(self.deliver()['expires_at']), target)

    def test_lost_renewal_response_is_not_applied_twice_after_restart(self):
        self.pay(); self.deliver()
        self.pay(2, 300)
        self.cloud.lose_response = 'set'
        with self.assertRaises(CloudflareLicenseError):
            self.store.claim_delivery(2)
        target = self.cloud.get(10)['expires_at']
        self.now += 21
        self.restart()
        self.assertEqual(epoch(self.deliver(2)['expires_at']), target)
        self.assertEqual(self.cloud.calls.count('set'), 1)

    def test_two_pending_orders_accumulate_and_duplicate_payment_is_idempotent(self):
        self.pay(1, 120); self.pay(2, 180)
        self.store.confirm(1, 'stars', 'test-charge-1', 145, 'XTR', 10)
        self.assertEqual(epoch(self.deliver(2)['expires_at']), self.now + 300)
        self.assertEqual(epoch(self.deliver(1)['expires_at']), self.now + 300)

    def test_device_status_and_admin_actions_are_remote(self):
        self.pay(); self.deliver()
        self.cloud.rows[10]['device_bound'] = True
        self.cloud.rows[10]['last_check_at'] = self.now
        self.assertTrue(self.store.refresh(10)['device_hash'])
        self.store.edit(10, 'reset')
        self.assertIsNone(self.store.get(10)['device_hash'])
        self.store.edit(10, 'extend', 20)
        self.assertEqual(self.cloud.get(10)['expires_at'], self.now + 140)
        self.store.edit(10, 'set', 40)
        self.assertEqual(self.cloud.get(10)['expires_at'], self.now + 40)
        self.store.edit(10, 'toggle')
        self.assertFalse(self.store.eligible(10))
        self.store.edit(10, 'toggle')
        self.assertTrue(self.store.eligible(10))

    def test_refund_block_is_persisted_and_retried_after_outage(self):
        self.pay(); self.deliver()
        self.cloud.offline = True
        with self.assertRaises(CloudflareLicenseError):
            self.store.cancel_payment(1, True)
        self.cloud.offline = False
        self.restart(); self.store.sync_revocations()
        self.assertEqual(self.cloud.get(10)['status'], 'BLOCKED')
        self.assertFalse(self.store.eligible(10))

    def test_deleted_remote_license_is_not_recreated_from_cache(self):
        self.pay(); self.deliver()
        del self.cloud.rows[10]
        self.assertIsNone(self.store.refresh(10))
        self.assertIsNone(self.cloud.get(10))

    def test_legacy_paid_grant_is_not_doubled_on_import(self):
        path = Path(self.tmp.name) / 'old.db'
        old = SubscriptionStore(path, clock=lambda: self.now)
        old.migrate()
        with old.transaction() as c:
            c.execute('''INSERT INTO orders(id,user_id,payment_method,amount,currency,status,created_at,duration_seconds)
                VALUES (1,10,'stars',145,'XTR','INVOICE',?,120)''', (stamp(self.now),))
        before = old.confirm(1, 'stars', 'legacy-charge', 145, 'XTR', 10)
        self.store = CloudflareStore(path, client=self.cloud, clock=lambda: self.now)
        self.store.migrate()
        after = self.deliver()
        self.assertEqual(after['license_id'], before['license_id'])
        self.assertEqual(after['expires_at'], before['expires_at'])

    def test_client_authorization_cannot_use_local_cache(self):
        with self.assertRaises(RuntimeError):
            self.store.authorize('activate', 'DBR-ABCD-EFGH-JKLM', 'a' * 64)

    def test_lost_legacy_import_response_preserves_block_on_retry(self):
        with self.store.transaction() as conn:
            conn.execute('''INSERT INTO licenses(user_id,source_order_id,purchased_at,active,license_id,expires_at)
                VALUES (10,0,?,0,'DBR-ABCD-EFGH-JKLM',?)''', (stamp(self.now), stamp(self.now + 120)))
        self.cloud.lose_response = 'create'
        with self.assertRaises(CloudflareLicenseError):
            self.store.refresh(10)
        self.assertFalse(self.store.refresh(10)['active'])
        self.assertEqual(self.cloud.get(10)['status'], 'BLOCKED')


class ClientTests(unittest.TestCase):
    def setUp(self):
        self.client = CloudflareLicenseClient(DEFAULT_LICENSE_URL, 'offline-test-key')

    def test_exact_routes_header_and_payload(self):
        row = dict(user_id=10, debris_id='DBR-ABCD-EFGH-JKLM', status='ACTIVE', expires_at=1800000120)
        class Response(BytesIO):
            def __enter__(self): return self
            def __exit__(self, *args): self.close()
        with patch.object(self.client.opener, 'open', return_value=Response(json.dumps({'ok': True, 'license': row}).encode())) as request:
            self.assertEqual(self.client.create(10, 120), row)
            sent = request.call_args.args[0]
            self.assertEqual(sent.full_url, DEFAULT_LICENSE_URL + '/api/admin/v1/licenses/create')
            self.assertEqual(sent.get_header('X-admin-key'), 'offline-test-key')
            self.assertEqual(json.loads(sent.data), {'user_id': 10, 'duration_seconds': 120})

    def test_not_found_and_auth_errors_are_distinct(self):
        for status, code in [(404, 'license_not_found'), (401, 'unauthorized')]:
            error = urllib.error.HTTPError(DEFAULT_LICENSE_URL, status, '', {}, BytesIO(json.dumps({'error': code}).encode()))
            with patch.object(self.client.opener, 'open', side_effect=error):
                if status == 404:
                    self.assertIsNone(self.client.get(10))
                else:
                    with self.assertRaisesRegex(CloudflareLicenseError, 'unauthorized'):
                        self.client.get(10)

    def test_invalid_json_is_a_retryable_client_error(self):
        with patch.object(self.client.opener, 'open', return_value=BytesIO(b'<html>failure</html>')):
            with self.assertRaisesRegex(CloudflareLicenseError, 'invalid_api_response'):
                self.client.get(10)

    def test_unsafe_urls_and_missing_secret_rejected(self):
        for url in ['http://example.org', 'https://key@example.org', 'https://example.org/?key=x']:
            with self.assertRaises(ValueError):
                CloudflareLicenseClient(url, 'test').validate_config()
        with self.assertRaises(ValueError):
            CloudflareLicenseClient(DEFAULT_LICENSE_URL, '').validate_config()


class MigrationTests(unittest.TestCase):
    def test_real_uploaded_database_schema_and_existing_rows_preserved(self):
        source = os.getenv('TEST_UPLOADED_DB')
        if not source:
            self.skipTest('Set TEST_UPLOADED_DB to test a copy of the uploaded DB')
        with tempfile.TemporaryDirectory() as tmp:
            target = Path(tmp) / 'bot.db'
            shutil.copy2(source, target)
            with closing(sqlite3.connect(target)) as c:
                originals = {}
                for (name,) in c.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall():
                    cols = [r[1] for r in c.execute(f'PRAGMA table_info({name})')]
                    originals[name] = (cols, c.execute(f'SELECT * FROM {name}').fetchall())
            store = CloudflareStore(target, client=FakeCloud(lambda: 1800000000))
            store.migrate(); store.migrate()
            with closing(store.connect()) as c:
                self.assertEqual(c.execute('PRAGMA integrity_check').fetchone()[0], 'ok')
                self.assertIn('license_id', [r[1] for r in c.execute('PRAGMA table_info(licenses)')])
                for name, (cols, values) in originals.items():
                    self.assertEqual([tuple(r) for r in c.execute(f'SELECT {",".join(cols)} FROM {name}')], values)
            self.assertEqual(len(list(Path(tmp).glob('*.bak'))), 1)


class StartupTests(unittest.IsolatedAsyncioTestCase):
    async def test_main_reaches_polling_and_cleans_up_with_mock_services(self):
        import bot
        env = {'BOT_TOKEN': '123456:' + 'x' * 35, 'ADMIN_IDS': '10',
               'LICENSE_ADMIN_API_KEY': 'offline-test-key', 'LICENSE_PUBLIC_URL': DEFAULT_LICENSE_URL,
               'LICENSE_API_ENABLED': '0', 'LICENSE_ALLOW_LOCAL_HTTP': '0',
               'PLATEGA_MERCHANT_ID': '', 'PLATEGA_SECRET': ''}
        fake_bot = SimpleNamespace(get_me=AsyncMock(return_value=SimpleNamespace(username='offline_test')),
                                   session=SimpleNamespace(close=AsyncMock()))
        dispatcher = SimpleNamespace(include_router=lambda router: None, start_polling=AsyncMock())
        with tempfile.TemporaryDirectory() as tmp:
            store = CloudflareStore(Path(tmp) / 'bot.db', client=CloudflareLicenseClient(DEFAULT_LICENSE_URL, 'offline-test-key'))
            with patch.dict(os.environ, env), patch.object(bot, 'store', store), patch.object(bot, 'BOT_TOKEN', env['BOT_TOKEN']), \
                    patch.object(bot, 'Bot', return_value=fake_bot), patch.object(bot, 'Dispatcher', return_value=dispatcher), \
                    patch.object(bot, 'prepare_jar', return_value=Path('test.jar')), patch.object(store.client, 'health', return_value=True), \
                    patch.object(store.client, 'get', return_value=None), patch.object(bot, 'ADMIN_IDS', {10}), \
                    patch.object(bot, 'resume_release_broadcasts', new=AsyncMock()), patch.object(bot, 'platega_enabled', return_value=False):
                await bot.main()
            dispatcher.start_polling.assert_awaited_once()
            fake_bot.session.close.assert_awaited_once()
            self.assertTrue(store.health())


class CloudDeliveryTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        import bot
        from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
        self.bot = bot
        self.tmp = tempfile.TemporaryDirectory()
        self.now = 1900000000
        self.cloud = FakeCloud(lambda: self.now)
        self.store = CloudflareStore(Path(self.tmp.name) / 'bot.db', client=self.cloud, clock=lambda: self.now)
        self.store.migrate()
        self.engine = create_async_engine(f'sqlite+aiosqlite:///{self.store.path}')
        self.patches = [patch.object(bot, 'store', self.store), patch.object(bot, 'engine', self.engine),
            patch.object(bot, 'Session', async_sessionmaker(self.engine, expire_on_commit=False)),
            patch.object(bot, 'MOD_FILE_PATH', str(Path(bot.__file__).with_name('Debris-1.21.8-integrated.jar'))),
            patch.dict(os.environ, {'DELIVERY_CACHE_DIR': str(Path(self.tmp.name) / 'cache'),
                       'LICENSE_PUBLIC_URL': DEFAULT_LICENSE_URL, 'LICENSE_ALLOW_LOCAL_HTTP': '0'})]
        for item in self.patches: item.start()
        self.telegram = SimpleNamespace(send_document=AsyncMock(), send_message=AsyncMock())

    async def asyncTearDown(self):
        await self.engine.dispose()
        for item in reversed(self.patches): item.stop()
        self.tmp.cleanup()

    async def payment(self):
        order = await self.bot.create_order(user_id=10, username=None, payment_method='stars', amount=145,
            currency='XTR', promo_code=None, status='INVOICE', duration_seconds=120)
        message = SimpleNamespace(successful_payment=SimpleNamespace(invoice_payload=f'order:{order.id}',
            currency='XTR', total_amount=145, telegram_payment_charge_id=f'fake-charge-{order.id}'),
            from_user=SimpleNamespace(id=10), answer=AsyncMock())
        return order, message

    async def test_concurrent_payment_callbacks_deliver_cloudflare_id_and_https_jar_once(self):
        from zipfile import ZipFile
        order, message = await self.payment()
        await asyncio.gather(*(self.bot.successful_payment(message, self.telegram) for _ in range(5)))
        self.telegram.send_document.assert_awaited_once()
        args = self.telegram.send_document.await_args
        self.assertIn(self.cloud.get(10)['debris_id'], args.kwargs['caption'])
        with ZipFile(args.args[1].path) as jar:
            props = jar.read('debris-license-defaults.properties').decode()
            self.assertIn('api_url=' + DEFAULT_LICENSE_URL, props)
            self.assertIn('allow_local_http=false', props)
        self.assertEqual((await self.bot.get_order(order.id)).status, 'DELIVERED')
        self.assertEqual(self.cloud.get(10)['expires_at'], self.now + 120)

    async def test_paid_callback_survives_outage_and_retry_delivers(self):
        order, message = await self.payment()
        self.cloud.offline = True
        await self.bot.successful_payment(message, self.telegram)
        self.telegram.send_document.assert_not_awaited()
        self.assertEqual((await self.bot.get_order(order.id)).status, 'PAID')
        self.cloud.offline = False
        self.now += 21
        self.assertTrue((await self.bot.deliver_order(self.telegram, order.id))[0])
        self.telegram.send_document.assert_awaited_once()

    async def test_telegram_failure_retries_delivery_without_renewal(self):
        order, message = await self.payment()
        self.telegram.send_document.side_effect = OSError('simulated send failure')
        await self.bot.successful_payment(message, self.telegram)
        target = self.cloud.get(10)['expires_at']
        self.telegram.send_document.side_effect = None
        self.now += 21
        self.assertTrue((await self.bot.deliver_order(self.telegram, order.id))[0])
        self.assertEqual(self.cloud.get(10)['expires_at'], target)


if __name__ == '__main__':
    unittest.main()
