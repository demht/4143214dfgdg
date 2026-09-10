import asyncio
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
import os
from pathlib import Path
import sqlite3
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

# Imports never read the operator's environment file or contact Telegram.
os.environ['PYTHON_DOTENV_DISABLED']='1'
os.environ['BOT_TOKEN']=''
os.environ['DB_PATH']=':memory:'
os.environ['LICENSE_PUBLIC_URL']='http://127.0.0.1:8081'
os.environ['LICENSE_ALLOW_LOCAL_HTTP']='1'
import bot
from subscription_store import SubscriptionStore, epoch, stamp
from license_api import create_app
from jar_delivery import prepare_jar, endpoint_from_env
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from aiohttp.test_utils import TestServer, TestClient
from aiogram.exceptions import TelegramRetryAfter
from aiogram.methods import SendDocument

JAR=Path(__file__).resolve().parents[1]/'Debris-1.21.8-integrated.jar'


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.now=1_800_000_000.0
        self.store=SubscriptionStore(Path(self.temp.name)/'bot.db',clock=lambda:self.now)
        self.store.migrate()
    def tearDown(self):
        self.temp.cleanup()
    def order(self, oid=1, user=10, provider='stars', seconds=30*86400):
        with self.store.transaction() as c:
            c.execute('''INSERT INTO orders(id,user_id,payment_method,amount,currency,status,created_at,duration_seconds)
                VALUES (?,?,?,?,?,'INVOICE',?,?)''',(oid,user,provider,145,'XTR' if provider=='stars' else 'RUB',stamp(self.now),seconds))
    def pay(self,oid=1):
        return self.store.confirm(oid,'stars',f'charge-{oid}',145,'XTR',10)
    def test_one_grant_for_concurrent_duplicates_and_old_order_replay(self):
        self.order()
        with ThreadPoolExecutor(max_workers=8) as pool:
            rows=list(pool.map(lambda _:self.pay(),range(16)))
        self.assertEqual(len({r['license_id'] for r in rows}),1)
        self.assertEqual(epoch(rows[0]['expires_at']),self.now+30*86400)
        self.order(2)
        row=self.pay(2)
        self.pay(1)
        self.assertEqual(self.store.get(10)['license_id'],rows[0]['license_id'])
        self.assertEqual(epoch(self.store.get(10)['expires_at']),self.now+60*86400)
    def test_rejects_unpaid_and_foreign_or_reused_payment(self):
        self.order()
        with self.assertRaises(ValueError): self.store.claim_delivery(1)
        for args in [('platega','x',145,'XTR',10),('stars','x',144,'XTR',10),('stars','x',145,'RUB',10),('stars','x',145,'XTR',99)]:
            with self.assertRaises(ValueError):self.store.confirm(1,*args)
        self.assertIsNone(self.store.get(10))
        self.pay()
        self.order(2)
        with self.assertRaises(ValueError): self.store.confirm(2,'stars','charge-1',145,'XTR',10)
        with self.assertRaises(ValueError): self.store.confirm(1,'stars','other-charge',145,'XTR',10)
    def test_expiration_binding_reset_extension_and_block(self):
        self.order(); row=self.pay();id=row['license_id']; device='a'*64
        result=self.store.authorize('activate',id,device);token=result['binding_token']
        self.assertEqual(result['status'],'ACTIVE')
        self.assertIsNone(self.store.get(10)['binding_token'])
        self.assertNotEqual(self.store.get(10)['binding_token_hash'],token)
        self.assertEqual(self.store.authorize('activate',id,'b'*64)['status'],'DEVICE_MISMATCH')
        self.now+=30*86400
        self.assertEqual(self.store.authorize('check',id,device,token)['status'],'EXPIRED')
        self.store.edit(10,'extend',120)
        self.assertEqual(self.store.authorize('check',id,device,token)['status'],'ACTIVE')
        self.store.edit(10,'toggle')
        self.order(2);self.pay(2)
        self.assertEqual(self.store.authorize('check',id,device,token)['status'],'BLOCKED')
        self.store.edit(10,'toggle');self.store.edit(10,'reset')
        self.assertEqual(self.store.authorize('check',id,device,token)['status'],'DEVICE_RESET')
        self.assertEqual(self.store.authorize('check',id,'b'*64)['status'],'REAUTH_REQUIRED')
        self.assertEqual(self.store.authorize('activate',id,'b'*64)['status'],'ACTIVE')
    def test_delivery_claim_crash_retry_never_extends(self):
        self.order();row=self.pay()
        first=self.store.claim_delivery(1)
        with self.assertRaises(ValueError):self.store.claim_delivery(1)
        self.now+=121
        fresh=SubscriptionStore(self.store.path,clock=lambda:self.now)
        fresh.migrate()
        second=fresh.claim_delivery(1)
        fresh.finish_delivery(1,first[2])
        self.assertIsNotNone(fresh.claim_delivery if second else None)
        fresh.finish_delivery(1,second[2])
        self.assertIsNone(fresh.claim_delivery(1))
        self.assertEqual(fresh.get(10)['expires_at'],row['expires_at'])
    def test_chargeback_of_earlier_order_blocks_and_cannot_be_replayed(self):
        self.order();self.pay();self.order(2);self.pay(2)
        self.assertTrue(self.store.cancel_payment(1,True))
        self.assertFalse(self.store.cancel_payment(1,True))
        self.assertFalse(self.store.eligible(10))
        with self.assertRaises(ValueError):self.pay()
    def test_admin_reject_cannot_overwrite_paid_order(self):
        self.order();self.pay()
        with self.assertRaises(ValueError):self.store.reject_manual(1)
        self.assertTrue(self.store.eligible(10))
        self.assertIn(1,self.store.pending_deliveries())
    def test_history_migration_preserves_id_expiry_orders_and_backup(self):
        path=Path(self.temp.name)/'legacy.db'
        with closing(sqlite3.connect(path)) as c:
            c.executescript(Path(bot.__file__).with_name('schema.sql').read_text(encoding='utf-8'))
            for oid in (1,2):
                c.execute("INSERT INTO orders(id,user_id,payment_method,amount,currency,status,created_at,duration_seconds) VALUES (?,10,'stars',145,'XTR','DELIVERED',?,2592000)",(oid,stamp(self.now-60*86400)))
            c.execute("INSERT INTO licenses(user_id,source_order_id,purchased_at,active,license_id,expires_at,binding_token,device_hash) VALUES (10,2,?,1,'DBR-AAAA-BBBB-CCCC',?,'old-secret','old-device')",(stamp(self.now-60*86400),stamp(self.now-86400)))
            c.commit()
        migrated=SubscriptionStore(path,clock=lambda:self.now);migrated.migrate();migrated.migrate()
        row=migrated.get(10)
        self.assertEqual(row['license_id'],'DBR-AAAA-BBBB-CCCC')
        self.assertEqual(epoch(row['expires_at']),self.now-86400)
        self.assertIsNone(row['binding_token'])
        self.assertEqual(len(list(path.parent.glob('legacy.db.pre-integration-*.bak'))),1)
        with closing(migrated.connect()) as c:
            self.assertEqual(c.execute('SELECT COUNT(*) FROM purchase_grants').fetchone()[0],2)
            self.assertEqual(c.execute('PRAGMA integrity_check').fetchone()[0],'ok')


class FakeBot:
    def __init__(self):
        self.documents=[];self.messages=[];self.hook=None
    async def send_document(self,user_id,document,**kwargs):
        if self.hook: await self.hook(user_id)
        self.documents.append((user_id,document,kwargs))
    async def send_message(self,user_id,text,**kwargs):self.messages.append((user_id,text))


class BotTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.clock=[1_900_000_000.0]
        bot.store=SubscriptionStore(Path(self.temp.name)/'bot.db',clock=lambda:self.clock[0])
        bot.store.migrate()
        bot.engine=create_async_engine(f'sqlite+aiosqlite:///{bot.store.path}')
        bot.Session=async_sessionmaker(bot.engine,expire_on_commit=False)
        bot.MOD_FILE_PATH=str(JAR)
        self.env=patch.dict(os.environ,{'DELIVERY_CACHE_DIR':str(Path(self.temp.name)/'cache')})
        self.env.start()
        self.fake=FakeBot()
    async def asyncTearDown(self):
        await bot.engine.dispose(); self.env.stop();self.temp.cleanup()
    async def order(self,user=10,provider='stars',duration=2592000):
        return await bot.create_order(user_id=user,username=None,payment_method=provider,amount=145,
            currency='XTR' if provider=='stars' else 'RUB',promo_code=None,status='INVOICE',duration_seconds=duration)
    async def paid(self,user=10):
        o=await self.order(user)
        bot.store.confirm(o.id,'stars',f'charge-{o.id}',145,'XTR',user)
        return o
    async def test_actual_successful_payment_handler_sends_jar_and_id_together_once(self):
        o=await self.order()
        payment=SimpleNamespace(invoice_payload=f'order:{o.id}',currency='XTR',total_amount=145,telegram_payment_charge_id='paid-real-shape')
        answers=[]
        async def answer(text):answers.append(text)
        message=SimpleNamespace(successful_payment=payment,from_user=SimpleNamespace(id=10),answer=answer)
        await asyncio.gather(*(bot.successful_payment(message,self.fake) for _ in range(5)))
        self.assertEqual(len(self.fake.documents),1)
        row=bot.store.get(10)
        self.assertIn(row['license_id'],self.fake.documents[0][2]['caption'])
        self.assertEqual(epoch(row['expires_at']),self.clock[0]+2592000)
        self.assertEqual((await bot.get_order(o.id)).status,'DELIVERED')
        self.assertTrue(Path(self.fake.documents[0][1].path).is_file())
    async def test_all_three_tariffs_deliver_selected_duration_and_expire(self):
        for days in (30,90,180):
            with self.subTest(days=days):
                start=self.clock[0]
                order=await self.order(user=days,provider='platega',duration=days*86400)
                transaction=f'tariff-{days}'
                await bot.set_order_provider_transaction(order.id,transaction)
                order=await bot.get_order(order.id)
                proof=dict(id=transaction,status='CONFIRMED',paymentDetails=dict(amount=145,currency='RUB'))
                self.assertTrue((await bot.process_platega_status(self.fake,order,proof))[0])
                row=bot.store.get(days)
                self.assertEqual(epoch(row['expires_at']),start+days*86400)
                self.assertIn(f'{days} дн.',self.fake.documents[-1][2]['caption'])
                token=bot.store.authorize('activate',row['license_id'],'a'*64)['binding_token']
                self.clock[0]=start+days*86400-0.1
                self.assertEqual(bot.store.authorize('check',row['license_id'],'a'*64,token)['status'],'ACTIVE')
                self.clock[0]=start+days*86400
                self.assertEqual(bot.store.authorize('check',row['license_id'],'a'*64,token)['status'],'EXPIRED')
    async def test_failed_send_persists_paid_and_retry_after_restart_has_same_expiry(self):
        o=await self.paid(); before=bot.store.get(10)['expires_at']
        async def fail(_):raise OSError('test transport failure')
        self.fake.hook=fail
        self.assertFalse((await bot.deliver_order(self.fake,o.id))[0])
        self.assertEqual((await bot.get_order(o.id)).status,'PAID')
        self.clock[0]+=20
        bot.store=SubscriptionStore(bot.store.path,clock=lambda:self.clock[0]);bot.store.migrate()
        self.fake.hook=None
        self.assertIn(o.id,bot.store.pending_deliveries())
        self.assertTrue((await bot.deliver_order(self.fake,o.id))[0])
        self.assertEqual(bot.store.get(10)['expires_at'],before)
    async def test_platega_requires_matching_transaction_amount_currency(self):
        o=await self.order(provider='platega')
        await bot.set_order_provider_transaction(o.id,'txn-one');o=await bot.get_order(o.id)
        for data in [dict(id='txn-one',status='CONFIRMED'),dict(id='other',status='CONFIRMED',paymentDetails=dict(amount=145,currency='RUB')),
                     dict(id='txn-one',status='CONFIRMED',paymentDetails=dict(amount=144,currency='RUB'))]:
            self.assertFalse((await bot.process_platega_status(self.fake,o,data))[0])
        self.assertIsNone(bot.store.get(10))
        data=dict(id='txn-one',status='CONFIRMED',paymentDetails=dict(amount=145,currency='RUB'))
        await asyncio.gather(*(bot.process_platega_status(self.fake,o,data) for _ in range(3)))
        self.assertEqual(len(self.fake.documents),1)
        self.assertEqual(epoch(bot.store.get(10)['expires_at']),self.clock[0]+2592000)
    async def test_release_rechecks_after_telegram_delay_and_skips_expired_blocked(self):
        for user in (10,11,12,13,14): await self.paid(user)
        bot.store.edit(12,'set',1)
        bot.store.edit(13,'toggle')
        self.clock[0]+=2
        # The ORM uses real UTC for its initial recipient snapshot.
        with patch.object(bot,'utc_now',lambda:__import__('datetime').datetime.fromtimestamp(self.clock[0],__import__('datetime').timezone.utc)):
            release=await bot.create_release_record(version='test',file_path=str(JAR),file_name='test.jar',notes='',created_by=99)
            delayed=False
            async def hook(user):
                nonlocal delayed
                if user==10:
                    bot.store.edit(11,'toggle')  # blocked after snapshot
                if user==14 and not delayed:
                    delayed=True
                    bot.store.edit(14,'set',1);self.clock[0]+=2
                    raise TelegramRetryAfter(method=SendDocument(chat_id=user,document='fake'),message='test',retry_after=0)
            self.fake.hook=hook
            await asyncio.gather(bot.broadcast_release(self.fake,release.id,99),bot.broadcast_release(self.fake,release.id,99))
            await bot.broadcast_release(self.fake,release.id,99)
        self.assertEqual([d[0] for d in self.fake.documents],[10])
        async with bot.Session() as s:
            r=await s.get(bot.Release,release.id)
            self.assertEqual(r.sent_count,1)
            self.assertEqual(r.failed_count,0)
    async def test_http_contract_invalid_requests_reset_and_health(self):
        await self.paid()
        client=TestClient(TestServer(create_app(bot.store)))
        await client.start_server()
        try:
            self.assertEqual((await client.get('/api/v1/health')).status,200)
            payload=dict(debris_id=bot.store.get(10)['license_id'],device_id='a'*64)
            response=await client.post('/api/v1/license/activate',json=payload)
            self.assertEqual(response.status,200)
            self.assertEqual(response.headers['Cache-Control'],'no-store')
            data=await response.json();self.assertEqual(data['status'],'ACTIVE')
            self.assertIsInstance(data['expires_at'],float)
            self.assertEqual((await client.post('/api/v1/license/check',json={})).status,400)
            self.assertEqual((await client.post('/api/v1/license/check',json=[])).status,400)
            bot.store.edit(10,'reset')
            data=await (await client.post('/api/v1/license/check',json={**payload,'binding_token':data['binding_token']})).json()
            self.assertEqual(data['status'],'DEVICE_RESET')
        finally:await client.close()
    async def test_webhook_verifies_provider_get_and_never_trusts_posted_status(self):
        o=await self.order(provider='platega')
        await bot.set_order_provider_transaction(o.id,'webhook-txn')
        from aiohttp import web
        app=web.Application();app['bot']=self.fake
        app.router.add_post('/platega/paymentStatus',bot.platega_webhook)
        client=TestClient(TestServer(app));await client.start_server()
        async def pending(_):
            return dict(id='webhook-txn',status='PENDING',paymentDetails=dict(amount=145,currency='RUB'))
        try:
            with patch.object(bot,'PLATEGA_MERCHANT_ID','test-merchant'),patch.object(bot,'PLATEGA_SECRET','test-secret'),patch.object(bot,'get_platega_status',pending):
                headers={'X-MerchantId':'test-merchant','X-Secret':'test-secret'}
                self.assertEqual((await client.post('/platega/paymentStatus',json={'id':'webhook-txn'})).status,401)
                self.assertEqual((await client.post('/platega/paymentStatus',json=[],headers=headers)).status,400)
                self.assertEqual((await client.post('/platega/paymentStatus',json={'id':'webhook-txn','status':'CONFIRMED'},headers=headers)).status,200)
                self.assertIsNone(bot.store.get(10))
        finally:await client.close()
    async def test_resumed_broadcast_does_not_repeat_sent_rows(self):
        await self.paid(10);await self.paid(11)
        release=await bot.create_release_record(version='resume',file_path=str(JAR),file_name='test.jar',notes='',created_by=99)
        async with bot.Session() as s:
            r=await s.get(bot.Release,release.id);r.status='SENDING';r.total_count=2
            s.add(bot.ReleaseDelivery(release_id=r.id,user_id=10,status='SENT'))
            s.add(bot.ReleaseDelivery(release_id=r.id,user_id=11,status='PENDING'))
            await s.commit()
        await bot.broadcast_release(self.fake,release.id,99)
        self.assertEqual([d[0] for d in self.fake.documents],[11])
        async with bot.Session() as s:
            self.assertEqual((await s.get(bot.Release,release.id)).sent_count,2)
    async def test_long_release_caption_fits_telegram_limit(self):
        import html,re
        caption=bot.release_caption('x'*64,'😀<&'*1000)
        plain=html.unescape(re.sub('<[^>]*>','',caption))
        self.assertLessEqual(len(plain.encode('utf-16-le'))//2,1024)
    async def test_stars_refund_revokes_only_matching_record(self):
        order=await self.paid()
        payment=SimpleNamespace(invoice_payload=f'order:{order.id}',currency='XTR',total_amount=145,telegram_payment_charge_id='wrong')
        message=SimpleNamespace(refunded_payment=payment)
        await bot.refunded_payment(message)
        self.assertTrue(bot.store.eligible(10))
        payment.telegram_payment_charge_id=f'charge-{order.id}'
        await bot.refunded_payment(message)
        self.assertFalse(bot.store.eligible(10))
        self.assertEqual((await bot.get_order(order.id)).status,'CHARGEBACK')
    async def test_jar_includes_public_endpoint_and_rejects_http_external(self):
        from zipfile import ZipFile
        with patch.dict(os.environ,{'LICENSE_PUBLIC_URL':'https://licenses.example.org'}):
            target=prepare_jar(JAR)
            with ZipFile(target) as z:self.assertIn(b'https://licenses.example.org',z.read('debris-license-defaults.properties'))
        with patch.dict(os.environ,{'LICENSE_PUBLIC_URL':'http://licenses.example.org'}):
            with self.assertRaises(ValueError):endpoint_from_env()


if __name__=='__main__':unittest.main()
