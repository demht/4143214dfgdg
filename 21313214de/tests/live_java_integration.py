"""Exercise the shipped JAR against a real local API after a simulated payment.

TEST_PROBE_CLASSPATH must contain ClientProbe.class and Gson (not LicenseClient).
No real payment, Telegram message or Minecraft launch occurs.
"""
import asyncio
import hashlib
import json
import os
from pathlib import Path
import tempfile
import time
from aiohttp import web

os.environ['PYTHON_DOTENV_DISABLED']='1'
os.environ['BOT_TOKEN']=''
os.environ['DB_PATH']=':memory:'
import bot
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker
from subscription_store import SubscriptionStore, epoch
from license_api import create_app
from jar_delivery import prepare_jar


async def main():
    processes=[]
    runner=None
    with tempfile.TemporaryDirectory() as tmp:
        root=Path(tmp)
        bot.store=SubscriptionStore(root/'bot.db',check_interval=1,lease_seconds=3)
        bot.store.migrate()
        bot.engine=create_async_engine(f'sqlite+aiosqlite:///{bot.store.path}')
        bot.Session=async_sessionmaker(bot.engine,expire_on_commit=False)
        runner=web.AppRunner(create_app(bot.store),access_log=None)
        await runner.setup()
        site=web.TCPSite(runner,'127.0.0.1',0);await site.start()
        port=site._server.sockets[0].getsockname()[1]
        os.environ['LICENSE_PUBLIC_URL']=f'http://127.0.0.1:{port}'
        os.environ['LICENSE_ALLOW_LOCAL_HTTP']='1'
        os.environ['DELIVERY_CACHE_DIR']=str(root/'cache')
        bot.MOD_FILE_PATH=str(Path(bot.__file__).with_name('Debris-1.21.8-integrated.jar'))
        class FakeTelegram:
            async def send_document(self,user,document,**kwargs):self.document=document;self.caption=kwargs['caption']
        fake=FakeTelegram()
        try:
            order=await bot.create_order(user_id=10,username=None,payment_method='stars',amount=145,currency='XTR',promo_code=None,status='INVOICE',duration_seconds=30*86400)
            start=time.time()
            license=bot.store.confirm(order.id,'stars','simulated-successful-payment',145,'XTR',10)
            assert abs(epoch(license['expires_at'])-start-30*86400)<2
            assert (await bot.deliver_order(fake,order.id))[0]
            assert license['license_id'] in fake.caption
            delivered=Path(fake.document.path)
            cp=str(delivered)+os.pathsep+os.environ['TEST_PROBE_CLASSPATH']
            async def launch(directory,machine):
                directory.mkdir(exist_ok=True)
                p=await asyncio.create_subprocess_exec('java','-cp',cp,'ClientProbe',str(directory),hashlib.sha256(machine.encode()).hexdigest(),
                    stdin=asyncio.subprocess.PIPE,stdout=asyncio.subprocess.PIPE,stderr=asyncio.subprocess.PIPE)
                processes.append(p);return p
            async def command(p,text='STATUS'):
                p.stdin.write((text+'\n').encode());await p.stdin.drain()
                line=await asyncio.wait_for(p.stdout.readline(),20)
                if not line:raise AssertionError((await p.stderr.read()).decode(errors='replace'))
                return json.loads(line)
            async def until(p,status,allowed,timeout=7):
                state=await command(p)  # wait for JVM startup before timing the HTTP check
                end=time.monotonic()+timeout
                while time.monotonic()<end:
                    if state['status'] in (status if isinstance(status,tuple) else (status,)) and state['allowed']==allowed:return
                    await asyncio.sleep(.1)
                    state=await command(p)
                raise AssertionError(f'Expected {status}/{allowed}: {state}')
            p=await launch(root/'client-one','machine-one')
            assert not (await command(p))['allowed']
            await command(p,'ACTIVATE '+license['license_id']);await until(p,'ACTIVE',True)
            print('PASS payment -> delivered JAR + ID -> embedded URL -> activation',flush=True)
            bot.store.edit(10,'set',2)
            await until(p,'EXPIRED',False)
            bot.store.edit(10,'extend',60);await until(p,'ACTIVE',True)
            print('PASS expiration -> blocked access -> extension without restart',flush=True)
            p2=await launch(root/'client-two','machine-two')
            await command(p2,'ACTIVATE '+license['license_id']);await until(p2,'DEVICE_MISMATCH',False)
            bot.store.edit(10,'reset');await until(p,'DEVICE_RESET',False)
            await command(p2,'ACTIVATE '+license['license_id']);await until(p2,'ACTIVE',True)
            bot.store.edit(10,'toggle');await until(p2,'BLOCKED',False)
            bot.store.edit(10,'toggle');await until(p2,'ACTIVE',True)
            print('PASS device binding, reset, block and unblock',flush=True)
            await runner.cleanup();runner=None
            await until(p2,('NETWORK_ERROR','LEASE_ENDED'),False)
            p2.stdin.write(b'CLOSE\n');await p2.stdin.drain();await p2.wait()
            restarted=await launch(root/'client-two','machine-two')
            await until(restarted,'NETWORK_ERROR',False)
            print('PASS network outage and restart never grant offline access',flush=True)
            # An old generated external config must not override the URL in an update.
            external=root/'client-one'/'debris-license-api.properties'
            external.write_text('api_url=https://retired.invalid\nallow_local_http=false\n',encoding='utf-8')
            runner=web.AppRunner(create_app(bot.store),access_log=None);await runner.setup()
            await web.TCPSite(runner,'127.0.0.1',port).start()
            bot.store.edit(10,'reset')
            updated=await launch(root/'client-one','machine-one')
            await command(updated,'ACTIVATE '+license['license_id']);await until(updated,'ACTIVE',True)
            print('PASS bundled endpoint wins over stale generated config',flush=True)
        finally:
            for p in processes:
                if p.returncode is None:
                    p.terminate()
                    await p.wait()
            if runner:await runner.cleanup()
            await bot.engine.dispose()


if __name__=='__main__':asyncio.run(main())
