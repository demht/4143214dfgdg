"""Single SQLite authority for purchases, subscriptions and client authorization."""
import hashlib
import hmac
import re
import secrets
import sqlite3
import time
from contextlib import contextmanager, closing
from datetime import datetime, timezone
from pathlib import Path


def stamp(value):
    return datetime.fromtimestamp(value, timezone.utc).strftime('%Y-%m-%d %H:%M:%S.%f')


def epoch(value):
    if not value:
        return 0.0
    parsed = datetime.fromisoformat(value)
    return parsed.replace(tzinfo=timezone.utc).timestamp() if parsed.tzinfo is None else parsed.timestamp()


def digest(value):
    return hashlib.sha256(value.encode('ascii')).hexdigest()


class SubscriptionStore:
    def __init__(self, db_path, *, clock=time.time, check_interval=5, lease_seconds=15):
        self.path = Path(db_path).resolve()
        self.clock = clock
        self.check_interval = int(check_interval)
        self.lease_seconds = int(lease_seconds)
        if not 1 <= self.check_interval <= 300 or not self.check_interval + 2 <= self.lease_seconds <= 600:
            raise ValueError('Invalid license interval/lease')

    def connect(self):
        conn = sqlite3.connect(self.path, timeout=15, isolation_level=None)
        conn.row_factory = sqlite3.Row
        conn.execute('PRAGMA foreign_keys=ON')
        return conn

    @contextmanager
    def transaction(self):
        with closing(self.connect()) as conn:
            conn.execute('BEGIN IMMEDIATE')
            try:
                yield conn
                conn.commit()
            except BaseException:
                conn.rollback()
                raise

    def migrate(self):
        self.path.parent.mkdir(parents=True, exist_ok=True)
        with self.transaction() as conn:
            tables = {r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")}
            already_migrated = 'subscription_schema' in tables
            if already_migrated:
                version = conn.execute('SELECT version FROM subscription_schema').fetchone()
                if not version or version[0] != 1:
                    raise RuntimeError('Unsupported database schema')
            if tables and not already_migrated:
                backup = self.path.with_name(self.path.name + '.pre-integration-' + secrets.token_hex(6) + '.bak')
                with closing(self.connect()) as source, closing(sqlite3.connect(backup)) as target:
                    source.backup(target)
                    if target.execute('PRAGMA integrity_check').fetchone()[0] != 'ok':
                        raise RuntimeError('Database backup failed')
            # Add missing legacy columns BEFORE creating any index on them.
            statements = [sql.strip() for sql in Path(__file__).with_name('schema.sql').read_text(encoding='utf-8').split(';') if sql.strip()]
            for sql in statements:
                if sql.startswith('CREATE TABLE '):
                    conn.execute(sql.replace('CREATE TABLE ', 'CREATE TABLE IF NOT EXISTS ', 1))
            additions = {
                'orders': [('duration_seconds', 'INTEGER'), ('promo_code', 'TEXT'),
                           ('telegram_payment_charge_id', 'TEXT'), ('provider_payment_charge_id', 'TEXT'),
                           ('paid_at', 'DATETIME'), ('delivered_at', 'DATETIME')],
                'licenses': [('license_id', 'TEXT'), ('expires_at', 'DATETIME'),
                             ('device_hash', 'TEXT'), ('activated_at', 'DATETIME'),
                             ('last_seen_at', 'DATETIME'), ('binding_token', 'TEXT'),
                             ('last_version_sent', 'TEXT'), ('last_release_sent_at', 'DATETIME'),
                             ('binding_token_hash', 'TEXT'), ('binding_version', 'INTEGER NOT NULL DEFAULT 0')],
            }
            for table, fields in additions.items():
                columns = {r[1] for r in conn.execute(f'PRAGMA table_info({table})')}
                for name, kind in fields:
                    if name not in columns:
                        conn.execute(f'ALTER TABLE {table} ADD COLUMN {name} {kind}')
            for sql in statements:
                if 'INDEX ' in sql:
                    conn.execute(sql.replace('INDEX ', 'INDEX IF NOT EXISTS ', 1))
            if already_migrated:
                return
            conn.execute('CREATE UNIQUE INDEX IF NOT EXISTS unique_license_id ON licenses(license_id)')
            conn.execute('''CREATE TABLE purchase_grants (
                order_id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, seconds INTEGER NOT NULL,
                granted_at TEXT NOT NULL, expires_at TEXT, historical INTEGER NOT NULL DEFAULT 0,
                revoked INTEGER NOT NULL DEFAULT 0)''')
            conn.execute('''CREATE TABLE payment_events (
                provider TEXT NOT NULL, charge_id TEXT NOT NULL, order_id INTEGER NOT NULL,
                PRIMARY KEY(provider,charge_id), UNIQUE(provider,order_id))''')
            conn.execute('''CREATE TABLE delivery_jobs (
                order_id INTEGER PRIMARY KEY, claim TEXT, lease_until REAL NOT NULL DEFAULT 0,
                attempts INTEGER NOT NULL DEFAULT 0, retry_at REAL NOT NULL DEFAULT 0, error TEXT)''')
            conn.execute('''CREATE TABLE broadcast_claims (
                release_id INTEGER PRIMARY KEY, owner TEXT NOT NULL, lease_until REAL NOT NULL)''')
            conn.execute('''CREATE TABLE subscription_audit (
                id INTEGER PRIMARY KEY, user_id INTEGER, action TEXT, at TEXT)''')
            # V4 used a different device fingerprint and plaintext session tokens.
            # Invalidate those sessions once; ID and purchased expiry stay intact.
            conn.execute('UPDATE licenses SET binding_token=NULL,device_hash=NULL,activated_at=NULL,last_seen_at=NULL')
            preserved_users = {r[0] for r in conn.execute('SELECT user_id FROM licenses')}
            for row in conn.execute('SELECT * FROM licenses').fetchall():
                conn.execute('UPDATE licenses SET license_id=?,expires_at=? WHERE user_id=?',
                             (row['license_id'] or self._new_id(conn), row['expires_at'] or
                              stamp(epoch(row['purchased_at']) + 30*86400), row['user_id']))
            # Never replay historical deliveries when migrating or restarting.
            for order in conn.execute("SELECT * FROM orders WHERE status='DELIVERED' ORDER BY id").fetchall():
                row = conn.execute('SELECT * FROM licenses WHERE user_id=?', (order['user_id'],)).fetchone()
                if order['user_id'] not in preserved_users:
                    self._apply(conn, order, historical=True)
                else:
                    conn.execute('INSERT INTO purchase_grants VALUES (?,?,?,?,?,1,0)',
                                 (order['id'],order['user_id'],order['duration_seconds'] or 30*86400,
                                  order['paid_at'] or order['created_at'],row['expires_at']))
            for order in conn.execute('SELECT * FROM orders').fetchall():
                charge = order['telegram_payment_charge_id'] if order['payment_method'] == 'stars' else order['provider_payment_charge_id']
                if charge:
                    conn.execute('INSERT INTO payment_events VALUES (?,?,?)', (order['payment_method'],charge,order['id']))
            conn.execute('CREATE TABLE subscription_schema (version INTEGER NOT NULL)')
            conn.execute('INSERT INTO subscription_schema VALUES (1)')

    def _new_id(self, conn):
        alphabet = 'ABCDEFGHJKLMNPQRSTUVWXYZ23456789'
        while True:
            value = 'DBR-' + '-'.join(''.join(secrets.choice(alphabet) for _ in range(4)) for _ in range(3))
            if not conn.execute('SELECT 1 FROM licenses WHERE license_id=?', (value,)).fetchone():
                return value

    def _apply(self, conn, order, historical=False):
        if conn.execute('SELECT 1 FROM purchase_grants WHERE order_id=?', (order['id'],)).fetchone():
            return
        seconds = order['duration_seconds'] or 30*86400
        if not 1 <= seconds <= 10*366*86400:
            raise ValueError('Invalid purchased duration')
        now = epoch(order['paid_at'] or order['created_at']) if historical else self.clock()
        row = conn.execute('SELECT * FROM licenses WHERE user_id=?', (order['user_id'],)).fetchone()
        end = max(now, epoch(row['expires_at']) if row else 0) + seconds
        if row:
            conn.execute('UPDATE licenses SET expires_at=?,username=?,source_order_id=? WHERE user_id=?',
                         (stamp(end),order['username'],order['id'],order['user_id']))
        else:
            conn.execute('''INSERT INTO licenses (user_id,username,source_order_id,purchased_at,active,license_id,expires_at)
                VALUES (?,?,?,?,1,?,?)''', (order['user_id'],order['username'],order['id'],stamp(now),self._new_id(conn),stamp(end)))
        conn.execute('INSERT INTO purchase_grants VALUES (?,?,?,?,?,?,0)',
                     (order['id'],order['user_id'],seconds,stamp(now),stamp(end),int(historical)))

    def confirm(self, order_id, provider, charge_id, amount, currency, user_id=None):
        if not isinstance(charge_id, str) or not 1 <= len(charge_id) <= 256:
            raise ValueError('Missing payment confirmation ID')
        with self.transaction() as conn:
            order = conn.execute('SELECT * FROM orders WHERE id=?', (order_id,)).fetchone()
            if not order or order['payment_method'] != provider or (user_id is not None and order['user_id'] != user_id):
                raise ValueError('Payment/order mismatch')
            if amount != order['amount'] or currency != order['currency']:
                raise ValueError('Payment amount/currency mismatch')
            if order['status'] not in {'INVOICE','PAID','DELIVERED'}:
                raise ValueError('Order is not payable')
            column = 'telegram_payment_charge_id' if provider == 'stars' else 'provider_payment_charge_id'
            if order[column] and order[column] != charge_id:
                raise ValueError('Payment transaction mismatch')
            old = conn.execute('SELECT order_id FROM payment_events WHERE provider=? AND charge_id=?', (provider,charge_id)).fetchone()
            if old and old[0] != order_id:
                raise ValueError('Payment already belongs to another order')
            conn.execute('INSERT OR IGNORE INTO payment_events VALUES (?,?,?)', (provider,charge_id,order_id))
            conn.execute(f'UPDATE orders SET {column}=?,paid_at=COALESCE(paid_at,?),status=? WHERE id=?',
                         (charge_id,stamp(self.clock()),'DELIVERED' if order['status']=='DELIVERED' else 'PAID',order_id))
            self._apply(conn, conn.execute('SELECT * FROM orders WHERE id=?',(order_id,)).fetchone())
            return self.get(order['user_id'], conn=conn)

    def claim_delivery(self, order_id, expected_user_id=None, manual=False):
        with self.transaction() as conn:
            order = conn.execute('SELECT * FROM orders WHERE id=?',(order_id,)).fetchone()
            if not order or (expected_user_id is not None and order['user_id'] != expected_user_id):
                raise ValueError('Заказ не найден или принадлежит другому пользователю.')
            if order['status'] == 'DELIVERED':
                return None
            if manual and order['status']=='WAITING' and order['payment_method'] not in {'stars','platega'}:
                conn.execute("UPDATE orders SET status='PAID',paid_at=? WHERE id=?",(stamp(self.clock()),order_id))
                order = conn.execute('SELECT * FROM orders WHERE id=?',(order_id,)).fetchone()
            if order['status'] != 'PAID':
                raise ValueError('Оплата ещё не подтверждена.')
            self._apply(conn, order)
            conn.execute('INSERT OR IGNORE INTO delivery_jobs(order_id) VALUES (?)',(order_id,))
            job = conn.execute('SELECT * FROM delivery_jobs WHERE order_id=?',(order_id,)).fetchone()
            now = self.clock()
            if job['lease_until'] > now or job['retry_at'] > now:
                raise ValueError('Выдача уже выполняется или ожидает повторной попытки.')
            claim = secrets.token_hex(16)
            conn.execute('UPDATE delivery_jobs SET claim=?,lease_until=?,attempts=attempts+1 WHERE order_id=?',
                         (claim,now+120,order_id))
            return dict(order), self.get(order['user_id'], conn=conn), claim

    def finish_delivery(self, order_id, claim, error=None, retry_after=0):
        with self.transaction() as conn:
            job = conn.execute('SELECT * FROM delivery_jobs WHERE order_id=? AND claim=?',(order_id,claim)).fetchone()
            if not job:
                return
            if error is None:
                conn.execute("UPDATE orders SET status='DELIVERED',delivered_at=? WHERE id=? AND status='PAID'",(stamp(self.clock()),order_id))
            wait = max(retry_after, min(3600, 5 * 2**min(job['attempts'],10))) if error else 0
            conn.execute('UPDATE delivery_jobs SET claim=NULL,lease_until=0,error=?,retry_at=? WHERE order_id=?',
                         (error,self.clock()+wait,order_id))

    def pending_deliveries(self):
        with closing(self.connect()) as conn:
            return [r[0] for r in conn.execute("""SELECT o.id FROM orders o LEFT JOIN delivery_jobs j ON j.order_id=o.id
                WHERE o.status='PAID' AND COALESCE(j.lease_until,0)<=? AND COALESCE(j.retry_at,0)<=? ORDER BY o.id LIMIT 100""",
                (self.clock(),self.clock()))]

    def get(self, user_id, conn=None):
        if conn is None:
            with closing(self.connect()) as connection:
                return self.get(user_id, conn=connection)
        row = conn.execute('SELECT * FROM licenses WHERE user_id=?',(user_id,)).fetchone()
        return dict(row) if row else None

    def eligible(self, user_id):
        row = self.get(user_id)
        return bool(row and row['active'] and epoch(row['expires_at']) > self.clock())

    def edit(self, user_id, action, seconds=0):
        with self.transaction() as conn:
            row = self.get(user_id, conn=conn)
            if row is None:
                raise ValueError('Лицензия не найдена.')
            if action in {'set','extend'}:
                if not 1 <= seconds <= 10*366*86400:
                    raise ValueError('Invalid duration')
                base = max(self.clock(),epoch(row['expires_at'])) if action=='extend' else self.clock()
                conn.execute('UPDATE licenses SET expires_at=? WHERE user_id=?',(stamp(base+seconds),user_id))
            elif action == 'reset':
                conn.execute('''UPDATE licenses SET device_hash=NULL,binding_token=NULL,binding_token_hash=NULL,
                    activated_at=NULL,last_seen_at=NULL,binding_version=binding_version+1 WHERE user_id=?''',(user_id,))
            elif action == 'toggle':
                conn.execute('UPDATE licenses SET active=NOT active WHERE user_id=?',(user_id,))
            else:
                raise ValueError('Invalid action')
            conn.execute('INSERT INTO subscription_audit(user_id,action,at) VALUES (?,?,?)',(user_id,action,stamp(self.clock())))

    def cancel_payment(self, order_id, chargeback=False):
        with self.transaction() as conn:
            order = conn.execute('SELECT * FROM orders WHERE id=?',(order_id,)).fetchone()
            if not order:
                return False
            if chargeback:
                if order['status']=='CHARGEBACK':
                    return False
                conn.execute("UPDATE orders SET status='CHARGEBACK' WHERE id=?",(order_id,))
                conn.execute('UPDATE purchase_grants SET revoked=1 WHERE order_id=?',(order_id,))
                # A disputed payment requires admin review even after a later renewal.
                if conn.execute('SELECT 1 FROM purchase_grants WHERE order_id=?',(order_id,)).fetchone():
                    conn.execute('UPDATE licenses SET active=0 WHERE user_id=?',(order['user_id'],))
                return True
            conn.execute("UPDATE orders SET status='CANCELED' WHERE id=? AND status IN ('WAITING','INVOICE')",(order_id,))
            return False

    def reject_manual(self, order_id):
        with self.transaction() as conn:
            order = conn.execute('SELECT * FROM orders WHERE id=?',(order_id,)).fetchone()
            if not order or order['status'] != 'WAITING':
                raise ValueError('Можно отклонить только ручной заказ, ожидающий проверки.')
            if order['payment_method'] in {'stars','platega'}:
                raise ValueError('Онлайн-платёж отменяется через платёжную систему.')
            conn.execute("UPDATE orders SET status='REJECTED' WHERE id=?",(order_id,))
            return order['user_id']

    def broadcast_claim(self, release_id, owner):
        with self.transaction() as conn:
            row = conn.execute('SELECT * FROM broadcast_claims WHERE release_id=?',(release_id,)).fetchone()
            if row and row['owner'] != owner and row['lease_until'] > self.clock():
                return False
            conn.execute('INSERT OR REPLACE INTO broadcast_claims VALUES (?,?,?)',(release_id,owner,self.clock()+120))
            return True

    def broadcast_release_claim(self, release_id, owner):
        with self.transaction() as conn:
            conn.execute('DELETE FROM broadcast_claims WHERE release_id=? AND owner=?',(release_id,owner))

    def health(self):
        with closing(self.connect()) as conn:
            return conn.execute('SELECT version FROM subscription_schema').fetchone()[0] == 1

    def authorize(self, action, debris_id, device_id, binding_token=None):
        if not isinstance(debris_id,str) or len(debris_id)>32 or not debris_id.isascii():
            raise ValueError('Invalid ID')
        debris_id = debris_id.strip().upper()
        if not re.fullmatch(r'DBR-[A-Z2-9]{4}-[A-Z2-9]{4}-[A-Z2-9]{4}',debris_id):
            raise ValueError('Invalid ID')
        if action not in {'activate','check'} or not isinstance(device_id,str) or not re.fullmatch('[a-f0-9]{64}',device_id):
            raise ValueError('Invalid request')
        if binding_token is not None and (not isinstance(binding_token,str) or not re.fullmatch('[A-Za-z0-9_-]{43}',binding_token)):
            raise ValueError('Invalid token')
        with self.transaction() as conn:
            row = conn.execute('SELECT * FROM licenses WHERE license_id=?',(debris_id,)).fetchone()
            now = float(self.clock())
            def result(code, **extra):
                return dict(status=code,code=code,server_time=now,**extra)
            if not row:
                return result('INVALID_LICENSE')
            if action=='check':
                if not binding_token:
                    return result('REAUTH_REQUIRED')
                if not row['binding_token_hash'] or not hmac.compare_digest(row['binding_token_hash'],digest(binding_token)):
                    return result('DEVICE_RESET')
            if row['device_hash'] and not hmac.compare_digest(row['device_hash'],digest(device_id)):
                return result('DEVICE_MISMATCH')
            if not row['active']:
                return result('BLOCKED')
            end = epoch(row['expires_at'])
            if end <= now:
                return result('EXPIRED')
            extra = {}
            if action=='activate':
                token = secrets.token_urlsafe(32)
                conn.execute('''UPDATE licenses SET device_hash=?,binding_token_hash=?,binding_token=NULL,
                    binding_version=binding_version+1,activated_at=? WHERE user_id=?''',
                    (digest(device_id),digest(token),stamp(now),row['user_id']))
                extra['binding_token'] = token
            conn.execute('UPDATE licenses SET last_seen_at=? WHERE user_id=?',(stamp(now),row['user_id']))
            return result('ACTIVE',expires_at=end,lease_seconds=min(self.lease_seconds,end-now),
                          check_interval_seconds=self.check_interval,**extra)
