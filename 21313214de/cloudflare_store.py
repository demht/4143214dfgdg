"""Cloudflare owns licenses; SQLite holds orders, retries and a display cache."""
from contextlib import closing
import threading

from cloudflare_license_client import CloudflareLicenseClient, CloudflareLicenseError
from subscription_store import SubscriptionStore, stamp, epoch


class CloudflareStore(SubscriptionStore):
    def __init__(self, *args, client=None, **kwargs):
        super().__init__(*args, **kwargs)
        self.client = client or CloudflareLicenseClient()
        self._cloud_lock = threading.RLock()

    def migrate(self):
        super().migrate()
        with self.transaction() as conn:
            first_cloud_migration = not conn.execute("SELECT 1 FROM sqlite_master WHERE name='cloudflare_grants'").fetchone()
            conn.execute('''CREATE TABLE IF NOT EXISTS cloudflare_grants (
                order_id INTEGER PRIMARY KEY, action TEXT NOT NULL, target_expiry INTEGER,
                done INTEGER NOT NULL DEFAULT 0)''')
            conn.execute('CREATE TABLE IF NOT EXISTS cloudflare_known_users (user_id INTEGER PRIMARY KEY)')
            conn.execute('''CREATE TABLE IF NOT EXISTS cloudflare_revocations (
                order_id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL, done INTEGER NOT NULL DEFAULT 0)''')
            if first_cloud_migration:
                # These grants already contributed to a legacy expiry imported by refresh().
                conn.execute('''INSERT OR IGNORE INTO cloudflare_grants(order_id,action,done)
                    SELECT order_id,'legacy',1 FROM purchase_grants WHERE expires_at IS NOT NULL''')

    def _apply(self, conn, order, historical=False):
        if historical:
            return super()._apply(conn, order, historical=True)
        if conn.execute('SELECT 1 FROM purchase_grants WHERE order_id=?', (order['id'],)).fetchone():
            return
        seconds = order['duration_seconds'] or 30 * 86400
        if not 1 <= seconds <= 10 * 366 * 86400:
            raise ValueError('Invalid purchased duration')
        # Payment is committed even if Cloudflare is unavailable. No local DBR is issued.
        conn.execute('INSERT INTO purchase_grants VALUES (?,?,?,?,NULL,0,0)',
                     (order['id'], order['user_id'], seconds, stamp(self.clock())))

    def _cache(self, remote):
        user = int(remote['user_id'])
        # The old menu model requires an expiry; its lifetime display is handled separately.
        end = stamp(253402214400) if remote.get('legacy_lifetime') else stamp(remote['expires_at'])
        with self.transaction() as conn:
            old = self.get(user, conn=conn)
            if not old:
                order = conn.execute('SELECT id,username FROM orders WHERE user_id=? ORDER BY id DESC LIMIT 1', (user,)).fetchone()
                conn.execute('''INSERT INTO licenses(user_id,username,source_order_id,purchased_at,active)
                    VALUES (?,?,?,?,?)''', (user, order['username'] if order else None,
                    order['id'] if order else 0, stamp(remote.get('created_at') or self.clock()),
                    remote['status'] != 'BLOCKED'))
            conn.execute('''UPDATE licenses SET license_id=?,expires_at=?,active=?,device_hash=?,
                activated_at=?,last_seen_at=?,binding_token=NULL,binding_token_hash=NULL,binding_version=?
                WHERE user_id=?''', (remote['debris_id'], end, remote['status'] != 'BLOCKED',
                'cloudflare-bound' if remote.get('device_bound') else None,
                stamp(remote['activated_at']) if remote.get('activated_at') else None,
                stamp(remote['last_check_at']) if remote.get('last_check_at') else None,
                remote.get('binding_version', 0), user))
            conn.execute('INSERT OR IGNORE INTO cloudflare_known_users VALUES (?)', (user,))
        return self.get(user)

    def refresh(self, user_id):
        with self._cloud_lock:
            remote = self.client.get(user_id)
            local = self.get(user_id)
            with closing(self.connect()) as conn:
                known = conn.execute('SELECT 1 FROM cloudflare_known_users WHERE user_id=?', (user_id,)).fetchone()
            if remote is None:
                # Import a pre-Cloudflare subscription once, preserving its DBR and remaining time.
                if local and not known and epoch(local['expires_at']) > self.clock():
                    remote = self.client.create(user_id, max(1, int(epoch(local['expires_at']) - self.clock())), local['license_id'])
                else:
                    if local:
                        with self.transaction() as conn:
                            conn.execute('UPDATE licenses SET active=0 WHERE user_id=?', (user_id,))
                    return None
            # Retry the block even when create succeeded but its response was lost.
            if local and not known and not local['active']:
                remote = self.client.set_blocked(user_id, True)
            return self._cache(remote)

    def _sync_grant(self, order_id):
        with closing(self.connect()) as conn:
            grant = conn.execute('SELECT * FROM purchase_grants WHERE order_id=?', (order_id,)).fetchone()
            plan = conn.execute('SELECT * FROM cloudflare_grants WHERE order_id=?', (order_id,)).fetchone()
        if not grant or grant['historical'] or grant['revoked'] or (plan and plan['done']):
            return
        user = grant['user_id']
        if plan is None:
            self.refresh(user)
            remote = self.client.get(user)
            if remote is None:
                action, target = 'create', None
            elif remote.get('legacy_lifetime'):
                action, target = 'lifetime', None
            else:
                action, target = 'set', max(int(self.clock()), int(remote['expires_at'])) + grant['seconds']
            # Freeze the absolute target before mutation: a lost HTTP response cannot double a renewal.
            with self.transaction() as conn:
                conn.execute('INSERT INTO cloudflare_grants VALUES (?,?,?,0)', (order_id, action, target))
            plan = {'action': action, 'target_expiry': target}
        if plan['action'] == 'create':
            # Worker create is idempotent on user_id and returns the existing license on a retry.
            remote = self.client.create(user, grant['seconds'])
        elif plan['action'] == 'lifetime':
            remote = self.client.get(user)
        else:
            remote = self.client.get(user)
            target = plan['target_expiry']
            if remote is None:
                raise CloudflareLicenseError('license_not_found')
            if not remote.get('legacy_lifetime') and remote['expires_at'] < target - 1:
                remaining = target - int(self.clock())
                if remaining <= 0:
                    raise CloudflareLicenseError('grant_needs_admin_review')
                remote = self.client.set_duration(user, remaining)
        if remote is None:
            raise CloudflareLicenseError('license_not_found')
        self._cache(remote)
        with self.transaction() as conn:
            conn.execute('UPDATE cloudflare_grants SET done=1 WHERE order_id=?', (order_id,))
            conn.execute('UPDATE purchase_grants SET expires_at=? WHERE order_id=?',
                         (stamp(remote['expires_at']) if remote.get('expires_at') else None, order_id))

    def claim_delivery(self, order_id, expected_user_id=None, manual=False):
        with self._cloud_lock:
            claimed = super().claim_delivery(order_id, expected_user_id, manual)
            if claimed is None:
                return None
            order, _, claim = claimed
            try:
                with closing(self.connect()) as conn:
                    pending = [r[0] for r in conn.execute('''SELECT g.order_id FROM purchase_grants g
                        JOIN orders o ON o.id=g.order_id WHERE g.user_id=? AND g.historical=0
                        AND g.revoked=0 AND o.status IN ('PAID','DELIVERED') ORDER BY g.order_id''', (order['user_id'],))]
                for oid in pending:
                    self._sync_grant(oid)
                row = self.refresh(order['user_id'])
                if row is None:
                    raise CloudflareLicenseError('license_not_found')
                return order, row, claim
            except Exception as exc:
                self.finish_delivery(order_id, claim, type(exc).__name__)
                raise

    def eligible(self, user_id):
        row = self.refresh(user_id)
        return bool(row and row['active'] and epoch(row['expires_at']) > self.clock())

    def edit(self, user_id, action, seconds=0):
        with self._cloud_lock:
            remote = self.client.get(user_id)
            if remote is None:
                self.refresh(user_id)
                remote = self.client.get(user_id)
            if remote is None:
                raise ValueError('Лицензия не найдена.')
            if action == 'extend':
                remote = self.client.extend(user_id, seconds)
            elif action == 'set':
                remote = self.client.set_duration(user_id, seconds)
            elif action == 'reset':
                remote = self.client.reset_device(user_id)
            elif action == 'toggle':
                remote = self.client.set_blocked(user_id, remote['status'] != 'BLOCKED')
            else:
                raise ValueError('Invalid action')
            self._cache(remote)
            with self.transaction() as conn:
                conn.execute('INSERT INTO subscription_audit(user_id,action,at) VALUES (?,?,?)', (user_id, action, stamp(self.clock())))

    def issue(self, user_id, seconds):
        with self._cloud_lock:
            row = self.client.create(user_id, seconds)
            return self._cache(row)

    def cancel_payment(self, order_id, chargeback=False):
        with self._cloud_lock:
            # Persist the block job before the local refund, so crashes cannot lose it.
            if chargeback:
                with self.transaction() as conn:
                    row = conn.execute('''SELECT o.user_id FROM orders o JOIN purchase_grants g ON o.id=g.order_id
                        WHERE o.id=?''', (order_id,)).fetchone()
                    if row:
                        conn.execute('INSERT OR IGNORE INTO cloudflare_revocations VALUES (?,?,0)', (order_id, row[0]))
            changed = super().cancel_payment(order_id, chargeback)
            if chargeback:
                self.sync_revocations()
            return changed

    def sync_revocations(self):
        with self._cloud_lock:
            with closing(self.connect()) as conn:
                jobs = conn.execute('SELECT * FROM cloudflare_revocations WHERE done=0').fetchall()
            for job in jobs:
                remote = self.client.get(job['user_id'])
                if remote:
                    self._cache(self.client.set_blocked(job['user_id'], True))
                with self.transaction() as conn:
                    conn.execute('UPDATE cloudflare_revocations SET done=1 WHERE order_id=?', (job['order_id'],))

    def authorize(self, *args, **kwargs):
        raise RuntimeError('Clients must use the Cloudflare Worker directly.')
