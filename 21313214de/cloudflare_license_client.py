"""DBR-v1 admin client. Credentials are loaded lazily and never written to JARs."""
import json
import os
import re
import urllib.error
import urllib.request
from urllib.parse import urlsplit

DEFAULT_LICENSE_URL = 'https://debris-api.steamdemhr.workers.dev'
MAX_DURATION = 10 * 366 * 86400


class CloudflareLicenseError(RuntimeError):
    def __init__(self, code):
        self.code = code
        super().__init__(code)


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


class CloudflareLicenseClient:
    def __init__(self, base_url=None, admin_key=None):
        self.base_url = (base_url or os.getenv('LICENSE_PUBLIC_URL') or DEFAULT_LICENSE_URL).strip().rstrip('/')
        self.admin_key = admin_key
        self.opener = urllib.request.build_opener(NoRedirect)

    def validate_config(self):
        p = urlsplit(self.base_url)
        if (p.scheme != 'https' or not p.hostname or p.username is not None or p.password is not None
                or p.query or p.fragment or p.path not in ('', '/') or not self.base_url.isascii()
                or any(c.isspace() for c in self.base_url) or '\\' in self.base_url):
            raise ValueError('LICENSE_PUBLIC_URL must be an HTTPS origin.')
        _ = p.port
        key = self.admin_key if self.admin_key is not None else os.getenv('LICENSE_ADMIN_API_KEY', '').strip()
        if not key or not key.isascii() or any(c.isspace() for c in key):
            raise ValueError('Set LICENSE_ADMIN_API_KEY to the existing Cloudflare ADMIN_API_KEY.')
        return key

    def _request(self, path, payload=None):
        key = self.validate_config()
        headers = {'Accept': 'application/json', 'User-Agent': 'Debris-Bot/Cloudflare-1'}
        if payload is not None:
            headers.update({'Content-Type': 'application/json', 'X-Admin-Key': key})
        request = urllib.request.Request(self.base_url + path,
            data=None if payload is None else json.dumps(payload).encode('utf-8'), headers=headers)
        try:
            with self.opener.open(request, timeout=15) as response:
                data = json.loads(response.read(1024 * 1024).decode('utf-8'))
        except urllib.error.HTTPError as exc:
            # Only a short machine code is accepted; response text can contain secrets.
            try:
                code = json.loads(exc.read(4096)).get('error')
            except (ValueError, AttributeError):
                code = None
            finally:
                exc.close()
            if not isinstance(code, str) or not re.fullmatch('[a-z_]{1,64}', code):
                code = f'http_{exc.code}'
            raise CloudflareLicenseError(code) from None
        except (TimeoutError, urllib.error.URLError, OSError):
            raise CloudflareLicenseError('cloudflare_unavailable') from None
        except (ValueError, UnicodeError):
            raise CloudflareLicenseError('invalid_api_response') from None
        if not isinstance(data, dict) or data.get('ok') is not True:
            raise CloudflareLicenseError('invalid_api_response')
        return data

    def _license(self, action, user_id, **fields):
        user_id = int(user_id)
        if not 0 < user_id <= 9007199254740991:
            raise ValueError('Invalid Telegram user ID')
        if 'duration_seconds' in fields and not 1 <= int(fields['duration_seconds']) <= MAX_DURATION:
            raise ValueError('Duration must be between 1 second and 10 years')
        result = self._request('/api/admin/v1/licenses/' + action, {'user_id': user_id, **fields})
        row = result.get('license')
        if (not isinstance(row, dict) or row.get('user_id') != user_id
                or not re.fullmatch(r'DBR-[A-Z2-9]{4}-[A-Z2-9]{4}-[A-Z2-9]{4}', str(row.get('debris_id', '')))
                or row.get('status') not in {'ACTIVE', 'BLOCKED', 'EXPIRED'}
                or (not row.get('legacy_lifetime') and
                    (not isinstance(row.get('expires_at'), (int, float)) or row['expires_at'] < 0))):
            raise CloudflareLicenseError('invalid_api_response')
        return row

    def get(self, user_id):
        try:
            return self._license('get', user_id)
        except CloudflareLicenseError as exc:
            if exc.code == 'license_not_found':
                return None
            raise

    def create(self, user_id, duration_seconds, debris_id=None):
        extra = {'debris_id': debris_id} if debris_id else {}
        return self._license('create', user_id, duration_seconds=int(duration_seconds), **extra)

    def extend(self, user_id, duration_seconds):
        return self._license('extend', user_id, duration_seconds=int(duration_seconds))

    def set_duration(self, user_id, duration_seconds):
        return self._license('set-duration', user_id, duration_seconds=int(duration_seconds))

    def reset_device(self, user_id):
        return self._license('reset-device', user_id)

    def set_blocked(self, user_id, blocked):
        return self._license('set-blocked', user_id, blocked=bool(blocked))

    def health(self):
        result = self._request('/health')
        return result.get('database') is True and result.get('protocol') == 'DBR-v1'
