"""Insert the public license endpoint; never insert server secrets into a JAR."""
import hashlib
import os
from pathlib import Path
import tempfile
from urllib.parse import urlsplit
from zipfile import ZipFile, ZIP_DEFLATED

RESOURCE = 'debris-license-defaults.properties'


def endpoint_from_env():
    endpoint = os.getenv('LICENSE_PUBLIC_URL','https://debris-api.steamdemhr.workers.dev').strip().rstrip('/')
    allow_local = os.getenv('LICENSE_ALLOW_LOCAL_HTTP','0') == '1'
    parsed = urlsplit(endpoint)
    local = parsed.hostname in {'localhost','127.0.0.1','::1'}
    if (not endpoint.isascii() or any(c.isspace() for c in endpoint) or '\\' in endpoint or
        not parsed.hostname or parsed.username is not None or parsed.password is not None or
        parsed.query or parsed.fragment or
        not (parsed.scheme=='https' or (parsed.scheme=='http' and local and allow_local))):
        raise ValueError('Set LICENSE_PUBLIC_URL to HTTPS; local testing requires LICENSE_ALLOW_LOCAL_HTTP=1.')
    _ = parsed.port  # Reject malformed/out-of-range ports.
    return endpoint, allow_local and local and parsed.scheme=='http'


def inspect_jar(path):
    with ZipFile(path) as jar:
        if len(jar.namelist()) != len(set(jar.namelist())):
            raise ValueError('Duplicate entries in JAR')
        code = jar.read('x/q/license/LicenseClient.class')
        if RESOURCE.encode('ascii') not in code or 'x/q/license/LicenseGate.class' not in jar.namelist():
            raise ValueError('This JAR does not support the integrated license API. Use the supplied integrated JAR.')
        if jar.testzip() is not None:
            raise ValueError('Damaged JAR')


def prepare_jar(source, cache_dir=None):
    endpoint, local = endpoint_from_env()
    source = Path(source).resolve()
    contents = source.read_bytes()
    key = hashlib.sha256(contents + endpoint.encode('ascii') + str(local).encode('ascii')).hexdigest()
    cache = Path(cache_dir or os.getenv('DELIVERY_CACHE_DIR','delivery-cache')).resolve()
    cache.mkdir(parents=True,exist_ok=True)
    target = cache / (key + '.jar')
    if target.is_file():
        inspect_jar(target)
        return target
    inspect_jar(source)
    handle, temp_name = tempfile.mkstemp(prefix='jar-',suffix='.tmp',dir=cache)
    os.close(handle)
    temp = Path(temp_name)
    try:
        with ZipFile(source) as original, ZipFile(temp,'w',compression=ZIP_DEFLATED) as output:
            for entry in original.infolist():
                if entry.filename != RESOURCE:
                    output.writestr(entry,original.read(entry))
            output.writestr(RESOURCE,f'api_url={endpoint}\nallow_local_http={str(local).lower()}\n')
        temp.replace(target)
    finally:
        temp.unlink(missing_ok=True)
    return target
