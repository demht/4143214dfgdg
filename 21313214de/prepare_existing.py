"""Import an existing V4 bot's settings and DB into this NEW folder.

Never modifies the source. Refuses to overwrite an existing destination .env/DB.
Run only after stopping the old bot to avoid two live purchase databases.
"""
import argparse
from contextlib import closing
from pathlib import Path
import shutil
import sqlite3


def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('source',type=Path)
    args=parser.parse_args()
    source=args.source.resolve()
    target=Path(__file__).resolve().parent
    if source==target or (target/'.env').exists() or (target/'bot.db').exists():
        raise SystemExit('Use a NEW folder with no .env or bot.db. Source is never overwritten.')
    from dotenv import dotenv_values
    settings=dotenv_values(source/'.env')
    database=Path(settings.get('DB_PATH') or 'bot.db')
    if not database.is_absolute():database=source/database
    if not (source/'.env').is_file() or not database.is_file():
        raise SystemExit('The source must contain .env and its configured database.')
    text=(source/'.env').read_text(encoding='utf-8-sig')
    overrides={
        'MOD_FILE_PATH':'Debris-1.21.8-integrated.jar',
        'DB_PATH':'bot.db',
        'START_IMAGE_PATH':'assets/debris_banner.png',
        'RELEASES_DIR':'releases',
        'LICENSE_API_ENABLED':'0',
        'LICENSE_API_HOST':'127.0.0.1',
        'LICENSE_API_PORT':'8081',
        'LICENSE_PUBLIC_URL':'https://debris-api.steamdemhr.workers.dev',
        'LICENSE_ALLOW_LOCAL_HTTP':'0',
        'LICENSE_CHECK_INTERVAL_SECONDS':'5',
        'LICENSE_LEASE_SECONDS':'15',
    }
    # Preserve payment credentials/tariffs/admin IDs verbatim; replace only public
    # integration settings. Duplicate assignments for replaced keys are removed.
    kept=[]
    for line in text.splitlines():
        key=line.partition('=')[0].strip().removeprefix('export ').strip()
        if key not in overrides:kept.append(line)
    text='\n'.join(kept)+'\n\n# Cloudflare license endpoint\n'
    text+='\n'.join(f'{key}={value}' for key,value in overrides.items())+'\n'
    with closing(sqlite3.connect(database.resolve().as_uri()+'?mode=ro',uri=True)) as old:
        with closing(sqlite3.connect(target/'bot.db')) as new:
            old.backup(new)
            if new.execute('PRAGMA integrity_check').fetchone()[0]!='ok':
                raise SystemExit('Source database integrity check failed.')
    # Preserve previous releases if present and rewrite their source-relative paths.
    release_dir=Path(settings.get('RELEASES_DIR') or 'releases')
    if not release_dir.is_absolute():release_dir=source/release_dir
    if release_dir.is_dir():shutil.copytree(release_dir,target/'releases',dirs_exist_ok=True)
    with closing(sqlite3.connect(target/'bot.db')) as conn:
        for rid,path in conn.execute('SELECT id,file_path FROM releases').fetchall():
            candidate=Path(path)
            if not candidate.is_absolute():candidate=source/candidate
            try:relative=candidate.resolve().relative_to(release_dir.resolve())
            except ValueError:continue
            conn.execute('UPDATE releases SET file_path=? WHERE id=?',(str(Path('releases')/relative),rid))
        conn.commit()
    (target/'.env').write_text(text,encoding='utf-8')
    print('Settings and database copied. Original folder unchanged. Set LICENSE_ADMIN_API_KEY, then run python bot.py --check.')


if __name__=='__main__':main()
