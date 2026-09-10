"""Optional hosting console for the same Cloudflare licenses used by the bot."""
import argparse
import json
import os
from pathlib import Path
from dotenv import load_dotenv
from cloudflare_store import CloudflareStore
from license_service import parse_duration
from preflight import SingleInstance


def main():
    root = Path(__file__).resolve().parent
    load_dotenv(root / '.env')
    os.chdir(root)
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest='command', required=True)
    sub.add_parser('migrate')
    for name in ['info', 'create', 'extend', 'set', 'block', 'unblock', 'reset']:
        command = sub.add_parser(name)
        command.add_argument('user_id', type=int)
        if name in {'create', 'extend', 'set'}:
            command.add_argument('duration', type=parse_duration)
    args = parser.parse_args()
    service = CloudflareStore(os.getenv('DB_PATH', 'data/bot.db'))
    with SingleInstance(service.path):
        service.migrate()
        if args.command == 'migrate':
            print('Migration complete')
            return
        if args.command == 'create':
            result = service.issue(args.user_id, args.duration)
        elif args.command in {'extend', 'set', 'reset'}:
            service.edit(args.user_id, args.command, getattr(args, 'duration', 0))
            result = service.refresh(args.user_id)
        elif args.command in {'block', 'unblock'}:
            result = service._cache(service.client.set_blocked(args.user_id, args.command == 'block'))
        else:
            result = service.refresh(args.user_id)
        print(json.dumps(result, ensure_ascii=False, indent=2))


if __name__ == '__main__':
    main()
