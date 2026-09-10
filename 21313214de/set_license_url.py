"""Set a public URL on a separate JAR copy (server .env is not edited)."""
import argparse
import os
from pathlib import Path
import shutil
import tempfile
from jar_delivery import prepare_jar

parser=argparse.ArgumentParser()
parser.add_argument('input',type=Path)
parser.add_argument('output',type=Path)
parser.add_argument('url')
parser.add_argument('--local-http',action='store_true')
args=parser.parse_args()
if args.input.resolve()==args.output.resolve() or args.output.exists():
    raise SystemExit('Output must be a new path; the input JAR is preserved.')
os.environ['LICENSE_PUBLIC_URL']=args.url
os.environ['LICENSE_ALLOW_LOCAL_HTTP']='1' if args.local_http else '0'
with tempfile.TemporaryDirectory() as tmp:
    ready=prepare_jar(args.input,tmp)
    args.output.parent.mkdir(parents=True,exist_ok=True)
    shutil.copyfile(ready,args.output)
print('Public API URL embedded in the new JAR.')
