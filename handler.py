#!/usr/bin/env python3

import glob
import hashlib
import hmac
import os
import sys


secret = os.environ.get("SECRET", "")
sync_dir = os.environ.get("SYNC_DIR")
if sync_dir is None or not os.path.isdir(sync_dir):
    raise Exception("Missing or invalid SYNC_DIR in environment")

data = sys.stdin.read()
check = os.environ.get("HTTP_X_HUB_SIGNATURE_256")
signature = "sha256=" + hmac.new(str.encode(secret), data.encode('utf-8'), digestmod=hashlib.sha256).hexdigest()
if signature != check:
    print("Content-type: text/plain")
    print("Status: 400 Bad Request", end="\r\n\r\n", flush=True)
    print("Unexpected payload")
else:
    print("Content-type: text/plain", end="\r\n\r\n", flush=True)
    os.chdir(sync_dir)
    os.system("git pull")
    files = " ".join(glob.glob("*.gpkg"))
    cmd = "geoserver-sync --user admin --workspace ehri " + files
    if os.system(cmd) != 0:
        raise Exception(f"Command '{cmd}' exited with non-zero code")