#!/usr/bin/env python3

import hashlib
import hmac
import os
import sys

secret = os.environ.get("SECRET", "")
sync_dir = os.environ.get("SYNC_DIR")
user = os.environ.get("GEOSERVER_USERNAME")
workspace = os.environ.get("GEOSERVER_WORKSPACE")
# NB GEOSERVER_PASSWORD should be in the env too
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
    cmd = f"geoserver-sync --user {user} --workspace {workspace} --pattern '*.gpkg'"
    if os.system(cmd) != 0:
        raise Exception(f"Command '{cmd}' exited with non-zero code")
