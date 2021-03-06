#!/usr/bin/env python3

#
# Import GeoPackage files into a Geoserver instance
#

import argparse
from configparser import ConfigParser
import glob
import os
import sys

import requests
import json
import logging
import sqlite3
from http import HTTPStatus

logger = logging.getLogger(__name__)


class ImportException(Exception):
    pass


class Importer:
    def __init__(self, config, files):
        self.config = config
        self.files = files

        self.session = requests.Session()
        self.session.auth = (config.get('user'), config.get('pass'))
        self.session.headers.update({"content-type": "application/json", "accept": "application/json"})
        self.base_url = f"http://{config.get('host')}:{config.get('port')}/geoserver/rest/workspaces/{config.get('workspace')}"

    def sync(self) -> None:
        if not self.config.get('pattern') and not self.files:
            raise ImportException("No files or search pattern supplied")

        if not os.path.isdir(self.config.get('dir')):
            raise ImportException(f"Invalid directory supplied: {self.config.get('dir')}")

        files = self.files
        if not files:
            for d, _, _ in os.walk(os.path.abspath(self.config.get('dir'))):
                files.extend(glob.glob(os.path.join(d, self.config.get('pattern'))))
        for filepath in files:
            self.sync_file(filepath)

    def sync_file(self, filepath: str) -> None:
        print(f"Import file: {filepath}")
        layers = []
        conn = sqlite3.connect(filepath)
        try:
            cursor = conn.cursor()
            for row in cursor.execute("SELECT * FROM gpkg_contents"):
                logger.debug(f"Row data: {row}")
                layers.append(row)
        except sqlite3.DatabaseError:
            raise ImportException("Error reading GeoPackage (is it the right format?)")
        finally:
            conn.close()

        if not layers:
            raise ImportException(f"Can't find any layers in GeoPackage {filepath}")

        store_id, created = self.create_or_update_datastore(filepath)
        print(f"Store exists for {filepath}: {created}")
        for layerdata in layers:
            created = self.create_or_update_layer(store_id, layerdata)
            print(f"Layer exists for {filepath}: {created}")

    def create_or_update_datastore(self, filepath: str) -> (str, bool):
        identifier, _ = os.path.splitext(os.path.basename(filepath))
        list_url = f"{self.base_url}/datastores"
        r = self.session.get(list_url)

        if r.status_code == HTTPStatus.NOT_FOUND:
            raise ImportException(f"Cannot list workspace datastores. Does workspace '{self.config.get('workspace')}' exist?")
        stores = []
        try:
            stores = [s["name"] for s in r.json()["dataStores"]["dataStore"]]
        except (AttributeError, TypeError):
            # no stores
            pass

        url = list_url
        method = "POST"
        if identifier in stores:
            url = f"{self.base_url}/datastores/{identifier}"
            method = "PUT"

        data = dict(
            dataStore=dict(
                name=identifier,
                enabled=True,
                connectionParameters=dict(
                    entry=[
                        {"@key": "database", "$": f"file://{os.path.abspath(filepath)}"},
                        {"@key": "dbtype", "$": "geopkg"}
                    ]
                )
            )
        )
        logger.debug(f"{method}ing {url} store {identifier}: {data}")
        r = self.session.request(method, url, data=json.dumps(data))
        r.raise_for_status()

        return identifier, identifier in stores

    def create_or_update_layer(self, store: str, layerdata):
        identifier, dtype, title, description, *_ = layerdata
        list_url = f"{self.base_url}/datastores/{store}/featuretypes"
        r = self.session.get(list_url)
        layers = []
        try:
            layers = [data["name"] for data in r.json().get("featureTypes", {}).get("featureType", [])]
        except AttributeError:
            # no layers
            pass

        url = list_url
        method = "POST"
        if identifier in layers:
            url = f"{self.base_url}/datastores/{store}/featuretypes/{identifier}"
            method = "PUT"

        data = dict(
            featureType=dict(
                name=identifier,
                nativeName=identifier,
                namespace=dict(
                    name=self.config.get('workspace'),
                    href=f"{self.base_url}.json"
                ),
                title=title,
                description=description,
                keywords=dict(string=[dtype])
            )
        )

        logger.debug(f"{method}ing {url} layer {identifier}: {data}")
        r = self.session.request(method, url, data=json.dumps(data))
        if r.status_code not in [HTTPStatus.OK, HTTPStatus.CREATED]:
            raise ImportException(f"Unexpected import status code: {r.status_code} ({method} {url})")
        return identifier in layers


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        usage="%(prog)s [OPTION] [FILE]...",
        description="""Import GeoPackage layers into a local Geoserver instance.\n
        The password for Geoserver must be given via a GEOSERVER_PASSWORD environment variable.
        The -c|--config option accepts the path to an ini-style config file containing a [sync] section."""
    )
    parser.add_argument("--host", dest="host", default="localhost", help="Geoserver host")
    parser.add_argument("-u", "--user", dest="user", default="admin", help="Geoserver user")
    parser.add_argument("-p", "--port", dest="port", default=8080, help="Geoserver port")
    parser.add_argument("-w", "--workspace", dest="workspace", default="TEST", help="Geoserver workspace")
    parser.add_argument("-c", "--config", dest="config", help="optional config file")
    parser.add_argument("-d", "--dir", dest="dir", default=os.getcwd(), help="directory under which to apply pattern")
    parser.add_argument("--pattern", dest="pattern", default="", help="File pattern to find from dir (or CWD)")
    parser.add_argument("--debug", dest="debug", action="store_true", help="Show debug info")
    parser.add_argument('files', nargs='*', help="One or more GeoPackage files")

    args = parser.parse_args()

    config = ConfigParser(defaults={
        'workspace': args.workspace,
        'host': args.host,
        'user': args.user,
        'dir': args.dir,
        'pattern': args.pattern,
        'port': str(args.port),
        'pass': os.environ.get("GEOSERVER_PASSWORD", "")
    })
    config.add_section('sync')

    if args.config:
        config.read(args.config)

    logging.basicConfig(level=logging.WARNING)
    if args.debug:
        logger.setLevel(logging.DEBUG)

    importer = Importer(config['sync'], args.files)
    try:
        importer.sync()
    except ImportException as e:
        print(str(e), file=sys.stderr)
        sys.exit(1)
