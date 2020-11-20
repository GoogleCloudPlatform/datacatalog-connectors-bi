#!/usr/bin/python
#
# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import logging
import sys

from google.datacatalog_connectors.qlik import sync


class Qlik2DataCatalogCli:
    __DEFAULT_DATACATALOG_LOCATION_ID = 'us'
    __DEFAULT_QLIK_DOMAIN = '.'

    @classmethod
    def run(cls, argv):
        cls.__setup_logging()

        args = cls._parse_args(argv)
        args.func(args)

    @classmethod
    def __setup_logging(cls):
        logging.basicConfig(level=logging.INFO)

    @classmethod
    def _parse_args(cls, argv):
        parser = argparse.ArgumentParser(
            description=__doc__,
            formatter_class=argparse.RawDescriptionHelpFormatter)

        parser.add_argument('--qlik-server', help='Qlik server', required=True)
        parser.add_argument('--qlik-ad-domain',
                            help='Qlik Active Directory domain',
                            default=cls.__DEFAULT_QLIK_DOMAIN)
        parser.add_argument('--qlik-username',
                            help='Qlik username',
                            required=True)
        parser.add_argument('--qlik-password',
                            help='Qlik password',
                            required=True)
        parser.add_argument('--datacatalog-project-id',
                            help='Google Cloud Project ID',
                            required=True)
        parser.add_argument('--datacatalog-location-id',
                            help='Location ID to be used Google Data Catalog'
                            'to store the metadata',
                            default=cls.__DEFAULT_DATACATALOG_LOCATION_ID)

        parser.set_defaults(func=cls.__run_synchronizer)

        return parser.parse_args(argv)

    @classmethod
    def __run_synchronizer(cls, args):
        sync.MetadataSynchronizer(
            qlik_server_address=args.qlik_server,
            qlik_ad_domain=args.qlik_ad_domain,
            qlik_username=args.qlik_username,
            qlik_password=args.qlik_password,
            datacatalog_project_id=args.datacatalog_project_id,
            datacatalog_location_id=args.datacatalog_location_id).run()


def main():
    argv = sys.argv
    Qlik2DataCatalogCli.run(argv[1:] if len(argv) > 0 else argv)
