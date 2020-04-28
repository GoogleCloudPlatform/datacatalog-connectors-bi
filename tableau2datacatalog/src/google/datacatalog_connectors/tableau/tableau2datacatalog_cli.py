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

from google.datacatalog_connectors.tableau import sync


class Tableau2DataCatalogCli:
    __DATACATALOG_LOCATION_ID = 'us-central1'

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

        parser.add_argument('--tableau-server',
                            help='Tableau Server address',
                            required=True)
        parser.add_argument('--tableau-api-version',
                            help='Tableau API version',
                            required=True)
        parser.add_argument('--tableau-username',
                            help='Tableau username',
                            required=True)
        parser.add_argument('--tableau-password',
                            help='Tableau password',
                            required=True)
        parser.add_argument('--tableau-site', help='Tableau site')
        parser.add_argument('--datacatalog-project-id',
                            help='Google Cloud Project ID',
                            required=True)

        parser.set_defaults(func=cls.__run_synchronizer)

        return parser.parse_args(argv)

    @classmethod
    def __run_synchronizer(cls, args):
        sync.DataCatalogSynchronizer(
            tableau_server_address=args.tableau_server,
            tableau_api_version=args.tableau_api_version,
            tableau_username=args.tableau_username,
            tableau_password=args.tableau_password,
            tableau_site=args.tableau_site,
            datacatalog_project_id=args.datacatalog_project_id,
            datacatalog_location_id=cls.__DATACATALOG_LOCATION_ID).run()


def main():
    argv = sys.argv
    Tableau2DataCatalogCli.run(argv[1:] if len(argv) > 0 else argv)
