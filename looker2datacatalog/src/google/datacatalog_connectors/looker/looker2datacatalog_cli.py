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

from google.datacatalog_connectors.looker import sync


class Looker2DataCatalogCli:
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

        parser.add_argument('--datacatalog-project-id',
                            help='Google Cloud Project ID',
                            required=True)
        parser.add_argument('--looker-credentials-file',
                            help='Looker credentials file',
                            required=True)

        parser.set_defaults(func=cls.__run_synchronizer)

        return parser.parse_args(argv)

    @classmethod
    def __run_synchronizer(cls, args):
        sync.MetadataSynchronizer(
            datacatalog_project_id=args.datacatalog_project_id,
            datacatalog_location_id=cls.__DATACATALOG_LOCATION_ID,
            looker_credentials_file=args.looker_credentials_file).run()


def main():
    argv = sys.argv
    Looker2DataCatalogCli.run(argv[1:] if len(argv) > 0 else argv)
