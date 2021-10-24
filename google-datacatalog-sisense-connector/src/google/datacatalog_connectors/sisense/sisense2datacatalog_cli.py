#!/usr/bin/python
#
# Copyright 2021 Google LLC
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

from google.datacatalog_connectors.sisense import addons, sync


class Sisense2DataCatalogCli:

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

        subparsers = parser.add_subparsers()
        _SyncCatalogCliHelper().add_parser(subparsers)
        _FindElastiCubeDepsCliHelper().add_parser(subparsers)
        _ListElastiCubeDepsCliHelper().add_parser(subparsers)

        return parser.parse_args(argv)


class _SyncCatalogCliHelper:
    __DEFAULT_DATACATALOG_LOCATION_ID = 'us'

    @classmethod
    def add_parser(cls, subparsers):
        parser = subparsers.add_parser(
            'sync-catalog',
            help='Synchronize Data Catalog with a Sisense server')

        parser.add_argument('--sisense-server',
                            help='Sisense Server address',
                            required=True)

        parser.add_argument('--sisense-username',
                            help='Sisense username',
                            required=True)

        parser.add_argument('--sisense-password',
                            help='Sisense password',
                            required=True)

        parser.add_argument('--datacatalog-project-id',
                            help='ID of the Google Cloud Project where the'
                            ' Data Catalog entries, tag templates, and tags'
                            ' created by the connector are stored',
                            required=True)

        parser.add_argument('--datacatalog-location-id',
                            help='ID of the Location to be used by Google Data'
                            ' Catalog to store the metadata',
                            default=cls.__DEFAULT_DATACATALOG_LOCATION_ID)

        parser.set_defaults(func=cls.__sync_catalog)

    @classmethod
    def __sync_catalog(cls, args):
        synchronizer = sync.MetadataSynchronizer(
            sisense_server_address=args.sisense_server,
            sisense_username=args.sisense_username,
            sisense_password=args.sisense_password,
            datacatalog_project_id=args.datacatalog_project_id,
            datacatalog_location_id=cls.__DEFAULT_DATACATALOG_LOCATION_ID)
        synchronizer.run()


class _FindElastiCubeDepsCliHelper:

    @classmethod
    def add_parser(cls, subparsers):
        parser = subparsers.add_parser(
            'find-elasticube-deps',
            help='Find ElastiCube dependencies through catalog search')

        parser.add_argument('--datasource',
                            help='An ElastiCube Data Source name')

        parser.add_argument('--table', help='An ElastiCube Table name')

        parser.add_argument('--column', help='An ElastiCube Column name')

        parser.add_argument('--datacatalog-project-id',
                            help='ID of the Google Cloud Project in which the'
                            ' catalog search will be scoped',
                            required=True)

        parser.set_defaults(func=cls.__find_elasticube_deps)

    @classmethod
    def __find_elasticube_deps(cls, args):
        try:
            dependencies = addons.TagBasedFinder(
                args.datacatalog_project_id).find(args.datasource, args.table,
                                                  args.column)

            addons.ElastiCubeDependencyPrinter.print_dependency_finder_results(
                dependencies)
        except Exception as e:
            raise SystemExit(e)


class _ListElastiCubeDepsCliHelper:

    @classmethod
    def add_parser(cls, subparsers):
        parser = subparsers.add_parser(
            'list-elasticube-deps',
            help='List all ElastiCube dependencies for a given Sisense asset')

        parser.add_argument('--asset-url',
                            help='An ElastiCube Dashboard or Widget url',
                            required=True)

        parser.add_argument('--datacatalog-project-id',
                            help='ID of the Google Cloud Project in which the'
                            ' catalog search will be scoped',
                            required=True)

        parser.set_defaults(func=cls.__list_elasticube_deps)

    @classmethod
    def __list_elasticube_deps(cls, args):
        try:
            dependencies = addons.LinkedResourceBasedFinder(
                args.datacatalog_project_id).find(args.asset_url)

            addons.ElastiCubeDependencyPrinter.print_dependency_finder_results(
                dependencies)
        except Exception as e:
            raise SystemExit(e)


def main():
    argv = sys.argv
    Sisense2DataCatalogCli.run(argv[1:] if len(argv) > 0 else argv)
