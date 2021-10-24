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

import unittest
from unittest import mock

from google.datacatalog_connectors import sisense
from google.datacatalog_connectors.sisense import sisense2datacatalog_cli


class Sisense2DataCatalogCliTest(unittest.TestCase):

    def test_parse_args_sync_catalog_should_require_sisense_server(self):
        self.assertRaises(
            SystemExit,
            sisense2datacatalog_cli.Sisense2DataCatalogCli._parse_args, [
                'sync-catalog', '--sisense-username', 'test-username',
                '--sisense-password', 'test-password',
                '--datacatalog-project-id', 'dc-project-id'
            ])

    def test_parse_args_sync_catalog_should_require_sisense_username(self):
        self.assertRaises(
            SystemExit,
            sisense2datacatalog_cli.Sisense2DataCatalogCli._parse_args, [
                'sync-catalog', '--sisense-server', 'test-server',
                '--sisense-password', 'test-password',
                '--datacatalog-project-id', 'dc-project-id'
            ])

    def test_parse_args_sync_catalog_should_require_sisense_password(self):
        self.assertRaises(
            SystemExit,
            sisense2datacatalog_cli.Sisense2DataCatalogCli._parse_args, [
                'sync-catalog', '--sisense-server', 'test-server',
                '--sisense-username', 'test-username',
                '--datacatalog-project-id', 'dc-project-id'
            ])

    def test_parse_args_sync_catalog_should_require_project_id(self):
        self.assertRaises(
            SystemExit,
            sisense2datacatalog_cli.Sisense2DataCatalogCli._parse_args, [
                'sync-catalog', '--sisense-server', 'test-server',
                '--sisense-username', 'test-username', '--sisense-password',
                'test-password'
            ])

    @mock.patch('google.datacatalog_connectors.sisense.sisense2datacatalog_cli'
                '.sync.MetadataSynchronizer')
    def test_sync_catalog_should_use_synchronizer(self,
                                                  mock_metadata_synchonizer):

        sisense2datacatalog_cli.Sisense2DataCatalogCli.run([
            'sync-catalog', '--sisense-server', 'test-server',
            '--sisense-username', 'test-username', '--sisense-password',
            'test-password', '--datacatalog-project-id', 'dc-project-id'
        ])

        mock_metadata_synchonizer.assert_called_once_with(
            sisense_server_address='test-server',
            sisense_username='test-username',
            sisense_password='test-password',
            datacatalog_project_id='dc-project-id',
            datacatalog_location_id='us')

        synchonizer = mock_metadata_synchonizer.return_value
        synchonizer.run.assert_called_once()

    def test_parse_args_find_elasticube_deps_should_require_project_id(self):
        self.assertRaises(
            SystemExit,
            sisense2datacatalog_cli.Sisense2DataCatalogCli._parse_args, [
                'find-elasticube-deps', '--datasource', 'test-datasource',
                '--table', 'test-table', '--column', 'test-column'
            ])

    @mock.patch('google.datacatalog_connectors.sisense.sisense2datacatalog_cli'
                '.addons.ElastiCubeDependencyPrinter')
    @mock.patch('google.datacatalog_connectors.sisense.sisense2datacatalog_cli'
                '.addons.TagBasedFinder')
    def test_find_elasticube_deps_should_find_and_print_dependencies(
            self, mock_deps_finder, mock_deps_printer):

        sisense2datacatalog_cli.Sisense2DataCatalogCli.run([
            'find-elasticube-deps', '--datasource', 'test-datasource',
            '--table', 'test-table', '--column', 'test-column',
            '--datacatalog-project-id', 'dc-project-id'
        ])

        mock_deps_finder.assert_called_once_with('dc-project-id')

        finder = mock_deps_finder.return_value
        finder.find.assert_called_once_with('test-datasource', 'test-table',
                                            'test-column')

        mock_deps_printer.print_dependency_finder_results.assert_called_once()

    @mock.patch('google.datacatalog_connectors.sisense.sisense2datacatalog_cli'
                '.addons.TagBasedFinder')
    def test_find_elasticube_deps_should_exit_on_exception(
            self, mock_deps_finder):

        mock_deps_finder.return_value.find.side_effect = Exception()

        self.assertRaises(SystemExit,
                          sisense2datacatalog_cli.Sisense2DataCatalogCli.run, [
                              'find-elasticube-deps',
                              '--datacatalog-project-id', 'dc-project-id'
                          ])

    @mock.patch('google.datacatalog_connectors.sisense.sisense2datacatalog_cli'
                '.Sisense2DataCatalogCli')
    def test_main_should_call_cli_run(self, mock_cli):
        sisense.main()
        mock_cli.run.assert_called_once()
