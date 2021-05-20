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

    def test_parse_args_missing_sisense_server_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit,
            sisense2datacatalog_cli.Sisense2DataCatalogCli._parse_args, [
                '--sisense-username', 'test-username', '--sisense-password',
                'test-password', '--datacatalog-project-id', 'dc-project-id'
            ])

    def test_parse_args_missing_sisense_username_should_raise_system_exit(
            self):

        self.assertRaises(
            SystemExit,
            sisense2datacatalog_cli.Sisense2DataCatalogCli._parse_args, [
                '--sisense-server', 'test-server', '--sisense-password',
                'test-password', '--datacatalog-project-id', 'dc-project-id'
            ])

    def test_parse_args_missing_sisense_password_should_raise_system_exit(
            self):

        self.assertRaises(
            SystemExit,
            sisense2datacatalog_cli.Sisense2DataCatalogCli._parse_args, [
                '--sisense-server', 'test-server', '--sisense-username',
                'test-username', '--datacatalog-project-id', 'dc-project-id'
            ])

    def test_parse_args_missing_project_id_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit,
            sisense2datacatalog_cli.Sisense2DataCatalogCli._parse_args, [
                '--sisense-server', 'test-server', '--sisense-username',
                'test-username', '--sisense-password', 'test-password'
            ])

    @mock.patch(
        'google.datacatalog_connectors.sisense.sync.MetadataSynchronizer')
    def test_run_should_call_synchronizer(self, mock_metadata_synchonizer):
        sisense2datacatalog_cli.Sisense2DataCatalogCli.run([
            '--sisense-server', 'test-server', '--sisense-username',
            'test-username', '--sisense-password', 'test-password',
            '--datacatalog-project-id', 'dc-project-id'
        ])

        mock_metadata_synchonizer.assert_called_once_with(
            sisense_server_address='test-server',
            sisense_username='test-username',
            sisense_password='test-password',
            datacatalog_project_id='dc-project-id',
            datacatalog_location_id='us')

        synchonizer = mock_metadata_synchonizer.return_value
        synchonizer.run.assert_called_once()

    @mock.patch('google.datacatalog_connectors.sisense.sisense2datacatalog_cli'
                '.Sisense2DataCatalogCli')
    def test_main_should_call_cli_run(self, mock_cli):
        sisense.main()
        mock_cli.run.assert_called_once()
