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

import unittest
from unittest import mock

from google.datacatalog_connectors import qlik
from google.datacatalog_connectors.qlik import qlik2datacatalog_cli


class Qlik2DataCatalogCliTest(unittest.TestCase):

    def test_parse_args_missing_qlik_server_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit, qlik2datacatalog_cli.Qlik2DataCatalogCli._parse_args, [
                '--qlik-username', 'test-username', '--qlik-password',
                'test-password', '--datacatalog-project-id', 'dc-project-id'
            ])

    def test_parse_args_missing_qlik_username_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit, qlik2datacatalog_cli.Qlik2DataCatalogCli._parse_args, [
                '--qlik-server', 'test-server', '--qlik-password',
                'test-password', '--datacatalog-project-id', 'dc-project-id'
            ])

    def test_parse_args_missing_qlik_password_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit, qlik2datacatalog_cli.Qlik2DataCatalogCli._parse_args, [
                '--qlik-server', 'test-server', '--qlik-username',
                'test-username', '--datacatalog-project-id', 'dc-project-id'
            ])

    def test_parse_args_missing_project_id_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit, qlik2datacatalog_cli.Qlik2DataCatalogCli._parse_args, [
                '--qlik-server', 'test-server', '--qlik-username',
                'test-username', '--qlik-password', 'test-password'
            ])

    @mock.patch('google.datacatalog_connectors.qlik.sync.MetadataSynchronizer')
    def test_run_should_call_synchronizer(self, mock_metadata_synchonizer):
        qlik2datacatalog_cli.Qlik2DataCatalogCli.run([
            '--qlik-server', 'test-server', '--qlik-username', 'test-username',
            '--qlik-password', 'test-password', '--datacatalog-project-id',
            'dc-project-id'
        ])

        mock_metadata_synchonizer.assert_called_once_with(
            qlik_server_address='test-server',
            qlik_ad_domain='.',
            qlik_username='test-username',
            qlik_password='test-password',
            datacatalog_project_id='dc-project-id',
            datacatalog_location_id='us')

        synchonizer = mock_metadata_synchonizer.return_value
        synchonizer.run.assert_called_once()

    @mock.patch('google.datacatalog_connectors.qlik.qlik2datacatalog_cli'
                '.Qlik2DataCatalogCli')
    def test_main_should_call_cli_run(self, mock_cli):
        qlik.main()
        mock_cli.run.assert_called_once()
