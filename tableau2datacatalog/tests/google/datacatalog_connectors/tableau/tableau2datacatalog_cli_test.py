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

from google.datacatalog_connectors import tableau
from google.datacatalog_connectors.tableau import tableau2datacatalog_cli


class Tableau2DataCatalogCliTest(unittest.TestCase):

    def test_parse_args_missing_tableau_server_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit,
            tableau2datacatalog_cli.Tableau2DataCatalogCli._parse_args, [
                '--tableau-api-version', 'test-api-version',
                '--tableau-username', 'test-username', '--tableau-password',
                'test-password', '--tableau-site', 'test-site',
                '--datacatalog-project-id', 'dc-project-id'
            ])

    def test_parse_args_missing_api_version_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit,
            tableau2datacatalog_cli.Tableau2DataCatalogCli._parse_args, [
                '--tableau-server', 'test-server', '--tableau-username',
                'test-username', '--tableau-password', 'test-password',
                '--tableau-site', 'test-site', '--datacatalog-project-id',
                'dc-project-id'
            ])

    def test_parse_args_missing_tableau_username_raise_system_exit(self):
        self.assertRaises(
            SystemExit,
            tableau2datacatalog_cli.Tableau2DataCatalogCli._parse_args, [
                '--tableau-server', 'test-server', '--tableau-api-version',
                'test-api-version', '--tableau-password', 'test-password',
                '--tableau-site', 'test-site', '--datacatalog-project-id',
                'dc-project-id'
            ])

    def test_parse_args_missing_tableau_password_raise_system_exit(self):
        self.assertRaises(
            SystemExit,
            tableau2datacatalog_cli.Tableau2DataCatalogCli._parse_args, [
                '--tableau-server', 'test-server', '--tableau-api-version',
                'test-api-version', '--tableau-username', 'test-username',
                '--tableau-site', 'test-site', '--datacatalog-project-id',
                'dc-project-id'
            ])

    def test_parse_args_missing_project_id_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit,
            tableau2datacatalog_cli.Tableau2DataCatalogCli._parse_args, [
                '--tableau-server', 'test-server', '--tableau-api-version',
                'test-api-version', '--tableau-username', 'test-username',
                '--tableau-password', 'test-password', '--tableau-site',
                'test-site'
            ])

    @mock.patch('google.datacatalog_connectors.tableau.sync'
                '.DataCatalogSynchronizer')
    def test_run_should_call_synchronizer(self, mock_datacatalog_synchonizer):
        tableau2datacatalog_cli.Tableau2DataCatalogCli.run([
            '--tableau-server', 'test-server', '--tableau-api-version',
            'test-api-version', '--tableau-username', 'test-username',
            '--tableau-password', 'test-password', '--tableau-site',
            'test-site', '--datacatalog-project-id', 'dc-project-id'
        ])

        mock_datacatalog_synchonizer.assert_called_once_with(
            tableau_server_address='test-server',
            tableau_api_version='test-api-version',
            tableau_username='test-username',
            tableau_password='test-password',
            tableau_site='test-site',
            datacatalog_project_id='dc-project-id',
            datacatalog_location_id='us-central1')

        synchonizer = mock_datacatalog_synchonizer.return_value
        synchonizer.run.assert_called_once()

    @mock.patch('google.datacatalog_connectors.tableau'
                '.tableau2datacatalog_cli.Tableau2DataCatalogCli')
    def test_main_should_call_cli_run(self, mock_cli):
        tableau.main()
        mock_cli.run.assert_called_once()
