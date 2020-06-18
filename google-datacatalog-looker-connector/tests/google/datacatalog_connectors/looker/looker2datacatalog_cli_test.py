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

from google.datacatalog_connectors import looker
from google.datacatalog_connectors.looker import looker2datacatalog_cli


class Looker2DataCatalogCliTest(unittest.TestCase):

    def test_parse_args_missing_project_id_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit,
            looker2datacatalog_cli.Looker2DataCatalogCli._parse_args,
            ['--looker-credentials-file', 'a-file-path'])

    def test_parse_args_missing_looker_creds_should_raise_system_exit(self):
        self.assertRaises(
            SystemExit,
            looker2datacatalog_cli.Looker2DataCatalogCli._parse_args,
            ['--datacatalog-project-id', 'dc-project_id'])

    @mock.patch('google.datacatalog_connectors.looker.sync'
                '.MetadataSynchronizer')
    def test_run_should_call_synchronizer(self, mock_metadata_synchonizer):
        looker2datacatalog_cli.Looker2DataCatalogCli.run([
            '--datacatalog-project-id', 'dc-project_id',
            '--looker-credentials-file', 'a-file-path'
        ])

        mock_metadata_synchonizer.assert_called_once_with(
            datacatalog_project_id='dc-project_id',
            datacatalog_location_id='us-central1',
            looker_credentials_file='a-file-path')

        synchonizer = mock_metadata_synchonizer.return_value
        synchonizer.run.assert_called_once()

    @mock.patch('google.datacatalog_connectors.looker.looker2datacatalog_cli'
                '.Looker2DataCatalogCli')
    def test_main_should_call_cli_run(self, mock_cli):
        looker.main()
        mock_cli.run.assert_called_once()
