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

from google.datacatalog_connectors.sisense.prepare import \
    sisense_connector_strings_helper


class SisenseConnectorStringsHelperTest(unittest.TestCase):
    __COMMONS_PREP_PACKAGE = 'google.datacatalog_connectors.commons.prepare'

    @mock.patch(f'{__COMMONS_PREP_PACKAGE}.DataCatalogStringsHelper'
                f'.truncate_string', lambda *args: args[0])
    def test_format_column_name_should_not_change_compliant_string(self):
        formatted_column_name = sisense_connector_strings_helper\
            .SisenseConnectorStringsHelper.format_column_name('# (test)')

        self.assertEqual('# (test)', formatted_column_name)

    @mock.patch(f'{__COMMONS_PREP_PACKAGE}.DataCatalogStringsHelper'
                f'.truncate_string', lambda *args: args[0])
    def test_format_column_name_should_normalize_non_compliant_string(self):
        formatted_column_name = sisense_connector_strings_helper\
            .SisenseConnectorStringsHelper.format_column_name('#.(test)')

        self.assertEqual('#_(test)', formatted_column_name)

    @mock.patch(f'{__COMMONS_PREP_PACKAGE}.DataCatalogStringsHelper'
                f'.truncate_string', lambda *args: args[0])
    def test_format_column_name_can_optionally_avoid_normalizing_string(self):
        formatted_column_name = sisense_connector_strings_helper\
            .SisenseConnectorStringsHelper\
            .format_column_name('#.(test)', False)

        self.assertEqual('#.(test)', formatted_column_name)

    @mock.patch(f'{__COMMONS_PREP_PACKAGE}'
                f'.DataCatalogStringsHelper.truncate_string')
    def test_format_column_name_should_return_truncated_string(
            self, mock_truncate_string):

        expected_value = 'truncated_str...'
        mock_truncate_string.return_value = expected_value

        formatted_column_name = sisense_connector_strings_helper\
            .SisenseConnectorStringsHelper\
            .format_column_name('# (test) # (test)')

        mock_truncate_string.assert_called_once_with('# (test) # (test)', 300)
        self.assertEqual(expected_value, formatted_column_name)
