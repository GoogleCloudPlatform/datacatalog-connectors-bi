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

from google.cloud import datacatalog

from google.datacatalog_connectors.sisense import addons

from . import elasticube_dependency_mocks as mocks


class ElastiCubeDependencyPrinterTest(unittest.TestCase):
    __ADDONS_PACKAGE = 'google.datacatalog_connectors.sisense.addons'
    __PRINTER_MODULE = f'{__ADDONS_PACKAGE}.elasticube_dependency_printer'
    __PRINTER_CLASS = f'{__PRINTER_MODULE}.ElastiCubeDependencyPrinter'
    __PRIVATE_METHOD_PREFIX = f'{__PRINTER_CLASS}._ElastiCubeDependencyPrinter'

    @mock.patch(f'{__PRINTER_MODULE}.base_finder'
                f'.ElastiCubeDependencyFinder.filter_jaql_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_asset_metadata_value')
    def test_print_dependency_finder_results_should_skip_if_no_results(
            self, mock_get_asset_metadata_value, mock_filter_jaql_tags):

        addons.ElastiCubeDependencyPrinter.print_dependency_finder_results({})

        mock_get_asset_metadata_value.assert_not_called()
        mock_filter_jaql_tags.assert_not_called()

    @mock.patch(f'{__PRINTER_MODULE}.base_finder.ElastiCubeDependencyFinder'
                f'.filter_jaql_tags', lambda *args: [])
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_asset_metadata_value')
    def test_print_dependency_finder_results_should_get_asset_metadata_values(
            self, mock_get_asset_metadata_value):

        fake_find_results = {'entries/test-entry': (datacatalog.Entry(), [])}

        addons.ElastiCubeDependencyPrinter.print_dependency_finder_results(
            fake_find_results)

        mock_get_asset_metadata_value.assert_has_calls(
            [mock.call([], 'dashboard_title'),
             mock.call([], 'datasource')])

    @mock.patch(f'{__PRINTER_MODULE}.base_finder'
                f'.ElastiCubeDependencyFinder.filter_jaql_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_asset_metadata_value',
                lambda *args: None)
    def test_print_dependency_finder_results_should_filter_jaql_tags(
            self, mock_filter_jaql_tags):

        fake_tag = mocks.make_fake_tag(
            string_fields=[('table', 'test-table'), ('column', 'test-column')])

        # Just to improve code coverage. The Below return value does not impact
        # test results but causes the script to print Tag's data.
        mock_filter_jaql_tags.return_value = [fake_tag]

        fake_find_results = {
            'entries/test-entry': (datacatalog.Entry(), [fake_tag])
        }

        addons.ElastiCubeDependencyPrinter.print_dependency_finder_results(
            fake_find_results)

        mock_filter_jaql_tags.assert_called_once_with([fake_tag])

    @mock.patch(f'{__PRINTER_MODULE}.base_finder'
                f'.ElastiCubeDependencyFinder.filter_asset_metadata_tag')
    def test_get_asset_metadata_value_should_return_value_if_available(
            self, mock_filter_asset_metadata_tag):

        fake_tag = mocks.make_fake_tag(string_fields=[('datasource',
                                                       'test-datasource')])

        mock_filter_asset_metadata_tag.return_value = fake_tag

        value = addons.ElastiCubeDependencyPrinter\
            ._ElastiCubeDependencyPrinter__get_asset_metadata_value(
                [fake_tag], 'datasource')

        self.assertEqual('test-datasource', value)

        mock_filter_asset_metadata_tag.assert_called_once_with([fake_tag])

    @mock.patch(f'{__PRINTER_MODULE}.base_finder'
                f'.ElastiCubeDependencyFinder.filter_asset_metadata_tag')
    def test_get_asset_metadata_value_should_return_none_if_not_available(
            self, mock_filter_asset_metadata_tag):

        fake_tag = mocks.make_fake_tag(string_fields=[('field', 'test-field')])

        mock_filter_asset_metadata_tag.return_value = fake_tag

        value = addons.ElastiCubeDependencyPrinter\
            ._ElastiCubeDependencyPrinter__get_asset_metadata_value(
                [fake_tag], 'datasource')

        self.assertIsNone(value)

        mock_filter_asset_metadata_tag.assert_called_once_with([fake_tag])
