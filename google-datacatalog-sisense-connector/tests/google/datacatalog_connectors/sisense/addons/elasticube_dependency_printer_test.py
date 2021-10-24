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

        # when
        addons.ElastiCubeDependencyPrinter.print_dependency_finder_results({})

        # then
        mock_get_asset_metadata_value.assert_not_called()
        mock_filter_jaql_tags.assert_not_called()

    @mock.patch(f'{__PRINTER_MODULE}.base_finder.ElastiCubeDependencyFinder'
                f'.filter_jaql_tags', lambda *args: [])
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_asset_metadata_value')
    def test_print_dependency_finder_results_should_get_asset_metadata_values(
            self, mock_get_asset_metadata_value):

        # given
        fake_find_results = {'entries/test-entry': (datacatalog.Entry(), [])}

        # when
        addons.ElastiCubeDependencyPrinter.print_dependency_finder_results(
            fake_find_results)

        # then
        mock_get_asset_metadata_value.assert_has_calls(
            [mock.call([], 'dashboard_title'),
             mock.call([], 'datasource')])

    @mock.patch(f'{__PRINTER_MODULE}.base_finder'
                f'.ElastiCubeDependencyFinder.filter_jaql_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_asset_metadata_value',
                lambda *args: None)
    def test_print_dependency_finder_results_should_filter_jaql_tags(
            self, mock_filter_jaql_tags):

        # given
        fake_tag = mocks.make_fake_tag()
        fake_find_results = {
            'entries/test-entry': (datacatalog.Entry(), [fake_tag])
        }

        # when
        addons.ElastiCubeDependencyPrinter.print_dependency_finder_results(
            fake_find_results)

        # then
        mock_filter_jaql_tags.assert_called_once_with([fake_tag])

    @mock.patch(f'{__PRINTER_MODULE}.tabulate.tabulate')
    @mock.patch(f'{__PRINTER_MODULE}.base_finder'
                f'.ElastiCubeDependencyFinder.filter_jaql_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_asset_metadata_value',
                lambda *args: None)
    def test_print_dependency_finder_results_should_tabulate_tags_data(
            self, mock_filter_jaql_tags, mock_tabulate):

        # given
        fake_tag = mocks.make_fake_tag(
            string_fields=[('table', 'test-table'), ('column', 'test-column')])
        mock_filter_jaql_tags.return_value = [fake_tag]
        fake_find_results = {
            'entries/test-entry': (datacatalog.Entry(), [fake_tag])
        }

        # when
        addons.ElastiCubeDependencyPrinter.print_dependency_finder_results(
            fake_find_results)

        # then
        mock_filter_jaql_tags.assert_called_once_with([fake_tag])
        mock_tabulate.assert_called_once()

    @mock.patch(f'{__PRINTER_MODULE}.base_finder'
                f'.ElastiCubeDependencyFinder.filter_asset_metadata_tag')
    def test_get_asset_metadata_value_should_return_value_if_available(
            self, mock_filter_asset_metadata_tag):

        # given
        fake_tag = mocks.make_fake_tag(string_fields=[('datasource',
                                                       'test-datasource')])
        mock_filter_asset_metadata_tag.return_value = fake_tag

        # when
        value = addons.ElastiCubeDependencyPrinter\
            ._ElastiCubeDependencyPrinter__get_asset_metadata_value(
                [fake_tag], 'datasource')

        # then
        self.assertEqual('test-datasource', value)
        mock_filter_asset_metadata_tag.assert_called_once_with([fake_tag])

    @mock.patch(f'{__PRINTER_MODULE}.base_finder'
                f'.ElastiCubeDependencyFinder.filter_asset_metadata_tag')
    def test_get_asset_metadata_value_should_return_none_if_not_available(
            self, mock_filter_asset_metadata_tag):

        # given
        fake_tag = mocks.make_fake_tag(string_fields=[('field', 'test-field')])
        mock_filter_asset_metadata_tag.return_value = fake_tag

        # when
        value = addons.ElastiCubeDependencyPrinter\
            ._ElastiCubeDependencyPrinter__get_asset_metadata_value(
                [fake_tag], 'datasource')

        # then
        self.assertIsNone(value)
        mock_filter_asset_metadata_tag.assert_called_once_with([fake_tag])
