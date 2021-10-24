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

from google.datacatalog_connectors.sisense import addons

from . import elasticube_dependency_mocks as mocks


class LinkedResourceBasedDependencyFinderTest(unittest.TestCase):
    __ADDONS_PACKAGE = 'google.datacatalog_connectors.sisense.addons'
    __FINDER_MODULE = (f'{__ADDONS_PACKAGE}'
                       f'.linked_resource_based_dependency_finder')
    __FINDER_CLASS = f'{__FINDER_MODULE}.LinkedResourceBasedDependencyFinder'
    __PRIVATE_METHOD_PREFIX = (f'{__FINDER_CLASS}'
                               f'._LinkedResourceBasedDependencyFinder')

    @mock.patch(f'{__ADDONS_PACKAGE}.elasticube_dependency_finder'
                f'.commons.DataCatalogFacade', lambda *args: None)
    def setUp(self):
        self.__finder = addons.LinkedResourceBasedDependencyFinder(
            project_id='test-project')

    @mock.patch(f'{__FINDER_CLASS}._make_query')
    def test_find_should_raise_exception_if_empty_args(self, mock_make_query):
        # when
        with self.assertRaises(Exception):
            self.__finder.find('')

        # then
        mock_make_query.assert_not_called()

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_relevant_tags')
    @mock.patch(f'{__FINDER_CLASS}._list_tags')
    @mock.patch(f'{__FINDER_CLASS}._get_entries')
    @mock.patch(f'{__FINDER_CLASS}._search_catalog')
    @mock.patch(f'{__FINDER_CLASS}._make_query')
    def test_find_should_return_assembled_entries_and_tags(
            self, mock_make_query, mock_search_catalog, mock_get_entries,
            mock_list_tags, mock_filter_relevant_tags):

        # given
        fake_query = 'tag:id:"test-dashboard"'
        mock_make_query.return_value = fake_query
        mock_search_catalog.return_value = ['fake_entries/dashboard-entry']
        fake_entry = mocks.make_fake_entry('dashboard-entry',
                                           linked_resource='test-dashboard')
        mock_get_entries.return_value = {
            'fake_entries/dashboard-entry': fake_entry
        }
        fake_tag = mocks.make_fake_tag()
        mock_list_tags.return_value = {
            'fake_entries/dashboard-entry': [fake_tag]
        }
        mock_filter_relevant_tags.return_value = [fake_tag]

        # when
        results = self.__finder.find('test-dashboard/')

        # then
        self.assertEqual(1, len(results))
        self.assertTrue('fake_entries/dashboard-entry' in results)
        self.assertEqual((fake_entry, [fake_tag]),
                         results['fake_entries/dashboard-entry'])
        mock_make_query.assert_called_once_with(['Dashboard', 'Widget'],
                                                asset_id='test-dashboard')
        mock_search_catalog.assert_called_once_with(fake_query)
        mock_get_entries.assert_called_once_with(
            ['fake_entries/dashboard-entry'])
        mock_list_tags.assert_called_once_with(
            ['fake_entries/dashboard-entry'])
        mock_filter_relevant_tags.assert_called_once_with(
            fake_entry, [fake_tag])

    @mock.patch(f'{__FINDER_CLASS}._search_catalog')
    @mock.patch(f'{__FINDER_CLASS}._make_query')
    def test_find_should_return_empty_dict_if_no_search_results(
            self, mock_make_query, mock_search_catalog):

        # given
        fake_query = 'tag:id:"test-dashboard"'
        mock_make_query.return_value = fake_query
        mock_search_catalog.return_value = []

        # when
        results = self.__finder.find('test-dashboard/')

        # then
        self.assertEqual(0, len(results))
        mock_search_catalog.assert_called_once_with(fake_query)

    @mock.patch(f'{__FINDER_CLASS}._get_entries')
    @mock.patch(f'{__FINDER_CLASS}._search_catalog')
    @mock.patch(f'{__FINDER_CLASS}._make_query')
    def test_find_should_return_empty_dict_if_no_entry_match(
            self, mock_make_query, mock_search_catalog, mock_get_entries):

        # given
        fake_query = 'tag:id:"test-dashboard"'
        mock_make_query.return_value = fake_query
        mock_search_catalog.return_value = ['fake_entries/dashboard-entry']
        mock_get_entries.return_value = {
            'fake_entries/dashboard-entry':
                mocks.make_fake_entry('dashboard-entry')
        }

        # when
        results = self.__finder.find('test-dashboard/')

        # then
        self.assertEqual(0, len(results))
        mock_search_catalog.assert_called_once_with(fake_query)
        mock_get_entries.assert_called_once_with(
            ['fake_entries/dashboard-entry'])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_relevant_tags')
    @mock.patch(f'{__FINDER_CLASS}._list_tags')
    @mock.patch(f'{__FINDER_CLASS}._get_entries')
    @mock.patch(f'{__FINDER_CLASS}._search_catalog')
    @mock.patch(f'{__FINDER_CLASS}._make_query')
    def test_find_should_include_nested_widgets_if_asset_is_dashboard(
            self, mock_make_query, mock_search_catalog, mock_get_entries,
            mock_list_tags, mock_filter_relevant_tags):

        # given
        fake_query = 'tag:id:"test-dashboard"'
        mock_make_query.return_value = fake_query
        mock_search_catalog.return_value = [
            'fake_entries/dashboard-entry', 'fake_entries/widget-entry'
        ]
        fake_dashboard_entry = mocks.make_fake_entry(
            'dashboard-entry',
            linked_resource='test-dashboard',
            user_specified_type='Dashboard')
        fake_widget_entry = mocks.make_fake_entry('widget-entry',
                                                  user_specified_type='Widget')
        mock_get_entries.return_value = {
            'fake_entries/dashboard-entry': fake_dashboard_entry,
            'fake_entries/widget-entry': fake_widget_entry
        }
        fake_tag = mocks.make_fake_tag()
        mock_list_tags.return_value = {
            'fake_entries/dashboard-entry': [fake_tag],
            'fake_entries/widget-entry': [fake_tag]
        }
        mock_filter_relevant_tags.return_value = [fake_tag]

        # when
        results = self.__finder.find('test-dashboard/')

        # then
        self.assertEqual(2, len(results))
        self.assertTrue('fake_entries/dashboard-entry' in results)
        self.assertTrue('fake_entries/widget-entry' in results)
        self.assertEqual((fake_dashboard_entry, [fake_tag]),
                         results['fake_entries/dashboard-entry'])
        self.assertEqual((fake_widget_entry, [fake_tag]),
                         results['fake_entries/widget-entry'])
        mock_make_query.assert_called_once_with(['Dashboard', 'Widget'],
                                                asset_id='test-dashboard')
        mock_search_catalog.assert_called_once_with(fake_query)
        mock_get_entries.assert_called_once_with(
            ['fake_entries/dashboard-entry', 'fake_entries/widget-entry'])
        mock_list_tags.assert_called_once_with(
            ['fake_entries/dashboard-entry', 'fake_entries/widget-entry'])
        mock_filter_relevant_tags.has_calls(
            mock.call(fake_dashboard_entry, [fake_tag]),
            mock.call(fake_widget_entry, [fake_tag]))

    @mock.patch(f'{__FINDER_CLASS}._sort_tags_by_schema')
    @mock.patch(f'{__FINDER_CLASS}.filter_jaql_tags')
    @mock.patch(f'{__FINDER_CLASS}.filter_asset_metadata_tag')
    def test_filter_relevant_tags_should_filter_and_sort_tags(
            self, mock_filter_asset_metadata_tag, mock_filter_jaql_tags,
            mock_sort_tags_by_schema):

        # given
        fake_entry = mocks.make_fake_entry('some-entry')
        fake_asset_metadata_tag = mocks.make_fake_tag()
        fake_table_matching_tag = mocks.make_fake_tag()
        mock_filter_asset_metadata_tag.return_value = fake_asset_metadata_tag
        mock_filter_jaql_tags.return_value = [fake_table_matching_tag]
        mock_sort_tags_by_schema.return_value = [fake_table_matching_tag]

        # when
        tags = self.__finder\
            ._LinkedResourceBasedDependencyFinder__filter_relevant_tags(
                fake_entry, [fake_asset_metadata_tag, fake_table_matching_tag])

        # then
        self.assertEqual(2, len(tags))
        self.assertEqual(fake_asset_metadata_tag, tags[0])
        self.assertEqual(fake_table_matching_tag, tags[1])
        mock_filter_asset_metadata_tag.assert_called_once_with(
            [fake_asset_metadata_tag, fake_table_matching_tag])
        mock_filter_jaql_tags.assert_called_once_with(
            [fake_asset_metadata_tag, fake_table_matching_tag])
        mock_sort_tags_by_schema.assert_called_once_with(
            [], '', [fake_table_matching_tag])
