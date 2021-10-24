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


class TagBasedDependencyFinderTest(unittest.TestCase):
    __ADDONS_PACKAGE = 'google.datacatalog_connectors.sisense.addons'
    __FINDER_MODULE = f'{__ADDONS_PACKAGE}.tag_based_dependency_finder'
    __FINDER_CLASS = f'{__FINDER_MODULE}.TagBasedDependencyFinder'
    __PRIVATE_METHOD_PREFIX = f'{__FINDER_CLASS}._TagBasedDependencyFinder'

    @mock.patch(f'{__ADDONS_PACKAGE}.elasticube_dependency_finder'
                f'.commons.DataCatalogFacade', lambda *args: None)
    def setUp(self):
        self.__finder = addons.TagBasedDependencyFinder(
            project_id='test-project')

    @mock.patch(f'{__FINDER_CLASS}._make_query')
    def test_find_should_raise_exception_if_empty_args(self, mock_make_query):
        # when
        with self.assertRaises(Exception):
            self.__finder.find()

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
        fake_query = 'tag:table:"test-table"'
        mock_make_query.return_value = fake_query
        mock_search_catalog.return_value = ['fake_entries/widget-entry']
        fake_entry = mocks.make_fake_entry('widget-entry')
        mock_get_entries.return_value = {
            'fake_entries/widget-entry': fake_entry
        }
        fake_tag = mocks.make_fake_tag()
        mock_list_tags.return_value = {'fake_entries/widget-entry': [fake_tag]}
        mock_filter_relevant_tags.return_value = [fake_tag]

        # when
        results = self.__finder.find(table='test-table')

        self.assertEqual(1, len(results))
        self.assertTrue('fake_entries/widget-entry' in results)
        self.assertEqual((fake_entry, [fake_tag]),
                         results['fake_entries/widget-entry'])

        # then
        mock_make_query.assert_called_once_with(['Dashboard', 'Widget'],
                                                column=None,
                                                datasource=None,
                                                table='test-table')
        mock_search_catalog.assert_called_once_with(fake_query)
        mock_get_entries.assert_called_once_with(['fake_entries/widget-entry'])
        mock_list_tags.assert_called_once_with(['fake_entries/widget-entry'])
        mock_filter_relevant_tags.assert_called_once_with(
            fake_entry, [fake_tag], 'test-table', None)

    @mock.patch(f'{__FINDER_CLASS}._search_catalog')
    @mock.patch(f'{__FINDER_CLASS}._make_query')
    def test_find_should_return_empty_dict_if_no_search_results(
            self, mock_make_query, mock_search_catalog):

        # given
        fake_query = 'tag:table:"test-table"'
        mock_make_query.return_value = fake_query
        mock_search_catalog.return_value = []

        # when
        results = self.__finder.find(table='test-table')

        self.assertEqual(0, len(results))

        # then
        mock_search_catalog.assert_called_once_with(fake_query)

    @mock.patch(f'{__FINDER_CLASS}._sort_tags_by_schema')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}'
                f'__filter_table_column_matching_tags')
    @mock.patch(f'{__FINDER_CLASS}.filter_asset_metadata_tag')
    def test_filter_relevant_tags_should_filter_and_sort_tags(
            self, mock_filter_asset_metadata_tag,
            mock_filter_table_column_matching_tags, mock_sort_tags_by_schema):

        # given
        fake_entry = mocks.make_fake_entry('some-entry')
        fake_asset_metadata_tag = mocks.make_fake_tag()
        fake_table_matching_tag = mocks.make_fake_tag()
        mock_filter_asset_metadata_tag.return_value = fake_asset_metadata_tag
        mock_filter_table_column_matching_tags.return_value = [
            fake_table_matching_tag
        ]
        mock_sort_tags_by_schema.return_value = [fake_table_matching_tag]

        # when
        tags = self.__finder\
            ._TagBasedDependencyFinder__filter_relevant_tags(
                fake_entry, [fake_asset_metadata_tag, fake_table_matching_tag],
                'test-table', 'test-column')

        # then
        self.assertEqual(2, len(tags))
        self.assertEqual(fake_asset_metadata_tag, tags[0])
        self.assertEqual(fake_table_matching_tag, tags[1])
        mock_filter_asset_metadata_tag.assert_called_once_with(
            [fake_asset_metadata_tag, fake_table_matching_tag])
        mock_filter_table_column_matching_tags.assert_called_once_with(
            [fake_asset_metadata_tag, fake_table_matching_tag], 'test-table',
            'test-column')
        mock_sort_tags_by_schema.assert_called_once_with(
            [], '', [fake_table_matching_tag])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_column_matching_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_table_matching_tags')
    @mock.patch(f'{__FINDER_CLASS}.filter_jaql_tags', lambda *args: args)
    def test_filter_table_column_matching_tags_should_return_matching_table(
            self, mock_filter_table_matching_tags,
            mock_filter_column_matching_tags):

        # given
        fake_table_matching_tag = mocks.make_fake_tag()
        fake_non_matching_tag = mocks.make_fake_tag()
        mock_filter_table_matching_tags.return_value = [
            fake_table_matching_tag
        ]

        # when
        tags = self.__finder\
            ._TagBasedDependencyFinder__filter_table_column_matching_tags(
                [fake_table_matching_tag, fake_non_matching_tag],
                table='test-table')

        # then
        self.assertEqual(1, len(tags))
        self.assertEqual(fake_table_matching_tag, tags[0])
        mock_filter_column_matching_tags.assert_called_once_with(
            [fake_table_matching_tag, fake_non_matching_tag], None)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_column_matching_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_table_matching_tags',
                lambda *args, **kwargs: None)
    @mock.patch(f'{__FINDER_CLASS}.filter_jaql_tags', lambda *args: args)
    def test_filter_table_column_matching_tags_should_return_matching_column(
            self, mock_filter_column_matching_tags):

        # given
        fake_non_matching_tag = mocks.make_fake_tag()
        fake_column_matching_tag = mocks.make_fake_tag()
        mock_filter_column_matching_tags.return_value = [
            fake_column_matching_tag
        ]

        # when
        tags = self.__finder\
            ._TagBasedDependencyFinder__filter_table_column_matching_tags(
                [fake_non_matching_tag, fake_column_matching_tag],
                column='test-column')

        # then
        self.assertEqual(1, len(tags))
        self.assertEqual(fake_column_matching_tag, tags[0])
        mock_filter_column_matching_tags.assert_called_once_with(
            [fake_non_matching_tag, fake_column_matching_tag], 'test-column')

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_column_matching_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_table_matching_tags')
    @mock.patch(f'{__FINDER_CLASS}.filter_jaql_tags', lambda *args: args)
    def test_filter_table_column_matching_tags_should_return_matching_both(
            self, mock_filter_table_matching_tags,
            mock_filter_column_matching_tags):

        # given
        fake_table_only_matching_tag = mocks.make_fake_tag()
        fake_column_only_matching_tag = mocks.make_fake_tag()
        fake_table_column_matching_tag = mocks.make_fake_tag()
        mock_filter_table_matching_tags.return_value = [
            fake_table_only_matching_tag, fake_table_column_matching_tag
        ]
        mock_filter_column_matching_tags.return_value = [
            fake_table_column_matching_tag
        ]

        # when
        tags = self.__finder\
            ._TagBasedDependencyFinder__filter_table_column_matching_tags(
                [
                    fake_table_only_matching_tag,
                    fake_column_only_matching_tag,
                    fake_table_column_matching_tag
                ],
                'test-table', 'test-column')

        # then
        self.assertEqual(1, len(tags))
        self.assertEqual(fake_table_column_matching_tag, tags[0])
        mock_filter_column_matching_tags.assert_called_once_with(
            [fake_table_only_matching_tag, fake_table_column_matching_tag],
            'test-column')

    def test_filter_table_matching_tags_should_none_if_no_table_name(self):
        # given
        fake_table_related_tag = mocks.make_fake_tag(
            string_fields=[('table', 'test-table')])

        # when
        tags = self.__finder \
            ._TagBasedDependencyFinder__filter_table_matching_tags(
                [fake_table_related_tag])

        # then
        self.assertEqual(0, len(tags))

    def test_filter_table_matching_tags_should_return_only_matching_tags(self):
        # given
        fake_non_matching_tag = mocks.make_fake_tag(
            string_fields=[('column', 'test-column')])
        fake_table_matching_tag = mocks.make_fake_tag(
            string_fields=[('table', 'test-table')])

        # when
        tags = self.__finder \
            ._TagBasedDependencyFinder__filter_table_matching_tags(
                [fake_non_matching_tag, fake_table_matching_tag],
                table='test-table')

        # then
        self.assertEqual(1, len(tags))
        self.assertEqual(fake_table_matching_tag, tags[0])

    def test_filter_column_matching_tags_should_none_if_no_column_name(self):
        # given
        fake_column_related_tag = mocks.make_fake_tag(
            string_fields=[('column', 'test-column')])

        # when
        tags = self.__finder \
            ._TagBasedDependencyFinder__filter_column_matching_tags(
                [fake_column_related_tag])

        # then
        self.assertEqual(0, len(tags))

    def test_filter_column_matching_tags_should_return_only_matching_tags(
            self):

        # given
        fake_non_matching_tag = mocks.make_fake_tag(
            string_fields=[('table', 'test-table')])
        fake_column_matching_tag = mocks.make_fake_tag(
            string_fields=[('column', 'test-column')])

        # when
        tags = self.__finder \
            ._TagBasedDependencyFinder__filter_column_matching_tags(
                [fake_non_matching_tag, fake_column_matching_tag],
                column='test-column')

        # then
        self.assertEqual(1, len(tags))
        self.assertEqual(fake_column_matching_tag, tags[0])
