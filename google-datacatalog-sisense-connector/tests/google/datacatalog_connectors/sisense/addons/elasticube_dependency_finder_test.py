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


class ElastiCubeDependencyFinderTest(unittest.TestCase):
    __ADDONS_PACKAGE = 'google.datacatalog_connectors.sisense.addons'
    __FINDER_MODULE = f'{__ADDONS_PACKAGE}.elasticube_dependency_finder'
    __FINDER_CLASS = f'{__FINDER_MODULE}.ElastiCubeDependencyFinder'
    __PRIVATE_METHOD_PREFIX = f'{__FINDER_CLASS}._ElastiCubeDependencyFinder'

    @mock.patch(f'{__FINDER_MODULE}.commons.DataCatalogFacade')
    def setUp(self, mock_facade):
        self.__finder = addons.ElastiCubeDependencyFinder(
            project_id='test-project')
        self.__datacatalog_facade = mock_facade.return_value

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_query')
    def test_find_should_raise_exception_if_empty_args(self, mock_make_query):
        # when
        with self.assertRaises(Exception):
            self.__finder.find()

        # then
        mock_make_query.assert_not_called()

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_find_relevant_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__list_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_entries')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__search_catalog')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_query')
    def test_find_should_return_assembled_entries_and_tags(
            self, mock_make_query, mock_search_catalog, mock_get_entries,
            mock_list_tags, mock_filter_find_relevant_tags):

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
        mock_filter_find_relevant_tags.return_value = [fake_tag]

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
        mock_filter_find_relevant_tags.assert_called_once_with(
            fake_entry, [fake_tag], 'test-table', None)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__search_catalog')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_query')
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

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_query')
    def test_list_all_should_raise_exception_if_empty_args(
            self, mock_make_query):

        # when
        with self.assertRaises(Exception):
            self.__finder.list_all('')

        # then
        mock_make_query.assert_not_called()

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_list_relevant_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__list_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_entries')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__search_catalog')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_query')
    def test_list_all_should_return_assembled_entries_and_tags(
            self, mock_make_query, mock_search_catalog, mock_get_entries,
            mock_list_tags, mock_filter_list_relevant_tags):

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
        mock_filter_list_relevant_tags.return_value = [fake_tag]

        # when
        results = self.__finder.list_all('test-dashboard/')

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
        mock_filter_list_relevant_tags.assert_called_once_with(
            fake_entry, [fake_tag])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__search_catalog')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_query')
    def test_list_all_should_return_empty_dict_if_no_search_results(
            self, mock_make_query, mock_search_catalog):

        # given
        fake_query = 'tag:id:"test-dashboard"'
        mock_make_query.return_value = fake_query
        mock_search_catalog.return_value = []

        # when
        results = self.__finder.list_all('test-dashboard/')

        # then
        self.assertEqual(0, len(results))
        mock_search_catalog.assert_called_once_with(fake_query)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_entries')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__search_catalog')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_query')
    def test_list_all_should_return_empty_dict_if_no_entry_match(
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
        results = self.__finder.list_all('test-dashboard/')

        # then
        self.assertEqual(0, len(results))
        mock_search_catalog.assert_called_once_with(fake_query)
        mock_get_entries.assert_called_once_with(
            ['fake_entries/dashboard-entry'])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_list_relevant_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__list_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_entries')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__search_catalog')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_query')
    def test_list_all_should_include_nested_widgets_if_asset_is_dashboard(
            self, mock_make_query, mock_search_catalog, mock_get_entries,
            mock_list_tags, mock_filter_list_relevant_tags):

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
        mock_filter_list_relevant_tags.return_value = [fake_tag]

        # when
        results = self.__finder.list_all('test-dashboard/')

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
        mock_filter_list_relevant_tags.has_calls(
            mock.call(fake_dashboard_entry, [fake_tag]),
            mock.call(fake_widget_entry, [fake_tag]))

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_column_search_term',
                lambda *args: None)
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_table_search_term',
                lambda *args: None)
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_datasource_search_term',
                lambda *args: None)
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_dashboard_id_search_term',
                lambda *args: None)
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_asset_id_search_term',
                lambda *args: None)
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_type_search_term')
    def test_make_query_should_handle_mandatory_search_terms(
            self, mock_make_type_search_term):

        # given
        mock_make_type_search_term.return_value = '(type=Dashboard)'

        # when
        query = self.__finder._ElastiCubeDependencyFinder__make_query(
            ['Dashboard'])

        # then
        self.assertEqual('system=Sisense (type=Dashboard)', query)
        mock_make_type_search_term.assert_called_once_with(['Dashboard'])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_column_search_term')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_table_search_term')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_datasource_search_term')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_dashboard_id_search_term')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_asset_id_search_term')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_type_search_term',
                lambda *args: '(type=Dashboard)')
    def test_make_query_should_handle_optional_search_terms(
            self, mock_make_asset_id_search_term,
            mock_make_dashboard_id_search_term,
            mock_make_datasource_search_term, mock_make_table_search_term,
            mock_make_column_search_term):

        # given
        mock_make_asset_id_search_term.return_value = 'tag:id:"test-id"'
        mock_make_dashboard_id_search_term.return_value = \
            'tag:dashboard_id:"test-dashboard"'
        mock_make_datasource_search_term.return_value = \
            'tag:datasource:"test-datasource"'
        mock_make_table_search_term.return_value = 'tag:table:"test-table"'
        mock_make_column_search_term.return_value = 'tag:column:"test-column"'

        # when
        query = self.__finder._ElastiCubeDependencyFinder__make_query(
            ['Dashboard'],
            asset_id='test-id',
            dashboard_id='test-dashboard',
            datasource='test-datasource',
            table='test-table',
            column='test-column')

        # then
        self.assertTrue('tag:id:"test-id"' in query)
        self.assertTrue('tag:dashboard_id:"test-dashboard"' in query)
        self.assertTrue('tag:datasource:"test-datasource"' in query)
        self.assertTrue('tag:table:"test-table"' in query)
        self.assertTrue('tag:column:"test-column"' in query)
        mock_make_asset_id_search_term.assert_called_once_with('test-id')
        mock_make_dashboard_id_search_term.assert_called_once_with(
            'test-dashboard')
        mock_make_datasource_search_term.assert_called_once_with(
            'test-datasource')
        mock_make_table_search_term.assert_called_once_with('test-table')
        mock_make_column_search_term.assert_called_once_with('test-column')

    def test_make_type_search_term_should_return_none_if_no_types(self):
        # when
        query = self\
            .__finder._ElastiCubeDependencyFinder__make_type_search_term([])

        # then
        self.assertIsNone(query)

    def test_make_type_search_term_should_not_use_or_for_single_type(self):
        # when
        query = \
            self.__finder._ElastiCubeDependencyFinder__make_type_search_term(
                ['Dashboard'])

        # then
        self.assertEqual('(type=Dashboard)', query)

    def test_make_type_search_term_should_use_or_for_multiple_types(self):
        # when
        query = \
            self.__finder._ElastiCubeDependencyFinder__make_type_search_term(
                ['Dashboard', 'Widget'])

        # then
        self.assertEqual('(type=Dashboard OR type=Widget)', query)

    def test_make_asset_id_search_term_should_return_none_if_no_id(self):
        # when
        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_asset_id_search_term()

        # then
        self.assertIsNone(query)

    def test_make_asset_id_search_term_should_return_tag_qualifier(self):
        # when
        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_asset_id_search_term('test-id')

        # then
        self.assertEqual('tag:id:test-id', query)

    def test_make_dashboard_id_search_term_should_return_none_if_no_id(self):
        # when
        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_dashboard_id_search_term()

        # then
        self.assertIsNone(query)

    def test_make_dashboard_id_search_term_should_return_tag_qualifier(self):
        # when
        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_dashboard_id_search_term(
                'test-dashboard')

        # then
        self.assertEqual('tag:dashboard_id:test-dashboard', query)

    def test_make_datasource_search_term_should_return_none_if_no_datasource(
            self):

        # when
        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_datasource_search_term()

        # then
        self.assertIsNone(query)

    def test_make_datasource_search_term_should_return_tag_qualifier(self):
        # when
        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_datasource_search_term(
                'test-datasource')

        # then
        self.assertEqual('tag:datasource:"test-datasource"', query)

    def test_make_table_search_term_should_return_none_if_no_table(self):
        # when
        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_table_search_term()

        # then
        self.assertIsNone(query)

    def test_make_table_search_term_should_return_tag_qualifier(self):
        # when
        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_table_search_term('test-table')

        # then
        self.assertEqual('tag:table:"test-table"', query)

    def test_make_column_search_term_should_return_none_if_no_column(self):
        # when
        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_column_search_term()

        # then
        self.assertIsNone(query)

    def test_make_column_search_term_should_return_tag_qualifier(self):
        # when
        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_column_search_term(
                'test-column')

        # then
        self.assertEqual('tag:column:"test-column"', query)

    def test_search_catalog_should_delegate_to_facade(self):
        # given
        self.__datacatalog_facade.search_catalog_relative_resource_name\
            .return_value = ['fake_entries/some-entry']

        # when
        entry_names = self.__finder\
            ._ElastiCubeDependencyFinder__search_catalog(
                'tag:id:test-dashboard')

        # then
        self.assertEqual(1, len(entry_names))
        self.assertEqual('fake_entries/some-entry', entry_names[0])
        self.__datacatalog_facade.search_catalog_relative_resource_name\
            .assert_called_once_with('tag:id:test-dashboard')

    def test_get_entries_should_return_dict(self):
        # given
        fake_entry = mocks.make_fake_entry('some-entry')
        self.__datacatalog_facade.get_entry.return_value = fake_entry

        # when
        entries = self.__finder._ElastiCubeDependencyFinder__get_entries(
            ['fake_entries/some-entry'])

        # then
        self.assertEqual(1, len(entries))
        self.assertEqual(fake_entry, entries['fake_entries/some-entry'])
        self.__datacatalog_facade.get_entry.assert_called_once_with(
            'fake_entries/some-entry')

    def test_list_tags_should_return_dict(self):
        # given
        fake_tag = mocks.make_fake_tag()
        self.__datacatalog_facade.list_tags.return_value = [fake_tag]

        # when
        tags = self.__finder._ElastiCubeDependencyFinder__list_tags(
            ['fake_entries/some-entry'])

        # then
        self.assertEqual(1, len(tags))
        self.assertEqual(fake_tag, tags['fake_entries/some-entry'][0])
        self.__datacatalog_facade.list_tags.assert_called_once_with(
            'fake_entries/some-entry')

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__sort_tags_by_schema')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}'
                f'__filter_table_column_matching_tags')
    @mock.patch(f'{__FINDER_CLASS}.filter_asset_metadata_tag')
    def test_filter_find_relevant_tags_should_filter_and_sort_tags(
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
            ._ElastiCubeDependencyFinder__filter_find_relevant_tags(
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

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__sort_tags_by_schema')
    @mock.patch(f'{__FINDER_CLASS}.filter_jaql_tags')
    @mock.patch(f'{__FINDER_CLASS}.filter_asset_metadata_tag')
    def test_filter_list_relevant_tags_should_filter_and_sort_tags(
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
            ._ElastiCubeDependencyFinder__filter_list_relevant_tags(
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

    def test_filter_asset_metadata_tag_should_return_dashboard_metadata_tag(
            self):

        # given
        fake_jaql_related_tag = mocks.make_fake_tag(
            template='test/tagTemplates/sisense_jaql_metadata')
        fake_dashboard_metadata_tag = mocks.make_fake_tag(
            template='test/tagTemplates/sisense_dashboard_metadata')

        # when
        tag = self.__finder.filter_asset_metadata_tag(
            [fake_jaql_related_tag, fake_dashboard_metadata_tag])

        # then
        self.assertEqual(fake_dashboard_metadata_tag, tag)

    def test_filter_asset_metadata_tag_should_return_widget_metadata_tag(self):
        # given
        fake_jaql_related_tag = mocks.make_fake_tag(
            template='test/tagTemplates/sisense_jaql_metadata')
        fake_widget_metadata_tag = mocks.make_fake_tag(
            template='test/tagTemplates/sisense_widget_metadata')

        # when
        tag = self.__finder.filter_asset_metadata_tag(
            [fake_jaql_related_tag, fake_widget_metadata_tag])

        # then
        self.assertEqual(fake_widget_metadata_tag, tag)

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
            ._ElastiCubeDependencyFinder__filter_table_column_matching_tags(
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
            ._ElastiCubeDependencyFinder__filter_table_column_matching_tags(
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
            ._ElastiCubeDependencyFinder__filter_table_column_matching_tags(
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

    def test_filter_jaql_tags_should_return_only_jaql_tags(self):
        # given
        fake_jaql_related_tag_1 = mocks.make_fake_tag(
            template='test/tagTemplates/sisense_jaql_metadata')
        fake_widget_metadata_tag = mocks.make_fake_tag(
            template='test/tagTemplates/sisense_widget_metadata')
        fake_jaql_related_tag_2 = mocks.make_fake_tag(
            template='test/tagTemplates/sisense_jaql_metadata')

        # when
        tags = self.__finder.filter_jaql_tags([
            fake_jaql_related_tag_1, fake_widget_metadata_tag,
            fake_jaql_related_tag_2
        ])

        # then
        self.assertEqual(2, len(tags))
        self.assertEqual(fake_jaql_related_tag_1, tags[0])
        self.assertEqual(fake_jaql_related_tag_2, tags[1])

    def test_filter_table_matching_tags_should_none_if_no_table_name(self):
        # given
        fake_table_related_tag = mocks.make_fake_tag(
            string_fields=[('table', 'test-table')])

        # when
        tags = self.__finder \
            ._ElastiCubeDependencyFinder__filter_table_matching_tags(
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
            ._ElastiCubeDependencyFinder__filter_table_matching_tags(
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
            ._ElastiCubeDependencyFinder__filter_column_matching_tags(
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
            ._ElastiCubeDependencyFinder__filter_column_matching_tags(
                [fake_non_matching_tag, fake_column_matching_tag],
                column='test-column')

        # then
        self.assertEqual(1, len(tags))
        self.assertEqual(fake_column_matching_tag, tags[0])

    def test_sort_tags_by_schema_should_return_sorted_tags(self):
        # given
        fields_column = datacatalog.ColumnSchema()
        fields_column.column = 'fields'

        field1_column = datacatalog.ColumnSchema()
        field1_column.column = 'field1'

        field2_column = datacatalog.ColumnSchema()
        field2_column.column = 'field2'

        subfields_column = datacatalog.ColumnSchema()
        subfields_column.column = 'subfields'

        subfield1_column = datacatalog.ColumnSchema()
        subfield1_column.column = 'subfield1'

        fields_column.subcolumns.append(field1_column)
        subfields_column.subcolumns.append(subfield1_column)
        field2_column.subcolumns.append(subfields_column)
        fields_column.subcolumns.append(field2_column)

        schema = datacatalog.Schema()
        schema.columns.append(fields_column)

        fake_column_level_tag_1 = mocks.make_fake_tag(column='fields.field1')
        fake_column_level_tag_2 = mocks.make_fake_tag(column='fields.field2')
        fake_column_level_tag_3 = mocks.make_fake_tag(
            column='fields.field2.subfields.subfield1')

        # when
        tags = self.__finder \
            ._ElastiCubeDependencyFinder__sort_tags_by_schema(
                schema.columns, '',
                [
                    fake_column_level_tag_2,
                    fake_column_level_tag_3,
                    fake_column_level_tag_1
                ])

        # then
        self.assertEqual(3, len(tags))
        self.assertEqual(fake_column_level_tag_1, tags[0])
        self.assertEqual(fake_column_level_tag_2, tags[1])
        self.assertEqual(fake_column_level_tag_3, tags[2])
