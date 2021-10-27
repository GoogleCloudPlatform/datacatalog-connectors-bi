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

from google.datacatalog_connectors.sisense.addons import \
    elasticube_dependency_finder as base_finder

from . import elasticube_dependency_mocks as mocks


class ElastiCubeDependencyFinderTest(unittest.TestCase):
    __ADDONS_PACKAGE = 'google.datacatalog_connectors.sisense.addons'
    __FINDER_MODULE = f'{__ADDONS_PACKAGE}.elasticube_dependency_finder'
    __FINDER_CLASS = f'{__FINDER_MODULE}.ElastiCubeDependencyFinder'
    __PRIVATE_METHOD_PREFIX = f'{__FINDER_CLASS}._ElastiCubeDependencyFinder'

    @mock.patch(f'{__FINDER_MODULE}.commons.DataCatalogFacade')
    def setUp(self, mock_facade):
        self.__finder = base_finder.ElastiCubeDependencyFinder(
            project_id='test-project')
        self.__datacatalog_facade = mock_facade.return_value

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
        query = self.__finder._make_query(['Dashboard'])

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
        query = self.__finder._make_query(['Dashboard'],
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
        entry_names = self.__finder._search_catalog('tag:id:test-dashboard')

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
        entries = self.__finder._get_entries(['fake_entries/some-entry'])

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
        tags = self.__finder._list_tags(['fake_entries/some-entry'])

        # then
        self.assertEqual(1, len(tags))
        self.assertEqual(fake_tag, tags['fake_entries/some-entry'][0])
        self.__datacatalog_facade.list_tags.assert_called_once_with(
            'fake_entries/some-entry')

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
        tags = self.__finder._sort_tags_by_schema(schema.columns, '', [
            fake_column_level_tag_2, fake_column_level_tag_3,
            fake_column_level_tag_1
        ])

        # then
        self.assertEqual(3, len(tags))
        self.assertEqual(fake_column_level_tag_1, tags[0])
        self.assertEqual(fake_column_level_tag_2, tags[1])
        self.assertEqual(fake_column_level_tag_3, tags[2])
