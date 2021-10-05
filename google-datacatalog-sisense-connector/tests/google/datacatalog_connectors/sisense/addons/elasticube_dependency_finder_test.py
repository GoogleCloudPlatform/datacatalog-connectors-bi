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
from typing import Tuple

from google.cloud import datacatalog
from google.cloud.datacatalog import Entry, SearchCatalogResult, Tag

from google.datacatalog_connectors.sisense import addons


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

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_relevant_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__list_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__get_entries')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_query')
    def test_run_should_return_assembled_entries_and_tags(
            self, mock_make_query, mock_get_entries, mock_list_tags,
            mock_filter_relevant_tags):

        fake_query = 'tag:table:"test-table"'
        mock_make_query.return_value = fake_query

        self.__datacatalog_facade.search_catalog.return_value = [
            self.__make_fake_search_result('table-entry')
        ]

        fake_entry = self.__make_fake_entry('table-entry')
        mock_get_entries.return_value = {
            'fake_entries/table-entry': fake_entry
        }

        fake_tag = self.__make_fake_tag()
        mock_list_tags.return_value = {'fake_entries/table-entry': [fake_tag]}

        mock_filter_relevant_tags.return_value = [fake_tag]

        results = self.__finder.find(table='test-table')

        self.assertEqual(1, len(results))
        self.assertTrue('fake_entries/table-entry' in results)
        self.assertEqual((fake_entry, [fake_tag]),
                         results['fake_entries/table-entry'])

        self.__datacatalog_facade.search_catalog.assert_called_once_with(
            fake_query)

        mock_make_query.assert_called_once_with(datasource=None,
                                                table='test-table',
                                                column=None)
        mock_get_entries.assert_called_once_with(['fake_entries/table-entry'])
        mock_list_tags.assert_called_once_with(['fake_entries/table-entry'])
        mock_filter_relevant_tags.assert_called_once_with(
            fake_entry, [fake_tag], 'test-table', None)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_query')
    def test_run_should_return_empty_dict_if_no_search_results(
            self, mock_make_query):

        fake_query = 'tag:table:"test-table"'
        mock_make_query.return_value = fake_query

        self.__datacatalog_facade.search_catalog.return_value = []

        results = self.__finder.find(table='test-table')

        self.assertEqual(0, len(results))

        self.__datacatalog_facade.search_catalog.assert_called_once_with(
            fake_query)

    def test_make_query_should_raise_exception_if_empty_args(self):
        self.assertRaises(
            Exception, self.__finder._ElastiCubeDependencyFinder__make_query)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_column_search_term',
                lambda *args: None)
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_table_search_term',
                lambda *args: None)
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_datasource_search_term',
                lambda *args: None)
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_type_search_term')
    def test_make_query_should_handle_static_search_terms(
            self, mock_make_type_search_term):

        mock_make_type_search_term.return_value = \
            '(type=Dashboard OR type=Widget)'

        query = self.__finder._ElastiCubeDependencyFinder__make_query(
            table='test-table')

        self.assertTrue('system=Sisense' in query)
        mock_make_type_search_term.assert_called_once_with(
            ['Dashboard', 'Widget'])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_column_search_term')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_table_search_term')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_datasource_search_term')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_type_search_term',
                lambda *args: '(type=Dashboard OR type=Widget')
    def test_make_query_should_handle_dynamic_search_terms(
            self, mock_make_datasource_search_term,
            mock_make_table_search_term, mock_make_column_search_term):

        mock_make_datasource_search_term.return_value = \
            'tag:datasource:"test-datasource"'
        mock_make_table_search_term.return_value = 'tag:table:"test-table"'
        mock_make_column_search_term.return_value = 'tag:column:"test-column"'

        query = self.__finder._ElastiCubeDependencyFinder__make_query(
            datasource='test-datasource',
            table='test-table',
            column='test-column')

        self.assertTrue('tag:datasource:"test-datasource"' in query)
        self.assertTrue('tag:table:"test-table"' in query)
        self.assertTrue('tag:column:"test-column"' in query)

        mock_make_datasource_search_term.assert_called_once_with(
            'test-datasource')
        mock_make_table_search_term.assert_called_once_with('test-table')
        mock_make_column_search_term.assert_called_once_with('test-column')

    def test_make_type_search_term_should_return_none_if_no_types(self):
        query = self\
            .__finder._ElastiCubeDependencyFinder__make_type_search_term([])

        self.assertIsNone(query)

    def test_make_type_search_term_should_not_use_or_for_single_type(self):
        query = \
            self.__finder._ElastiCubeDependencyFinder__make_type_search_term(
                ['Dashboard'])

        self.assertEqual('(type=Dashboard)', query)

    def test_make_type_search_term_should_use_or_for_multiple_types(self):
        query = \
            self.__finder._ElastiCubeDependencyFinder__make_type_search_term(
                ['Dashboard', 'Widget'])

        self.assertEqual('(type=Dashboard OR type=Widget)', query)

    def test_make_datasource_search_term_should_return_none_if_no_datasource(
            self):

        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_datasource_search_term()

        self.assertIsNone(query)

    def test_make_datasource_search_term_should_return_tag_qualifier(self):
        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_datasource_search_term(
                'test-datasource')

        self.assertEqual('tag:datasource:"test-datasource"', query)

    def test_make_table_search_term_should_return_none_if_no_table(self):
        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_table_search_term()

        self.assertIsNone(query)

    def test_make_table_search_term_should_return_tag_qualifier(self):
        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_table_search_term('test-table')

        self.assertEqual('tag:table:"test-table"', query)

    def test_make_column_search_term_should_return_none_if_no_column(self):
        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_column_search_term()

        self.assertIsNone(query)

    def test_make_column_search_term_should_return_tag_qualifier(self):
        query = self.__finder\
            ._ElastiCubeDependencyFinder__make_column_search_term(
                'test-column')

        self.assertEqual('tag:column:"test-column"', query)

    def test_get_entries_should_return_dict(self):
        fake_entry = self.__make_fake_entry('some-entry')
        self.__datacatalog_facade.get_entry.return_value = fake_entry

        entries = self.__finder._ElastiCubeDependencyFinder__get_entries(
            ['fake_entries/some-entry'])

        self.assertEqual(1, len(entries))
        self.assertEqual(fake_entry, entries['fake_entries/some-entry'])

        self.__datacatalog_facade.get_entry.assert_called_once_with(
            'fake_entries/some-entry')

    def test_list_tags_should_return_dict(self):
        fake_tag = self.__make_fake_tag()
        self.__datacatalog_facade.list_tags.return_value = [fake_tag]

        tags = self.__finder._ElastiCubeDependencyFinder__list_tags(
            ['fake_entries/some-entry'])

        self.assertEqual(1, len(tags))
        self.assertEqual(fake_tag, tags['fake_entries/some-entry'][0])

        self.__datacatalog_facade.list_tags.assert_called_once_with(
            'fake_entries/some-entry')

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__sort_tags_by_schema')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}'
                f'__filter_table_column_matching_tags')
    @mock.patch(f'{__FINDER_CLASS}.filter_asset_metadata_tag')
    def test_filter_relevant_tags_should_filter_and_sort_tags(
            self, mock_filter_asset_metadata_tag,
            mock_filter_table_column_matching_tags, mock_sort_tags_by_schema):

        fake_entry = self.__make_fake_entry('some-entry')
        fake_asset_metadata_tag = self.__make_fake_tag()
        fake_table_matching_tag = self.__make_fake_tag()

        mock_filter_asset_metadata_tag.return_value = fake_asset_metadata_tag
        mock_filter_table_column_matching_tags.return_value = [
            fake_table_matching_tag
        ]
        mock_sort_tags_by_schema.return_value = [fake_table_matching_tag]

        tags = self.__finder._ElastiCubeDependencyFinder__filter_relevant_tags(
            fake_entry, [fake_asset_metadata_tag, fake_table_matching_tag],
            table='test-table',
            column='test-column')

        self.assertEqual(2, len(tags))
        self.assertEqual(fake_asset_metadata_tag, tags[0])
        self.assertEqual(fake_table_matching_tag, tags[1])

        mock_filter_asset_metadata_tag.assert_called_once_with(
            [fake_asset_metadata_tag, fake_table_matching_tag])
        mock_filter_table_column_matching_tags.assert_called_once_with(
            [fake_asset_metadata_tag, fake_table_matching_tag],
            table='test-table',
            column='test-column')
        mock_sort_tags_by_schema.assert_called_once_with(
            [], '', [fake_table_matching_tag])

    def test_filter_asset_metadata_tag_should_return_dashboard_metadata_tag(
            self):

        fake_jaql_related_tag = self.__make_fake_tag(
            template='test/tagTemplates/sisense_jaql_metadata')
        fake_dashboard_metadata_tag = self.__make_fake_tag(
            template='test/tagTemplates/sisense_dashboard_metadata')

        tag = self.__finder.filter_asset_metadata_tag(
            [fake_jaql_related_tag, fake_dashboard_metadata_tag])

        self.assertEqual(fake_dashboard_metadata_tag, tag)

    def test_filter_asset_metadata_tag_should_return_widget_metadata_tag(self):
        fake_jaql_related_tag = self.__make_fake_tag(
            template='test/tagTemplates/sisense_jaql_metadata')
        fake_widget_metadata_tag = self.__make_fake_tag(
            template='test/tagTemplates/sisense_widget_metadata')

        tag = self.__finder.filter_asset_metadata_tag(
            [fake_jaql_related_tag, fake_widget_metadata_tag])

        self.assertEqual(fake_widget_metadata_tag, tag)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_column_matching_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_table_matching_tags')
    @mock.patch(f'{__FINDER_CLASS}.filter_jaql_tags', lambda *args: args)
    def test_filter_table_column_matching_tags_should_return_matching_table(
            self, mock_filter_table_matching_tags,
            mock_filter_column_matching_tags):

        fake_table_matching_tag = self.__make_fake_tag()
        fake_non_matching_tag = self.__make_fake_tag()

        mock_filter_table_matching_tags.return_value = [
            fake_table_matching_tag
        ]

        tags = self.__finder\
            ._ElastiCubeDependencyFinder__filter_table_column_matching_tags(
                [fake_table_matching_tag, fake_non_matching_tag],
                table='test-table')

        self.assertEqual(1, len(tags))
        self.assertEqual(fake_table_matching_tag, tags[0])

        mock_filter_column_matching_tags.assert_called_once_with(
            None, [fake_table_matching_tag, fake_non_matching_tag])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_column_matching_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_table_matching_tags',
                lambda *args: None)
    @mock.patch(f'{__FINDER_CLASS}.filter_jaql_tags', lambda *args: args)
    def test_filter_table_column_matching_tags_should_return_matching_column(
            self, mock_filter_column_matching_tags):

        fake_non_matching_tag = self.__make_fake_tag()
        fake_column_matching_tag = self.__make_fake_tag()

        mock_filter_column_matching_tags.return_value = [
            fake_column_matching_tag
        ]

        tags = self.__finder\
            ._ElastiCubeDependencyFinder__filter_table_column_matching_tags(
                [fake_non_matching_tag, fake_column_matching_tag],
                column='test-column')

        self.assertEqual(1, len(tags))
        self.assertEqual(fake_column_matching_tag, tags[0])

        mock_filter_column_matching_tags.assert_called_once_with(
            'test-column', [fake_non_matching_tag, fake_column_matching_tag])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_column_matching_tags')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__filter_table_matching_tags')
    @mock.patch(f'{__FINDER_CLASS}.filter_jaql_tags', lambda *args: args)
    def test_filter_table_column_matching_tags_should_return_matching_both(
            self, mock_filter_table_matching_tags,
            mock_filter_column_matching_tags):

        fake_table_only_matching_tag = self.__make_fake_tag()
        fake_column_only_matching_tag = self.__make_fake_tag()
        fake_table_column_matching_tag = self.__make_fake_tag()

        mock_filter_table_matching_tags.return_value = [
            fake_table_only_matching_tag, fake_table_column_matching_tag
        ]
        mock_filter_column_matching_tags.return_value = [
            fake_table_column_matching_tag
        ]

        tags = self.__finder\
            ._ElastiCubeDependencyFinder__filter_table_column_matching_tags(
                [
                    fake_table_only_matching_tag,
                    fake_column_only_matching_tag,
                    fake_table_column_matching_tag
                ],
                table='test-table', column='test-column')

        self.assertEqual(1, len(tags))
        self.assertEqual(fake_table_column_matching_tag, tags[0])

        mock_filter_column_matching_tags.assert_called_once_with(
            'test-column',
            [fake_table_only_matching_tag, fake_table_column_matching_tag])

    def test_filter_jaql_tags_should_return_only_jaql_tags(self):

        fake_jaql_related_tag_1 = self.__make_fake_tag(
            template='test/tagTemplates/sisense_jaql_metadata')
        fake_widget_metadata_tag = self.__make_fake_tag(
            template='test/tagTemplates/sisense_widget_metadata')
        fake_jaql_related_tag_2 = self.__make_fake_tag(
            template='test/tagTemplates/sisense_jaql_metadata')

        tags = self.__finder.filter_jaql_tags([
            fake_jaql_related_tag_1, fake_widget_metadata_tag,
            fake_jaql_related_tag_2
        ])

        self.assertEqual(2, len(tags))
        self.assertEqual(fake_jaql_related_tag_1, tags[0])
        self.assertEqual(fake_jaql_related_tag_2, tags[1])

    def test_filter_table_matching_tags_should_none_if_no_table_name(self):
        fake_table_related_tag = self.__make_fake_tag(
            string_fields=(('table', 'test-table'),))

        tags = self.__finder \
            ._ElastiCubeDependencyFinder__filter_table_matching_tags(
                None, [fake_table_related_tag])

        self.assertEqual(0, len(tags))

    def test_filter_table_matching_tags_should_return_only_matching_tags(self):
        fake_non_matching_tag = self.__make_fake_tag(
            string_fields=(('column', 'test-column'),))
        fake_table_matching_tag = self.__make_fake_tag(
            string_fields=(('table', 'test-table'),))

        tags = self.__finder \
            ._ElastiCubeDependencyFinder__filter_table_matching_tags(
                'test-table', [fake_non_matching_tag, fake_table_matching_tag])

        self.assertEqual(1, len(tags))
        self.assertEqual(fake_table_matching_tag, tags[0])

    def test_filter_column_matching_tags_should_none_if_no_column_name(self):
        fake_column_related_tag = self.__make_fake_tag(
            string_fields=(('column', 'test-column'),))

        tags = self.__finder \
            ._ElastiCubeDependencyFinder__filter_column_matching_tags(
                None, [fake_column_related_tag])

        self.assertEqual(0, len(tags))

    def test_filter_column_matching_tags_should_return_only_matching_tags(
            self):

        fake_non_matching_tag = self.__make_fake_tag(
            string_fields=(('table', 'test-table'),))
        fake_column_matching_tag = self.__make_fake_tag(
            string_fields=(('column', 'test-column'),))

        tags = self.__finder \
            ._ElastiCubeDependencyFinder__filter_column_matching_tags(
                'test-column',
                [fake_non_matching_tag, fake_column_matching_tag])

        self.assertEqual(1, len(tags))
        self.assertEqual(fake_column_matching_tag, tags[0])

    def test_sort_tags_by_schema_should_return_sorted_tags(self):
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

        fake_column_level_tag_1 = self.__make_fake_tag(column='fields.field1')
        fake_column_level_tag_2 = self.__make_fake_tag(column='fields.field2')
        fake_column_level_tag_3 = self.__make_fake_tag(
            column='fields.field2.subfields.subfield1')

        tags = self.__finder \
            ._ElastiCubeDependencyFinder__sort_tags_by_schema(
                schema.columns, '',
                [
                    fake_column_level_tag_2,
                    fake_column_level_tag_3,
                    fake_column_level_tag_1
                ])

        self.assertEqual(3, len(tags))
        self.assertEqual(fake_column_level_tag_1, tags[0])
        self.assertEqual(fake_column_level_tag_2, tags[1])
        self.assertEqual(fake_column_level_tag_3, tags[2])

    @classmethod
    def __make_fake_search_result(cls, entry_id: str) -> SearchCatalogResult:
        result = datacatalog.SearchCatalogResult()
        result.relative_resource_name = f'fake_entries/{entry_id}'
        return result

    @classmethod
    def __make_fake_entry(cls, entry_id: str) -> Entry:
        entry = datacatalog.Entry()
        entry.name = f'fake_entries/{entry_id}'
        entry.schema = datacatalog.Schema()
        return entry

    @classmethod
    def __make_fake_tag(cls,
                        template: str = 'template',
                        column: str = None,
                        string_fields: Tuple[Tuple[str, str]] = None) -> Tag:

        tag = datacatalog.Tag()
        tag.template = template

        if column:
            tag.column = column

        if string_fields:
            for string_field in string_fields:
                tag_field = datacatalog.TagField()
                tag_field.string_value = string_field[1]
                tag.fields[string_field[0]] = tag_field

        return tag
