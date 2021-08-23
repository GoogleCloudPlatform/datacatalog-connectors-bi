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

from datetime import datetime
import unittest
from unittest import mock

from google.cloud import datacatalog

from google.datacatalog_connectors.sisense.prepare import \
    datacatalog_entry_factory


class DataCatalogEntryFactoryTest(unittest.TestCase):
    __FACTORY_PACKAGE = 'google.datacatalog_connectors.sisense.prepare'
    __FACTORY_MODULE = f'{__FACTORY_PACKAGE}.datacatalog_entry_factory'
    __FACTORY_CLASS = f'{__FACTORY_MODULE}.DataCatalogEntryFactory'
    __PRIVATE_METHOD_PREFIX = f'{__FACTORY_CLASS}._DataCatalogEntryFactory'

    __DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'

    def setUp(self):
        self.__factory = datacatalog_entry_factory.DataCatalogEntryFactory(
            project_id='test-project',
            location_id='test-location',
            entry_group_id='test-entry-group',
            user_specified_system='test-system',
            server_address='https://test.server.com')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__factory.__dict__

        self.assertEqual('test-project',
                         attrs['_DataCatalogEntryFactory__project_id'])
        self.assertEqual('test-location',
                         attrs['_DataCatalogEntryFactory__location_id'])
        self.assertEqual('test-entry-group',
                         attrs['_DataCatalogEntryFactory__entry_group_id'])
        self.assertEqual(
            'test-system',
            attrs['_DataCatalogEntryFactory__user_specified_system'])
        self.assertEqual('https://test.server.com',
                         attrs['_DataCatalogEntryFactory__server_address'])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_schema_for_dashboard')
    def test_make_entry_for_dashboard_should_set_all_available_fields(
            self, mock_make_schema_for_dashboard):

        metadata = {
            '_id': 'a123-b456',
            'oid': 'a123-b457',
            'title': 'Test dashboard',
            'desc': 'Test dashboard description',
            'created': '2019-09-12T16:30:00.005Z',
            'lastUpdated': '2019-09-12T16:31:00.005Z',
        }

        schema = datacatalog.Schema()
        mock_make_schema_for_dashboard.return_value = schema

        entry_id, entry = self.__factory.make_entry_for_dashboard(metadata)

        self.assertEqual('sss_test_server_com_db_a123_b457', entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'sss_test_server_com_db_a123_b457', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('Dashboard', entry.user_specified_type)
        self.assertEqual('Test dashboard', entry.display_name)
        self.assertEqual('Test dashboard description', entry.description)
        self.assertEqual(
            'https://test.server.com/app/main#/dashboards/a123-b457',
            entry.linked_resource)

        created_datetime = datetime.strptime('2019-09-12T16:30:00.005+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            created_datetime.timestamp(),
            entry.source_system_timestamps.create_time.timestamp())

        updated_datetime = datetime.strptime('2019-09-12T16:31:00.005+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            updated_datetime.timestamp(),
            entry.source_system_timestamps.update_time.timestamp())

        mock_make_schema_for_dashboard.assert_called_once_with(metadata)
        self.assertEqual(schema, entry.schema)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_schema_for_dashboard',
                lambda *args: datacatalog.Schema())
    def test_make_entry_for_dashboard_should_succeed_on_missing_created_date(
            self):

        metadata = {
            '_id': 'a123-b456',
            'oid': 'a123-b457',
            'title': 'Test dashboard',
        }

        entry_id, entry = self.__factory.make_entry_for_dashboard(metadata)

        self.assertIsNone(entry.source_system_timestamps.create_time)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_schema_for_dashboard',
                lambda *args: datacatalog.Schema())
    def test_make_entry_for_dashboard_should_use_created_on_no_updated_date(
            self):

        metadata = {
            '_id': 'a123-b456',
            'oid': 'a123-b457',
            'title': 'Test dashboard',
            'created': '2019-09-12T16:30:00.005Z',
        }

        entry_id, entry = self.__factory.make_entry_for_dashboard(metadata)

        created_datetime = datetime.strptime('2019-09-12T16:30:00.005+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            created_datetime.timestamp(),
            entry.source_system_timestamps.update_time.timestamp())

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_column_schema_for_jaql')
    def test_make_schema_for_dashboard_should_make_filters_column(
            self, mock_make_column_schema_for_jaql):

        jaql_metadata = {'datatype': 'datetime', 'title': 'TEST'}
        metadata = {'filters': [{'jaql': jaql_metadata}]}

        column = datacatalog.ColumnSchema()
        mock_make_column_schema_for_jaql.return_value = column

        schema = \
            self.__factory._DataCatalogEntryFactory__make_schema_for_dashboard(
                metadata)

        self.assertEqual('filters', schema.columns[0].column)
        mock_make_column_schema_for_jaql.assert_called_once_with(jaql_metadata)
        self.assertEqual(column, schema.columns[0].subcolumns[0])

    def test_make_schema_for_dashboard_should_skip_if_no_filters(self):
        metadata = {'filters': []}

        schema = \
            self.__factory._DataCatalogEntryFactory__make_schema_for_dashboard(
                metadata)

        self.assertIsNone(schema)

    def test_make_entry_for_folder_should_set_all_available_fields(self):
        metadata = {
            '_id': 'a123-b456',
            'oid': 'a123-b457',
            'name': 'Test folder',
            'created': '2019-09-12T16:30:00.005Z',
            'lastUpdated': '2019-09-12T16:31:00.005Z',
        }

        entry_id, entry = self.__factory.make_entry_for_folder(metadata)

        self.assertEqual('sss_test_server_com_fd_a123_b457', entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'sss_test_server_com_fd_a123_b457', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('Folder', entry.user_specified_type)
        self.assertEqual('Test folder', entry.display_name)
        self.assertEqual('https://test.server.com/app/main#/home/a123-b457',
                         entry.linked_resource)

        created_datetime = datetime.strptime('2019-09-12T16:30:00.005+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            created_datetime.timestamp(),
            entry.source_system_timestamps.create_time.timestamp())

        updated_datetime = datetime.strptime('2019-09-12T16:31:00.005+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            updated_datetime.timestamp(),
            entry.source_system_timestamps.update_time.timestamp())

    def test_make_entry_for_folder_should_succeed_on_missing_oid(self):
        metadata = {
            '_id': 'a123-b456',
            'name': 'Test folder',
            'created': '2019-09-12T16:30:00.005Z',
            'lastUpdated': '2019-09-12T16:31:00.005Z',
        }

        entry_id, entry = self.__factory.make_entry_for_folder(metadata)

        self.assertEqual('', entry.linked_resource)

    def test_make_entry_for_folder_should_succeed_on_missing_created_date(
            self):

        metadata = {
            '_id': 'a123-b456',
            'oid': 'a123-b457',
            'name': 'Test folder',
        }

        entry_id, entry = self.__factory.make_entry_for_folder(metadata)

        self.assertIsNone(entry.source_system_timestamps.create_time)

    def test_make_entry_for_folder_should_use_created_on_missing_updated_date(
            self):

        metadata = {
            '_id': 'a123-b456',
            'oid': 'a123-b457',
            'name': 'Test folder',
            'created': '2019-09-12T16:30:00.005Z',
        }

        entry_id, entry = self.__factory.make_entry_for_folder(metadata)

        created_datetime = datetime.strptime('2019-09-12T16:30:00.005+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            created_datetime.timestamp(),
            entry.source_system_timestamps.update_time.timestamp())

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_schema_for_widget')
    def test_make_entry_for_widget_should_set_all_available_fields(
            self, mock_make_schema_for_widget):

        metadata = {
            '_id': 'a123-b456',
            'oid': 'a123-b457',
            'title': 'Test widget',
            'desc': 'Test widget description',
            'created': '2019-09-12T16:30:00.005Z',
            'lastUpdated': '2019-09-12T16:31:00.005Z',
            'dashboardid': 'a123',
        }

        schema = datacatalog.Schema()
        mock_make_schema_for_widget.return_value = schema

        entry_id, entry = self.__factory.make_entry_for_widget(metadata)

        self.assertEqual('sss_test_server_com_wg_a123_b457', entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'sss_test_server_com_wg_a123_b457', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('Widget', entry.user_specified_type)
        self.assertEqual('Test widget', entry.display_name)
        self.assertEqual('Test widget description', entry.description)
        self.assertEqual(
            'https://test.server.com/app/main#/dashboards/a123'
            '/widgets/a123-b457', entry.linked_resource)

        created_datetime = datetime.strptime('2019-09-12T16:30:00.005+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            created_datetime.timestamp(),
            entry.source_system_timestamps.create_time.timestamp())

        updated_datetime = datetime.strptime('2019-09-12T16:31:00.005+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            updated_datetime.timestamp(),
            entry.source_system_timestamps.update_time.timestamp())

        mock_make_schema_for_widget.assert_called_once_with(metadata)
        self.assertEqual(schema, entry.schema)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_schema_for_widget',
                lambda *args: datacatalog.Schema())
    def test_make_entry_for_widget_should_set_unnamed_on_missing_title(self):
        metadata = {
            '_id': 'a123-b456',
            'oid': 'a123-b457',
        }

        entry_id, entry = self.__factory.make_entry_for_widget(metadata)

        self.assertEqual('Unnamed', entry.display_name)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_schema_for_widget',
                lambda *args: datacatalog.Schema())
    def test_make_entry_for_widget_should_succeed_on_missing_created_date(
            self):

        metadata = {
            '_id': 'a123-b456',
            'oid': 'a123-b457',
            'title': 'Test widget',
        }

        entry_id, entry = self.__factory.make_entry_for_widget(metadata)

        self.assertIsNone(entry.source_system_timestamps.create_time)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_schema_for_widget',
                lambda *args: datacatalog.Schema())
    def test_make_entry_for_widget_should_use_created_on_no_updated_date(self):
        metadata = {
            '_id': 'a123-b456',
            'oid': 'a123-b457',
            'title': 'Test widget',
            'created': '2019-09-12T16:30:00.005Z',
        }

        entry_id, entry = self.__factory.make_entry_for_widget(metadata)

        created_datetime = datetime.strptime('2019-09-12T16:30:00.005+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            created_datetime.timestamp(),
            entry.source_system_timestamps.update_time.timestamp())

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_filters_column_for_widget',
                lambda *args: None)
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_fields_column_for_widget')
    def test_make_schema_for_widget_make_fields_column(
            self, mock_make_fields_column_for_widget):

        metadata = {'metadata': {'panels': [{}]}}

        column = datacatalog.ColumnSchema()
        column.column = 'test'
        mock_make_fields_column_for_widget.return_value = column

        schema = self.__factory\
            ._DataCatalogEntryFactory__make_schema_for_widget(metadata)

        mock_make_fields_column_for_widget.assert_called_once_with(metadata)
        self.assertEqual(column, schema.columns[0])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_filters_column_for_widget')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_fields_column_for_widget',
                lambda *args: None)
    def test_make_schema_for_widget_make_filters_column(
            self, mock_make_filters_column_for_widget):

        metadata = {'metadata': {'panels': [{}]}}

        column = datacatalog.ColumnSchema()
        column.column = 'test'
        mock_make_filters_column_for_widget.return_value = column

        schema = self.__factory\
            ._DataCatalogEntryFactory__make_schema_for_widget(metadata)

        mock_make_filters_column_for_widget.assert_called_once_with(metadata)
        self.assertEqual(column, schema.columns[0])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_column_schema_for_jaql')
    def test_make_fields_column_for_widget_should_return_column(
            self, mock_make_column_schema_for_jaql):

        jaql_metadata = {'datatype': 'datetime', 'title': 'TEST'}
        metadata = {
            'metadata': {
                'panels': [{
                    'name': 'test',
                    'items': [{
                        'jaql': jaql_metadata
                    }]
                }]
            }
        }

        column = datacatalog.ColumnSchema()
        mock_make_column_schema_for_jaql.return_value = column

        schema = self.__factory\
            ._DataCatalogEntryFactory__make_fields_column_for_widget(metadata)

        mock_make_column_schema_for_jaql.assert_called_once_with(jaql_metadata)
        self.assertEqual(column, schema.subcolumns[0])

    def test_make_fields_column_for_widget_should_skip_if_no_panels(self):
        metadata = {'metadata': {'panels': []}}

        schema = self.__factory\
            ._DataCatalogEntryFactory__make_fields_column_for_widget(metadata)

        self.assertIsNone(schema)

    def test_make_fields_column_for_widget_should_skip_if_only_filters(self):
        metadata = {'metadata': {'panels': [{'name': 'filters', 'items': []}]}}

        schema = self.__factory\
            ._DataCatalogEntryFactory__make_fields_column_for_widget(metadata)

        self.assertIsNone(schema)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_column_schema_for_jaql')
    def test_make_filters_column_for_widget_should_return_column(
            self, mock_make_column_schema_for_jaql):

        jaql_metadata = {'datatype': 'datetime', 'title': 'TEST'}
        metadata = {
            'metadata': {
                'panels': [{
                    'name': 'filters',
                    'items': [{
                        'jaql': jaql_metadata
                    }]
                }]
            }
        }

        column = datacatalog.ColumnSchema()
        mock_make_column_schema_for_jaql.return_value = column

        schema = self.__factory\
            ._DataCatalogEntryFactory__make_filters_column_for_widget(metadata)

        mock_make_column_schema_for_jaql.assert_called_once_with(jaql_metadata)
        self.assertEqual(column, schema.subcolumns[0])

    def test_make_filters_column_for_widget_should_skip_if_no_panels(self):
        metadata = {'metadata': {'panels': []}}

        schema = self.__factory\
            ._DataCatalogEntryFactory__make_filters_column_for_widget(metadata)

        self.assertIsNone(schema)

    def test_make_filters_column_for_widget_should_skip_if_no_filters(self):
        metadata = {'metadata': {'panels': [{'name': 'test', 'items': []}]}}

        schema = self.__factory\
            ._DataCatalogEntryFactory__make_filters_column_for_widget(metadata)

        self.assertIsNone(schema)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}'
                f'__make_column_schema_for_jaql_formula')
    @mock.patch(f'{__FACTORY_PACKAGE}.sisense_connector_strings_helper'
                f'.SisenseConnectorStringsHelper.format_column_name',
                lambda *args: args[0])
    def test_make_column_schema_for_jaql_should_set_all_available_fields(
            self, mock_make_column_schema_for_jaql_formula):

        metadata = {'datatype': 'datetime', 'title': 'TEST'}

        column = datacatalog.ColumnSchema()
        column.column = 'formula'
        mock_make_column_schema_for_jaql_formula.return_value = column

        column = self.__factory\
            ._DataCatalogEntryFactory__make_column_schema_for_jaql(metadata)

        self.assertEqual('TEST', column.column)
        self.assertEqual('datetime', column.type)
        mock_make_column_schema_for_jaql_formula.assert_called_once_with(
            metadata)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}'
                f'__make_column_schema_for_jaql_formula', lambda *args: None)
    @mock.patch(f'{__FACTORY_PACKAGE}.sisense_connector_strings_helper'
                f'.SisenseConnectorStringsHelper.format_column_name',
                lambda *args: args[0])
    def test_make_column_schema_for_jaql_should_use_type_field_fallback(self):
        metadata = {'type': 'datetime', 'title': 'TEST'}

        column = self.__factory\
            ._DataCatalogEntryFactory__make_column_schema_for_jaql(metadata)

        self.assertEqual('TEST', column.column)
        self.assertEqual('datetime', column.type)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_column_schema_for_jaql')
    def test_make_column_schema_for_jaql_formula_should_process_all_fields(
            self, mock_make_column_schema_for_jaql):

        metadata = {
            'formula': 'AVG([OrderDateYears], [CountOrderID])',
            'context': {
                '[OrderDateYears]': {
                    'dim': '[Orders.OrderDate (Calendar)]',
                    'level': 'years',
                },
                '[CountOrderID]': {
                    'dim': '[Orders.OrderID]',
                    'agg': 'count',
                },
            },
            'title': 'JAQL Formula test',
        }

        column = datacatalog.ColumnSchema()
        mock_make_column_schema_for_jaql.return_value = column

        column = self.__factory \
            ._DataCatalogEntryFactory__make_column_schema_for_jaql_formula(
                metadata)

        self.assertEqual('formula', column.column)
        self.assertEqual('array', column.type)
        self.assertEqual('The JAQL Formula test formula', column.description)
        self.assertEqual(2, len(column.subcolumns))

    def test_make_column_schema_for_jaql_formula_should_skip_if_no_formula(
            self):

        metadata = {
            'context': {
                '[OrderDateYears]': {
                    'dim': '[Orders.OrderDate (Calendar)]',
                },
            },
        }

        column = self.__factory \
            ._DataCatalogEntryFactory__make_column_schema_for_jaql_formula(
                metadata)

        self.assertIsNone(column)

    def test_make_column_schema_for_jaql_formula_should_skip_if_no_context(
            self):

        metadata = {
            'formula': 'AVG([OrderDateYears], [CountOrderID])',
        }

        column = self.__factory \
            ._DataCatalogEntryFactory__make_column_schema_for_jaql_formula(
                metadata)

        self.assertIsNone(column)
