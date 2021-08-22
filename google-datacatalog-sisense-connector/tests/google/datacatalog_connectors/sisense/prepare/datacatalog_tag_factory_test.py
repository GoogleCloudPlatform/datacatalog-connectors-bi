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
    datacatalog_tag_factory


class DataCatalogEntryFactoryTest(unittest.TestCase):
    __FACTORY_PACKAGE = 'google.datacatalog_connectors.sisense.prepare'
    __FACTORY_MODULE = f'{__FACTORY_PACKAGE}.datacatalog_tag_factory'
    __FACTORY_CLASS = f'{__FACTORY_MODULE}.DataCatalogTagFactory'
    __PRIVATE_METHOD_PREFIX = f'{__FACTORY_CLASS}._DataCatalogTagFactory'

    __DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'

    def setUp(self):
        self.__factory = datacatalog_tag_factory.DataCatalogTagFactory(
            'https://test.com')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__factory.__dict__
        self.assertEqual('https://test.com',
                         attrs['_DataCatalogTagFactory__server_address'])

    def test_make_tag_for_dashboard_should_set_all_available_fields(self):
        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_dashboard_metadata'

        metadata = {
            'oid': 'test-dashboard',
            'folderData': {
                'oid': 'parent-folder',
                'name': 'Parent Folder',
            },
            'ownerData': {
                'userName': 'johndoe@test.com',
                'firstName': 'John',
                'lastName': 'Doe',
            },
            'datasource': {
                'title': 'Test Data Source',
            },
            'lastPublish': '2021-05-01T13:00:15.400Z',
            'lastOpened': '2021-05-02T23:00:22.939Z',
        }

        tag = self.__factory.make_tag_for_dashboard(tag_template, metadata)

        self.assertEqual('tagTemplates/sisense_dashboard_metadata',
                         tag.template)

        self.assertEqual('test-dashboard', tag.fields['id'].string_value)
        self.assertEqual('parent-folder', tag.fields['folder_id'].string_value)
        self.assertEqual('Parent Folder',
                         tag.fields['folder_name'].string_value)
        self.assertEqual('johndoe@test.com',
                         tag.fields['owner_username'].string_value)
        self.assertEqual('John Doe', tag.fields['owner_name'].string_value)
        self.assertEqual('Test Data Source',
                         tag.fields['datasource'].string_value)

        publish_datetime = datetime.strptime('2021-05-01T13:00:15.400+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            publish_datetime.timestamp(),
            tag.fields['last_publish'].timestamp_value.timestamp())

        opened_datetime = datetime.strptime('2021-05-02T23:00:22.939+0000',
                                            self.__DATETIME_FORMAT)
        self.assertEqual(opened_datetime.timestamp(),
                         tag.fields['last_opened'].timestamp_value.timestamp())

        self.assertEqual('https://test.com',
                         tag.fields['server_url'].string_value)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_tags_for_jaql')
    def test_make_tags_for_dashboard_filters_should_process_all_filters(
            self, mock_make_tags_for_jaql):

        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_jaql_metadata'

        metadata = {
            'filters': [
                {
                    'jaql': {
                        'datatype': 'text',
                    },
                },
                {
                    'jaql': {
                        'datatype': 'datetime',
                    },
                },
            ],
        }

        mock_make_tags_for_jaql.return_value = [datacatalog.Tag()]

        tags = self.__factory.make_tags_for_dashboard_filters(
            tag_template, metadata)

        self.assertEqual(2, len(tags))
        self.assertEqual(2, mock_make_tags_for_jaql.call_count)

        calls = [
            mock.call(tag_template, {
                'datatype': 'text',
            }, 'filters'),
            mock.call(tag_template, {
                'datatype': 'datetime',
            }, 'filters')
        ]
        mock_make_tags_for_jaql.assert_has_calls(calls)

    def test_make_tags_for_dashboard_filters_should_skip_if_no_filters(self):
        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_jaql_metadata'

        tags = self.__factory.make_tags_for_dashboard_filters(tag_template, {})

        self.assertEqual(0, len(tags))

    def test_make_tag_for_folder_should_set_all_available_fields(self):
        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_folder_metadata'

        metadata = {
            'oid': 'test-folder',
            'name': 'Test folder',
            'ownerData': {
                'userName': 'johndoe@test.com',
                'firstName': 'John',
                'lastName': 'Doe',
            },
            'parentFolderData': {
                'oid': 'parent-folder',
                'name': 'Parent folder',
            },
            'folders': [{
                'oid': 'child-folder',
            }],
            'dashboards': [{
                'oid': 'test-dashboard',
            }],
        }

        tag = self.__factory.make_tag_for_folder(tag_template, metadata)

        self.assertEqual('tagTemplates/sisense_folder_metadata', tag.template)

        self.assertEqual('test-folder', tag.fields['id'].string_value)
        self.assertEqual('parent-folder', tag.fields['parent_id'].string_value)
        self.assertEqual('Parent folder',
                         tag.fields['parent_name'].string_value)
        self.assertEqual('johndoe@test.com',
                         tag.fields['owner_username'].string_value)
        self.assertEqual('John Doe', tag.fields['owner_name'].string_value)
        self.assertEqual(True, tag.fields['has_children'].bool_value)
        self.assertEqual(1, tag.fields['child_count'].double_value)
        self.assertEqual(True, tag.fields['has_dashboards'].bool_value)
        self.assertEqual(1, tag.fields['dashboard_count'].double_value)

        self.assertEqual('https://test.com',
                         tag.fields['server_url'].string_value)

    def test_make_tag_for_widget_should_set_all_available_fields(self):
        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_widget_metadata'

        metadata = {
            'oid': 'test-widget',
            'type': 'indicator',
            'subtype': 'indicator/numeric',
            'dashboardData': {
                'oid': 'dashboard-id',
                'title': 'Dashboard Title',
            },
            'ownerData': {
                'userName': 'johndoe@test.com',
                'firstName': 'John',
                'lastName': 'Doe',
            },
            'datasource': {
                'title': 'Test Data Source',
            },
        }

        tag = self.__factory.make_tag_for_widget(tag_template, metadata)

        self.assertEqual('tagTemplates/sisense_widget_metadata', tag.template)

        self.assertEqual('test-widget', tag.fields['id'].string_value)
        self.assertEqual('indicator', tag.fields['type'].string_value)
        self.assertEqual('indicator/numeric',
                         tag.fields['subtype'].string_value)
        self.assertEqual('dashboard-id',
                         tag.fields['dashboard_id'].string_value)
        self.assertEqual('Dashboard Title',
                         tag.fields['dashboard_title'].string_value)
        self.assertEqual('johndoe@test.com',
                         tag.fields['owner_username'].string_value)
        self.assertEqual('John Doe', tag.fields['owner_name'].string_value)
        self.assertEqual('Test Data Source',
                         tag.fields['datasource'].string_value)
        self.assertEqual('https://test.com',
                         tag.fields['server_url'].string_value)

    def test_make_tag_for_widget_should_set_datasource_from_string(self):
        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_widget_metadata'

        metadata = {
            'dashboardData': {},
            'datasource': 'Test Data Source',
        }

        tag = self.__factory.make_tag_for_widget(tag_template, metadata)

        self.assertEqual('Test Data Source',
                         tag.fields['datasource'].string_value)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_tags_for_jaql')
    def test_make_tags_for_widget_fields_should_process_all_fields(
            self, mock_make_tags_for_jaql):

        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_jaql_metadata'

        metadata = {
            'metadata': {
                'panels': [{
                    'name':
                        'testField',
                    'items': [
                        {
                            'jaql': {
                                'datatype': 'text',
                            },
                        },
                        {
                            'jaql': {
                                'datatype': 'datetime',
                            },
                        },
                    ],
                }],
            },
        }

        mock_make_tags_for_jaql.return_value = [datacatalog.Tag()]

        tags = self.__factory.make_tags_for_widget_fields(
            tag_template, metadata)

        self.assertEqual(2, len(tags))
        self.assertEqual(2, mock_make_tags_for_jaql.call_count)

        calls = [
            mock.call(tag_template, {
                'datatype': 'text',
            }, 'fields'),
            mock.call(tag_template, {
                'datatype': 'datetime',
            }, 'fields')
        ]
        mock_make_tags_for_jaql.assert_has_calls(calls)

    def test_make_tags_for_widget_fields_should_skip_if_no_panels(self):
        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_jaql_metadata'

        tags = self.__factory.make_tags_for_widget_fields(tag_template, {})

        self.assertEqual(0, len(tags))

    def test_make_tags_for_widget_fields_should_skip_if_no_fields(self):
        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_jaql_metadata'

        metadata = {
            'metadata': {
                'panels': [{
                    'name': 'filters',
                }],
            },
        }

        tags = self.__factory.make_tags_for_widget_fields(
            tag_template, metadata)

        self.assertEqual(0, len(tags))

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_tags_for_jaql')
    def test_make_tags_for_widget_filters_should_process_all_filters(
            self, mock_make_tags_for_jaql):

        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_jaql_metadata'

        metadata = {
            'metadata': {
                'panels': [{
                    'name':
                        'filters',
                    'items': [
                        {
                            'jaql': {
                                'datatype': 'text',
                            },
                        },
                        {
                            'jaql': {
                                'datatype': 'datetime',
                            },
                        },
                    ],
                }],
            },
        }

        mock_make_tags_for_jaql.return_value = [datacatalog.Tag()]

        tags = self.__factory.make_tags_for_widget_filters(
            tag_template, metadata)

        self.assertEqual(2, len(tags))
        self.assertEqual(2, mock_make_tags_for_jaql.call_count)

        calls = [
            mock.call(tag_template, {
                'datatype': 'text',
            }, 'filters'),
            mock.call(tag_template, {
                'datatype': 'datetime',
            }, 'filters')
        ]
        mock_make_tags_for_jaql.assert_has_calls(calls)

    def test_make_tags_for_widget_filters_should_skip_if_no_panels(self):
        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_jaql_metadata'

        tags = self.__factory.make_tags_for_widget_filters(tag_template, {})

        self.assertEqual(0, len(tags))

    def test_make_tags_for_widget_filters_should_skip_if_no_filters(self):
        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_jaql_metadata'

        metadata = {
            'metadata': {
                'panels': [{
                    'name': 'testField',
                }],
            },
        }

        tags = self.__factory.make_tags_for_widget_filters(
            tag_template, metadata)

        self.assertEqual(0, len(tags))

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_tags_for_jaql_formula')
    def test_make_tags_for_jaql_should_set_all_available_fields(
            self, mock_make_tags_for_jaql_formula):

        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_jaql_metadata'

        metadata = {
            'dim': '[table_a.column_a]',
            'formula': 'AVG([ODY], [COID])',
            'context': {
                '[ODY]': {
                    'dim': '[Orders.OrderDate (Calendar)]',
                    'title': 'OrderDateYears',
                },
                '[COID]': {
                    'dim': '[Orders.OrderID]',
                    'title': 'CountOrderID',
                },
            },
            'agg': 'avg',
            'title': 'TEST',
        }

        mock_make_tags_for_jaql_formula.return_value = []

        tags = self.__factory._DataCatalogTagFactory__make_tags_for_jaql(
            tag_template, metadata, 'test')

        self.assertEqual(1, len(tags))

        tag = tags[0]
        self.assertEqual('tagTemplates/sisense_jaql_metadata', tag.template)

        self.assertEqual('table_a', tag.fields['table'].string_value)
        self.assertEqual('column_a', tag.fields['column'].string_value)
        self.assertEqual('[table_a.column_a]',
                         tag.fields['dimension'].string_value)
        self.assertEqual('AVG([OrderDateYears], [CountOrderID])',
                         tag.fields['formula'].string_value)
        self.assertEqual('avg', tag.fields['aggregation'].string_value)
        self.assertEqual('https://test.com',
                         tag.fields['server_url'].string_value)

        mock_make_tags_for_jaql_formula.assert_called_once()

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_tags_for_jaql_formula',
                lambda *args: [])
    def test_make_tags_for_jaql_should_read_table_and_column_fields(self):
        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_jaql_metadata'

        metadata = {
            'table': 'table_a',
            'column': 'column_a',
            'dim': '[table_b.column_b]',
            'title': 'TEST',
        }

        tags = self.__factory._DataCatalogTagFactory__make_tags_for_jaql(
            tag_template, metadata, 'test')

        self.assertEqual(1, len(tags))

        tag = tags[0]
        self.assertEqual('tagTemplates/sisense_jaql_metadata', tag.template)

        # The ``table`` field takes priority over ``dim``.
        self.assertEqual('table_a', tag.fields['table'].string_value)
        # The ``column`` field takes priority over ``dim``.
        self.assertEqual('column_a', tag.fields['column'].string_value)
        self.assertEqual('[table_b.column_b]',
                         tag.fields['dimension'].string_value)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_tags_for_jaql_formula',
                lambda *args: [])
    def test_make_tags_for_jaql_should_fulfill_formula_as_is_if_no_context(
            self):

        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_jaql_metadata'

        metadata = {
            'formula': 'AVG([ODY], [COID])',
            'title': 'TEST',
        }

        tags = self.__factory._DataCatalogTagFactory__make_tags_for_jaql(
            tag_template, metadata, 'test')

        tag = tags[0]
        self.assertEqual('AVG([ODY], [COID])',
                         tag.fields['formula'].string_value)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_tags_for_jaql')
    def test_make_tags_for_jaql_formula_should_process_all_fields(
            self, mock_make_tags_for_jaql):

        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_jaql_metadata'

        metadata = {
            'formula': 'AVG([ODY], [COID])',
            'context': {
                '[ODY]': {
                    'dim': '[Orders.OrderDate (Calendar)]',
                },
                '[COID]': {
                    'dim': '[Orders.OrderID]',
                },
            },
        }

        tag = datacatalog.Tag()
        mock_make_tags_for_jaql.return_value = [tag]

        tags = self.__factory \
            ._DataCatalogTagFactory__make_tags_for_jaql_formula(
                tag_template, metadata, 'JAQL Formula test')

        self.assertEqual(2, len(tags))
        self.assertEqual(2, mock_make_tags_for_jaql.call_count)

        calls = [
            mock.call(tag_template, {
                'dim': '[Orders.OrderDate (Calendar)]',
            }, 'JAQL Formula test.formula'),
            mock.call(tag_template, {
                'dim': '[Orders.OrderID]',
            }, 'JAQL Formula test.formula')
        ]
        mock_make_tags_for_jaql.assert_has_calls(calls)

    def test_make_tags_for_jaql_formula_should_skip_if_no_formula(self):
        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_jaql_metadata'

        metadata = {
            'context': {
                '[ODY]': {
                    'dim': '[Orders.OrderDate (Calendar)]',
                },
            },
        }

        tags = self.__factory \
            ._DataCatalogTagFactory__make_tags_for_jaql_formula(
                tag_template, metadata, 'JAQL Formula test')

        self.assertEqual(0, len(tags))
