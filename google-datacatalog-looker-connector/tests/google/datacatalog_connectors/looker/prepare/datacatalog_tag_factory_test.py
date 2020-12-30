#!/usr/bin/python
#
# Copyright 2020 Google LLC
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
import json
import unittest

from looker_sdk import models
from looker_sdk.rtl import serialize

from google.datacatalog_connectors.looker import entities, prepare
from google.datacatalog_connectors.looker.prepare import \
    datacatalog_tag_factory


class DataCatalogEntryFactoryTest(unittest.TestCase):
    __DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S%z'

    def setUp(self):
        self.__tag_template_factory = \
            prepare.DataCatalogTagTemplateFactory(
                'test-project', 'test-location')
        self.__factory = datacatalog_tag_factory.DataCatalogTagFactory(
            'https://test.server.com')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__factory.__dict__
        self.assertEqual('https://test.server.com',
                         attrs['_DataCatalogTagFactory__instance_url'])

    def test_make_tag_for_dashboard_should_set_all_available_fields(self):
        tag_template = \
            self.__tag_template_factory.make_tag_template_for_dashboard()

        dashboard_data = {
            'id': 'a123-b456',
            'description': 'Test description',
            'space': {
                'id': 'test-folder',
                'name': 'Test folder',
                'parent_id': '',
            },
            'hidden': True,
            'user_id': 10,
            'view_count': 200,
            'favorite_count': 3,
            'last_accessed_at': '2019-09-12T16:30:00.000Z',
            'last_viewed_at': '2019-09-12T16:35:00.000Z',
            'deleted': True,
            'deleted_at': '2019-09-12T16:40:00.000Z',
            'deleter_id': 12,
        }

        tag = self.__factory.make_tag_for_dashboard(
            tag_template,
            serialize.deserialize31(data=json.dumps(dashboard_data),
                                    structure=models.Dashboard))

        self.assertEqual(tag_template.name, tag.template)

        self.assertEqual('a123-b456', tag.fields['id'].string_value)
        self.assertEqual('Test description',
                         tag.fields['description'].string_value)
        self.assertEqual('test-folder', tag.fields['folder_id'].string_value)
        self.assertEqual('Test folder', tag.fields['folder_name'].string_value)
        self.assertEqual(True, tag.fields['is_hidden'].bool_value)
        self.assertEqual(10, tag.fields['user_id'].double_value)
        self.assertEqual(200, tag.fields['view_count'].double_value)
        self.assertEqual(3, tag.fields['favorite_count'].double_value)

        last_accessed_at = self.__parse_datetime('2019-09-12T16:30:00+0000')
        self.assertEqual(
            last_accessed_at.timestamp(),
            tag.fields['last_accessed_at'].timestamp_value.timestamp())

        last_viewed_at = self.__parse_datetime('2019-09-12T16:35:00+0000')
        self.assertEqual(
            last_viewed_at.timestamp(),
            tag.fields['last_viewed_at'].timestamp_value.timestamp())

        self.assertEqual(True, tag.fields['is_deleted'].bool_value)

        deleted_at = self.__parse_datetime('2019-09-12T16:40:00+0000')
        self.assertEqual(deleted_at.timestamp(),
                         tag.fields['deleted_at'].timestamp_value.timestamp())

        self.assertEqual(12, tag.fields['deleter_id'].double_value)
        self.assertEqual('https://test.server.com',
                         tag.fields['instance_url'].string_value)

    def test_make_tag_for_dashboard_tile_should_set_all_available_fields(self):
        tag_template = self.__tag_template_factory\
            .make_tag_template_for_dashboard_element()

        dashboard_element_data = {
            'id': '196',
            'type': 'vis',
            'dashboard_id': 'a123-b456',
            'look_id': '25',
            'look': {
                'title': 'Test look',
            },
            'lookml_link_id': 'Test link ID',
            'query_id': 837,
        }

        dashboard_data = {
            'title': 'Test dashboard',
        }

        tag = self.__factory.make_tag_for_dashboard_element(
            tag_template,
            serialize.deserialize31(data=json.dumps(dashboard_element_data),
                                    structure=models.DashboardElement),
            serialize.deserialize31(data=json.dumps(dashboard_data),
                                    structure=models.Dashboard))

        self.assertEqual(tag_template.name, tag.template)

        self.assertEqual('196', tag.fields['id'].string_value)
        self.assertEqual('vis', tag.fields['type'].string_value)
        self.assertEqual('a123-b456', tag.fields['dashboard_id'].string_value)
        self.assertEqual('Test dashboard',
                         tag.fields['dashboard_title'].string_value)
        self.assertEqual('25', tag.fields['look_id'].string_value)
        self.assertEqual('Test look', tag.fields['look_title'].string_value)
        self.assertEqual('Test link ID',
                         tag.fields['lookml_link_id'].string_value)
        self.assertEqual(837, tag.fields['query_id'].double_value)
        self.assertEqual('https://test.server.com',
                         tag.fields['instance_url'].string_value)

    def test_make_tag_for_dashboard_tile_result_maker_should_succeed(self):
        tag_template = self.__tag_template_factory\
            .make_tag_template_for_dashboard_element()

        dashboard_element_data = {
            'id': '196',
            'type': 'vis',
            'dashboard_id': 'a123-b456',
            'result_maker': {
                'query_id': 837,
            },
        }

        tag = self.__factory.make_tag_for_dashboard_element(
            tag_template,
            serialize.deserialize31(data=json.dumps(dashboard_element_data),
                                    structure=models.DashboardElement), None)

        self.assertEqual(837, tag.fields['query_id'].double_value)

    def test_make_tag_for_folder_should_set_all_available_fields(self):
        tag_template = \
            self.__tag_template_factory.make_tag_template_for_folder()

        folder_data = {
            'id': 'a123-b456',
            'name': 'Test name',
            'parent_id': 'test-parent',
            'child_count': 10,
            'dashboards': [{}, {}],
            'looks': [{}],
        }

        tag = self.__factory.make_tag_for_folder(
            tag_template,
            serialize.deserialize31(data=json.dumps(folder_data),
                                    structure=models.Folder))

        self.assertEqual(tag_template.name, tag.template)

        self.assertEqual('a123-b456', tag.fields['id'].string_value)
        self.assertEqual('Test name', tag.fields['name'].string_value)
        self.assertEqual('test-parent', tag.fields['parent_id'].string_value)
        self.assertEqual(True, tag.fields['has_children'].bool_value)
        self.assertEqual(10, tag.fields['children_count'].double_value)
        self.assertEqual(True, tag.fields['has_dashboards'].bool_value)
        self.assertEqual(2, tag.fields['dashboards_count'].double_value)
        self.assertEqual(True, tag.fields['has_looks'].bool_value)
        self.assertEqual(1, tag.fields['looks_count'].double_value)
        self.assertEqual('https://test.server.com',
                         tag.fields['instance_url'].string_value)

    def test_make_tag_for_folder_should_set_empty_meaning_values(self):
        tag_template = \
            self.__tag_template_factory.make_tag_template_for_folder()

        folder_data = {
            'id': 'a123-b456',
            'name': 'Test name',
            'parent_id': 'test-parent',
            'child_count': 0,
            'dashboards': [],
            'looks': [],
        }

        tag = self.__factory.make_tag_for_folder(
            tag_template,
            serialize.deserialize31(data=json.dumps(folder_data),
                                    structure=models.Folder))

        self.assertEqual(False, tag.fields['has_children'].bool_value)
        self.assertEqual(0, tag.fields['children_count'].double_value)
        self.assertEqual(False, tag.fields['has_dashboards'].bool_value)
        self.assertEqual(0, tag.fields['dashboards_count'].double_value)
        self.assertEqual(False, tag.fields['has_looks'].bool_value)
        self.assertEqual(0, tag.fields['looks_count'].double_value)

    def test_make_tag_for_look_should_set_all_available_fields(self):
        tag_template = self.__tag_template_factory.make_tag_template_for_look()

        look_data = {
            'id': 5,
            'description': 'Test description',
            'space': {
                'id': 'test-folder',
                'name': 'Test folder',
                'parent_id': '',
            },
            'public': True,
            'user_id': 10,
            'last_updater_id': 12,
            'query_id': 33,
            'short_url': 'short/url',
            'public_url': 'public/url',
            'excel_file_url': 'excel-file/url',
            'google_spreadsheet_formula': '+formula',
            'view_count': 200,
            'favorite_count': 3,
            'last_accessed_at': '2019-09-12T16:30:00.000Z',
            'last_viewed_at': '2019-09-12T16:35:00.000Z',
            'deleted': True,
            'deleted_at': '2019-09-12T16:40:00.000Z',
            'deleter_id': 12,
        }

        tag = self.__factory.make_tag_for_look(
            tag_template,
            serialize.deserialize31(data=json.dumps(look_data),
                                    structure=models.Look))

        self.assertEqual(tag_template.name, tag.template)

        self.assertEqual(5, tag.fields['id'].double_value)
        self.assertEqual('Test description',
                         tag.fields['description'].string_value)
        self.assertEqual('test-folder', tag.fields['folder_id'].string_value)
        self.assertEqual('Test folder', tag.fields['folder_name'].string_value)
        self.assertEqual(True, tag.fields['is_public'].bool_value)
        self.assertEqual(10, tag.fields['user_id'].double_value)
        self.assertEqual(12, tag.fields['last_updater_id'].double_value)
        self.assertEqual('short/url', tag.fields['short_url'].string_value)
        self.assertEqual('public/url', tag.fields['public_url'].string_value)
        self.assertEqual('excel-file/url',
                         tag.fields['excel_file_url'].string_value)
        self.assertEqual('+formula',
                         tag.fields['google_spreadsheet_formula'].string_value)
        self.assertEqual(200, tag.fields['view_count'].double_value)
        self.assertEqual(3, tag.fields['favorite_count'].double_value)

        last_accessed_at = self.__parse_datetime('2019-09-12T16:30:00+0000')
        self.assertEqual(
            last_accessed_at.timestamp(),
            tag.fields['last_accessed_at'].timestamp_value.timestamp())

        last_viewed_at = self.__parse_datetime('2019-09-12T16:35:00+0000')
        self.assertEqual(
            last_viewed_at.timestamp(),
            tag.fields['last_viewed_at'].timestamp_value.timestamp())

        self.assertEqual(True, tag.fields['is_deleted'].bool_value)

        deleted_at = self.__parse_datetime('2019-09-12T16:40:00+0000')
        self.assertEqual(deleted_at.timestamp(),
                         tag.fields['deleted_at'].timestamp_value.timestamp())

        self.assertEqual(12, tag.fields['deleter_id'].double_value)
        self.assertEqual('https://test.server.com',
                         tag.fields['instance_url'].string_value)

    def test_make_tag_for_look_should_set_url_field_if_available(self):
        tag_template = self.__tag_template_factory.make_tag_template_for_look()

        look_data = {
            'space': {
                'id': 'test-folder',
                'name': 'Test folder',
                'parent_id': '',
            },
            'url': 'test/url',
        }

        tag = self.__factory.make_tag_for_look(
            tag_template,
            serialize.deserialize31(data=json.dumps(look_data),
                                    structure=models.LookWithQuery))

        self.assertEqual('test/url', tag.fields['url'].string_value)

    def test_make_tag_for_query_should_set_all_available_fields(self):
        tag_template = \
            self.__tag_template_factory.make_tag_template_for_query()

        query_data = {
            'id': 5,
            'fields': [
                'field_a',
                'field_b',
            ],
            'pivots': [
                'pivot_a',
                'pivot_b',
            ],
            'sorts': [
                'sort_a',
                'sort_b',
            ],
            'runtime': 0.5,
            'client_id': 'test-client-id',
            'query_timezone': 'America/Sao_Paulo',
            'model': 'test-model',
            'view': 'test-view',
        }

        explore_data = {
            'project_name': 'test-project',
            'connection_name': 'test-connection',
        }

        connection_data = {
            'host': '127.0.0.1',
            'database': 'test_database',
            'dialect_name': 'bigquery_dialect',
            'username': 'user@database'
        }

        assembled_metadata = entities.AssembledQueryMetadata(
            serialize.deserialize31(data=json.dumps(query_data),
                                    structure=models.Query), 'select *',
            serialize.deserialize31(data=json.dumps(explore_data),
                                    structure=models.LookmlModelExplore),
            serialize.deserialize31(data=json.dumps(connection_data),
                                    structure=models.DBConnection))

        tag = self.__factory.make_tag_for_query(tag_template,
                                                assembled_metadata)

        self.assertEqual(tag_template.name, tag.template)

        self.assertEqual(5, tag.fields['id'].double_value)
        self.assertEqual('field_a, field_b', tag.fields['fields'].string_value)
        self.assertEqual('pivot_a, pivot_b', tag.fields['pivots'].string_value)
        self.assertEqual('sort_a, sort_b', tag.fields['sorts'].string_value)
        self.assertEqual(0.5, tag.fields['runtime'].double_value)
        self.assertEqual('test-client-id',
                         tag.fields['client_id'].string_value)
        self.assertEqual('America/Sao_Paulo',
                         tag.fields['query_timezone'].string_value)
        self.assertEqual('test-model', tag.fields['lookml_model'].string_value)
        self.assertEqual('test-view', tag.fields['explore_name'].string_value)
        self.assertEqual('select *', tag.fields['sql'].string_value)
        self.assertEqual('test-project',
                         tag.fields['lookml_project'].string_value)
        self.assertEqual('test-connection',
                         tag.fields['connection'].string_value)
        self.assertEqual('127.0.0.1', tag.fields['host'].string_value)
        self.assertEqual('test_database', tag.fields['database'].string_value)
        self.assertEqual('bigquery_dialect',
                         tag.fields['connection_dialect'].string_value)
        self.assertEqual('user@database',
                         tag.fields['connection_username'].string_value)

        self.assertEqual('https://test.server.com',
                         tag.fields['instance_url'].string_value)

    def test_make_tag_for_query_no_model_connection_should_succeed(self):
        tag_template = \
            self.__tag_template_factory.make_tag_template_for_query()

        query_data = {
            'id': 5,
            'runtime': 0.5,
            'client_id': 'test-client-id',
            'query_timezone': 'America/Sao_Paulo',
            'model': 'test-model',
            'view': 'test-view',
        }

        assembled_metadata = entities.AssembledQueryMetadata(
            serialize.deserialize31(data=json.dumps(query_data),
                                    structure=models.Query), 'select *', None,
            None)

        tag = self.__factory.make_tag_for_query(tag_template,
                                                assembled_metadata)

        self.assertEqual(5, tag.fields['id'].double_value)
        self.assertFalse('lookml_project' in tag.fields)
        self.assertFalse('host' in tag.fields)

    def test_make_tag_empty_string_value_should_skip_field(self):
        tag_template = \
            self.__tag_template_factory.make_tag_template_for_folder()

        folder_data = {
            'id': 'a123-b456',
            'name': 'Test name',
            'parent_id': '',
            'child_count': 10,
        }

        tag = self.__factory.make_tag_for_folder(
            tag_template,
            serialize.deserialize31(data=json.dumps(folder_data),
                                    structure=models.Folder))

        self.assertFalse('parent_id' in tag.fields)

    @classmethod
    def __parse_datetime(cls, string):
        return datetime.strptime(string, cls.__DATETIME_FORMAT)
