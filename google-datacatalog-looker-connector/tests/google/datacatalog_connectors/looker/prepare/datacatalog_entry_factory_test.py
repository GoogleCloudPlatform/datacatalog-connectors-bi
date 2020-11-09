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

from datetime import datetime, timedelta
import json
import unittest

from looker_sdk import models
from looker_sdk.rtl import serialize

from google.datacatalog_connectors.looker.prepare import \
    datacatalog_entry_factory


class DataCatalogEntryFactoryTest(unittest.TestCase):
    __DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S%z'

    def setUp(self):
        self.__factory = datacatalog_entry_factory.DataCatalogEntryFactory(
            'test-project', 'test-location', 'test-entry-group', 'test-system',
            'https://test.server.com')

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
                         attrs['_DataCatalogEntryFactory__instance_url'])
        self.assertEqual('test.server.com',
                         attrs['_DataCatalogEntryFactory__server_id'])

    def test_make_entry_for_dashboard_should_set_all_available_fields(self):
        dashboard_data = {
            'id': 'a123-b456',
            'title': 'Test Name',
            'created_at': '2019-09-12T16:30:00.000Z',
        }

        entry_id, entry = self.__factory.make_entry_for_dashboard(
            serialize.deserialize(json.dumps(dashboard_data),
                                  models.Dashboard))
        self.assertEqual('lkr_test_server_com_db_a123_b456', entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'lkr_test_server_com_db_a123_b456', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('dashboard', entry.user_specified_type)
        self.assertEqual('Test Name', entry.display_name)
        self.assertEqual('https://test.server.com/dashboards/a123-b456',
                         entry.linked_resource)

        created_datetime = self.__parse_datetime('2019-09-12T16:30:00+0000')
        self.assertEqual(
            created_datetime.timestamp(),
            entry.source_system_timestamps.create_time.timestamp())
        self.assertEqual(
            (created_datetime + timedelta(seconds=10)).timestamp(),
            entry.source_system_timestamps.update_time.timestamp())

    def test_make_entry_for_dashboard_no_created_at_should_succeed(self):
        dashboard_data = {
            'id': 'a123-b456',
            'title': 'Test Name',
        }

        entry_id, entry = self.__factory.make_entry_for_dashboard(
            serialize.deserialize(json.dumps(dashboard_data),
                                  models.Dashboard))

        self.assertIsNone(entry.source_system_timestamps.create_time)
        self.assertIsNone(entry.source_system_timestamps.update_time)

    def test_make_entry_for_dashboard_tile_should_set_fields(self):
        dashboard_element_data = {
            'id': '196',
            'title': 'Test Name',
        }

        entry_id, entry = self.__factory.make_entry_for_dashboard_element(
            serialize.deserialize(json.dumps(dashboard_element_data),
                                  models.DashboardElement))
        self.assertEqual('lkr_test_server_com_de_196', entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'lkr_test_server_com_de_196', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('dashboard_element', entry.user_specified_type)
        self.assertEqual('Test Name', entry.display_name)

    def test_make_entry_for_dashboard_tile_should_handle_title_text(self):
        dashboard_element_data = {
            'id': '196',
            'title': '',
            'title_text': 'Test Name',
        }

        entry_id, entry = self.__factory.make_entry_for_dashboard_element(
            serialize.deserialize(json.dumps(dashboard_element_data),
                                  models.DashboardElement))
        self.assertEqual('Test Name', entry.display_name)

    def test_make_entry_for_dashboard_tile_should_skip_empty_titles(self):
        dashboard_element_data = {
            'id': '196',
        }

        entry_id, entry = self.__factory.make_entry_for_dashboard_element(
            serialize.deserialize(json.dumps(dashboard_element_data),
                                  models.DashboardElement))

        self.assertIsNone(entry_id)
        self.assertIsNone(entry)

    def test_make_entry_for_folder_should_set_all_available_fields(self):
        folder_data = {
            'id': 'a123-b456',
            'name': 'Test Name',
            'parent_id': '',
        }

        entry_id, entry = self.__factory.make_entry_for_folder(
            serialize.deserialize(json.dumps(folder_data), models.Folder))
        self.assertEqual('lkr_test_server_com_fd_a123_b456', entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'lkr_test_server_com_fd_a123_b456', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('folder', entry.user_specified_type)
        self.assertEqual('Test Name', entry.display_name)
        self.assertEqual('https://test.server.com/folders/a123-b456',
                         entry.linked_resource)

    def test_make_entry_for_look_should_set_all_available_fields(self):
        look_data = {
            'id': 123,
            'title': 'Test Name',
            'created_at': '2019-09-12T16:30:00.000Z',
            'updated_at': '2019-09-12T17:35:00.000Z',
        }

        entry_id, entry = self.__factory.make_entry_for_look(
            serialize.deserialize(json.dumps(look_data), models.Look))
        self.assertEqual('lkr_test_server_com_lk_123', entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'lkr_test_server_com_lk_123', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('look', entry.user_specified_type)
        self.assertEqual('Test Name', entry.display_name)
        self.assertEqual('https://test.server.com/looks/123',
                         entry.linked_resource)

        created_datetime = self.__parse_datetime('2019-09-12T16:30:00+0000')
        self.assertEqual(
            created_datetime.timestamp(),
            entry.source_system_timestamps.create_time.timestamp())
        updated_datetime = self.__parse_datetime('2019-09-12T17:35:10+0000')
        self.assertEqual(
            updated_datetime.timestamp(),
            entry.source_system_timestamps.update_time.timestamp())

    def test_make_entry_for_look_no_updated_at_should_succeed(self):
        look_data = {
            'id': 123,
            'title': 'Test Name',
            'created_at': '2019-09-12T16:30:00.000Z',
        }

        entry_id, entry = self.__factory.make_entry_for_look(
            serialize.deserialize(json.dumps(look_data), models.Look))

        created_datetime = self.__parse_datetime('2019-09-12T16:30:00+0000')
        self.assertEqual(
            created_datetime.timestamp(),
            entry.source_system_timestamps.create_time.timestamp())
        self.assertEqual(
            (created_datetime + timedelta(seconds=10)).timestamp(),
            entry.source_system_timestamps.update_time.timestamp())

    def test_make_entry_for_query_should_set_all_available_fields(self):
        query_data = {
            'id': 837,
            'model': 'test-model',
            'view': 'test-view',
            'share_url': 'https://test-share-url',
        }

        entry_id, entry = self.__factory.make_entry_for_query(
            serialize.deserialize(json.dumps(query_data), models.Query))
        self.assertEqual('lkr_test_server_com_qr_837', entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'lkr_test_server_com_qr_837', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('query', entry.user_specified_type)
        self.assertEqual('Query 837 - model test-model - explore test-view',
                         entry.display_name)
        self.assertEqual('https://test-share-url', entry.linked_resource)

    @classmethod
    def __parse_datetime(cls, string):
        return datetime.strptime(string, cls.__DATETIME_FORMAT)
