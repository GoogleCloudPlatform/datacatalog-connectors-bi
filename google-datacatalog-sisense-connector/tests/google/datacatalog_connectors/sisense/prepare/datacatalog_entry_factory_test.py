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

from google.datacatalog_connectors.sisense.prepare import \
    datacatalog_entry_factory


class DataCatalogEntryFactoryTest(unittest.TestCase):
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

    def test_make_entry_for_dashboard_should_set_all_available_fields(self):
        metadata = {
            '_id': 'a123-b456',
            'oid': 'a123-b457',
            'title': 'Test dashboard',
            'desc': 'Test dashboard description',
            'created': '2019-09-12T16:30:00.005Z',
            'lastUpdated': '2019-09-12T16:31:00.005Z',
        }

        entry_id, entry = self.__factory.make_entry_for_dashboard(metadata)

        self.assertEqual('sss_test_server_com_db_a123_b457', entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'sss_test_server_com_db_a123_b457', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('dashboard', entry.user_specified_type)
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

    def test_make_entry_for_dashboard_should_succeed_on_missing_created_date(
            self):

        metadata = {
            '_id': 'a123-b456',
            'oid': 'a123-b457',
            'title': 'Test dashboard',
        }

        entry_id, entry = self.__factory.make_entry_for_dashboard(metadata)

        self.assertIsNone(entry.source_system_timestamps.create_time)

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
        self.assertEqual('folder', entry.user_specified_type)
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
