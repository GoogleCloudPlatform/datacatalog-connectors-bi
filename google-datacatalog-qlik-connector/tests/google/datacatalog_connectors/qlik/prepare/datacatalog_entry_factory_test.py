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
import unittest

from google.datacatalog_connectors.qlik.prepare import \
    datacatalog_entry_factory


class DataCatalogEntryFactoryTest(unittest.TestCase):
    __DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'

    def setUp(self):
        self.__factory = datacatalog_entry_factory.DataCatalogEntryFactory(
            project_id='test-project',
            location_id='test-location',
            entry_group_id='test-entry-group',
            user_specified_system='test-system',
            site_url='https://test.server.com')

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
                         attrs['_DataCatalogEntryFactory__site_url'])

    def test_make_entry_for_app_should_set_all_available_fields(self):
        metadata = {
            'id': 'a123-b456',
            'name': 'Test app',
            'description': 'Description of the Test app',
            'createdDate': '2019-09-12T16:30:00.005Z',
            'modifiedDate': '2019-09-12T16:31:00.005Z',
        }

        entry_id, entry = self.__factory.make_entry_for_app(metadata)

        self.assertEqual('qlik_app_a123_b456', entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'qlik_app_a123_b456', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('app', entry.user_specified_type)
        self.assertEqual('Test app', entry.display_name)
        self.assertEqual('Description of the Test app', entry.description)
        self.assertEqual('https://test.server.com/sense/app/a123-b456',
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

    def test_make_entry_for_app_should_use_created_on_missing_modified_date(
            self):

        metadata = {
            'id': 'a123-b456',
            'createdDate': '2019-09-12T16:30:00.005Z',
        }

        entry_id, entry = self.__factory.make_entry_for_app(metadata)

        created_datetime = datetime.strptime('2019-09-12T16:30:00.005+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            created_datetime.timestamp(),
            entry.source_system_timestamps.update_time.timestamp())

    def test_make_entry_for_custom_property_definition_should_set_all_available_fields(  # noqa E510
            self):

        metadata = {
            'id': 'a123-b456',
            'name': 'Test custom property definition',
            'createdDate': '2019-09-12T16:30:00.005Z',
            'modifiedDate': '2019-09-12T16:31:00.005Z',
        }

        entry_id, entry = \
            self.__factory.make_entry_for_custom_property_definition(metadata)

        self.assertEqual('qlik_cpd_a123_b456', entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'qlik_cpd_a123_b456', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('custom_property_definition',
                         entry.user_specified_type)
        self.assertEqual('Test custom property definition', entry.display_name)
        self.assertIsNone(entry.linked_resource)

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

    def test_make_entry_for_sheet_should_set_all_available_fields(self):
        metadata = {
            'qInfo': {
                'qId': 'a123-b456',
            },
            'qMeta': {
                'title': 'Test sheet',
                'description': 'Description of the Test sheet',
                'createdDate': '2019-09-12T16:30:00.005Z',
                'modifiedDate': '2019-09-12T16:31:00.005Z',
            },
            'app': {
                'id': 'app-id'
            },
        }

        entry_id, entry = self.__factory.make_entry_for_sheet(metadata)

        self.assertEqual('qlik_sht_a123_b456', entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'qlik_sht_a123_b456', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('sheet', entry.user_specified_type)
        self.assertEqual('Test sheet', entry.display_name)
        self.assertEqual('Description of the Test sheet', entry.description)
        self.assertEqual(
            'https://test.server.com'
            '/sense/app/app-id/sheet/a123-b456', entry.linked_resource)

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

    def test_make_entry_for_sheet_should_use_created_on_missing_modified_date(
            self):

        metadata = {
            'qInfo': {
                'qId': 'a123-b456',
            },
            'qMeta': {
                'createdDate': '2019-09-12T16:30:00.005Z',
            },
            'app': {
                'id': 'app-id'
            },
        }

        entry_id, entry = self.__factory.make_entry_for_sheet(metadata)

        created_datetime = datetime.strptime('2019-09-12T16:30:00.005+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            created_datetime.timestamp(),
            entry.source_system_timestamps.update_time.timestamp())

    def test_make_entry_for_stream_should_set_all_available_fields(self):
        metadata = {
            'id': 'a123-b456',
            'name': 'Test stream',
            'createdDate': '2019-09-12T16:30:00.005Z',
            'modifiedDate': '2019-09-12T16:31:00.005Z',
        }

        entry_id, entry = self.__factory.make_entry_for_stream(metadata)

        self.assertEqual('qlik_str_a123_b456', entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'qlik_str_a123_b456', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('stream', entry.user_specified_type)
        self.assertEqual('Test stream', entry.display_name)
        self.assertEqual('https://test.server.com/hub/stream/a123-b456',
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

    def test_make_entry_for_stream_should_use_created_on_missing_modified_date(
            self):

        metadata = {
            'id': 'a123-b456',
            'createdDate': '2019-09-12T16:30:00.005Z',
        }

        entry_id, entry = self.__factory.make_entry_for_stream(metadata)

        created_datetime = datetime.strptime('2019-09-12T16:30:00.005+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            created_datetime.timestamp(),
            entry.source_system_timestamps.update_time.timestamp())

    def test_make_entry_long_id_should_limit_result_id_length(self):
        metadata = {
            'id': '12345678901234567890123456789012'
                  '34567890123456789012345678901234',
            'name': 'Test stream',
            'createdDate': '2019-09-12T16:30:00.005Z',
            'modifiedDate': '2019-09-12T16:30:00.005Z',
        }

        entry_id, entry = self.__factory.make_entry_for_stream(metadata)

        self.assertEqual(
            'qlik_str_1234567890123456789012345678901234567890123456789012345',
            entry_id)
        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'qlik_str_1234567890123456789012345678901234567890123456789012345',
            entry.name)
