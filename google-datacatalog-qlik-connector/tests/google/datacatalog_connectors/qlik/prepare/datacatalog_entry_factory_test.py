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

    def test_make_entry_for_stream_should_set_all_available_fields(self):
        metadata = {
            'id': 'a123-b456',
            'name': 'Test Name',
            'createdDate': '2019-09-12T16:30:00.005Z',
            'modifiedDate': '2019-09-12T16:30:00.005Z',
        }

        entry_id, entry = self.__factory.make_entry_for_stream(metadata)
        self.assertEqual('qlik_st_a123_b456', entry_id)

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'qlik_st_a123_b456', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('stream', entry.user_specified_type)
        self.assertEqual('Test Name', entry.display_name)
        self.assertEqual('https://test.server.com/hub/stream/a123-b456',
                         entry.linked_resource)

        created_datetime = datetime.strptime('2019-09-12T16:30:00.005+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            created_datetime.timestamp(),
            entry.source_system_timestamps.create_time.timestamp())
        self.assertEqual(
            (created_datetime + timedelta(seconds=10)).timestamp(),
            entry.source_system_timestamps.update_time.timestamp())

    def test_make_entry_long_luid_should_limit_result_id_length(self):
        metadata = {
            'id': '12345678901234567890123456789012'
                  '34567890123456789012345678901234',
            'name': 'Test Name',
            'createdDate': '2019-09-12T16:30:00.005Z',
            'modifiedDate': '2019-09-12T16:30:00.005Z',
        }

        entry_id, entry = self.__factory.make_entry_for_stream(metadata)

        self.assertEqual(
            'qlik_st_12345678901234567890123456789012345678901234567890123456',
            entry_id)
        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            'qlik_st_12345678901234567890123456789012345678901234567890123456',
            entry.name)
