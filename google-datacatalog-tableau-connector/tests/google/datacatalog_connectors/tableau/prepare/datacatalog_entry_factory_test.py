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

from google.datacatalog_connectors.tableau.prepare import \
    datacatalog_entry_factory


class DataCatalogEntryFactoryTest(unittest.TestCase):

    def setUp(self):
        self.__factory = datacatalog_entry_factory.DataCatalogEntryFactory(
            project_id='test-project',
            location_id='test-location',
            entry_group_id='test-entry-group',
            user_specified_system='test-system',
            server_address='test-server')

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
        self.assertEqual('test-server',
                         attrs['_DataCatalogEntryFactory__server_address'])

    def test_make_entry_for_dashboard_should_set_all_available_fields(self):
        metadata = {
            'luid': 'a123-b456',
            'name': 'Test Name',
            'path': 'test/dashboard',
            'createdAt': '2019-09-12T16:30:00Z',
            'updatedAt': '2019-09-12T16:30:55Z',
            'workbook': {
                'site': {
                    'name': 'test-site'
                }
            }
        }

        entry = self.__factory.make_entry_for_dashboard(metadata)

        self.assertEqual('t__a123_b456', entry[0])

        entry = entry[1]
        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/t__a123_b456', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('dashboard', entry.user_specified_type)
        self.assertEqual('Test Name', entry.display_name)
        self.assertEqual('test-server/#/site/test-site/views/test/dashboard',
                         entry.linked_resource)

        datetime_format = '%Y-%m-%dT%H:%M:%S%z'
        created_datetime = datetime.strptime('2019-09-12T16:30:00+0000',
                                             datetime_format)
        self.assertEqual(created_datetime.timestamp(),
                         entry.source_system_timestamps.create_time.seconds)
        updated_datetime = datetime.strptime('2019-09-12T16:30:55+0000',
                                             datetime_format)
        self.assertEqual(updated_datetime.timestamp(),
                         entry.source_system_timestamps.update_time.seconds)

    def test_make_entry_for_sheet_should_set_all_available_fields(self):
        metadata = {
            'luid': 'a123-b456',
            'name': 'Test Name',
            'path': 'test/sheet',
            'createdAt': '2019-09-12T16:30:00Z',
            'updatedAt': '2019-09-12T16:30:55Z'
        }

        workbook_metadata = {'site': {'name': 'test-site'}}

        entry = self.__factory.make_entry_for_sheet(metadata,
                                                    workbook_metadata)

        self.assertEqual('t__a123_b456', entry[0])

        entry = entry[1]
        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/t__a123_b456', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('sheet', entry.user_specified_type)
        self.assertEqual('Test Name', entry.display_name)
        self.assertEqual('test-server/#/site/test-site/views/test/sheet',
                         entry.linked_resource)

        datetime_format = '%Y-%m-%dT%H:%M:%S%z'
        created_datetime = datetime.strptime('2019-09-12T16:30:00+0000',
                                             datetime_format)
        self.assertEqual(created_datetime.timestamp(),
                         entry.source_system_timestamps.create_time.seconds)
        updated_datetime = datetime.strptime('2019-09-12T16:30:55+0000',
                                             datetime_format)
        self.assertEqual(updated_datetime.timestamp(),
                         entry.source_system_timestamps.update_time.seconds)

    def test_make_entry_for_sheet_missing_luid_should_use_id_fallback(self):
        metadata = {
            'id': 'a123-b456',
            'luid': '',
            'name': 'Test Name',
            'path': 'test/sheet',
            'createdAt': '2019-09-12T16:30:00Z',
            'updatedAt': '2019-09-12T16:30:55Z'
        }

        workbook_metadata = {'site': {'name': 'test-site'}}

        entry = self.__factory.make_entry_for_sheet(metadata,
                                                    workbook_metadata)

        self.assertEqual('t__a123_b456', entry[0])

    def test_make_entry_for_workbook_should_set_all_available_fields(self):
        metadata = {
            'luid': 'a123-b456',
            'name': 'Test Name',
            'site': {
                'name': 'test-site'
            },
            'description': 'Test Description',
            'vizportalUrlId': 1,
            'createdAt': '2019-09-12T16:30:00Z',
            'updatedAt': '2019-09-12T16:30:55Z'
        }

        entry = self.__factory.make_entry_for_workbook(metadata)

        self.assertEqual('t__a123_b456', entry[0])

        entry = entry[1]
        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/t__a123_b456', entry.name)
        self.assertEqual('test-system', entry.user_specified_system)
        self.assertEqual('workbook', entry.user_specified_type)
        self.assertEqual('Test Name', entry.display_name)
        self.assertEqual('Test Description', entry.description)
        self.assertEqual('test-server/#/site/test-site/workbooks/1',
                         entry.linked_resource)

        datetime_format = '%Y-%m-%dT%H:%M:%S%z'
        created_datetime = datetime.strptime('2019-09-12T16:30:00+0000',
                                             datetime_format)
        self.assertEqual(created_datetime.timestamp(),
                         entry.source_system_timestamps.create_time.seconds)
        updated_datetime = datetime.strptime('2019-09-12T16:30:55+0000',
                                             datetime_format)
        self.assertEqual(updated_datetime.timestamp(),
                         entry.source_system_timestamps.update_time.seconds)

    def test_make_entry_long_luid_should_limit_result_id_length(self):
        metadata = {
            'luid': '12345678901234567890123456789012'
                    '34567890123456789012345678901234',
            'name': 'Test Name',
            'path': 'test/sheet',
            'createdAt': '2019-09-12T16:30:00Z',
            'updatedAt': '2019-09-12T16:30:55Z'
        }

        workbook_metadata = {'site': {'name': 'test-site'}}

        entry = self.__factory.make_entry_for_sheet(metadata,
                                                    workbook_metadata)

        self.assertEqual(
            't__1234567890123456789012345678901234567890123456789012345678901',
            entry[0])

        entry = entry[1]
        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'entryGroups/test-entry-group/entries/'
            't__1234567890123456789012345678901234567890123456789012345678901',
            entry.name)
