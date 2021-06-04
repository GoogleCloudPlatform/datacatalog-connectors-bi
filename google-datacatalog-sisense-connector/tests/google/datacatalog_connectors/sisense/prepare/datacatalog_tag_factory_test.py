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

from google.cloud import datacatalog

from google.datacatalog_connectors.sisense.prepare import \
    datacatalog_tag_factory


class DataCatalogEntryFactoryTest(unittest.TestCase):
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
