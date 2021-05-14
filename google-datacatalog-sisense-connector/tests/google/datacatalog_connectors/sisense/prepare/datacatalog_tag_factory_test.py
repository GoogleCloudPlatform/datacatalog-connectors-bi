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

from google.cloud import datacatalog

from google.datacatalog_connectors.sisense.prepare import \
    datacatalog_tag_factory


class DataCatalogEntryFactoryTest(unittest.TestCase):

    def setUp(self):
        self.__factory = datacatalog_tag_factory.DataCatalogTagFactory(
            'https://test.com')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__factory.__dict__
        self.assertEqual('https://test.com',
                         attrs['_DataCatalogTagFactory__server_address'])

    def test_make_tag_for_app_should_set_all_available_fields(self):
        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_folder_metadata'

        metadata = {
            'oid': 'test-folder',
            'name': 'Test folder',
            'parentId': 'parent-folder',
            'ownerData': {
                'userName': 'johndoe@test.com',
                'firstName': 'John',
                'lastName': 'Doe',
            },
            'folders': [{
                'oid': 'child-folder',
            }],
            'dashboards': [{
                'oid': 'test-dashboard',
            }],
        }

        tag = self.__factory.make_tag_for_folder(tag_template, metadata)

        self.assertEqual('test-folder', tag.fields['id'].string_value)
        self.assertEqual('Test folder', tag.fields['name'].string_value)
        self.assertEqual('parent-folder', tag.fields['parent_id'].string_value)
        self.assertEqual('johndoe@test.com',
                         tag.fields['owner_username'].string_value)
        self.assertEqual('John Doe', tag.fields['owner_name'].string_value)
        self.assertEqual(True, tag.fields['has_children'].bool_value)
        self.assertEqual(1, tag.fields['child_count'].double_value)
        self.assertEqual(True, tag.fields['has_dashboards'].bool_value)
        self.assertEqual(1, tag.fields['dashboard_count'].double_value)

        self.assertEqual('https://test.com',
                         tag.fields['server_url'].string_value)
