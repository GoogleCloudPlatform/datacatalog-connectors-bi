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

import unittest

from google.datacatalog_connectors.qlik import prepare
from google.datacatalog_connectors.qlik.prepare import \
    datacatalog_tag_factory


class DataCatalogEntryFactoryTest(unittest.TestCase):

    def setUp(self):
        self.__tag_template_factory = \
            prepare.DataCatalogTagTemplateFactory(
                'test-project', 'test-location')
        self.__factory = datacatalog_tag_factory.DataCatalogTagFactory(
            'https://test.server.com')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__factory.__dict__
        self.assertEqual('https://test.server.com',
                         attrs['_DataCatalogTagFactory__site_url'])

    def test_make_tag_for_stream_should_set_all_available_fields(self):
        tag_template = \
            self.__tag_template_factory.make_tag_template_for_stream()

        metadata = {
            'id': 'a123-b456',
            'owner': {
                'userDirectory': 'test-directory',
                'userId': 'test.userid',
                'name': 'Test user',
            },
            'modifiedByUserName': 'test-directory\\\\test.userid',
        }

        tag = self.__factory.make_tag_for_stream(tag_template, metadata)

        self.assertEqual('a123-b456', tag.fields['id'].string_value)
        self.assertEqual('test-directory\\\\test.userid',
                         tag.fields['owner_username'].string_value)
        self.assertEqual('Test user', tag.fields['owner_name'].string_value)
        self.assertEqual('test-directory\\\\test.userid',
                         tag.fields['modified_by_username'].string_value)
        self.assertEqual('https://test.server.com',
                         tag.fields['site_url'].string_value)
