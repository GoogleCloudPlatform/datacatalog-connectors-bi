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

from google.datacatalog_connectors.qlik import prepare
from google.datacatalog_connectors.qlik.prepare import \
    datacatalog_tag_factory


class DataCatalogEntryFactoryTest(unittest.TestCase):
    __DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'

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

    def test_make_tag_for_app_should_set_all_available_fields(self):
        tag_template = \
            self.__tag_template_factory.make_tag_template_for_app()

        metadata = {
            'id': 'c987-d654',
            'owner': {
                'userDirectory': 'test-directory',
                'userId': 'test.userid',
                'name': 'Test user',
            },
            'modifiedByUserName': 'test-directory\\\\test.userid',
            'publishTime': '2020-11-04T14:49:14.504Z',
            'published': True,
            'lastReloadTime': '2020-11-03T18:13:37.156Z',
            'stream': {
                'id': 'a123-b456',
                'name': 'Test stream',
            },
            'fileSize': 43394841,
            'thumbnail': '/appcontent/c987-d654/test-thumbnail.jpg',
            'savedInProductVersion': '12.763.4',
            'migrationHash': '504d4e39a7133ee172fbe29aa58348b1e4054149',
            'availabilityStatus': 1,
            'schemaPath': 'App',
        }

        tag = self.__factory.make_tag_for_app(tag_template, metadata)

        self.assertEqual('c987-d654', tag.fields['id'].string_value)
        self.assertEqual('test-directory\\\\test.userid',
                         tag.fields['owner_username'].string_value)
        self.assertEqual('Test user', tag.fields['owner_name'].string_value)
        self.assertEqual('test-directory\\\\test.userid',
                         tag.fields['modified_by_username'].string_value)

        publish_datetime = datetime.strptime('2020-11-04T14:49:14.504+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            publish_datetime.timestamp(),
            tag.fields['publish_time'].timestamp_value.timestamp())

        self.assertEqual(True, tag.fields['published'].bool_value)

        last_reload_datetime = datetime.strptime(
            '2020-11-03T18:13:37.156+0000', self.__DATETIME_FORMAT)
        self.assertEqual(
            last_reload_datetime.timestamp(),
            tag.fields['last_reload_time'].timestamp_value.timestamp())

        self.assertEqual('a123-b456', tag.fields['stream_id'].string_value)
        self.assertEqual('Test stream', tag.fields['stream_name'].string_value)
        self.assertEqual('41.38 MB', tag.fields['file_size'].string_value)
        self.assertEqual(
            'https://test.server.com'
            '/appcontent/c987-d654/test-thumbnail.jpg',
            tag.fields['thumbnail'].string_value)
        self.assertEqual('12.763.4',
                         tag.fields['saved_in_product_version'].string_value)
        self.assertEqual('504d4e39a7133ee172fbe29aa58348b1e4054149',
                         tag.fields['migration_hash'].string_value)
        self.assertEqual(1, tag.fields['availability_status'].double_value)
        self.assertEqual('App', tag.fields['schema_path'].string_value)

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
