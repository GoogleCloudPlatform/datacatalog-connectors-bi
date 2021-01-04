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
import re
import unittest
from unittest import mock

from google.datacatalog_connectors.qlik import prepare
from google.datacatalog_connectors.qlik.prepare import \
    datacatalog_tag_factory


class DataCatalogEntryFactoryTest(unittest.TestCase):
    __PREPARE_PACKAGE = 'google.datacatalog_connectors.qlik.prepare'

    __DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f%z'
    __TAG_TEMPLATE_NAME_PATTERN = r'^(.+?)/tagTemplates/(?P<id>.+?)$'

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
            'published': True,
            'publishTime': '2020-11-04T14:49:14.504Z',
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
        }

        tag = self.__factory.make_tag_for_app(tag_template, metadata)

        self.assertEqual('c987-d654', tag.fields['id'].string_value)

        self.assertEqual('test-directory\\\\test.userid',
                         tag.fields['owner_username'].string_value)
        self.assertEqual('Test user', tag.fields['owner_name'].string_value)
        self.assertEqual('test-directory\\\\test.userid',
                         tag.fields['modified_by_username'].string_value)

        self.assertEqual(True, tag.fields['published'].bool_value)
        publish_datetime = datetime.strptime('2020-11-04T14:49:14.504+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            publish_datetime.timestamp(),
            tag.fields['publish_time'].timestamp_value.timestamp())
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

        self.assertEqual('https://test.server.com',
                         tag.fields['site_url'].string_value)

    def test_make_tag_for_app_should_not_set_published_time_if_not_published(
            self):

        tag_template = \
            self.__tag_template_factory.make_tag_template_for_app()

        metadata = {
            'published': False,
        }

        tag = self.__factory.make_tag_for_app(tag_template, metadata)
        self.assertFalse('publish_time' in tag.fields)

    @mock.patch(f'{__PREPARE_PACKAGE}.datacatalog_tag_template_factory.dph'
                f'.DynamicPropertiesHelper')
    def test_make_tags_for_custom_properties_should_make_if_proper_template(
            self, mock_dph):

        mock_dph.make_display_name_for_custom_property_value_tag_template\
            .return_value = 'Test definition : Value 1'
        mock_dph.make_id_for_custom_property_value_tag_template\
            .return_value = 'a123||value_1'

        tag_template = self.__tag_template_factory\
            .make_tag_template_for_custom_property_value({}, 'Value 1')
        tag_template_id = re.match(pattern=self.__TAG_TEMPLATE_NAME_PATTERN,
                                   string=tag_template.name).group('id')

        tag_templates_dict = {tag_template_id: tag_template}

        metadata = {'id': 'c987-d654'}

        tags = self.__factory.make_tags_for_custom_properties(
            tag_templates_dict, [metadata])

        self.assertEqual(1, len(tags))
        self.assertEqual('c987-d654', tags[0].fields['id'].string_value)

    @mock.patch(f'{__PREPARE_PACKAGE}.datacatalog_tag_template_factory.dph'
                f'.DynamicPropertiesHelper')
    def test_make_tags_for_custom_properties_should_skip_if_no_templates(
            self, mock_dph):

        mock_dph.make_display_name_for_custom_property_value_tag_template\
            .return_value = 'Test definition : Value 1'
        mock_dph.make_id_for_custom_property_value_tag_template\
            .return_value = 'a123||value_1'

        metadata = {'id': 'c987-d654'}

        tags = self.__factory.make_tags_for_custom_properties({}, [metadata])

        self.assertIsNotNone(tags)
        self.assertEqual(0, len(tags))

    @mock.patch(f'{__PREPARE_PACKAGE}.datacatalog_tag_template_factory.dph'
                f'.DynamicPropertiesHelper')
    def test_make_tags_for_custom_properties_should_skip_if_no_proper_template(
            self, mock_dph):

        mock_dph.make_display_name_for_custom_property_value_tag_template\
            .return_value = 'Test definition : Value 1'
        mock_dph.make_id_for_custom_property_value_tag_template\
            .side_effect = ['a123||value_1', 'a123_b456||value_1']

        tag_template = self.__tag_template_factory\
            .make_tag_template_for_custom_property_value({}, 'Value 1')
        tag_template_id = re.match(pattern=self.__TAG_TEMPLATE_NAME_PATTERN,
                                   string=tag_template.name).group('id')

        tag_templates_dict = {tag_template_id: tag_template}

        metadata = {'id': 'c987-d654'}

        tags = self.__factory.make_tags_for_custom_properties(
            tag_templates_dict, [metadata])

        self.assertIsNotNone(tags)
        self.assertEqual(0, len(tags))

    @mock.patch(f'{__PREPARE_PACKAGE}.datacatalog_tag_template_factory.dph'
                f'.DynamicPropertiesHelper')
    def test_make_tag_for_custom_property_should_set_all_available_fields(
            self, mock_dph):

        mock_dph.make_display_name_for_custom_property_value_tag_template\
            .return_value = 'Test definition : Value 1'
        mock_dph.make_id_for_custom_property_value_tag_template\
            .return_value = 'a123||value_1'

        metadata = {
            'id': 'c987-d654',
            'createdDate': '2020-11-03T18:13:37.156Z',
            'modifiedDate': '2020-11-04T14:49:14.504Z',
            'modifiedByUserName': 'test-directory\\\\test.userid',
            'value': 'Value 1',
            'definition': {
                'id': 'a123-b456',
                'name': 'Test definition',
            },
        }

        tag_template = self.__tag_template_factory\
            .make_tag_template_for_custom_property_value(metadata, 'Value 1')

        tag = self.__factory.make_tag_for_custom_property(
            tag_template, metadata)

        self.assertEqual('c987-d654', tag.fields['id'].string_value)

        created_datetime = datetime.strptime('2020-11-03T18:13:37.156+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            created_datetime.timestamp(),
            tag.fields['created_date'].timestamp_value.timestamp())
        modified_datetime = datetime.strptime('2020-11-04T14:49:14.504+0000',
                                              self.__DATETIME_FORMAT)
        self.assertEqual(
            modified_datetime.timestamp(),
            tag.fields['modified_date'].timestamp_value.timestamp())

        self.assertEqual('test-directory\\\\test.userid',
                         tag.fields['modified_by_username'].string_value)
        self.assertEqual('Value 1', tag.fields['value'].string_value)

        self.assertEqual('a123-b456',
                         tag.fields['property_definition_id'].string_value)
        self.assertEqual('Test definition',
                         tag.fields['property_name'].string_value)

        self.assertEqual('https://test.server.com',
                         tag.fields['site_url'].string_value)

    def test_make_tag_for_custom_property_def_should_set_all_available_fields(
            self):

        tag_template = self.__tag_template_factory\
            .make_tag_template_for_custom_property_definition()

        metadata = {
            'id': 'a123-b456',
            'modifiedByUserName': 'test-directory\\\\test.userid',
            'valueType': 'Text',
            'choiceValues': [
                'Value 1',
                'Value 2',
            ],
            'objectTypes': [
                'App',
                'Stream',
            ],
        }

        tag = self.__factory.make_tag_for_custom_property_definition(
            tag_template, metadata)

        self.assertEqual('a123-b456', tag.fields['id'].string_value)
        self.assertEqual('test-directory\\\\test.userid',
                         tag.fields['modified_by_username'].string_value)
        self.assertEqual('Text', tag.fields['value_type'].string_value)
        self.assertEqual('Value 1, Value 2',
                         tag.fields['choice_values'].string_value)
        self.assertEqual('App, Stream',
                         tag.fields['object_types'].string_value)

        self.assertEqual('https://test.server.com',
                         tag.fields['site_url'].string_value)

    def test_make_tag_for_dimension_should_set_all_available_fields(self):
        tag_template = \
            self.__tag_template_factory.make_tag_template_for_dimension()

        metadata = {
            'qInfo': {
                'qId': 'c987-d654',
            },
            'qDim': {
                'qGrouping': 'H',
                'qFieldDefs': [
                    'FIELD_1',
                    'FIELD_2',
                ],
                'qFieldLabels': [
                    'Field 1',
                    'Field 2',
                ],
            },
            'qMetaDef': {
                'tags': [
                    'tag 1',
                    'tag 2',
                ],
            },
            'app': {
                'id': 'a123-b456',
                'name': 'Test app',
            },
        }

        tag = self.__factory.make_tag_for_dimension(tag_template, metadata)

        self.assertEqual('c987-d654', tag.fields['id'].string_value)

        self.assertEqual('Drill down',
                         tag.fields['grouping'].enum_value.display_name)
        self.assertEqual('FIELD_1, FIELD_2', tag.fields['fields'].string_value)
        self.assertEqual('Field 1, Field 2',
                         tag.fields['field_labels'].string_value)
        self.assertEqual('tag 1, tag 2', tag.fields['tags'].string_value)

        self.assertEqual('a123-b456', tag.fields['app_id'].string_value)
        self.assertEqual('Test app', tag.fields['app_name'].string_value)

        self.assertEqual('https://test.server.com',
                         tag.fields['site_url'].string_value)

    def test_make_tag_for_sheet_should_set_all_available_fields(self):
        tag_template = \
            self.__tag_template_factory.make_tag_template_for_sheet()

        metadata = {
            'qInfo': {
                'qId': 'c987-d654',
            },
            'qMeta': {
                'published': True,
                'publishTime': '2020-11-04T14:49:14.504Z',
                'approved': True,
                'owner': {
                    'userDirectory': 'test-directory',
                    'userId': 'test.userid',
                    'name': 'Test user',
                },
                'sourceObject': 'Test source object',
                'draftObject': 'Test draft object',
            },
            'app': {
                'id': 'a123-b456',
                'name': 'Test app',
            },
        }

        tag = self.__factory.make_tag_for_sheet(tag_template, metadata)

        self.assertEqual('c987-d654', tag.fields['id'].string_value)

        self.assertEqual('test-directory\\\\test.userid',
                         tag.fields['owner_username'].string_value)
        self.assertEqual('Test user', tag.fields['owner_name'].string_value)

        self.assertEqual(True, tag.fields['published'].bool_value)
        publish_datetime = datetime.strptime('2020-11-04T14:49:14.504+0000',
                                             self.__DATETIME_FORMAT)
        self.assertEqual(
            publish_datetime.timestamp(),
            tag.fields['publish_time'].timestamp_value.timestamp())
        self.assertEqual(True, tag.fields['approved'].bool_value)

        self.assertEqual('a123-b456', tag.fields['app_id'].string_value)
        self.assertEqual('Test app', tag.fields['app_name'].string_value)

        self.assertEqual('Test source object',
                         tag.fields['source_object'].string_value)
        self.assertEqual('Test draft object',
                         tag.fields['draft_object'].string_value)

        self.assertEqual('https://test.server.com',
                         tag.fields['site_url'].string_value)

    def test_make_tag_for_sheet_should_not_set_published_time_if_not_published(
            self):

        tag_template = \
            self.__tag_template_factory.make_tag_template_for_sheet()

        metadata = {
            'qInfo': {},
            'qMeta': {
                'published': False,
            },
        }

        tag = self.__factory.make_tag_for_sheet(tag_template, metadata)
        self.assertFalse('publish_time' in tag.fields)

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
