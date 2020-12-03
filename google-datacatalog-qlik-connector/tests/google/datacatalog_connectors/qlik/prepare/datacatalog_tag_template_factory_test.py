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

from google.cloud import datacatalog

from google.datacatalog_connectors.qlik.prepare import \
    datacatalog_tag_template_factory


class DataCatalogTagTemplateFactoryTest(unittest.TestCase):
    __BOOL_TYPE = datacatalog.FieldType.PrimitiveType.BOOL
    __DOUBLE_TYPE = datacatalog.FieldType.PrimitiveType.DOUBLE
    __STRING_TYPE = datacatalog.FieldType.PrimitiveType.STRING
    __TIMESTAMP_TYPE = datacatalog.FieldType.PrimitiveType.TIMESTAMP

    def setUp(self):
        self.__factory = datacatalog_tag_template_factory.\
            DataCatalogTagTemplateFactory('test-project', 'test-location')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__factory.__dict__

        self.assertEqual('test-project',
                         attrs['_DataCatalogTagTemplateFactory__project_id'])
        self.assertEqual('test-location',
                         attrs['_DataCatalogTagTemplateFactory__location_id'])

    def test_make_tag_template_for_app(self):
        tag_template = self.__factory.make_tag_template_for_app()

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/qlik_app_metadata', tag_template.name)

        self.assertEqual('Qlik App Metadata', tag_template.display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['id'].type.primitive_type)
        self.assertEqual('Unique Id', tag_template.fields['id'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['owner_username'].type.primitive_type)
        self.assertEqual('Owner username',
                         tag_template.fields['owner_username'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['owner_name'].type.primitive_type)
        self.assertEqual('Owner name',
                         tag_template.fields['owner_name'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['modified_by_username'].type.primitive_type)
        self.assertEqual(
            'Username who modified it',
            tag_template.fields['modified_by_username'].display_name)

        self.assertEqual(self.__BOOL_TYPE,
                         tag_template.fields['published'].type.primitive_type)
        self.assertEqual('Published',
                         tag_template.fields['published'].display_name)

        self.assertEqual(
            self.__TIMESTAMP_TYPE,
            tag_template.fields['publish_time'].type.primitive_type)
        self.assertEqual('Publish time',
                         tag_template.fields['publish_time'].display_name)

        self.assertEqual(
            self.__TIMESTAMP_TYPE,
            tag_template.fields['last_reload_time'].type.primitive_type)
        self.assertEqual('Last reload time',
                         tag_template.fields['last_reload_time'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['stream_id'].type.primitive_type)
        self.assertEqual('Stream Id',
                         tag_template.fields['stream_id'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['stream_name'].type.primitive_type)
        self.assertEqual('Stream name',
                         tag_template.fields['stream_name'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['stream_entry'].type.primitive_type)
        self.assertEqual('Data Catalog Entry for the Stream',
                         tag_template.fields['stream_entry'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['file_size'].type.primitive_type)
        self.assertEqual('File size',
                         tag_template.fields['file_size'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['thumbnail'].type.primitive_type)
        self.assertEqual('Thumbnail',
                         tag_template.fields['thumbnail'].display_name)

        self.assertEqual(
            self.__STRING_TYPE, tag_template.
            fields['saved_in_product_version'].type.primitive_type)
        self.assertEqual(
            'Saved in product version',
            tag_template.fields['saved_in_product_version'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['migration_hash'].type.primitive_type)
        self.assertEqual('Migration hash',
                         tag_template.fields['migration_hash'].display_name)

        self.assertEqual(
            self.__DOUBLE_TYPE,
            tag_template.fields['availability_status'].type.primitive_type)
        self.assertEqual(
            'Availability status',
            tag_template.fields['availability_status'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['schema_path'].type.primitive_type)
        self.assertEqual('Schema path',
                         tag_template.fields['schema_path'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['site_url'].type.primitive_type)
        self.assertEqual('Qlik Sense site url',
                         tag_template.fields['site_url'].display_name)

    def test_make_tag_template_for_sheet(self):
        tag_template = self.__factory.make_tag_template_for_sheet()

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/qlik_sheet_metadata', tag_template.name)

        self.assertEqual('Qlik Sheet Metadata', tag_template.display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['id'].type.primitive_type)
        self.assertEqual('Unique Id', tag_template.fields['id'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['owner_username'].type.primitive_type)
        self.assertEqual('Owner username',
                         tag_template.fields['owner_username'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['owner_name'].type.primitive_type)
        self.assertEqual('Owner name',
                         tag_template.fields['owner_name'].display_name)

        self.assertEqual(self.__BOOL_TYPE,
                         tag_template.fields['published'].type.primitive_type)
        self.assertEqual('Published',
                         tag_template.fields['published'].display_name)

        self.assertEqual(
            self.__TIMESTAMP_TYPE,
            tag_template.fields['publish_time'].type.primitive_type)
        self.assertEqual('Publish time',
                         tag_template.fields['publish_time'].display_name)

        self.assertEqual(self.__BOOL_TYPE,
                         tag_template.fields['approved'].type.primitive_type)
        self.assertEqual('Approved',
                         tag_template.fields['approved'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['app_id'].type.primitive_type)
        self.assertEqual('App Id', tag_template.fields['app_id'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['app_name'].type.primitive_type)
        self.assertEqual('App name',
                         tag_template.fields['app_name'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['app_entry'].type.primitive_type)
        self.assertEqual('Data Catalog Entry for the App',
                         tag_template.fields['app_entry'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['source_object'].type.primitive_type)
        self.assertEqual('Source object',
                         tag_template.fields['source_object'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['draft_object'].type.primitive_type)
        self.assertEqual('Draft object',
                         tag_template.fields['draft_object'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['site_url'].type.primitive_type)
        self.assertEqual('Qlik Sense site url',
                         tag_template.fields['site_url'].display_name)

    def test_make_tag_template_for_stream(self):
        tag_template = self.__factory.make_tag_template_for_stream()

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/qlik_stream_metadata', tag_template.name)

        self.assertEqual('Qlik Stream Metadata', tag_template.display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['id'].type.primitive_type)
        self.assertEqual('Unique Id', tag_template.fields['id'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['owner_username'].type.primitive_type)
        self.assertEqual('Owner username',
                         tag_template.fields['owner_username'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['owner_name'].type.primitive_type)
        self.assertEqual('Owner name',
                         tag_template.fields['owner_name'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['modified_by_username'].type.primitive_type)
        self.assertEqual(
            'Username who modified it',
            tag_template.fields['modified_by_username'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['site_url'].type.primitive_type)
        self.assertEqual('Qlik Sense site url',
                         tag_template.fields['site_url'].display_name)
