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
    datacatalog_tag_template_factory


class DataCatalogTagTemplateFactoryTest(unittest.TestCase):
    __PREPARE_PACKAGE = 'google.datacatalog_connectors.sisense.prepare'
    __FACTORY_MODULE = f'{__PREPARE_PACKAGE}.datacatalog_tag_template_factory'

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

    def test_make_tag_template_for_folder(self):
        tag_template = self.__factory.make_tag_template_for_folder()

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/sisense_folder_metadata', tag_template.name)

        self.assertEqual('Sisense Folder Metadata', tag_template.display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['id'].type.primitive_type)
        self.assertEqual('Id', tag_template.fields['id'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['name'].type.primitive_type)
        self.assertEqual('Name', tag_template.fields['name'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['owner_username'].type.primitive_type)
        self.assertEqual('Owner username',
                         tag_template.fields['owner_username'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['owner_name'].type.primitive_type)
        self.assertEqual('Owner name',
                         tag_template.fields['owner_name'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['parent_id'].type.primitive_type)
        self.assertEqual('Id of Parent',
                         tag_template.fields['parent_id'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['parent_folder_entry'].type.primitive_type)
        self.assertEqual(
            'Data Catalog Entry for the parent Folder',
            tag_template.fields['parent_folder_entry'].display_name)

        self.assertEqual(
            self.__BOOL_TYPE,
            tag_template.fields['has_children'].type.primitive_type)
        self.assertEqual('Has children',
                         tag_template.fields['has_children'].display_name)

        self.assertEqual(
            self.__DOUBLE_TYPE,
            tag_template.fields['child_count'].type.primitive_type)
        self.assertEqual('Child count',
                         tag_template.fields['child_count'].display_name)

        self.assertEqual(
            self.__BOOL_TYPE,
            tag_template.fields['has_dashboards'].type.primitive_type)
        self.assertEqual('Has dashboards',
                         tag_template.fields['has_dashboards'].display_name)

        self.assertEqual(
            self.__DOUBLE_TYPE,
            tag_template.fields['dashboard_count'].type.primitive_type)
        self.assertEqual('Dashboard count',
                         tag_template.fields['dashboard_count'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['server_url'].type.primitive_type)
        self.assertEqual('Sisense Server Url',
                         tag_template.fields['server_url'].display_name)
