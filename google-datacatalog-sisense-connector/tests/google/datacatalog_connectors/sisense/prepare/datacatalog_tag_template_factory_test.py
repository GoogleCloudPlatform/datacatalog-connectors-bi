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

    def test_make_tag_template_for_dashboard(self):
        tag_template = self.__factory.make_tag_template_for_dashboard()

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/sisense_dashboard_metadata', tag_template.name)

        self.assertEqual('Sisense Dashboard Metadata',
                         tag_template.display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['id'].type.primitive_type)
        self.assertEqual('Id', tag_template.fields['id'].display_name)
        self.assertTrue(tag_template.fields['id'].is_required)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['owner_username'].type.primitive_type)
        self.assertEqual('Owner username',
                         tag_template.fields['owner_username'].display_name)
        self.assertFalse(tag_template.fields['owner_username'].is_required)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['owner_name'].type.primitive_type)
        self.assertEqual('Owner name',
                         tag_template.fields['owner_name'].display_name)
        self.assertFalse(tag_template.fields['owner_name'].is_required)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['folder_id'].type.primitive_type)
        self.assertEqual('Folder Id',
                         tag_template.fields['folder_id'].display_name)
        self.assertFalse(tag_template.fields['folder_id'].is_required)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['folder_name'].type.primitive_type)
        self.assertEqual('Folder Name',
                         tag_template.fields['folder_name'].display_name)
        self.assertFalse(tag_template.fields['folder_name'].is_required)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['folder_entry'].type.primitive_type)
        self.assertEqual('Data Catalog Entry for the Folder',
                         tag_template.fields['folder_entry'].display_name)
        self.assertFalse(tag_template.fields['folder_entry'].is_required)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['datasource'].type.primitive_type)
        self.assertEqual('Data Source',
                         tag_template.fields['datasource'].display_name)
        self.assertFalse(tag_template.fields['datasource'].is_required)

        self.assertEqual(
            self.__TIMESTAMP_TYPE,
            tag_template.fields['last_publish'].type.primitive_type)
        self.assertEqual('Time it was last published',
                         tag_template.fields['last_publish'].display_name)
        self.assertFalse(tag_template.fields['last_publish'].is_required)

        self.assertEqual(
            self.__TIMESTAMP_TYPE,
            tag_template.fields['last_opened'].type.primitive_type)
        self.assertEqual('Time it was last opened',
                         tag_template.fields['last_opened'].display_name)
        self.assertFalse(tag_template.fields['last_opened'].is_required)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['server_url'].type.primitive_type)
        self.assertEqual('Sisense Server Url',
                         tag_template.fields['server_url'].display_name)
        self.assertTrue(tag_template.fields['server_url'].is_required)

    def test_make_tag_template_for_folder(self):
        tag_template = self.__factory.make_tag_template_for_folder()

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/sisense_folder_metadata', tag_template.name)

        self.assertEqual('Sisense Folder Metadata', tag_template.display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['id'].type.primitive_type)
        self.assertEqual('Id', tag_template.fields['id'].display_name)
        self.assertTrue(tag_template.fields['id'].is_required)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['owner_username'].type.primitive_type)
        self.assertEqual('Owner username',
                         tag_template.fields['owner_username'].display_name)
        self.assertFalse(tag_template.fields['owner_username'].is_required)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['owner_name'].type.primitive_type)
        self.assertEqual('Owner name',
                         tag_template.fields['owner_name'].display_name)
        self.assertFalse(tag_template.fields['owner_name'].is_required)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['parent_id'].type.primitive_type)
        self.assertEqual('Id of Parent',
                         tag_template.fields['parent_id'].display_name)
        self.assertFalse(tag_template.fields['parent_id'].is_required)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['parent_name'].type.primitive_type)
        self.assertEqual('Parent Folder',
                         tag_template.fields['parent_name'].display_name)
        self.assertFalse(tag_template.fields['parent_name'].is_required)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['parent_folder_entry'].type.primitive_type)
        self.assertEqual(
            'Data Catalog Entry for the parent Folder',
            tag_template.fields['parent_folder_entry'].display_name)
        self.assertFalse(
            tag_template.fields['parent_folder_entry'].is_required)

        self.assertEqual(
            self.__BOOL_TYPE,
            tag_template.fields['has_children'].type.primitive_type)
        self.assertEqual('Has children',
                         tag_template.fields['has_children'].display_name)
        self.assertTrue(tag_template.fields['has_children'].is_required)

        self.assertEqual(
            self.__DOUBLE_TYPE,
            tag_template.fields['child_count'].type.primitive_type)
        self.assertEqual('Child count',
                         tag_template.fields['child_count'].display_name)
        self.assertFalse(tag_template.fields['child_count'].is_required)

        self.assertEqual(
            self.__BOOL_TYPE,
            tag_template.fields['has_dashboards'].type.primitive_type)
        self.assertEqual('Has dashboards',
                         tag_template.fields['has_dashboards'].display_name)
        self.assertTrue(tag_template.fields['has_dashboards'].is_required)

        self.assertEqual(
            self.__DOUBLE_TYPE,
            tag_template.fields['dashboard_count'].type.primitive_type)
        self.assertEqual('Dashboard count',
                         tag_template.fields['dashboard_count'].display_name)
        self.assertFalse(tag_template.fields['dashboard_count'].is_required)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['server_url'].type.primitive_type)
        self.assertEqual('Sisense Server Url',
                         tag_template.fields['server_url'].display_name)
        self.assertTrue(tag_template.fields['server_url'].is_required)

    def test_make_tag_template_for_widget(self):
        tag_template = self.__factory.make_tag_template_for_widget()

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/sisense_widget_metadata', tag_template.name)

        self.assertEqual('Sisense Widget Metadata', tag_template.display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['id'].type.primitive_type)
        self.assertEqual('Id', tag_template.fields['id'].display_name)
        self.assertTrue(tag_template.fields['id'].is_required)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['type'].type.primitive_type)
        self.assertEqual('Type', tag_template.fields['type'].display_name)
        self.assertTrue(tag_template.fields['type'].is_required)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['subtype'].type.primitive_type)
        self.assertEqual('Subtype',
                         tag_template.fields['subtype'].display_name)
        self.assertFalse(tag_template.fields['subtype'].is_required)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['owner_username'].type.primitive_type)
        self.assertEqual('Owner username',
                         tag_template.fields['owner_username'].display_name)
        self.assertFalse(tag_template.fields['owner_username'].is_required)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['owner_name'].type.primitive_type)
        self.assertEqual('Owner name',
                         tag_template.fields['owner_name'].display_name)
        self.assertFalse(tag_template.fields['owner_name'].is_required)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['dashboard_id'].type.primitive_type)
        self.assertEqual('Dashboard Id',
                         tag_template.fields['dashboard_id'].display_name)
        self.assertTrue(tag_template.fields['dashboard_id'].is_required)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['dashboard_title'].type.primitive_type)
        self.assertEqual('Dashboard Title',
                         tag_template.fields['dashboard_title'].display_name)
        self.assertTrue(tag_template.fields['dashboard_title'].is_required)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['dashboard_entry'].type.primitive_type)
        self.assertEqual('Data Catalog Entry for the Dashboard',
                         tag_template.fields['dashboard_entry'].display_name)
        self.assertTrue(tag_template.fields['dashboard_entry'].is_required)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['datasource'].type.primitive_type)
        self.assertEqual('Data Source',
                         tag_template.fields['datasource'].display_name)
        self.assertFalse(tag_template.fields['datasource'].is_required)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['server_url'].type.primitive_type)
        self.assertEqual('Sisense Server Url',
                         tag_template.fields['server_url'].display_name)
        self.assertTrue(tag_template.fields['server_url'].is_required)
