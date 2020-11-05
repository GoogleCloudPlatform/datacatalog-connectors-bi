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

from google.datacatalog_connectors.looker.prepare import \
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

    def test_make_tag_template_for_dashboard(self):
        tag_template = self.__factory.make_tag_template_for_dashboard()

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/looker_dashboard_metadata', tag_template.name)

        self.assertEqual('Looker Dashboard Metadata',
                         tag_template.display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['id'].type.primitive_type)
        self.assertEqual('Unique Id', tag_template.fields['id'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['description'].type.primitive_type)
        self.assertEqual('Description',
                         tag_template.fields['description'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['folder_id'].type.primitive_type)
        self.assertEqual('Folder Id',
                         tag_template.fields['folder_id'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['folder_name'].type.primitive_type)
        self.assertEqual('Folder Name',
                         tag_template.fields['folder_name'].display_name)

        self.assertEqual(self.__BOOL_TYPE,
                         tag_template.fields['is_hidden'].type.primitive_type)
        self.assertEqual('Is hidden',
                         tag_template.fields['is_hidden'].display_name)

        self.assertEqual(self.__DOUBLE_TYPE,
                         tag_template.fields['user_id'].type.primitive_type)
        self.assertEqual('Id of User',
                         tag_template.fields['user_id'].display_name)

        self.assertEqual(self.__DOUBLE_TYPE,
                         tag_template.fields['view_count'].type.primitive_type)
        self.assertEqual('Number of views in the web UI',
                         tag_template.fields['view_count'].display_name)

        self.assertEqual(
            self.__DOUBLE_TYPE,
            tag_template.fields['favorite_count'].type.primitive_type)
        self.assertEqual('Number of times favorited',
                         tag_template.fields['favorite_count'].display_name)

        self.assertEqual(
            self.__TIMESTAMP_TYPE,
            tag_template.fields['last_accessed_at'].type.primitive_type)
        self.assertEqual('Time it was last accessed',
                         tag_template.fields['last_accessed_at'].display_name)

        self.assertEqual(
            self.__TIMESTAMP_TYPE,
            tag_template.fields['last_viewed_at'].type.primitive_type)
        self.assertEqual('Time last viewed in the web UI',
                         tag_template.fields['last_viewed_at'].display_name)

        self.assertEqual(self.__BOOL_TYPE,
                         tag_template.fields['is_deleted'].type.primitive_type)
        self.assertEqual('Is soft deleted',
                         tag_template.fields['is_deleted'].display_name)

        self.assertEqual(self.__TIMESTAMP_TYPE,
                         tag_template.fields['deleted_at'].type.primitive_type)
        self.assertEqual('Time it was soft deleted',
                         tag_template.fields['deleted_at'].display_name)

        self.assertEqual(self.__DOUBLE_TYPE,
                         tag_template.fields['deleter_id'].type.primitive_type)
        self.assertEqual('Id of User that soft deleted it',
                         tag_template.fields['deleter_id'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['instance_url'].type.primitive_type)
        self.assertEqual('Looker Instance Url',
                         tag_template.fields['instance_url'].display_name)

    def test_make_tag_template_for_folder(self):
        tag_template = self.__factory.make_tag_template_for_folder()

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/looker_folder_metadata', tag_template.name)

        self.assertEqual('Looker Folder Metadata', tag_template.display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['id'].type.primitive_type)
        self.assertEqual('Unique Id', tag_template.fields['id'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['name'].type.primitive_type)
        self.assertEqual('Unique Name',
                         tag_template.fields['name'].display_name)

        self.assertEqual(
            self.__BOOL_TYPE,
            tag_template.fields['has_children'].type.primitive_type)
        self.assertEqual('Has children',
                         tag_template.fields['has_children'].display_name)

        self.assertEqual(
            self.__DOUBLE_TYPE,
            tag_template.fields['children_count'].type.primitive_type)
        self.assertEqual('Children count',
                         tag_template.fields['children_count'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['parent_id'].type.primitive_type)
        self.assertEqual('Id of Parent',
                         tag_template.fields['parent_id'].display_name)

        self.assertEqual(
            self.__BOOL_TYPE,
            tag_template.fields['has_dashboards'].type.primitive_type)
        self.assertEqual('Has dashboards',
                         tag_template.fields['has_dashboards'].display_name)

        self.assertEqual(
            self.__DOUBLE_TYPE,
            tag_template.fields['dashboards_count'].type.primitive_type)
        self.assertEqual('Dashboards count',
                         tag_template.fields['dashboards_count'].display_name)

        self.assertEqual(self.__BOOL_TYPE,
                         tag_template.fields['has_looks'].type.primitive_type)
        self.assertEqual('Has looks',
                         tag_template.fields['has_looks'].display_name)

        self.assertEqual(
            self.__DOUBLE_TYPE,
            tag_template.fields['looks_count'].type.primitive_type)
        self.assertEqual('Looks count',
                         tag_template.fields['looks_count'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['instance_url'].type.primitive_type)
        self.assertEqual('Looker Instance Url',
                         tag_template.fields['instance_url'].display_name)

    def test_make_tag_template_for_look(self):
        tag_template = self.__factory.make_tag_template_for_look()

        self.assertEqual(
            'projects/test-project/locations/test-location/'
            'tagTemplates/looker_look_metadata', tag_template.name)

        self.assertEqual('Looker Look Metadata', tag_template.display_name)

        self.assertEqual(self.__DOUBLE_TYPE,
                         tag_template.fields['id'].type.primitive_type)
        self.assertEqual('Unique Id', tag_template.fields['id'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['description'].type.primitive_type)
        self.assertEqual('Description',
                         tag_template.fields['description'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['folder_id'].type.primitive_type)
        self.assertEqual('Folder Id',
                         tag_template.fields['folder_id'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['folder_name'].type.primitive_type)
        self.assertEqual('Folder Name',
                         tag_template.fields['folder_name'].display_name)

        self.assertEqual(self.__BOOL_TYPE,
                         tag_template.fields['is_public'].type.primitive_type)
        self.assertEqual('Is public',
                         tag_template.fields['is_public'].display_name)

        self.assertEqual(self.__DOUBLE_TYPE,
                         tag_template.fields['user_id'].type.primitive_type)
        self.assertEqual('Id of User',
                         tag_template.fields['user_id'].display_name)

        self.assertEqual(
            self.__DOUBLE_TYPE,
            tag_template.fields['last_updater_id'].type.primitive_type)
        self.assertEqual('Id of User that last updated it',
                         tag_template.fields['last_updater_id'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['url'].type.primitive_type)
        self.assertEqual('Url', tag_template.fields['url'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['short_url'].type.primitive_type)
        self.assertEqual('Short Url',
                         tag_template.fields['short_url'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['public_url'].type.primitive_type)
        self.assertEqual('Public Url',
                         tag_template.fields['public_url'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['excel_file_url'].type.primitive_type)
        self.assertEqual('Excel File Url',
                         tag_template.fields['excel_file_url'].display_name)

        self.assertEqual(
            self.__STRING_TYPE, tag_template.
            fields['google_spreadsheet_formula'].type.primitive_type)
        self.assertEqual(
            'Google Spreadsheet Formula',
            tag_template.fields['google_spreadsheet_formula'].display_name)

        self.assertEqual(self.__DOUBLE_TYPE,
                         tag_template.fields['view_count'].type.primitive_type)
        self.assertEqual('Number of views in the web UI',
                         tag_template.fields['view_count'].display_name)

        self.assertEqual(
            self.__DOUBLE_TYPE,
            tag_template.fields['favorite_count'].type.primitive_type)
        self.assertEqual('Number of times favorited',
                         tag_template.fields['favorite_count'].display_name)

        self.assertEqual(
            self.__TIMESTAMP_TYPE,
            tag_template.fields['last_accessed_at'].type.primitive_type)
        self.assertEqual('Time it was last accessed',
                         tag_template.fields['last_accessed_at'].display_name)

        self.assertEqual(
            self.__TIMESTAMP_TYPE,
            tag_template.fields['last_viewed_at'].type.primitive_type)
        self.assertEqual('Time last viewed in the web UI',
                         tag_template.fields['last_viewed_at'].display_name)

        self.assertEqual(self.__BOOL_TYPE,
                         tag_template.fields['is_deleted'].type.primitive_type)
        self.assertEqual('Is soft deleted',
                         tag_template.fields['is_deleted'].display_name)

        self.assertEqual(self.__TIMESTAMP_TYPE,
                         tag_template.fields['deleted_at'].type.primitive_type)
        self.assertEqual('Time it was soft deleted',
                         tag_template.fields['deleted_at'].display_name)

        self.assertEqual(self.__DOUBLE_TYPE,
                         tag_template.fields['deleter_id'].type.primitive_type)
        self.assertEqual('Id of User that soft deleted it',
                         tag_template.fields['deleter_id'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['instance_url'].type.primitive_type)
        self.assertEqual('Looker Instance Url',
                         tag_template.fields['instance_url'].display_name)
