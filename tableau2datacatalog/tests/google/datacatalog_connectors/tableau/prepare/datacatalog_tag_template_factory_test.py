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

from google.cloud.datacatalog import enums

from google.datacatalog_connectors.tableau import prepare


class DataCatalogTagTemplateFactoryTest(unittest.TestCase):
    __BOOL_TYPE = enums.FieldType.PrimitiveType.BOOL
    __STRING_TYPE = enums.FieldType.PrimitiveType.STRING

    def test_make_tag_template_for_dashboard(self):
        tag_template_factory = prepare.DataCatalogTagTemplateFactory(
            'project_id', 'us_central_1')
        template_id, tag_template = tag_template_factory\
            .make_tag_template_for_dashboard()

        self.assertEqual('Tableau Dashboard Metadata',
                         tag_template.display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['id'].type.primitive_type)
        self.assertEqual('Unique identifier for the Metadata API',
                         tag_template.fields['id'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['luid'].type.primitive_type)
        self.assertEqual('Unique identifier for the REST API',
                         tag_template.fields['luid'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['workbook_luid'].type.primitive_type)
        self.assertEqual('Workbook identifier for the REST API',
                         tag_template.fields['workbook_luid'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['workbook_name'].type.primitive_type)
        self.assertEqual('Workbook name',
                         tag_template.fields['workbook_name'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['workbook_entry'].type.primitive_type)
        self.assertEqual('Data Catalog Entry for the Workbook',
                         tag_template.fields['workbook_entry'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['site_name'].type.primitive_type)
        self.assertEqual('Site name',
                         tag_template.fields['site_name'].display_name)

        self.assertEqual(
            self.__BOOL_TYPE,
            tag_template.fields['has_external_url'].type.primitive_type)
        self.assertEqual('Has external URL',
                         tag_template.fields['has_external_url'].display_name)

    def test_make_tag_template_for_sheet(self):
        tag_template_factory = prepare.DataCatalogTagTemplateFactory(
            'project_id', 'us_central_1')
        template_id, tag_template = tag_template_factory\
            .make_tag_template_for_sheet()

        self.assertEqual('Tableau Sheet Metadata', tag_template.display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['id'].type.primitive_type)
        self.assertEqual('Unique identifier for the Metadata API',
                         tag_template.fields['id'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['luid'].type.primitive_type)
        self.assertEqual('Unique identifier for the REST API',
                         tag_template.fields['luid'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['workbook_luid'].type.primitive_type)
        self.assertEqual('Workbook identifier for the REST API',
                         tag_template.fields['workbook_luid'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['workbook_name'].type.primitive_type)
        self.assertEqual('Workbook name',
                         tag_template.fields['workbook_name'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['workbook_entry'].type.primitive_type)
        self.assertEqual('Data Catalog Entry for the Workbook',
                         tag_template.fields['workbook_entry'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['site_name'].type.primitive_type)
        self.assertEqual('Site name',
                         tag_template.fields['site_name'].display_name)

        self.assertEqual(
            self.__BOOL_TYPE,
            tag_template.fields['has_external_url'].type.primitive_type)
        self.assertEqual('Has external URL',
                         tag_template.fields['has_external_url'].display_name)

    def test_make_tag_template_for_workbook(self):
        tag_template_factory = prepare.DataCatalogTagTemplateFactory(
            'project_id', 'us_central_1')
        template_id, tag_template = tag_template_factory\
            .make_tag_template_for_workbook()

        self.assertEqual('Tableau Workbook Metadata',
                         tag_template.display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['luid'].type.primitive_type)
        self.assertEqual('Unique identifier for the REST API',
                         tag_template.fields['luid'].display_name)

        self.assertEqual(self.__STRING_TYPE,
                         tag_template.fields['site_name'].type.primitive_type)
        self.assertEqual('Site name',
                         tag_template.fields['site_name'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['project_name'].type.primitive_type)
        self.assertEqual('Project name',
                         tag_template.fields['project_name'].display_name)

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
            self.__STRING_TYPE, tag_template.
            fields['upstream_table_definition'].type.primitive_type)
        self.assertEqual(
            'upstream_tables values meaning',
            tag_template.fields['upstream_table_definition'].display_name)

        self.assertEqual(
            self.__STRING_TYPE,
            tag_template.fields['upstream_tables'].type.primitive_type)
        self.assertEqual('Tables used by the workbook',
                         tag_template.fields['upstream_tables'].display_name)
