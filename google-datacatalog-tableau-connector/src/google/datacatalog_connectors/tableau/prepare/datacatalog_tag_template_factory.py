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

from google.cloud import datacatalog
from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.tableau.prepare import constants


class DataCatalogTagTemplateFactory(prepare.BaseTagTemplateFactory):
    __BOOL_TYPE = datacatalog.FieldType.PrimitiveType.BOOL
    __STRING_TYPE = datacatalog.FieldType.PrimitiveType.STRING

    def __init__(self, project_id, location_id):
        self.__project_id = project_id
        self.__location_id = location_id

    def make_tag_template_for_dashboard(self):
        tag_template_id = constants.TAG_TEMPLATE_ID_DASHBOARD

        tag_template = datacatalog.TagTemplate()

        tag_template.name = \
            datacatalog.DataCatalogClient.tag_template_path(
                project=self.__project_id,
                location=self.__location_id,
                tag_template=tag_template_id)

        tag_template.display_name = 'Tableau Dashboard Metadata'

        self._add_primitive_type_field(
            tag_template, 'id', self.__STRING_TYPE,
            'Unique identifier for the Metadata API')

        self._add_primitive_type_field(tag_template, 'luid',
                                       self.__STRING_TYPE,
                                       'Unique identifier for the REST API')

        self._add_primitive_type_field(tag_template, 'workbook_luid',
                                       self.__STRING_TYPE,
                                       'Workbook identifier for the REST API')

        self._add_primitive_type_field(tag_template, 'workbook_name',
                                       self.__STRING_TYPE, 'Workbook name')

        self._add_primitive_type_field(tag_template, 'workbook_entry',
                                       self.__STRING_TYPE,
                                       'Data Catalog Entry for the Workbook')

        self._add_primitive_type_field(tag_template, 'site_name',
                                       self.__STRING_TYPE, 'Site name')

        self._add_primitive_type_field(tag_template, 'has_external_url',
                                       self.__BOOL_TYPE, 'Has external URL')

        return tag_template_id, tag_template

    def make_tag_template_for_sheet(self):
        tag_template_id = constants.TAG_TEMPLATE_ID_SHEET

        tag_template = datacatalog.TagTemplate()

        tag_template.name = \
            datacatalog.DataCatalogClient.tag_template_path(
                project=self.__project_id,
                location=self.__location_id,
                tag_template=tag_template_id)

        tag_template.display_name = 'Tableau Sheet Metadata'

        self._add_primitive_type_field(
            tag_template, 'id', self.__STRING_TYPE,
            'Unique identifier for the Metadata API')

        self._add_primitive_type_field(tag_template, 'luid',
                                       self.__STRING_TYPE,
                                       'Unique identifier for the REST API')

        self._add_primitive_type_field(tag_template, 'workbook_luid',
                                       self.__STRING_TYPE,
                                       'Workbook identifier for the REST API')

        self._add_primitive_type_field(tag_template, 'workbook_name',
                                       self.__STRING_TYPE, 'Workbook name')

        self._add_primitive_type_field(tag_template, 'workbook_entry',
                                       self.__STRING_TYPE,
                                       'Data Catalog Entry for the Workbook')

        self._add_primitive_type_field(tag_template, 'site_name',
                                       self.__STRING_TYPE, 'Site name')

        self._add_primitive_type_field(tag_template, 'has_external_url',
                                       self.__BOOL_TYPE, 'Has external URL')

        return tag_template_id, tag_template

    def make_tag_template_for_workbook(self):
        tag_template_id = constants.TAG_TEMPLATE_ID_WORKBOOK

        tag_template = datacatalog.TagTemplate()

        tag_template.name = \
            datacatalog.DataCatalogClient.tag_template_path(
                project=self.__project_id,
                location=self.__location_id,
                tag_template=tag_template_id)

        tag_template.display_name = 'Tableau Workbook Metadata'

        self._add_primitive_type_field(tag_template, 'luid',
                                       self.__STRING_TYPE,
                                       'Unique identifier for the REST API')

        self._add_primitive_type_field(tag_template, 'site_name',
                                       self.__STRING_TYPE, 'Site name')

        self._add_primitive_type_field(tag_template, 'project_name',
                                       self.__STRING_TYPE, 'Project name')

        self._add_primitive_type_field(tag_template, 'owner_username',
                                       self.__STRING_TYPE, 'Owner username')

        self._add_primitive_type_field(tag_template, 'owner_name',
                                       self.__STRING_TYPE, 'Owner name')

        self._add_primitive_type_field(tag_template,
                                       'upstream_table_definition',
                                       self.__STRING_TYPE,
                                       'upstream_tables values meaning')

        self._add_primitive_type_field(tag_template, 'upstream_tables',
                                       self.__STRING_TYPE,
                                       'Tables used by the workbook')

        return tag_template_id, tag_template
