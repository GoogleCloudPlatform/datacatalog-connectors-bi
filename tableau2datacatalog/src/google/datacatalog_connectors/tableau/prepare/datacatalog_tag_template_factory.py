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
from google.cloud.datacatalog import enums, types

from . import constant


class DataCatalogTagTemplateFactory:
    __BOOL_TYPE = enums.FieldType.PrimitiveType.BOOL
    __STRING_TYPE = enums.FieldType.PrimitiveType.STRING

    def __init__(self, project_id, location_id):
        self.__project_id = project_id
        self.__location_id = location_id

    def make_tag_template_for_dashboard(self):
        tag_template_id = constant.TAG_TEMPLATE_DASHBOARD_ID

        tag_template = types.TagTemplate()

        tag_template.name = \
            datacatalog.DataCatalogClient.tag_template_path(
                project=self.__project_id,
                location=self.__location_id,
                tag_template=tag_template_id)

        tag_template.display_name = 'Tableau Dashboard Metadata'

        tag_template.fields['id'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['id'].display_name = \
            'Unique identifier for the Metadata API'

        tag_template.fields['luid'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['luid'].display_name = \
            'Unique identifier for the REST API'

        tag_template.fields['workbook_luid'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['workbook_luid'].display_name = \
            'Workbook identifier for the REST API'

        tag_template.fields['workbook_name'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['workbook_name'].display_name = \
            'Workbook name'

        tag_template.fields[
            'workbook_entry'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['workbook_entry'].display_name = \
            'Data Catalog Entry for the Workbook'

        tag_template.fields['site_name'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['site_name'].display_name = \
            'Site name'

        tag_template.fields['has_external_url'].type.primitive_type = \
            self.__BOOL_TYPE
        tag_template.fields['has_external_url'].display_name = \
            'Has external URL'

        return tag_template_id, tag_template

    def make_tag_template_for_sheet(self):
        tag_template_id = constant.TAG_TEMPLATE_SHEET_ID

        tag_template = types.TagTemplate()

        tag_template.name = \
            datacatalog.DataCatalogClient.tag_template_path(
                project=self.__project_id,
                location=self.__location_id,
                tag_template=tag_template_id)

        tag_template.display_name = 'Tableau Sheet Metadata'

        tag_template.fields['id'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['id'].display_name = \
            'Unique identifier for the Metadata API'

        tag_template.fields['luid'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['luid'].display_name = \
            'Unique identifier for the REST API'

        tag_template.fields['workbook_luid'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['workbook_luid'].display_name = \
            'Workbook identifier for the REST API'

        tag_template.fields['workbook_name'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['workbook_name'].display_name = \
            'Workbook name'

        tag_template.fields[
            'workbook_entry'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['workbook_entry'].display_name = \
            'Data Catalog Entry for the Workbook'

        tag_template.fields['site_name'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['site_name'].display_name = \
            'Site name'

        tag_template.fields['has_external_url'].type.primitive_type = \
            self.__BOOL_TYPE
        tag_template.fields['has_external_url'].display_name = \
            'Has external URL'

        return tag_template_id, tag_template

    def make_tag_template_for_workbook(self):
        tag_template_id = constant.TAG_TEMPLATE_WORKBOOK_ID

        tag_template = types.TagTemplate()

        tag_template.name = \
            datacatalog.DataCatalogClient.tag_template_path(
                project=self.__project_id,
                location=self.__location_id,
                tag_template=tag_template_id)

        tag_template.display_name = 'Tableau Workbook Metadata'

        tag_template.fields['luid'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['luid'].display_name = \
            'Unique identifier for the REST API'

        tag_template.fields['site_name'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['site_name'].display_name = \
            'Site name'

        tag_template.fields['project_name'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['project_name'].display_name = \
            'Project name'

        tag_template.fields['owner_username'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['owner_username'].display_name = \
            'Owner username'

        tag_template.fields['owner_name'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['owner_name'].display_name = \
            'Owner name'

        tag_template.fields['upstream_table_definition']\
            .type.primitive_type = self.__STRING_TYPE
        tag_template.fields['upstream_table_definition'].display_name = \
            'upstream_tables values meaning'

        tag_template.fields['upstream_tables'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['upstream_tables'].display_name = \
            'Tables used by the workbook'

        return tag_template_id, tag_template
