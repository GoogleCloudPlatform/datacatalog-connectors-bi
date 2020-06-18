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
    __DOUBLE_TYPE = enums.FieldType.PrimitiveType.DOUBLE
    __STRING_TYPE = enums.FieldType.PrimitiveType.STRING
    __TIMESTAMP_TYPE = enums.FieldType.PrimitiveType.TIMESTAMP

    def __init__(self, project_id, location_id):
        self.__project_id = project_id
        self.__location_id = location_id

    def make_tag_template_for_dashboard(self):
        tag_template = types.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constant.TAG_TEMPLATE_ID_DASHBOARD)

        tag_template.display_name = 'Looker Dashboard Metadata'

        tag_template.fields['id'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['id'].display_name = 'Unique Id'

        tag_template.fields['description'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['description'].display_name = 'Description'

        tag_template.fields['folder_id'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['folder_id'].display_name = 'Folder Id'

        tag_template.fields[
            'folder_name'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['folder_name'].display_name = 'Folder Name'

        tag_template.fields[
            'folder_entry'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['folder_entry'].display_name = \
            'Data Catalog Entry for the Folder'

        tag_template.fields['is_hidden'].type.primitive_type = self.__BOOL_TYPE
        tag_template.fields['is_hidden'].display_name = 'Is hidden'

        tag_template.fields['user_id'].type.primitive_type = self.__DOUBLE_TYPE
        tag_template.fields['user_id'].display_name = 'Id of User'

        tag_template.fields['view_count'].type.primitive_type = \
            self.__DOUBLE_TYPE
        tag_template.fields['view_count'].display_name = \
            'Number of views in the web UI'

        tag_template.fields['favorite_count'].type.primitive_type = \
            self.__DOUBLE_TYPE
        tag_template.fields['favorite_count'].display_name = \
            'Number of times favorited'

        tag_template.fields['last_accessed_at'].type.primitive_type = \
            self.__TIMESTAMP_TYPE
        tag_template.fields['last_accessed_at'].display_name = \
            'Time it was last accessed'

        tag_template.fields['last_viewed_at'].type.primitive_type = \
            self.__TIMESTAMP_TYPE
        tag_template.fields['last_viewed_at'].display_name = \
            'Time last viewed in the web UI'

        tag_template.fields['is_deleted'].type.primitive_type = \
            self.__BOOL_TYPE
        tag_template.fields['is_deleted'].display_name = 'Is soft deleted'

        tag_template.fields['deleted_at'].type.primitive_type = \
            self.__TIMESTAMP_TYPE
        tag_template.fields['deleted_at'].display_name = \
            'Time it was soft deleted'

        tag_template.fields['deleter_id'].type.primitive_type = \
            self.__DOUBLE_TYPE
        tag_template.fields['deleter_id'].display_name = \
            'Id of User that soft deleted it'

        tag_template.fields['instance_url'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['instance_url'].display_name = \
            'Looker Instance Url'

        return tag_template

    def make_tag_template_for_dashboard_element(self):
        tag_template = types.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constant.TAG_TEMPLATE_ID_DASHBOARD_ELEMENT)

        tag_template.display_name = 'Looker Dashboard Element Metadata'

        tag_template.fields['id'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['id'].display_name = 'Unique Id'

        tag_template.fields['type'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['type'].display_name = 'Type'

        tag_template.fields['dashboard_id'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['dashboard_id'].display_name = 'Id of Dashboard'

        tag_template.fields['dashboard_title'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['dashboard_title'].display_name = \
            'Title of Dashboard'

        tag_template.fields[
            'dashboard_entry'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['dashboard_entry'].display_name = \
            'Data Catalog Entry for the Dashboard'

        tag_template.fields['look_id'].type.primitive_type = self.__DOUBLE_TYPE
        tag_template.fields['look_id'].display_name = 'Id Of Look'

        tag_template.fields['look_title'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['look_title'].display_name = 'Title Of Look'

        tag_template.fields[
            'look_entry'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['look_entry'].display_name = \
            'Data Catalog Entry for the Look'

        tag_template.fields['lookml_link_id'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['lookml_link_id'].display_name = 'LookML link ID'

        tag_template.fields['query_id'].type.primitive_type = \
            self.__DOUBLE_TYPE
        tag_template.fields['query_id'].display_name = 'Id Of Query'

        tag_template.fields[
            'query_entry'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['query_entry'].display_name = \
            'Data Catalog Entry for the Query'

        tag_template.fields['instance_url'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['instance_url'].display_name = \
            'Looker Instance Url'

        return tag_template

    def make_tag_template_for_folder(self):
        tag_template = types.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constant.TAG_TEMPLATE_ID_FOLDER)

        tag_template.display_name = 'Looker Folder Metadata'

        tag_template.fields['id'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['id'].display_name = 'Unique Id'

        tag_template.fields['name'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['name'].display_name = 'Unique Name'

        tag_template.fields['has_children'].type.primitive_type = \
            self.__BOOL_TYPE
        tag_template.fields['has_children'].display_name = 'Has children'

        tag_template.fields['children_count'].type.primitive_type = \
            self.__DOUBLE_TYPE
        tag_template.fields['children_count'].display_name = 'Children count'

        tag_template.fields['parent_id'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['parent_id'].display_name = 'Id of Parent'

        tag_template.fields[
            'parent_entry'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['parent_entry'].display_name = \
            'Data Catalog Entry for the parent Folder'

        tag_template.fields['has_dashboards'].type.primitive_type = \
            self.__BOOL_TYPE
        tag_template.fields['has_dashboards'].display_name = 'Has dashboards'

        tag_template.fields['dashboards_count'].type.primitive_type = \
            self.__DOUBLE_TYPE
        tag_template.fields['dashboards_count'].display_name = \
            'Dashboards count'

        tag_template.fields['has_looks'].type.primitive_type = self.__BOOL_TYPE
        tag_template.fields['has_looks'].display_name = 'Has looks'

        tag_template.fields['looks_count'].type.primitive_type = \
            self.__DOUBLE_TYPE
        tag_template.fields['looks_count'].display_name = 'Looks count'

        tag_template.fields['instance_url'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['instance_url'].display_name = \
            'Looker Instance Url'

        return tag_template

    def make_tag_template_for_look(self):
        tag_template = types.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constant.TAG_TEMPLATE_ID_LOOK)

        tag_template.display_name = 'Looker Look Metadata'

        tag_template.fields['id'].type.primitive_type = self.__DOUBLE_TYPE
        tag_template.fields['id'].display_name = 'Unique Id'

        tag_template.fields['description'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['description'].display_name = 'Description'

        tag_template.fields['folder_id'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['folder_id'].display_name = 'Folder Id'

        tag_template.fields[
            'folder_name'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['folder_name'].display_name = 'Folder Name'

        tag_template.fields[
            'folder_entry'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['folder_entry'].display_name = \
            'Data Catalog Entry for the Folder'

        tag_template.fields['is_public'].type.primitive_type = self.__BOOL_TYPE
        tag_template.fields['is_public'].display_name = 'Is public'

        tag_template.fields['user_id'].type.primitive_type = self.__DOUBLE_TYPE
        tag_template.fields['user_id'].display_name = 'Id of User'

        tag_template.fields['last_updater_id'].type.primitive_type = \
            self.__DOUBLE_TYPE
        tag_template.fields['last_updater_id'].display_name = \
            'Id of User that last updated it'

        tag_template.fields['query_id'].type.primitive_type = \
            self.__DOUBLE_TYPE
        tag_template.fields['query_id'].display_name = 'Query Id'

        tag_template.fields[
            'query_entry'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['query_entry'].display_name = \
            'Data Catalog Entry for the Query'

        tag_template.fields['url'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['url'].display_name = 'Url'

        tag_template.fields['short_url'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['short_url'].display_name = 'Short Url'

        tag_template.fields['public_url'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['public_url'].display_name = 'Public Url'

        tag_template.fields['excel_file_url'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['excel_file_url'].display_name = 'Excel File Url'

        tag_template.fields['google_spreadsheet_formula']\
            .type.primitive_type = self.__STRING_TYPE
        tag_template.fields['google_spreadsheet_formula'].display_name = \
            'Google Spreadsheet Formula'

        tag_template.fields['view_count'].type.primitive_type = \
            self.__DOUBLE_TYPE
        tag_template.fields['view_count'].display_name = \
            'Number of views in the web UI'

        tag_template.fields['favorite_count'].type.primitive_type = \
            self.__DOUBLE_TYPE
        tag_template.fields['favorite_count'].display_name = \
            'Number of times favorited'

        tag_template.fields['last_accessed_at'].type.primitive_type = \
            self.__TIMESTAMP_TYPE
        tag_template.fields['last_accessed_at'].display_name = \
            'Time it was last accessed'

        tag_template.fields['last_viewed_at'].type.primitive_type = \
            self.__TIMESTAMP_TYPE
        tag_template.fields['last_viewed_at'].display_name = \
            'Time last viewed in the web UI'

        tag_template.fields['is_deleted'].type.primitive_type = \
            self.__BOOL_TYPE
        tag_template.fields['is_deleted'].display_name = 'Is soft deleted'

        tag_template.fields['deleted_at'].type.primitive_type = \
            self.__TIMESTAMP_TYPE
        tag_template.fields['deleted_at'].display_name = \
            'Time it was soft deleted'

        tag_template.fields['deleter_id'].type.primitive_type = \
            self.__DOUBLE_TYPE
        tag_template.fields['deleter_id'].display_name = \
            'Id of User that soft deleted it'

        tag_template.fields['instance_url'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['instance_url'].display_name = \
            'Looker Instance Url'

        return tag_template

    def make_tag_template_for_query(self):
        tag_template = types.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constant.TAG_TEMPLATE_ID_QUERY)

        tag_template.display_name = 'Looker Query Metadata'

        tag_template.fields['id'].type.primitive_type = self.__DOUBLE_TYPE
        tag_template.fields['id'].display_name = 'Unique Id'

        tag_template.fields['fields'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['fields'].display_name = 'Fields'

        tag_template.fields['pivots'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['pivots'].display_name = 'Pivots'

        tag_template.fields['sorts'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['sorts'].display_name = 'Sorting for the results'

        tag_template.fields['runtime'].type.primitive_type = self.__DOUBLE_TYPE
        tag_template.fields['runtime'].display_name = 'Runtime'

        tag_template.fields['client_id'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['client_id'].display_name = 'Id for explore URLs'

        tag_template.fields['query_timezone'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['query_timezone'].display_name = 'Query Timezone'

        tag_template.fields['lookml_model'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['lookml_model'].display_name = 'LookML Model name'

        tag_template.fields['explore_name'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['explore_name'].display_name = 'Explore name'

        tag_template.fields['sql'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['sql'].display_name = 'Generated SQL'

        tag_template.fields['lookml_project'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['lookml_project'].display_name = 'LookML Project'

        tag_template.fields['connection'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['connection'].display_name = 'Connection name'

        tag_template.fields['host'].type.primitive_type = self.__STRING_TYPE
        tag_template.fields['host'].display_name = 'Server hostname or address'

        tag_template.fields['database'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['database'].display_name = 'Database name'

        tag_template.fields['connection_dialect'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['connection_dialect'].display_name = \
            'SQL Dialect name'

        tag_template.fields['connection_username'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['connection_username'].display_name = \
            'Username for server authentication'

        tag_template.fields['instance_url'].type.primitive_type = \
            self.__STRING_TYPE
        tag_template.fields['instance_url'].display_name = \
            'Looker Instance Url'

        return tag_template
