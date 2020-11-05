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

from . import constant


class DataCatalogTagTemplateFactory(prepare.BaseTagTemplateFactory):
    __BOOL_TYPE = datacatalog.FieldType.PrimitiveType.BOOL
    __DOUBLE_TYPE = datacatalog.FieldType.PrimitiveType.DOUBLE
    __STRING_TYPE = datacatalog.FieldType.PrimitiveType.STRING
    __TIMESTAMP_TYPE = datacatalog.FieldType.PrimitiveType.TIMESTAMP

    def __init__(self, project_id, location_id):
        self.__project_id = project_id
        self.__location_id = location_id

    def make_tag_template_for_dashboard(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constant.TAG_TEMPLATE_ID_DASHBOARD)

        tag_template.display_name = 'Looker Dashboard Metadata'

        self._add_primitive_type_field(tag_template, 'id', self.__STRING_TYPE,
                                       'Unique Id')

        self._add_primitive_type_field(tag_template, 'description',
                                       self.__STRING_TYPE, 'Description')

        self._add_primitive_type_field(tag_template, 'folder_id',
                                       self.__STRING_TYPE, 'Folder Id')

        self._add_primitive_type_field(tag_template, 'folder_name',
                                       self.__STRING_TYPE, 'Folder Name')

        self._add_primitive_type_field(tag_template, 'folder_entry',
                                       self.__STRING_TYPE,
                                       'Data Catalog Entry for the Folder')

        self._add_primitive_type_field(tag_template, 'is_hidden',
                                       self.__BOOL_TYPE, 'Is hidden')

        self._add_primitive_type_field(tag_template, 'user_id',
                                       self.__DOUBLE_TYPE, 'Id of User')

        self._add_primitive_type_field(tag_template, 'view_count',
                                       self.__DOUBLE_TYPE,
                                       'Number of views in the web UI')

        self._add_primitive_type_field(tag_template, 'favorite_count',
                                       self.__DOUBLE_TYPE,
                                       'Number of times favorited')

        self._add_primitive_type_field(tag_template, 'last_accessed_at',
                                       self.__TIMESTAMP_TYPE,
                                       'Time it was last accessed')

        self._add_primitive_type_field(tag_template, 'last_viewed_at',
                                       self.__TIMESTAMP_TYPE,
                                       'Time last viewed in the web UI')

        self._add_primitive_type_field(tag_template, 'is_deleted',
                                       self.__BOOL_TYPE, 'Is soft deleted')

        self._add_primitive_type_field(tag_template, 'deleted_at',
                                       self.__TIMESTAMP_TYPE,
                                       'Time it was soft deleted')

        self._add_primitive_type_field(tag_template, 'deleter_id',
                                       self.__DOUBLE_TYPE,
                                       'Id of User that soft deleted it')

        self._add_primitive_type_field(tag_template, 'instance_url',
                                       self.__STRING_TYPE,
                                       'Looker Instance Url')

        return tag_template

    def make_tag_template_for_dashboard_element(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constant.TAG_TEMPLATE_ID_DASHBOARD_ELEMENT)

        tag_template.display_name = 'Looker Dashboard Element Metadata'

        self._add_primitive_type_field(tag_template, 'id', self.__STRING_TYPE,
                                       'Unique Id')

        self._add_primitive_type_field(tag_template, 'type',
                                       self.__STRING_TYPE, 'Type')

        self._add_primitive_type_field(tag_template, 'dashboard_id',
                                       self.__STRING_TYPE, 'Id of Dashboard')

        self._add_primitive_type_field(tag_template, 'dashboard_title',
                                       self.__STRING_TYPE,
                                       'Title of Dashboard')

        self._add_primitive_type_field(tag_template, 'dashboard_entry',
                                       self.__STRING_TYPE,
                                       'Data Catalog Entry for the Dashboard')

        self._add_primitive_type_field(tag_template, 'look_id',
                                       self.__DOUBLE_TYPE, 'Id Of Look')

        self._add_primitive_type_field(tag_template, 'look_title',
                                       self.__STRING_TYPE, 'Title Of Look')

        self._add_primitive_type_field(tag_template, 'look_entry',
                                       self.__STRING_TYPE,
                                       'Data Catalog Entry for the Look')

        self._add_primitive_type_field(tag_template, 'lookml_link_id',
                                       self.__STRING_TYPE, 'LookML link ID')

        self._add_primitive_type_field(tag_template, 'query_id',
                                       self.__DOUBLE_TYPE, 'Id Of Query')

        self._add_primitive_type_field(tag_template, 'query_entry',
                                       self.__STRING_TYPE,
                                       'Data Catalog Entry for the Query')

        self._add_primitive_type_field(tag_template, 'instance_url',
                                       self.__STRING_TYPE,
                                       'Looker Instance Url')

        return tag_template

    def make_tag_template_for_folder(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constant.TAG_TEMPLATE_ID_FOLDER)

        tag_template.display_name = 'Looker Folder Metadata'

        self._add_primitive_type_field(tag_template, 'id', self.__STRING_TYPE,
                                       'Unique Id')

        self._add_primitive_type_field(tag_template, 'name',
                                       self.__STRING_TYPE, 'Unique Name')

        self._add_primitive_type_field(tag_template, 'has_children',
                                       self.__BOOL_TYPE, 'Has children')

        self._add_primitive_type_field(tag_template, 'children_count',
                                       self.__DOUBLE_TYPE, 'Children count')

        self._add_primitive_type_field(tag_template, 'parent_id',
                                       self.__STRING_TYPE, 'Id of Parent')

        self._add_primitive_type_field(
            tag_template, 'parent_entry', self.__STRING_TYPE,
            'Data Catalog Entry for the parent Folder')

        self._add_primitive_type_field(tag_template, 'has_dashboards',
                                       self.__BOOL_TYPE, 'Has dashboards')

        self._add_primitive_type_field(tag_template, 'dashboards_count',
                                       self.__DOUBLE_TYPE, 'Dashboards count')

        self._add_primitive_type_field(tag_template, 'has_looks',
                                       self.__BOOL_TYPE, 'Has looks')

        self._add_primitive_type_field(tag_template, 'looks_count',
                                       self.__DOUBLE_TYPE, 'Looks count')

        self._add_primitive_type_field(tag_template, 'instance_url',
                                       self.__STRING_TYPE,
                                       'Looker Instance Url')

        return tag_template

    def make_tag_template_for_look(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constant.TAG_TEMPLATE_ID_LOOK)

        tag_template.display_name = 'Looker Look Metadata'

        self._add_primitive_type_field(tag_template, 'id', self.__DOUBLE_TYPE,
                                       'Unique Id')

        self._add_primitive_type_field(tag_template, 'description',
                                       self.__STRING_TYPE, 'Description')

        self._add_primitive_type_field(tag_template, 'folder_id',
                                       self.__STRING_TYPE, 'Folder Id')

        self._add_primitive_type_field(tag_template, 'folder_name',
                                       self.__STRING_TYPE, 'Folder Name')

        self._add_primitive_type_field(tag_template, 'folder_entry',
                                       self.__STRING_TYPE,
                                       'Data Catalog Entry for the Folder')

        self._add_primitive_type_field(tag_template, 'is_public',
                                       self.__BOOL_TYPE, 'Is public')

        self._add_primitive_type_field(tag_template, 'user_id',
                                       self.__DOUBLE_TYPE, 'Id of User')

        self._add_primitive_type_field(tag_template, 'last_updater_id',
                                       self.__DOUBLE_TYPE,
                                       'Id of User that last updated it')

        self._add_primitive_type_field(tag_template, 'query_id',
                                       self.__DOUBLE_TYPE, 'Query Id')

        self._add_primitive_type_field(tag_template, 'query_entry',
                                       self.__STRING_TYPE,
                                       'Data Catalog Entry for the Query')

        self._add_primitive_type_field(tag_template, 'url', self.__STRING_TYPE,
                                       'Url')

        self._add_primitive_type_field(tag_template, 'short_url',
                                       self.__STRING_TYPE, 'Short Url')

        self._add_primitive_type_field(tag_template, 'public_url',
                                       self.__STRING_TYPE, 'Public Url')

        self._add_primitive_type_field(tag_template, 'excel_file_url',
                                       self.__STRING_TYPE, 'Excel File Url')

        self._add_primitive_type_field(tag_template,
                                       'google_spreadsheet_formula',
                                       self.__STRING_TYPE,
                                       'Google Spreadsheet Formula')

        self._add_primitive_type_field(tag_template, 'view_count',
                                       self.__DOUBLE_TYPE,
                                       'Number of views in the web UI')

        self._add_primitive_type_field(tag_template, 'favorite_count',
                                       self.__DOUBLE_TYPE,
                                       'Number of times favorited')

        self._add_primitive_type_field(tag_template, 'last_accessed_at',
                                       self.__TIMESTAMP_TYPE,
                                       'Time it was last accessed')

        self._add_primitive_type_field(tag_template, 'last_viewed_at',
                                       self.__TIMESTAMP_TYPE,
                                       'Time last viewed in the web UI')

        self._add_primitive_type_field(tag_template, 'is_deleted',
                                       self.__BOOL_TYPE, 'Is soft deleted')

        self._add_primitive_type_field(tag_template, 'deleted_at',
                                       self.__TIMESTAMP_TYPE,
                                       'Time it was soft deleted')

        self._add_primitive_type_field(tag_template, 'deleter_id',
                                       self.__DOUBLE_TYPE,
                                       'Id of User that soft deleted it')

        self._add_primitive_type_field(tag_template, 'instance_url',
                                       self.__STRING_TYPE,
                                       'Looker Instance Url')

        return tag_template

    def make_tag_template_for_query(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constant.TAG_TEMPLATE_ID_QUERY)

        tag_template.display_name = 'Looker Query Metadata'

        self._add_primitive_type_field(tag_template, 'id', self.__DOUBLE_TYPE,
                                       'Unique Id')

        self._add_primitive_type_field(tag_template, 'fields',
                                       self.__STRING_TYPE, 'Fields')

        self._add_primitive_type_field(tag_template, 'pivots',
                                       self.__STRING_TYPE, 'Pivots')

        self._add_primitive_type_field(tag_template, 'sorts',
                                       self.__STRING_TYPE,
                                       'Sorting for the results')

        self._add_primitive_type_field(tag_template, 'runtime',
                                       self.__DOUBLE_TYPE, 'Runtime')

        self._add_primitive_type_field(tag_template, 'client_id',
                                       self.__STRING_TYPE,
                                       'Id for explore URLs')

        self._add_primitive_type_field(tag_template, 'query_timezone',
                                       self.__STRING_TYPE, 'Query Timezone')

        self._add_primitive_type_field(tag_template, 'lookml_model',
                                       self.__STRING_TYPE, 'LookML Model name')

        self._add_primitive_type_field(tag_template, 'explore_name',
                                       self.__STRING_TYPE, 'Explore name')

        self._add_primitive_type_field(tag_template, 'sql', self.__STRING_TYPE,
                                       'Generated SQL')

        self._add_primitive_type_field(tag_template, 'lookml_project',
                                       self.__STRING_TYPE, 'LookML Project')

        self._add_primitive_type_field(tag_template, 'connection',
                                       self.__STRING_TYPE, 'Connection name')

        self._add_primitive_type_field(tag_template, 'host',
                                       self.__STRING_TYPE,
                                       'Server hostname or address')

        self._add_primitive_type_field(tag_template, 'database',
                                       self.__STRING_TYPE, 'Database name')

        self._add_primitive_type_field(tag_template, 'connection_dialect',
                                       self.__STRING_TYPE, 'SQL Dialect name')

        self._add_primitive_type_field(tag_template, 'connection_username',
                                       self.__STRING_TYPE,
                                       'Username for server authentication')

        self._add_primitive_type_field(tag_template, 'instance_url',
                                       self.__STRING_TYPE,
                                       'Looker Instance Url')

        return tag_template
