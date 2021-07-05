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

from google.cloud import datacatalog
from google.cloud.datacatalog import TagTemplate
from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.sisense.prepare import constants


class DataCatalogTagTemplateFactory(prepare.BaseTagTemplateFactory):
    __BOOL_TYPE = datacatalog.FieldType.PrimitiveType.BOOL
    __DOUBLE_TYPE = datacatalog.FieldType.PrimitiveType.DOUBLE
    __STRING_TYPE = datacatalog.FieldType.PrimitiveType.STRING
    __TIMESTAMP_TYPE = datacatalog.FieldType.PrimitiveType.TIMESTAMP

    def __init__(self, project_id: str, location_id: str):
        self.__project_id = project_id
        self.__location_id = location_id

    def make_tag_template_for_dashboard(self) -> TagTemplate:
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_DASHBOARD)

        tag_template.display_name = 'Sisense Dashboard Metadata'

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Id',
                                       is_required=True,
                                       order=10)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='owner_username',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Owner username',
                                       order=9)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='owner_name',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Owner name',
                                       order=8)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='folder_id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Folder Id',
                                       order=7)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='folder_name',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Folder Name',
                                       order=6)

        self._add_primitive_type_field(
            tag_template=tag_template,
            field_id='folder_entry',
            field_type=self.__STRING_TYPE,
            display_name='Data Catalog Entry for the Folder',
            order=5)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='datasource',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Data Source',
                                       order=4)

        self._add_primitive_type_field(
            tag_template=tag_template,
            field_id='last_publish',
            field_type=self.__TIMESTAMP_TYPE,
            display_name='Time it was last published',
            order=3)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='last_opened',
                                       field_type=self.__TIMESTAMP_TYPE,
                                       display_name='Time it was last opened',
                                       order=2)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='server_url',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Sisense Server Url',
                                       is_required=True,
                                       order=1)

        return tag_template

    def make_tag_template_for_folder(self) -> TagTemplate:
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_FOLDER)

        tag_template.display_name = 'Sisense Folder Metadata'

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Id',
                                       is_required=True,
                                       order=11)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='owner_username',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Owner username',
                                       order=10)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='owner_name',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Owner name',
                                       order=9)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='parent_id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Id of Parent',
                                       order=8)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='parent_name',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Parent Folder',
                                       order=7)

        self._add_primitive_type_field(
            tag_template=tag_template,
            field_id='parent_folder_entry',
            field_type=self.__STRING_TYPE,
            display_name='Data Catalog Entry for the parent Folder',
            order=6)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='has_children',
                                       field_type=self.__BOOL_TYPE,
                                       display_name='Has children',
                                       is_required=True,
                                       order=5)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='child_count',
                                       field_type=self.__DOUBLE_TYPE,
                                       display_name='Child count',
                                       order=4)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='has_dashboards',
                                       field_type=self.__BOOL_TYPE,
                                       display_name='Has dashboards',
                                       is_required=True,
                                       order=3)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='dashboard_count',
                                       field_type=self.__DOUBLE_TYPE,
                                       display_name='Dashboard count',
                                       order=2)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='server_url',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Sisense Server Url',
                                       is_required=True,
                                       order=1)

        return tag_template

    def make_tag_template_for_jaql(self) -> TagTemplate:
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_JAQL)

        tag_template.display_name = 'Sisense JAQL Metadata'

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='table',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Table',
                                       order=6)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='column',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Column',
                                       order=5)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='dimension',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Dimension',
                                       order=4)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='formula',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Formula',
                                       order=3)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='aggregation',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Aggregation',
                                       order=2)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='server_url',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Sisense Server Url',
                                       is_required=True,
                                       order=1)

        return tag_template

    def make_tag_template_for_widget(self) -> TagTemplate:
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_WIDGET)

        tag_template.display_name = 'Sisense Widget Metadata'

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Id',
                                       is_required=True,
                                       order=10)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='type',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Type',
                                       is_required=True,
                                       order=9)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='subtype',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Subtype',
                                       order=8)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='owner_username',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Owner username',
                                       order=7)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='owner_name',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Owner name',
                                       order=6)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='dashboard_id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Dashboard Id',
                                       is_required=True,
                                       order=5)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='dashboard_title',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Dashboard Title',
                                       is_required=True,
                                       order=4)

        self._add_primitive_type_field(
            tag_template=tag_template,
            field_id='dashboard_entry',
            field_type=self.__STRING_TYPE,
            display_name='Data Catalog Entry for the Dashboard',
            is_required=True,
            order=3)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='datasource',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Data Source',
                                       order=2)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='server_url',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Sisense Server Url',
                                       is_required=True,
                                       order=1)

        return tag_template
