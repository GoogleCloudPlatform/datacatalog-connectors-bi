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
from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.qlik.prepare import \
    constants, dynamic_properties_helper as dph


class DataCatalogTagTemplateFactory(prepare.BaseTagTemplateFactory):
    __ASCII_CHARACTER_ENCODING = 'ASCII'
    __BOOL_TYPE = datacatalog.FieldType.PrimitiveType.BOOL
    __DOUBLE_TYPE = datacatalog.FieldType.PrimitiveType.DOUBLE
    __STRING_TYPE = datacatalog.FieldType.PrimitiveType.STRING
    __TIMESTAMP_TYPE = datacatalog.FieldType.PrimitiveType.TIMESTAMP

    def __init__(self, project_id, location_id):
        self.__project_id = project_id
        self.__location_id = location_id

    def make_tag_template_for_app(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_APP)

        tag_template.display_name = 'Qlik App Metadata'

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Unique Id',
                                       is_required=True,
                                       order=16)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='owner_username',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Owner username',
                                       order=15)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='owner_name',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Owner name',
                                       order=14)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='modified_by_username',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Username who modified it',
                                       order=13)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='published',
                                       field_type=self.__BOOL_TYPE,
                                       display_name='Published',
                                       is_required=True,
                                       order=12)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='publish_time',
                                       field_type=self.__TIMESTAMP_TYPE,
                                       display_name='Publish time',
                                       order=11)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='last_reload_time',
                                       field_type=self.__TIMESTAMP_TYPE,
                                       display_name='Last reload time',
                                       order=10)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='stream_id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Stream Id',
                                       is_required=True,
                                       order=9)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='stream_name',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Stream name',
                                       is_required=True,
                                       order=8)

        self._add_primitive_type_field(
            tag_template=tag_template,
            field_id='stream_entry',
            field_type=self.__STRING_TYPE,
            display_name='Data Catalog Entry for the Stream',
            order=7)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='file_size',
                                       field_type=self.__STRING_TYPE,
                                       display_name='File size',
                                       order=6)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='thumbnail',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Thumbnail',
                                       order=5)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='saved_in_product_version',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Saved in product version',
                                       order=4)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='migration_hash',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Migration hash',
                                       order=3)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='availability_status',
                                       field_type=self.__DOUBLE_TYPE,
                                       display_name='Availability status',
                                       order=2)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='site_url',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Qlik Sense site url',
                                       is_required=True,
                                       order=1)

        return tag_template

    def make_tag_template_for_custom_property_definition(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_CUSTOM_PROPERTY_DEFINITION)

        tag_template.display_name = 'Qlik Custom Property Definition Metadata'

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Unique Id',
                                       is_required=True,
                                       order=6)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='modified_by_username',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Username who modified it',
                                       order=5)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='value_type',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Value type',
                                       order=4)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='choice_values',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Choice values',
                                       order=3)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='object_types',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Object types',
                                       order=2)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='site_url',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Qlik Sense site url',
                                       is_required=True,
                                       order=1)

        return tag_template

    def make_tag_template_for_custom_property_value(self, definition_metadata,
                                                    value):

        tag_template = datacatalog.TagTemplate()

        template_id = dph.DynamicPropertiesHelper\
            .make_id_for_custom_property_value_tag_template(
                definition_metadata, value)
        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=template_id)

        tag_template.display_name = dph.DynamicPropertiesHelper\
            .make_display_name_for_custom_property_value_tag_template(
                definition_metadata, value)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Unique Id',
                                       is_required=True,
                                       order=9)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='created_date',
                                       field_type=self.__TIMESTAMP_TYPE,
                                       display_name='Created date',
                                       order=8)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='modified_date',
                                       field_type=self.__TIMESTAMP_TYPE,
                                       display_name='Modified date',
                                       order=7)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='modified_by_username',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Username who modified it',
                                       order=6)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='value',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Value',
                                       is_required=True,
                                       order=5)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='property_definition_id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Property Definition Id',
                                       is_required=True,
                                       order=4)

        # According to the Qlik Analytics Platform Architecture Team, there was
        # no way of searching assets by the Custom Property values using Qlik
        # when this feature was implemented (Dec, 2020), which means the
        # catalog search might be helpful to address such a use case. Hence the
        # 'definition_' part was supressed from this Tag Field Id to turn seach
        # queries more intuitive, e.g. tag:property_name:<PROPERTY-NAME>.
        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='property_name',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Property Definition name',
                                       is_required=True,
                                       order=3)

        self._add_primitive_type_field(
            tag_template=tag_template,
            field_id='property_definition_entry',
            field_type=self.__STRING_TYPE,
            display_name='Data Catalog Entry for the Property Definition',
            is_required=True,
            order=2)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='site_url',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Qlik Sense site url',
                                       is_required=True,
                                       order=1)

        return tag_template

    def make_tag_template_for_dimension(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_DIMENSION)

        tag_template.display_name = 'Qlik Dimension Metadata'

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Unique Id',
                                       is_required=True,
                                       order=9)

        self._add_enum_type_field(
            tag_template=tag_template,
            field_id='grouping',
            values=[
                constants.DIMENSION_GROUPING_SINGLE_TAG_FIELD,
                constants.DIMENSION_GROUPING_DRILL_DOWN_TAG_FIELD
            ],
            display_name='Grouping',
            order=8)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='fields',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Fields',
                                       order=7)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='field_labels',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Field labels',
                                       order=6)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='tags',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Qlik tags',
                                       order=5)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='app_id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='App Id',
                                       is_required=True,
                                       order=4)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='app_name',
                                       field_type=self.__STRING_TYPE,
                                       display_name='App name',
                                       is_required=True,
                                       order=3)

        self._add_primitive_type_field(
            tag_template=tag_template,
            field_id='app_entry',
            field_type=self.__STRING_TYPE,
            display_name='Data Catalog Entry for the App',
            is_required=True,
            order=2)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='site_url',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Qlik Sense site url',
                                       is_required=True,
                                       order=1)

        return tag_template

    def make_tag_template_for_measure(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_MEASURE)

        tag_template.display_name = 'Qlik Measure Metadata'

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Unique Id',
                                       order=9)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='expression',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Expression',
                                       order=8)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='label_expression',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Label expression',
                                       order=7)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='is_custom_formatted',
                                       field_type=self.__BOOL_TYPE,
                                       display_name='Is custom formatted',
                                       order=6)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='tags',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Qlik tags',
                                       order=5)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='app_id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='App Id',
                                       is_required=True,
                                       order=4)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='app_name',
                                       field_type=self.__STRING_TYPE,
                                       display_name='App name',
                                       is_required=True,
                                       order=3)

        self._add_primitive_type_field(
            tag_template=tag_template,
            field_id='app_entry',
            field_type=self.__STRING_TYPE,
            display_name='Data Catalog Entry for the App',
            is_required=True,
            order=2)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='site_url',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Qlik Sense site url',
                                       is_required=True,
                                       order=1)

        return tag_template

    def make_tag_template_for_sheet(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_SHEET)

        tag_template.display_name = 'Qlik Sheet Metadata'

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Unique Id',
                                       is_required=True,
                                       order=12)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='owner_username',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Owner username',
                                       order=11)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='owner_name',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Owner name',
                                       order=10)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='published',
                                       field_type=self.__BOOL_TYPE,
                                       display_name='Published',
                                       order=9)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='publish_time',
                                       field_type=self.__TIMESTAMP_TYPE,
                                       display_name='Publish time',
                                       order=8)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='approved',
                                       field_type=self.__BOOL_TYPE,
                                       display_name='Approved',
                                       order=7)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='app_id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='App Id',
                                       is_required=True,
                                       order=6)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='app_name',
                                       field_type=self.__STRING_TYPE,
                                       display_name='App name',
                                       is_required=True,
                                       order=5)

        self._add_primitive_type_field(
            tag_template=tag_template,
            field_id='app_entry',
            field_type=self.__STRING_TYPE,
            display_name='Data Catalog Entry for the App',
            is_required=True,
            order=4)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='source_object',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Source object',
                                       order=3)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='draft_object',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Draft object',
                                       order=2)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='site_url',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Qlik Sense site url',
                                       is_required=True,
                                       order=1)

        return tag_template

    def make_tag_template_for_stream(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_STREAM)

        tag_template.display_name = 'Qlik Stream Metadata'

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Unique Id',
                                       is_required=True,
                                       order=5)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='owner_username',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Owner username',
                                       order=4)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='owner_name',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Owner name',
                                       order=3)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='modified_by_username',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Username who modified it',
                                       order=2)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='site_url',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Qlik Sense site url',
                                       is_required=True,
                                       order=1)

        return tag_template

    def make_tag_template_for_visualization(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_VISUALIZATION)

        tag_template.display_name = 'Qlik Visualization Metadata'

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Unique Id',
                                       is_required=True,
                                       order=10)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='title',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Title',
                                       order=9)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='subtitle',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Subtitle',
                                       order=8)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='footnote',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Footnote',
                                       order=7)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='type',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Type',
                                       order=6)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='tags',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Qlik tags',
                                       order=5)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='app_id',
                                       field_type=self.__STRING_TYPE,
                                       display_name='App Id',
                                       is_required=True,
                                       order=4)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='app_name',
                                       field_type=self.__STRING_TYPE,
                                       display_name='App name',
                                       is_required=True,
                                       order=3)

        self._add_primitive_type_field(
            tag_template=tag_template,
            field_id='app_entry',
            field_type=self.__STRING_TYPE,
            display_name='Data Catalog Entry for the App',
            is_required=True,
            order=2)

        self._add_primitive_type_field(tag_template=tag_template,
                                       field_id='site_url',
                                       field_type=self.__STRING_TYPE,
                                       display_name='Qlik Sense site url',
                                       is_required=True,
                                       order=1)

        return tag_template
