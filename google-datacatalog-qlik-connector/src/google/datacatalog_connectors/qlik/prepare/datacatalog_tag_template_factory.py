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

        self._add_primitive_type_field(tag_template, 'id', self.__STRING_TYPE,
                                       'Unique Id')

        self._add_primitive_type_field(tag_template, 'owner_username',
                                       self.__STRING_TYPE, 'Owner username')

        self._add_primitive_type_field(tag_template, 'owner_name',
                                       self.__STRING_TYPE, 'Owner name')

        self._add_primitive_type_field(tag_template, 'modified_by_username',
                                       self.__STRING_TYPE,
                                       'Username who modified it')

        self._add_primitive_type_field(tag_template, 'published',
                                       self.__BOOL_TYPE, 'Published')

        self._add_primitive_type_field(tag_template, 'publish_time',
                                       self.__TIMESTAMP_TYPE, 'Publish time')

        self._add_primitive_type_field(tag_template, 'last_reload_time',
                                       self.__TIMESTAMP_TYPE,
                                       'Last reload time')

        self._add_primitive_type_field(tag_template, 'stream_id',
                                       self.__STRING_TYPE, 'Stream Id')

        self._add_primitive_type_field(tag_template, 'stream_name',
                                       self.__STRING_TYPE, 'Stream name')

        self._add_primitive_type_field(tag_template, 'stream_entry',
                                       self.__STRING_TYPE,
                                       'Data Catalog Entry for the Stream')

        self._add_primitive_type_field(tag_template, 'file_size',
                                       self.__STRING_TYPE, 'File size')

        self._add_primitive_type_field(tag_template, 'thumbnail',
                                       self.__STRING_TYPE, 'Thumbnail')

        self._add_primitive_type_field(tag_template,
                                       'saved_in_product_version',
                                       self.__STRING_TYPE,
                                       'Saved in product version')

        self._add_primitive_type_field(tag_template, 'migration_hash',
                                       self.__STRING_TYPE, 'Migration hash')

        self._add_primitive_type_field(tag_template, 'availability_status',
                                       self.__DOUBLE_TYPE,
                                       'Availability status')

        self._add_primitive_type_field(tag_template, 'site_url',
                                       self.__STRING_TYPE,
                                       'Qlik Sense site url')

        return tag_template

    def make_tag_template_for_custom_property_definition(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_CUSTOM_PROPERTY_DEFINITION)

        tag_template.display_name = 'Qlik Custom Property Definition Metadata'

        self._add_primitive_type_field(tag_template, 'id', self.__STRING_TYPE,
                                       'Unique Id')

        self._add_primitive_type_field(tag_template, 'modified_by_username',
                                       self.__STRING_TYPE,
                                       'Username who modified it')

        self._add_primitive_type_field(tag_template, 'value_type',
                                       self.__STRING_TYPE, 'Value type')

        self._add_primitive_type_field(tag_template, 'choice_values',
                                       self.__STRING_TYPE, 'Choice values')

        self._add_primitive_type_field(tag_template, 'object_types',
                                       self.__STRING_TYPE, 'Object types')

        self._add_primitive_type_field(tag_template, 'site_url',
                                       self.__STRING_TYPE,
                                       'Qlik Sense site url')

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

        self._add_primitive_type_field(tag_template, 'id', self.__STRING_TYPE,
                                       'Unique Id')

        self._add_primitive_type_field(tag_template, 'created_date',
                                       self.__TIMESTAMP_TYPE, 'Created date')

        self._add_primitive_type_field(tag_template, 'modified_date',
                                       self.__TIMESTAMP_TYPE, 'Modified date')

        self._add_primitive_type_field(tag_template, 'modified_by_username',
                                       self.__STRING_TYPE,
                                       'Username who modified it')

        self._add_primitive_type_field(tag_template, 'value',
                                       self.__STRING_TYPE, 'Value')

        self._add_primitive_type_field(tag_template, 'property_definition_id',
                                       self.__STRING_TYPE,
                                       'Property Definition Id')

        # According to the Qlik Analytics Platform Architecture Team, there was
        # no way of searching assets by the Custom Property values using Qlik
        # when this feature was implemented (Dec, 2020), which means the
        # catalog search might be helpful to address such a use case. Hence the
        # 'definition_' part was supressed from this Tag Field Id to turn seach
        # queries more intuitive, e.g. tag:property_name:<PROPERTY-NAME>.
        self._add_primitive_type_field(tag_template, 'property_name',
                                       self.__STRING_TYPE,
                                       'Property Definition name')

        self._add_primitive_type_field(
            tag_template, 'property_definition_entry', self.__STRING_TYPE,
            'Data Catalog Entry for the Property Definition')

        self._add_primitive_type_field(tag_template, 'site_url',
                                       self.__STRING_TYPE,
                                       'Qlik Sense site url')

        return tag_template

    def make_tag_template_for_dimension(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_DIMENSION)

        tag_template.display_name = 'Qlik Dimension Metadata'

        self._add_primitive_type_field(tag_template, 'id', self.__STRING_TYPE,
                                       'Unique Id')

        self._add_enum_type_field(tag_template, 'grouping', [
            constants.DIMENSION_GROUPING_SINGLE_TAG_FIELD,
            constants.DIMENSION_GROUPING_DRILL_DOWN_TAG_FIELD
        ], 'Grouping')

        self._add_primitive_type_field(tag_template, 'fields',
                                       self.__STRING_TYPE, 'Fields')

        self._add_primitive_type_field(tag_template, 'field_labels',
                                       self.__STRING_TYPE, 'Field labels')

        self._add_primitive_type_field(tag_template, 'tags',
                                       self.__STRING_TYPE, 'Qlik tags')

        self._add_primitive_type_field(tag_template, 'app_id',
                                       self.__STRING_TYPE, 'App Id')

        self._add_primitive_type_field(tag_template, 'app_name',
                                       self.__STRING_TYPE, 'App name')

        self._add_primitive_type_field(tag_template, 'app_entry',
                                       self.__STRING_TYPE,
                                       'Data Catalog Entry for the App')

        self._add_primitive_type_field(tag_template, 'site_url',
                                       self.__STRING_TYPE,
                                       'Qlik Sense site url')

        return tag_template

    def make_tag_template_for_measure(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_MEASURE)

        tag_template.display_name = 'Qlik Measure Metadata'

        self._add_primitive_type_field(tag_template, 'id', self.__STRING_TYPE,
                                       'Unique Id')

        self._add_primitive_type_field(tag_template, 'expression',
                                       self.__STRING_TYPE, 'Expression')

        self._add_primitive_type_field(tag_template, 'label_expression',
                                       self.__STRING_TYPE, 'Label expression')

        self._add_primitive_type_field(tag_template, 'is_custom_formatted',
                                       self.__BOOL_TYPE, 'Is custom formatted')

        self._add_primitive_type_field(tag_template, 'tags',
                                       self.__STRING_TYPE, 'Qlik tags')

        self._add_primitive_type_field(tag_template, 'app_id',
                                       self.__STRING_TYPE, 'App Id')

        self._add_primitive_type_field(tag_template, 'app_name',
                                       self.__STRING_TYPE, 'App name')

        self._add_primitive_type_field(tag_template, 'app_entry',
                                       self.__STRING_TYPE,
                                       'Data Catalog Entry for the App')

        self._add_primitive_type_field(tag_template, 'site_url',
                                       self.__STRING_TYPE,
                                       'Qlik Sense site url')

        return tag_template

    def make_tag_template_for_sheet(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_SHEET)

        tag_template.display_name = 'Qlik Sheet Metadata'

        self._add_primitive_type_field(tag_template, 'id', self.__STRING_TYPE,
                                       'Unique Id')

        self._add_primitive_type_field(tag_template, 'owner_username',
                                       self.__STRING_TYPE, 'Owner username')

        self._add_primitive_type_field(tag_template, 'owner_name',
                                       self.__STRING_TYPE, 'Owner name')

        self._add_primitive_type_field(tag_template, 'published',
                                       self.__BOOL_TYPE, 'Published')

        self._add_primitive_type_field(tag_template, 'publish_time',
                                       self.__TIMESTAMP_TYPE, 'Publish time')

        self._add_primitive_type_field(tag_template, 'approved',
                                       self.__BOOL_TYPE, 'Approved')

        self._add_primitive_type_field(tag_template, 'app_id',
                                       self.__STRING_TYPE, 'App Id')

        self._add_primitive_type_field(tag_template, 'app_name',
                                       self.__STRING_TYPE, 'App name')

        self._add_primitive_type_field(tag_template, 'app_entry',
                                       self.__STRING_TYPE,
                                       'Data Catalog Entry for the App')

        self._add_primitive_type_field(tag_template, 'source_object',
                                       self.__STRING_TYPE, 'Source object')

        self._add_primitive_type_field(tag_template, 'draft_object',
                                       self.__STRING_TYPE, 'Draft object')

        self._add_primitive_type_field(tag_template, 'site_url',
                                       self.__STRING_TYPE,
                                       'Qlik Sense site url')

        return tag_template

    def make_tag_template_for_stream(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_STREAM)

        tag_template.display_name = 'Qlik Stream Metadata'

        self._add_primitive_type_field(tag_template, 'id', self.__STRING_TYPE,
                                       'Unique Id')

        self._add_primitive_type_field(tag_template, 'owner_username',
                                       self.__STRING_TYPE, 'Owner username')

        self._add_primitive_type_field(tag_template, 'owner_name',
                                       self.__STRING_TYPE, 'Owner name')

        self._add_primitive_type_field(tag_template, 'modified_by_username',
                                       self.__STRING_TYPE,
                                       'Username who modified it')

        self._add_primitive_type_field(tag_template, 'site_url',
                                       self.__STRING_TYPE,
                                       'Qlik Sense site url')

        return tag_template

    def make_tag_template_for_visualization(self):
        tag_template = datacatalog.TagTemplate()

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=constants.TAG_TEMPLATE_ID_VISUALIZATION)

        tag_template.display_name = 'Qlik Visualization Metadata'

        self._add_primitive_type_field(tag_template, 'id', self.__STRING_TYPE,
                                       'Unique Id')

        self._add_primitive_type_field(tag_template, 'title',
                                       self.__STRING_TYPE, 'Title')

        self._add_primitive_type_field(tag_template, 'subtitle',
                                       self.__STRING_TYPE, 'Subtitle')

        self._add_primitive_type_field(tag_template, 'footnote',
                                       self.__STRING_TYPE, 'Footnote')

        self._add_primitive_type_field(tag_template, 'type',
                                       self.__STRING_TYPE, 'Type')

        self._add_primitive_type_field(tag_template, 'tags',
                                       self.__STRING_TYPE, 'Qlik tags')

        self._add_primitive_type_field(tag_template, 'app_id',
                                       self.__STRING_TYPE, 'App Id')

        self._add_primitive_type_field(tag_template, 'app_name',
                                       self.__STRING_TYPE, 'App name')

        self._add_primitive_type_field(tag_template, 'app_entry',
                                       self.__STRING_TYPE,
                                       'Data Catalog Entry for the App')

        self._add_primitive_type_field(tag_template, 'site_url',
                                       self.__STRING_TYPE,
                                       'Qlik Sense site url')

        return tag_template
