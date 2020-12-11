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

import re
import six
import unicodedata

from google.cloud import datacatalog
from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.qlik.prepare import constants


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

    def make_tag_template_for_custom_property(self, definition_metadata):
        tag_template = datacatalog.TagTemplate()

        generated_id = f'{constants.TAG_TEMPLATE_ID_PREFIX_CUSTOM_PROPERTY}' \
                       f'{definition_metadata.get("id")}'

        tag_template.name = datacatalog.DataCatalogClient.tag_template_path(
            project=self.__project_id,
            location=self.__location_id,
            tag_template=re.sub(r'[^a-z0-9_]+', '_', generated_id))

        generated_display_name = f'Qlik {definition_metadata.get("name")}' \
                                 f' Custom Property'
        tag_template.display_name = self.__format_display_name(
            generated_display_name)

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

        self._add_primitive_type_field(tag_template, 'definition_id',
                                       self.__STRING_TYPE, 'Definition Id')

        self._add_primitive_type_field(tag_template, 'definition_name',
                                       self.__STRING_TYPE, 'Definition name')

        self._add_primitive_type_field(
            tag_template, 'definition_entry', self.__STRING_TYPE,
            'Data Catalog Entry for the Definition')

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

    @classmethod
    def __format_display_name(cls, source_name):
        return re.sub(r'[^\w\- ]+', '_',
                      cls.__normalize_ascii_chars(source_name).strip())

    @classmethod
    def __normalize_ascii_chars(cls, source_string):
        encoding = cls.__ASCII_CHARACTER_ENCODING
        normalized = unicodedata.normalize(
            'NFKD', source_string
            if isinstance(source_string, six.string_types) else u'')
        encoded = normalized.encode(encoding, 'ignore')
        return encoded.decode()
