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

from google.datacatalog_connectors.qlik.prepare import constants


class DataCatalogTagTemplateFactory(prepare.BaseTagTemplateFactory):
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

        self._add_primitive_type_field(tag_template, 'publish_time',
                                       self.__TIMESTAMP_TYPE, 'Publish time')

        self._add_primitive_type_field(tag_template, 'published',
                                       self.__BOOL_TYPE, 'Published')

        self._add_primitive_type_field(tag_template, 'stream_id',
                                       self.__STRING_TYPE, 'Stream Id')

        self._add_primitive_type_field(tag_template, 'stream_name',
                                       self.__STRING_TYPE, 'Stream Name')

        self._add_primitive_type_field(tag_template, 'stream_entry',
                                       self.__STRING_TYPE,
                                       'Data Catalog Entry for the Stream')

        self._add_primitive_type_field(tag_template,
                                       'saved_in_product_version',
                                       self.__STRING_TYPE,
                                       'Saved in product version')

        self._add_primitive_type_field(tag_template, 'migration_hash',
                                       self.__STRING_TYPE, 'Migration hash')

        self._add_primitive_type_field(tag_template, 'availability_status',
                                       self.__DOUBLE_TYPE,
                                       'Availability status')

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
