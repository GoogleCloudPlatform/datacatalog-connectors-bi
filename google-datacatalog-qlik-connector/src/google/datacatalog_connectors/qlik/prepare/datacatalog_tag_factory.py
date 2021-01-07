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

from datetime import datetime

from google.cloud import datacatalog
from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.qlik.prepare import \
    constants, dynamic_properties_helper as dph


class DataCatalogTagFactory(prepare.BaseTagFactory):
    __INCOMING_TIMESTAMP_UTC_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
    __QLIK_TO_DC_DIM_GROUPING_MAPPING = {
        constants.DIMENSION_GROUPING_SINGLE_QLIK:
            constants.DIMENSION_GROUPING_SINGLE_TAG_FIELD,
        constants.DIMENSION_GROUPING_DRILL_DOWN_QLIK:
            constants.DIMENSION_GROUPING_DRILL_DOWN_TAG_FIELD,
    }

    def __init__(self, site_url):
        self.__site_url = site_url

    def make_tag_for_app(self, tag_template, app_metadata):
        tag = datacatalog.Tag()

        tag.template = tag_template.name

        self._set_string_field(tag, 'id', app_metadata.get('id'))

        owner = app_metadata.get('owner')
        if owner:
            owner_user_dir = owner.get('userDirectory')
            owner_user_id = owner.get('userId')
            if owner_user_dir and owner_user_id:
                self._set_string_field(tag, 'owner_username',
                                       f'{owner_user_dir}\\\\{owner_user_id}')

            self._set_string_field(tag, 'owner_name', owner.get('name'))

        self._set_string_field(tag, 'modified_by_username',
                               app_metadata.get('modifiedByUserName'))

        published = app_metadata.get('published')
        self._set_bool_field(tag, 'published', published)

        publish_time = app_metadata.get('publishTime')
        # The publish time may come with a value such as
        # '1753-01-01T00:00:00.000Z' when the app has not been published,
        # so both inputs are tested before setting the tag field.
        if published and publish_time:
            self._set_timestamp_field(
                tag, 'publish_time',
                datetime.strptime(publish_time,
                                  self.__INCOMING_TIMESTAMP_UTC_FORMAT))

        last_reload_time = app_metadata.get('lastReloadTime')
        if last_reload_time:
            self._set_timestamp_field(
                tag, 'last_reload_time',
                datetime.strptime(last_reload_time,
                                  self.__INCOMING_TIMESTAMP_UTC_FORMAT))

        stream_metadata = app_metadata.get('stream')
        if stream_metadata:
            self._set_string_field(tag, 'stream_id', stream_metadata.get('id'))
            self._set_string_field(tag, 'stream_name',
                                   stream_metadata.get('name'))

        file_size = app_metadata.get('fileSize')
        if file_size is not None:
            self._set_string_field(
                tag, 'file_size',
                self.__get_human_readable_size_value(file_size))

        if app_metadata.get('thumbnail'):
            self._set_string_field(
                tag, 'thumbnail',
                f'{self.__site_url}{app_metadata.get("thumbnail")}')

        self._set_string_field(tag, 'saved_in_product_version',
                               app_metadata.get('savedInProductVersion'))

        self._set_string_field(tag, 'migration_hash',
                               app_metadata.get('migrationHash'))

        self._set_double_field(tag, 'availability_status',
                               app_metadata.get('availabilityStatus'))

        self._set_string_field(tag, 'site_url', self.__site_url)

        return tag

    def make_tags_for_custom_properties(self, tag_templates_dict,
                                        custom_properties):

        tags = []

        if not (tag_templates_dict and custom_properties):
            return tags

        for property_metadata in custom_properties:
            definition = property_metadata.get('definition')
            value = property_metadata.get('value')
            template_id = dph.DynamicPropertiesHelper\
                .make_id_for_custom_property_value_tag_template(
                    definition, value)
            property_value_tag_template = tag_templates_dict.get(template_id)

            if not property_value_tag_template:
                continue

            tags.append(
                self.make_tag_for_custom_property(property_value_tag_template,
                                                  property_metadata))

        return tags

    def make_tag_for_custom_property(self, tag_template,
                                     custom_property_metadata):

        tag = datacatalog.Tag()

        tag.template = tag_template.name

        self._set_string_field(tag, 'id', custom_property_metadata.get('id'))

        created_date = custom_property_metadata.get('createdDate')
        if created_date:
            self._set_timestamp_field(
                tag, 'created_date',
                datetime.strptime(created_date,
                                  self.__INCOMING_TIMESTAMP_UTC_FORMAT))

        modified_date = custom_property_metadata.get('modifiedDate')
        if modified_date:
            self._set_timestamp_field(
                tag, 'modified_date',
                datetime.strptime(modified_date,
                                  self.__INCOMING_TIMESTAMP_UTC_FORMAT))

        self._set_string_field(
            tag, 'modified_by_username',
            custom_property_metadata.get('modifiedByUserName'))

        self._set_string_field(tag, 'value',
                               custom_property_metadata.get('value'))

        definition = custom_property_metadata.get('definition')
        if definition:
            self._set_string_field(tag, 'property_definition_id',
                                   definition.get('id'))
            self._set_string_field(tag, 'property_name',
                                   definition.get('name'))

        self._set_string_field(tag, 'site_url', self.__site_url)

        return tag

    def make_tag_for_custom_property_definition(self, tag_template,
                                                custom_property_def_metadata):

        tag = datacatalog.Tag()

        tag.template = tag_template.name

        self._set_string_field(tag, 'id',
                               custom_property_def_metadata.get('id'))

        self._set_string_field(
            tag, 'modified_by_username',
            custom_property_def_metadata.get('modifiedByUserName'))

        self._set_string_field(tag, 'value_type',
                               custom_property_def_metadata.get('valueType'))

        choice_values = custom_property_def_metadata.get('choiceValues') or []
        self._set_string_field(tag, 'choice_values', ', '.join(choice_values))

        object_types = custom_property_def_metadata.get('objectTypes') or []
        self._set_string_field(tag, 'object_types', ', '.join(object_types))

        self._set_string_field(tag, 'site_url', self.__site_url)

        return tag

    def make_tag_for_dimension(self, tag_template, dimension_metadata):
        tag = datacatalog.Tag()

        tag.template = tag_template.name

        self._set_string_field(tag, 'id',
                               dimension_metadata.get('qInfo').get('qId'))

        q_dim = dimension_metadata.get('qDim')
        grouping = self.__QLIK_TO_DC_DIM_GROUPING_MAPPING.get(
            q_dim.get('qGrouping'))
        self.__set_enum_field(tag, 'grouping', grouping)

        self._set_string_field(tag, 'fields',
                               ', '.join(q_dim.get('qFieldDefs')))
        self._set_string_field(tag, 'field_labels',
                               ', '.join(q_dim.get('qFieldLabels')))

        q_meta_def = dimension_metadata.get('qMetaDef')
        self._set_string_field(tag, 'tags', ', '.join(q_meta_def.get('tags')))

        app_metadata = dimension_metadata.get('app')
        if app_metadata:
            self._set_string_field(tag, 'app_id', app_metadata.get('id'))
            self._set_string_field(tag, 'app_name', app_metadata.get('name'))

        self._set_string_field(tag, 'site_url', self.__site_url)

        return tag

    def make_tag_for_measure(self, tag_template, measure_metadata):
        tag = datacatalog.Tag()

        tag.template = tag_template.name

        self._set_string_field(tag, 'id',
                               measure_metadata.get('qInfo').get('qId'))

        q_measure = measure_metadata.get('qMeasure')

        self._set_string_field(tag, 'expression', q_measure.get('qDef'))
        self._set_string_field(tag, 'label_expression',
                               q_measure.get('qLabelExpression'))
        self._set_bool_field(tag, 'is_custom_formatted',
                             q_measure.get('isCustomFormatted'))

        q_meta_def = measure_metadata.get('qMetaDef')
        self._set_string_field(tag, 'tags', ', '.join(q_meta_def.get('tags')))

        app_metadata = measure_metadata.get('app')
        if app_metadata:
            self._set_string_field(tag, 'app_id', app_metadata.get('id'))
            self._set_string_field(tag, 'app_name', app_metadata.get('name'))

        self._set_string_field(tag, 'site_url', self.__site_url)

        return tag

    def make_tag_for_sheet(self, tag_template, sheet_metadata):
        tag = datacatalog.Tag()

        tag.template = tag_template.name

        self._set_string_field(tag, 'id',
                               sheet_metadata.get('qInfo').get('qId'))

        q_meta = sheet_metadata.get('qMeta')
        owner = q_meta.get('owner')
        if owner:
            owner_user_dir = owner.get('userDirectory')
            owner_user_id = owner.get('userId')
            if owner_user_dir and owner_user_id:
                self._set_string_field(tag, 'owner_username',
                                       f'{owner_user_dir}\\\\{owner_user_id}')

            self._set_string_field(tag, 'owner_name', owner.get('name'))

        published = q_meta.get('published')
        self._set_bool_field(tag, 'published', published)

        publish_time = q_meta.get('publishTime')
        # The publish time may come with a value such as
        # '1753-01-01T00:00:00.000Z' when the sheet has not been published,
        # so both inputs are tested before setting the tag field.
        if published and publish_time:
            self._set_timestamp_field(
                tag, 'publish_time',
                datetime.strptime(publish_time,
                                  self.__INCOMING_TIMESTAMP_UTC_FORMAT))

        self._set_bool_field(tag, 'approved', q_meta.get('approved'))

        app_metadata = sheet_metadata.get('app')
        if app_metadata:
            self._set_string_field(tag, 'app_id', app_metadata.get('id'))
            self._set_string_field(tag, 'app_name', app_metadata.get('name'))

        self._set_string_field(tag, 'source_object',
                               q_meta.get('sourceObject'))

        self._set_string_field(tag, 'draft_object', q_meta.get('draftObject'))

        self._set_string_field(tag, 'site_url', self.__site_url)

        return tag

    def make_tag_for_stream(self, tag_template, stream_metadata):
        tag = datacatalog.Tag()

        tag.template = tag_template.name

        self._set_string_field(tag, 'id', stream_metadata.get('id'))

        owner = stream_metadata.get('owner')
        if owner:
            owner_user_dir = owner.get('userDirectory')
            owner_user_id = owner.get('userId')
            if owner_user_dir and owner_user_id:
                self._set_string_field(tag, 'owner_username',
                                       f'{owner_user_dir}\\\\{owner_user_id}')

            self._set_string_field(tag, 'owner_name', owner.get('name'))

        self._set_string_field(tag, 'modified_by_username',
                               stream_metadata.get('modifiedByUserName'))

        self._set_string_field(tag, 'site_url', self.__site_url)

        return tag

    def make_tag_for_visualization(self, tag_template, visualization_metadata):
        tag = datacatalog.Tag()

        tag.template = tag_template.name

        self._set_string_field(tag, 'id',
                               visualization_metadata.get('qInfo').get('qId'))

        self._set_string_field(tag, 'title',
                               visualization_metadata.get('title'))
        self._set_string_field(tag, 'subtitle',
                               visualization_metadata.get('subtitle'))
        self._set_string_field(tag, 'footnote',
                               visualization_metadata.get('footnote'))
        self._set_string_field(tag, 'type',
                               visualization_metadata.get('visualization'))

        q_meta_def = visualization_metadata.get('qMetaDef')
        self._set_string_field(tag, 'tags', ', '.join(q_meta_def.get('tags')))

        app_metadata = visualization_metadata.get('app')
        if app_metadata:
            self._set_string_field(tag, 'app_id', app_metadata.get('id'))
            self._set_string_field(tag, 'app_name', app_metadata.get('name'))

        self._set_string_field(tag, 'site_url', self.__site_url)

        return tag

    @classmethod
    def __get_human_readable_size_value(cls, size_bytes):
        """

        :param size_bytes: int or string, in bytes
        :return: human-readable size
        """
        size_val = int(size_bytes)
        units = ['bytes', 'KB', 'MB', 'GB']
        for unit in units:
            if size_val < 1024.0:
                human_readable_space = f'{round(size_val, 2)} {unit}'
                return human_readable_space
            size_val = size_val / 1024.0
        return f'{round(size_val, 2)} TB'

    @classmethod
    def __set_enum_field(cls, tag, field_id, value):
        if value is not None:
            enum_field = datacatalog.TagField()
            enum_field.enum_value.display_name = value
            tag.fields[field_id] = enum_field
