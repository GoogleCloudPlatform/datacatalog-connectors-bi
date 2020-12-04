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

from datetime import datetime

from google.cloud import datacatalog
from google.datacatalog_connectors.commons import prepare


class DataCatalogTagFactory(prepare.BaseTagFactory):
    __INCOMING_TIMESTAMP_UTC_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

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

        self._set_string_field(tag, 'schema_path',
                               app_metadata.get('schemaPath'))

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
