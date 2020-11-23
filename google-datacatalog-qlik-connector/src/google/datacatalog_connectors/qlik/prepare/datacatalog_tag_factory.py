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

        self._set_timestamp_field(
            tag, 'publish_time',
            datetime.strptime(app_metadata.get('publishTime'),
                              self.__INCOMING_TIMESTAMP_UTC_FORMAT))

        self._set_bool_field(tag, 'published', app_metadata.get('published'))

        self._set_timestamp_field(
            tag, 'last_reload_time',
            datetime.strptime(app_metadata.get('lastReloadTime'),
                              self.__INCOMING_TIMESTAMP_UTC_FORMAT))

        stream_metadata = app_metadata.get('stream')
        if stream_metadata:
            self._set_string_field(tag, 'stream_id', stream_metadata.get('id'))
            self._set_string_field(tag, 'stream_name',
                                   stream_metadata.get('name'))

        self._set_double_field(tag, 'file_size', app_metadata.get('fileSize'))

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
