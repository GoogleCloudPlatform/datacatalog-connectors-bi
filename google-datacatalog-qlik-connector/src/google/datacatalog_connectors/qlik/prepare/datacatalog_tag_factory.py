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


class DataCatalogTagFactory(prepare.BaseTagFactory):

    def __init__(self, site_url):
        self.__site_url = site_url

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
