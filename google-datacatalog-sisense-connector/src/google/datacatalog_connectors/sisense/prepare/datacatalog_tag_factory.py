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
from typing import Any, Dict

from google.cloud import datacatalog
from google.cloud.datacatalog import Tag, TagTemplate
from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.sisense.prepare import constants


class DataCatalogTagFactory(prepare.BaseTagFactory):
    __INCOMING_TIMESTAMP_UTC_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

    def __init__(self, server_address: str):
        self.__server_address = server_address

    def make_tag_for_folder(self, tag_template: TagTemplate,
                            folder_metadata: Dict[str, Any]) -> Tag:

        tag = datacatalog.Tag()

        tag.template = tag_template.name

        # The root folder does not have an ``_id`` field.
        folder_id = folder_metadata.get('_id') or folder_metadata.get('name')
        self._set_string_field(tag, 'id', folder_id)

        self._set_string_field(tag, 'name', folder_metadata.get('name'))

        owner = folder_metadata.get('owner')
        if owner:
            self._set_string_field(tag, 'owner_username',
                                   owner.get('userName'))

            first_name = owner.get('firstName') or ''
            last_name = owner.get('lastName') or ''
            self._set_string_field(tag, 'owner_name',
                                   f'{first_name} {last_name}')

        children = folder_metadata.get('children')
        child_count = len(children) if children else 0
        self._set_bool_field(tag, 'has_children', child_count > 0)
        if child_count:
            self._set_double_field(tag, 'child_count', child_count)

        dashboards = folder_metadata.get('dashboards')
        dashboard_count = len(dashboards) if dashboards else 0
        self._set_bool_field(tag, 'has_dashboards', dashboard_count > 0)
        if dashboard_count:
            self._set_double_field(tag, 'dashboard_count', dashboard_count)

        self._set_string_field(tag, 'server_url', self.__server_address)

        return tag
