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
from typing import Any, Dict, Tuple

from google.cloud import datacatalog
from google.cloud.datacatalog import Entry
from google.protobuf import timestamp_pb2
from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.sisense.prepare import constants


class DataCatalogEntryFactory(prepare.BaseEntryFactory):
    __INCOMING_TIMESTAMP_UTC_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

    def __init__(self, project_id: str, location_id: str, entry_group_id: str,
                 user_specified_system: str, instance_url: str):

        self.__project_id = project_id
        self.__location_id = location_id
        self.__entry_group_id = entry_group_id
        self.__user_specified_system = user_specified_system
        self.__instance_url = instance_url

        # Strip schema (http | https) and slashes from the server url.
        self.__server_id = instance_url[instance_url.find('//') + 2:]

    def make_entry_for_folder(
            self, folder_metadata: Dict[str, Any]) -> Tuple[str, Entry]:

        entry = datacatalog.Entry()

        # The root folder does not have an ``_id`` field.
        folder_id = folder_metadata['_id'] if folder_metadata.get(
            '_id') else folder_metadata.get('name')
        generated_id = self.__format_id(constants.ENTRY_ID_PART_FOLDER,
                                        folder_id)
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constants.USER_SPECIFIED_TYPE_FOLDER

        entry.display_name = self._format_display_name(
            folder_metadata.get('name'))

        entry.linked_resource = f'{self.__instance_url}/app/main#/home/' \
                                f'{folder_metadata.get("oid")}'

        created_datetime = datetime.strptime(
            folder_metadata.get('created'),
            self.__INCOMING_TIMESTAMP_UTC_FORMAT)
        create_timestamp = timestamp_pb2.Timestamp()
        create_timestamp.FromDatetime(created_datetime)
        entry.source_system_timestamps.create_time = create_timestamp

        modified_date = folder_metadata.get('lastUpdated')
        resolved_modified_date = modified_date or folder_metadata.get(
            'created')
        modified_datetime = datetime.strptime(
            resolved_modified_date, self.__INCOMING_TIMESTAMP_UTC_FORMAT)
        update_timestamp = timestamp_pb2.Timestamp()
        update_timestamp.FromDatetime(modified_datetime)
        entry.source_system_timestamps.update_time = update_timestamp

        return generated_id, entry

    def __format_id(self, source_type_identifier, source_id):
        prefixed_id = f'{constants.ENTRY_ID_PREFIX}' \
                      f'{self.__server_id}_' \
                      f'{source_type_identifier}{source_id}'
        return self._format_id(prefixed_id)
