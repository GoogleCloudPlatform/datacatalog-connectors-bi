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
from google.protobuf import timestamp_pb2
from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.qlik.prepare import constants


class DataCatalogEntryFactory(prepare.BaseEntryFactory):
    __INCOMING_TIMESTAMP_UTC_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

    def __init__(self, project_id, location_id, entry_group_id,
                 user_specified_system, site_url):

        self.__project_id = project_id
        self.__location_id = location_id
        self.__entry_group_id = entry_group_id
        self.__user_specified_system = user_specified_system
        self.__site_url = site_url

    def make_entry_for_app(self, app_metadata):
        entry = datacatalog.Entry()

        generated_id = self.__format_id(constants.ENTRY_ID_PART_APP,
                                        app_metadata.get('id'))
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constants.USER_SPECIFIED_TYPE_APP

        entry.display_name = self._format_display_name(
            app_metadata.get("name"))
        entry.description = app_metadata.get("description")

        entry.linked_resource = f'{self.__site_url}' \
                                f'/sense/app/{app_metadata.get("id")}'

        created_datetime = datetime.strptime(
            app_metadata.get('createdDate'),
            self.__INCOMING_TIMESTAMP_UTC_FORMAT)
        create_timestamp = timestamp_pb2.Timestamp()
        create_timestamp.FromDatetime(created_datetime)
        entry.source_system_timestamps.create_time = create_timestamp

        modified_date = app_metadata.get('modifiedDate')
        resolved_modified_date = modified_date or app_metadata.get(
            'createdDate')
        modified_datetime = datetime.strptime(
            resolved_modified_date, self.__INCOMING_TIMESTAMP_UTC_FORMAT)
        update_timestamp = timestamp_pb2.Timestamp()
        update_timestamp.FromDatetime(modified_datetime)
        entry.source_system_timestamps.update_time = update_timestamp

        return generated_id, entry

    def make_entry_for_sheet(self, sheet_metadata):
        entry = datacatalog.Entry()

        sheet_id = sheet_metadata.get('qInfo').get('qId')
        generated_id = self.__format_id(constants.ENTRY_ID_PART_SHEET,
                                        sheet_id)
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constants.USER_SPECIFIED_TYPE_SHEET

        q_meta = sheet_metadata.get('qMeta')
        entry.display_name = self._format_display_name(q_meta.get("title"))
        entry.description = q_meta.get("description")

        entry.linked_resource = f'{self.__site_url}' \
                                f'/sense/app/' \
                                f'{sheet_metadata.get("app").get("id")}' \
                                f'/sheet/{sheet_id}'

        created_datetime = datetime.strptime(
            q_meta.get('createdDate'), self.__INCOMING_TIMESTAMP_UTC_FORMAT)
        create_timestamp = timestamp_pb2.Timestamp()
        create_timestamp.FromDatetime(created_datetime)
        entry.source_system_timestamps.create_time = create_timestamp

        modified_date = q_meta.get('modifiedDate')
        resolved_modified_date = modified_date or q_meta.get('createdDate')
        modified_datetime = datetime.strptime(
            resolved_modified_date, self.__INCOMING_TIMESTAMP_UTC_FORMAT)
        update_timestamp = timestamp_pb2.Timestamp()
        update_timestamp.FromDatetime(modified_datetime)
        entry.source_system_timestamps.update_time = update_timestamp

        return generated_id, entry

    def make_entry_for_stream(self, stream_metadata):
        entry = datacatalog.Entry()

        generated_id = self.__format_id(constants.ENTRY_ID_PART_STREAM,
                                        stream_metadata.get('id'))
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constants.USER_SPECIFIED_TYPE_STREAM

        entry.display_name = self._format_display_name(
            stream_metadata.get("name"))

        entry.linked_resource = f'{self.__site_url}' \
                                f'/hub/stream/{stream_metadata.get("id")}'

        created_datetime = datetime.strptime(
            stream_metadata.get('createdDate'),
            self.__INCOMING_TIMESTAMP_UTC_FORMAT)
        create_timestamp = timestamp_pb2.Timestamp()
        create_timestamp.FromDatetime(created_datetime)
        entry.source_system_timestamps.create_time = create_timestamp

        modified_date = stream_metadata.get('modifiedDate')
        resolved_modified_date = modified_date or stream_metadata.get(
            'createdDate')
        modified_datetime = datetime.strptime(
            resolved_modified_date, self.__INCOMING_TIMESTAMP_UTC_FORMAT)
        update_timestamp = timestamp_pb2.Timestamp()
        update_timestamp.FromDatetime(modified_datetime)
        entry.source_system_timestamps.update_time = update_timestamp

        return generated_id, entry

    @classmethod
    def __format_id(cls, source_type_identifier, source_id):
        no_prefix_fmt_id = cls._format_id(
            f'{source_type_identifier}{source_id}')
        if len(no_prefix_fmt_id) > constants.NO_PREFIX_ENTRY_ID_MAX_LENGTH:
            no_prefix_fmt_id = \
                no_prefix_fmt_id[:constants.NO_PREFIX_ENTRY_ID_MAX_LENGTH]
        return f'{constants.ENTRY_ID_PREFIX}{no_prefix_fmt_id}'
