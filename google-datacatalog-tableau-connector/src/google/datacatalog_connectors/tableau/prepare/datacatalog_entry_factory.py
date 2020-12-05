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
import logging
import re

from google.cloud import datacatalog
from google.datacatalog_connectors.commons import prepare
from google.protobuf import timestamp_pb2

from google.datacatalog_connectors.tableau.prepare import constants


class DataCatalogEntryFactory(prepare.BaseEntryFactory):
    # The incoming timestamp format is UTC
    __INCOMING_TIMESTAMP_FORMAT = '%Y-%m-%dT%H:%M:%SZ'

    def __init__(self, project_id, location_id, entry_group_id,
                 user_specified_system, server_address):

        self.__project_id = project_id
        self.__location_id = location_id
        self.__entry_group_id = entry_group_id
        self.__user_specified_system = user_specified_system
        self.__server_address = server_address

    def make_entry_for_dashboard(self, dashboard_metadata):
        entry = datacatalog.Entry()

        generated_id = self.__format_id(dashboard_metadata.get('luid'))
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constants.USER_SPECIFIED_TYPE_DASHBOARD

        entry.display_name = self._format_display_name(
            dashboard_metadata.get('name'))

        path = dashboard_metadata.get('path')
        if path:
            site_content_url = self.__format_site_content_url(
                dashboard_metadata.get('workbook'))
            entry.linked_resource = \
                f'{self.__server_address}/#{site_content_url}/views/{path}'

        created_datetime = datetime.strptime(
            dashboard_metadata.get('createdAt'),
            self.__INCOMING_TIMESTAMP_FORMAT)
        create_timestamp = timestamp_pb2.Timestamp()
        create_timestamp.FromDatetime(created_datetime)
        entry.source_system_timestamps.create_time = create_timestamp

        updated_datetime = datetime.strptime(
            dashboard_metadata.get('updatedAt'),
            self.__INCOMING_TIMESTAMP_FORMAT)
        update_timestamp = timestamp_pb2.Timestamp()
        update_timestamp.FromDatetime(updated_datetime)
        entry.source_system_timestamps.update_time = update_timestamp

        return generated_id, entry

    def make_entry_for_sheet(self, sheet_metadata, workbook_metadata):
        entry = datacatalog.Entry()

        luid = sheet_metadata.get('luid')
        if luid:
            generated_id = self.__format_id(luid)
        else:
            generated_id = self.__format_id(sheet_metadata.get('id'))
            logging.info(
                'Sheet %s does not have a luid.'
                ' Its id attribute was used as a fallback.',
                sheet_metadata.get('name'))

        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constants.USER_SPECIFIED_TYPE_SHEET

        entry.display_name = self._format_display_name(
            sheet_metadata.get('name'))

        # A null path that means the sheet was hidden and included on a
        # dashboard, or deleted from server but it is still in the workbook.
        path = sheet_metadata.get('path')
        if path:
            site_content_url = self.__format_site_content_url(
                workbook_metadata)
            entry.linked_resource = \
                f'{self.__server_address}/#{site_content_url}/views/{path}'

        created_at = sheet_metadata.get('createdAt')
        if created_at:
            created_datetime = datetime.strptime(
                created_at, self.__INCOMING_TIMESTAMP_FORMAT)
            create_timestamp = timestamp_pb2.Timestamp()
            create_timestamp.FromDatetime(created_datetime)
            entry.source_system_timestamps.create_time = create_timestamp

        updated_at = sheet_metadata.get('updatedAt')
        if updated_at:
            updated_datetime = datetime.strptime(
                updated_at, self.__INCOMING_TIMESTAMP_FORMAT)
            update_timestamp = timestamp_pb2.Timestamp()
            update_timestamp.FromDatetime(updated_datetime)
            entry.source_system_timestamps.update_time = update_timestamp

        return generated_id, entry

    def make_entry_for_workbook(self, workbook_metadata):
        entry = datacatalog.Entry()

        generated_id = self.__format_id(workbook_metadata.get('luid'))
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constants.USER_SPECIFIED_TYPE_WORKBOOK

        entry.display_name = self._format_display_name(
            workbook_metadata.get('name'))
        entry.description = workbook_metadata.get('description')

        vizportal_url_id = workbook_metadata.get('vizportalUrlId')
        if vizportal_url_id:
            site_content_url = self.__format_site_content_url(
                workbook_metadata)
            entry.linked_resource = f'{self.__server_address}/' \
                f'#{site_content_url}/workbooks/{vizportal_url_id}'

        created_datetime = datetime.strptime(
            workbook_metadata.get('createdAt'),
            self.__INCOMING_TIMESTAMP_FORMAT)
        create_timestamp = timestamp_pb2.Timestamp()
        create_timestamp.FromDatetime(created_datetime)
        entry.source_system_timestamps.create_time = create_timestamp

        updated_datetime = datetime.strptime(
            workbook_metadata.get('updatedAt'),
            self.__INCOMING_TIMESTAMP_FORMAT)
        update_timestamp = timestamp_pb2.Timestamp()
        update_timestamp.FromDatetime(updated_datetime)
        entry.source_system_timestamps.update_time = update_timestamp

        return generated_id, entry

    @classmethod
    def __format_id(cls, source_id):
        no_prefix_fmt_id = cls._format_id(source_id)
        if len(no_prefix_fmt_id) > constants.NO_PREFIX_ENTRY_ID_LENGTH:
            no_prefix_fmt_id = \
                no_prefix_fmt_id[:constants.NO_PREFIX_ENTRY_ID_LENGTH]
        return f'{constants.ENTRY_ID_PREFIX}{no_prefix_fmt_id}'

    @classmethod
    def __format_site_content_url(cls, workbook_metadata):
        site_name = workbook_metadata.get('site').get('name')

        # TODO Remove the below workaround when there is a definitive fix for
        #  https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi/issues/43  # noqa E501
        #  Valid Linked Resource chars: letters, numbers, periods, colons,
        #  slashes, underscores, dashes and hashes.
        compliant_site_name = re.sub(r'[^\w.,/\-#]', '_', site_name)

        return '' \
            if site_name.lower() == 'default' \
            else f'/site/{compliant_site_name}'
