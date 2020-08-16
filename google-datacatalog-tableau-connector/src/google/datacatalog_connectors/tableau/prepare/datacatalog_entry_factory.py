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

from google.cloud import datacatalog
from google.cloud.datacatalog import types

from google.datacatalog_connectors.commons.prepare.base_entry_factory import \
    BaseEntryFactory
from . import constant


class DataCatalogEntryFactory(BaseEntryFactory):
    # Datetime format with timezone information
    __DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S%z'

    def __init__(self, project_id, location_id, entry_group_id,
                 user_specified_system, server_address):

        self.__project_id = project_id
        self.__location_id = location_id
        self.__entry_group_id = entry_group_id
        self.__user_specified_system = user_specified_system
        self.__server_address = server_address

    def make_entry_for_dashboard(self, dashboard_metadata):
        entry = types.Entry()

        generated_id = self.__format_id(dashboard_metadata.get('luid'))
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constant.USER_SPECIFIED_TYPE_DASHBOARD

        entry.display_name = self._format_display_name(
            dashboard_metadata.get('name'))

        path = dashboard_metadata.get('path')
        if path:
            site_content_url = self.__format_site_content_url(
                dashboard_metadata.get('workbook'))
            entry.linked_resource = \
                f'{self.__server_address}/#{site_content_url}/views/{path}'

        created_datetime = self.__convert_string_to_utc_datetime(
            dashboard_metadata.get('createdAt'))
        entry.source_system_timestamps.create_time.seconds = \
            self.__convert_datetime_to_seconds(
                created_datetime)
        updated_datetime = self.__convert_string_to_utc_datetime(
            dashboard_metadata.get('updatedAt'))
        entry.source_system_timestamps.update_time.seconds = \
            self.__convert_datetime_to_seconds(
                updated_datetime)

        return generated_id, entry

    def make_entry_for_sheet(self, sheet_metadata, workbook_metadata):
        entry = types.Entry()

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
        entry.user_specified_type = constant.USER_SPECIFIED_TYPE_SHEET

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
            created_datetime = self.__convert_string_to_utc_datetime(
                created_at)
            entry.source_system_timestamps.create_time.seconds = \
                self.__convert_datetime_to_seconds(
                    created_datetime)

        updated_at = sheet_metadata.get('updatedAt')
        if updated_at:
            updated_datetime = self.__convert_string_to_utc_datetime(
                updated_at)
            entry.source_system_timestamps.update_time.seconds = \
                self.__convert_datetime_to_seconds(
                    updated_datetime)

        return generated_id, entry

    def make_entry_for_workbook(self, workbook_metadata):
        entry = types.Entry()

        generated_id = self.__format_id(workbook_metadata.get('luid'))
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constant.USER_SPECIFIED_TYPE_WORKBOOK

        entry.display_name = self._format_display_name(
            workbook_metadata.get('name'))
        entry.description = workbook_metadata.get('description')

        vizportal_url_id = workbook_metadata.get('vizportalUrlId')
        if vizportal_url_id:
            site_content_url = self.__format_site_content_url(
                workbook_metadata)
            entry.linked_resource = f'{self.__server_address}/' \
                f'#{site_content_url}/workbooks/{vizportal_url_id}'

        created_datetime = self.__convert_string_to_utc_datetime(
            workbook_metadata.get('createdAt'))
        entry.source_system_timestamps.create_time.seconds = \
            self.__convert_datetime_to_seconds(
                created_datetime)
        updated_datetime = self.__convert_string_to_utc_datetime(
            workbook_metadata.get('updatedAt'))
        entry.source_system_timestamps.update_time.seconds = \
            self.__convert_datetime_to_seconds(
                updated_datetime)

        return generated_id, entry

    @classmethod
    def __format_id(cls, source_id):
        no_prefix_fmt_id = cls._format_id(source_id)
        if len(no_prefix_fmt_id) > constant.NO_PREFIX_ENTRY_ID_LENGTH:
            no_prefix_fmt_id = \
                no_prefix_fmt_id[:constant.NO_PREFIX_ENTRY_ID_LENGTH]
        return f'{constant.ENTRY_ID_PREFIX}{no_prefix_fmt_id}'

    @classmethod
    def __format_site_content_url(cls, workbook_metadata):
        site_name = workbook_metadata.get('site').get('name')
        return '' if site_name.lower() == 'default' else f'/site/{site_name}'

    @classmethod
    def __convert_string_to_utc_datetime(cls, datetime_string):
        """Converts a Tableau timestamp string into a UTC datetime object.

        Tableau timestamp strings are in format of '%Y-%m-%dT%H:%M:%SZ'.
        This method replaces the trailing 'Z' by '+0000' before parsing
        the string. By doing this, it makes possible adding UTC timezone
        information to the returned object.

        :param datetime_string: Tableau formatted timestamp string
        :return: UTC datetime object
        """
        utc_formatted_str = f'{datetime_string[:-1]}+0000'
        return datetime.strptime(utc_formatted_str, cls.__DATETIME_FORMAT)

    @classmethod
    def __convert_datetime_to_seconds(cls, datetime_object):
        return int(datetime_object.timestamp())
