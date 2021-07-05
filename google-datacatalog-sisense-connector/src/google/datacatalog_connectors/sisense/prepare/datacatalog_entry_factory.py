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
from typing import Any, Dict, Optional, Tuple

from google.cloud import datacatalog
from google.cloud.datacatalog import ColumnSchema, Entry, Schema
from google.protobuf import timestamp_pb2
from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.sisense.prepare import constants


class DataCatalogEntryFactory(prepare.BaseEntryFactory):
    __INCOMING_TIMESTAMP_UTC_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'
    __UNNAMED = 'Unnamed'

    def __init__(self, project_id: str, location_id: str, entry_group_id: str,
                 user_specified_system: str, server_address: str):

        self.__project_id = project_id
        self.__location_id = location_id
        self.__entry_group_id = entry_group_id
        self.__user_specified_system = user_specified_system
        self.__server_address = server_address

        # Strip schema (http | https) and slashes from the server url.
        self.__server_id = server_address[server_address.find('//') + 2:]

    def make_entry_for_dashboard(
            self, dashboard_metadata: Dict[str, Any]) -> Tuple[str, Entry]:

        entry = datacatalog.Entry()

        dashboard_id = dashboard_metadata.get('oid')

        generated_id = self.__format_id(constants.ENTRY_ID_PART_DASHBOARD,
                                        dashboard_id)
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constants.USER_SPECIFIED_TYPE_DASHBOARD

        entry.display_name = self._format_display_name(
            dashboard_metadata.get('title'))
        entry.description = dashboard_metadata.get('desc')

        entry.linked_resource = f'{self.__server_address}' \
                                f'/app/main#/dashboards/{dashboard_id}'

        if dashboard_metadata.get('created'):
            created_datetime = datetime.strptime(
                dashboard_metadata.get('created'),
                self.__INCOMING_TIMESTAMP_UTC_FORMAT)
            create_timestamp = timestamp_pb2.Timestamp()
            create_timestamp.FromDatetime(created_datetime)
            entry.source_system_timestamps.create_time = create_timestamp

            modified_date = dashboard_metadata.get('lastUpdated')
            resolved_modified_date = modified_date or dashboard_metadata.get(
                'created')
            modified_datetime = datetime.strptime(
                resolved_modified_date, self.__INCOMING_TIMESTAMP_UTC_FORMAT)
            update_timestamp = timestamp_pb2.Timestamp()
            update_timestamp.FromDatetime(modified_datetime)
            entry.source_system_timestamps.update_time = update_timestamp

        entry.schema = self.__make_schema_for_dashboard(dashboard_metadata)

        return generated_id, entry

    @classmethod
    def __make_schema_for_dashboard(
            cls, dashboard_metadata: Dict[str, Any]) -> Optional[Schema]:

        if not dashboard_metadata.get('filters'):
            return

        filters_column = datacatalog.ColumnSchema()
        filters_column.column = 'filters'
        filters_column.type = 'array'
        filters_column.description = 'The Dashboard filters'

        for dashboard_filter in dashboard_metadata['filters']:
            filters_column.subcolumns.append(
                cls.__make_column_schema_for_jaql(
                    dashboard_filter.get('jaql')))

        schema = datacatalog.Schema()
        schema.columns.append(filters_column)

        return schema

    def make_entry_for_folder(
            self, folder_metadata: Dict[str, Any]) -> Tuple[str, Entry]:

        entry = datacatalog.Entry()

        # The root folder's ``oid`` field is not fulfilled.
        folder_id = folder_metadata.get('oid') or folder_metadata.get('name')

        generated_id = self.__format_id(constants.ENTRY_ID_PART_FOLDER,
                                        folder_id)
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constants.USER_SPECIFIED_TYPE_FOLDER

        entry.display_name = self._format_display_name(
            folder_metadata.get('name'))

        if folder_metadata.get('oid'):
            entry.linked_resource = f'{self.__server_address}' \
                                    f'/app/main#/home' \
                                    f'/{folder_metadata.get("oid")}'

        if folder_metadata.get('created'):
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

    def make_entry_for_widget(
            self, widget_metadata: Dict[str, Any]) -> Tuple[str, Entry]:

        entry = datacatalog.Entry()

        widget_id = widget_metadata.get('oid')

        generated_id = self.__format_id(constants.ENTRY_ID_PART_WIDGET,
                                        widget_id)
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constants.USER_SPECIFIED_TYPE_WIDGET

        entry.display_name = self._format_display_name(
            widget_metadata.get('title') or self.__UNNAMED)
        entry.description = widget_metadata.get('desc')

        entry.linked_resource = f'{self.__server_address}' \
                                f'/app/main#/dashboards' \
                                f'/{widget_metadata.get("dashboardid")}' \
                                f'/widgets/{widget_id}'

        if widget_metadata.get('created'):
            created_datetime = datetime.strptime(
                widget_metadata.get('created'),
                self.__INCOMING_TIMESTAMP_UTC_FORMAT)
            create_timestamp = timestamp_pb2.Timestamp()
            create_timestamp.FromDatetime(created_datetime)
            entry.source_system_timestamps.create_time = create_timestamp

            modified_date = widget_metadata.get('lastUpdated')
            resolved_modified_date = modified_date or widget_metadata.get(
                'created')
            modified_datetime = datetime.strptime(
                resolved_modified_date, self.__INCOMING_TIMESTAMP_UTC_FORMAT)
            update_timestamp = timestamp_pb2.Timestamp()
            update_timestamp.FromDatetime(modified_datetime)
            entry.source_system_timestamps.update_time = update_timestamp

        entry.schema = self.__make_schema_for_widget(widget_metadata)

        return generated_id, entry

    @classmethod
    def __make_schema_for_widget(
            cls, widget_metadata: Dict[str, Any]) -> Optional[Schema]:

        if not (widget_metadata.get('metadata') and
                widget_metadata['metadata'].get('panels')):
            return

        panels = widget_metadata['metadata']['panels']
        filters = next((panel.get('items')
                        for panel in panels
                        if panel.get('name') == 'filters'), None)
        if not filters:
            return

        filters_column = datacatalog.ColumnSchema()
        filters_column.column = 'filters'
        filters_column.type = 'array'
        filters_column.description = 'The Widget filters'

        for widget_filter in filters:
            filters_column.subcolumns.append(
                cls.__make_column_schema_for_jaql(widget_filter.get('jaql')))

        schema = datacatalog.Schema()
        schema.columns.append(filters_column)

        return schema

    def __format_id(self, source_type_identifier, source_id):
        prefixed_id = f'{constants.ENTRY_ID_PREFIX}' \
                      f'{self.__server_id}_' \
                      f'{source_type_identifier}{source_id}'
        return self._format_id(prefixed_id)

    @classmethod
    def __make_column_schema_for_jaql(
            cls, jaql_metadata: Dict[str, Any]) -> ColumnSchema:

        column = datacatalog.ColumnSchema()
        column.column = jaql_metadata.get('title')
        column.type = jaql_metadata.get('datatype') or jaql_metadata.get(
            'type')

        return column
