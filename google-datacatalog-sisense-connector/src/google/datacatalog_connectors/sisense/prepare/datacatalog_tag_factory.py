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
import re
from typing import Any, Dict, List, Optional

from google.cloud import datacatalog
from google.cloud.datacatalog import Tag, TagTemplate
from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.sisense.prepare import \
    constants, sisense_connector_strings_helper


class DataCatalogTagFactory(prepare.BaseTagFactory):
    __INCOMING_TIMESTAMP_UTC_FORMAT = '%Y-%m-%dT%H:%M:%S.%fZ'

    def __init__(self, server_address: str):
        self.__server_address = server_address

    def make_tag_for_dashboard(self, tag_template: TagTemplate,
                               dashboard_metadata: Dict[str, Any]) -> Tag:

        tag = datacatalog.Tag()

        tag.template = tag_template.name

        self._set_string_field(tag, 'id', dashboard_metadata.get('oid'))

        owner = dashboard_metadata.get('ownerData')
        if owner:
            self._set_string_field(tag, 'owner_username',
                                   owner.get('userName'))

            first_name = owner.get('firstName') or ''
            last_name = owner.get('lastName') or ''
            self._set_string_field(tag, 'owner_name',
                                   f'{first_name} {last_name}')

        folder = dashboard_metadata.get('folderData')
        if folder:
            # The root folder's ``oid`` field is not fulfilled.
            folder_id = folder.get('oid') or folder.get('name')
            self._set_string_field(tag, 'folder_id', folder_id)
            self._set_string_field(tag, 'folder_name', folder.get('name'))

        datasource = dashboard_metadata.get('datasource')
        if datasource:
            self._set_string_field(tag, 'datasource', datasource.get('title'))

        last_publish_time = dashboard_metadata.get('lastPublish')
        if last_publish_time:
            self._set_timestamp_field(
                tag, 'last_publish',
                datetime.strptime(last_publish_time,
                                  self.__INCOMING_TIMESTAMP_UTC_FORMAT))

        last_opened_time = dashboard_metadata.get('lastOpened')
        if last_opened_time:
            self._set_timestamp_field(
                tag, 'last_opened',
                datetime.strptime(last_opened_time,
                                  self.__INCOMING_TIMESTAMP_UTC_FORMAT))

        self._set_string_field(tag, 'server_url', self.__server_address)

        return tag

    def make_tags_for_dashboard_filters(
            self, jaql_tag_template: TagTemplate,
            dashboard_metadata: Dict[str, Any]) -> List[Tag]:

        tags = []

        filters = dashboard_metadata.get(
            constants.DASHBOARD_FILTERS_FIELD_NAME)
        if not filters:
            return tags

        for dashboard_filter in filters:
            tags.extend(
                self.__make_tags_for_jaql(jaql_tag_template,
                                          dashboard_filter.get('jaql'),
                                          constants.ENTRY_COLUMN_FILTERS))

        return tags

    def make_tag_for_folder(self, tag_template: TagTemplate,
                            folder_metadata: Dict[str, Any]) -> Tag:

        tag = datacatalog.Tag()

        tag.template = tag_template.name

        # The root folder's ``oid`` field is not fulfilled.
        folder_id = folder_metadata.get('oid') or folder_metadata.get('name')
        self._set_string_field(tag, 'id', folder_id)

        parent = folder_metadata.get('parentFolderData')
        if parent:
            self._set_string_field(tag, 'parent_id', parent.get('oid'))
            self._set_string_field(tag, 'parent_name', parent.get('name'))

        owner = folder_metadata.get('ownerData')
        if owner:
            self._set_string_field(tag, 'owner_username',
                                   owner.get('userName'))

            first_name = owner.get('firstName') or ''
            last_name = owner.get('lastName') or ''
            self._set_string_field(tag, 'owner_name',
                                   f'{first_name} {last_name}')

        child_folders = folder_metadata.get('folders')
        child_count = len(child_folders) if child_folders else 0
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

    def make_tag_for_widget(self, tag_template: TagTemplate,
                            widget_metadata: Dict[str, Any]) -> Tag:

        tag = datacatalog.Tag()

        tag.template = tag_template.name

        self._set_string_field(tag, 'id', widget_metadata.get('oid'))
        self._set_string_field(tag, 'type', widget_metadata.get('type'))
        self._set_string_field(tag, 'subtype', widget_metadata.get('subtype'))

        owner = widget_metadata.get('ownerData')
        if owner:
            self._set_string_field(tag, 'owner_username',
                                   owner.get('userName'))

            first_name = owner.get('firstName') or ''
            last_name = owner.get('lastName') or ''
            self._set_string_field(tag, 'owner_name',
                                   f'{first_name} {last_name}')

        dashboard = widget_metadata.get('dashboardData')
        self._set_string_field(tag, 'dashboard_id', dashboard.get('oid'))
        self._set_string_field(tag, 'dashboard_title', dashboard.get('title'))

        datasource = widget_metadata.get('datasource')
        if isinstance(datasource, dict):
            self._set_string_field(tag, 'datasource', datasource.get('title'))
        elif isinstance(datasource, str):
            self._set_string_field(tag, 'datasource', datasource)

        self._set_string_field(tag, 'server_url', self.__server_address)

        return tag

    def make_tags_for_widget_fields(
            self, jaql_tag_template: TagTemplate,
            widget_metadata: Dict[str, Any]) -> List[Tag]:

        tags = []

        if not (widget_metadata.get('metadata') and
                widget_metadata['metadata'].get('panels')):
            return tags

        panels = widget_metadata['metadata']['panels']
        fields = [
            panel for panel in panels
            if not panel.get('name') == constants.WIDGET_FILTERS_PANEL_NAME
        ]
        if not fields:
            return tags

        for field in fields:
            for item in field.get('items'):
                tags.extend(
                    self.__make_tags_for_jaql(jaql_tag_template,
                                              item.get('jaql'),
                                              constants.ENTRY_COLUMN_FIELDS))

        return tags

    def make_tags_for_widget_filters(
            self, jaql_tag_template: TagTemplate,
            widget_metadata: Dict[str, Any]) -> List[Tag]:

        tags = []

        if not (widget_metadata.get('metadata') and
                widget_metadata['metadata'].get('panels')):
            return tags

        panels = widget_metadata['metadata']['panels']
        filters = next(
            (panel.get('items')
             for panel in panels
             if panel.get('name') == constants.WIDGET_FILTERS_PANEL_NAME),
            None)
        if not filters:
            return tags

        for widget_filter in filters:
            tags.extend(
                self.__make_tags_for_jaql(jaql_tag_template,
                                          widget_filter.get('jaql'),
                                          constants.ENTRY_COLUMN_FILTERS))

        return tags

    def __make_tags_for_jaql(self, tag_template: TagTemplate,
                             jaql_metadata: Dict[str, Any],
                             column_prefix: str) -> List[Tag]:

        tags = []

        if not jaql_metadata:
            return tags

        tag = datacatalog.Tag()
        tag.template = tag_template.name

        dim_table = None
        dim_column = None
        dimension = jaql_metadata.get('dim')
        if dimension:
            self._set_string_field(tag, 'dimension', dimension)

            # According to the Sisense Support Team, JAQL objects should
            # contain the ``table`` and ``column`` fields, but we have seen
            # some cases in which it does not happen -- e.g.: dashboards that
            # were created a long time ago and migrated from version to
            # version, as well as from platform to platform (Windows to Linux),
            # have the ``dim`` field, but not ``table`` and ``column``. So, we
            # decided to scrape table and column metadata from the dimension
            # when the appropriate fields are not available to avoid losing
            # relevant lineage information. A regex is used to do so.
            dim_match = re.search(r'^\[(?P<table>.*)\.(?P<column>.*)]$',
                                  dimension)
            dim_table = dim_match.group('table')
            dim_column = dim_match.group('column')

        self._set_string_field(tag, 'table',
                               jaql_metadata.get('table') or dim_table)
        self._set_string_field(tag, 'column',
                               jaql_metadata.get('column') or dim_column)

        formula = jaql_metadata.get(constants.JAQL_FORMULA_FIELD_NAME)
        context = jaql_metadata.get(constants.JAQL_CONTEXT_FIELD_NAME)
        human_readable_formula = formula
        # The formula and its fields (aka parts) are stored in distinct fields,
        # ``formula`` and ``context``. The below code seeks to replace the part
        # identifiers, usually system-generated strings, with the part titles,
        # which are human-readable strings. On success, the resulting formula
        # is equal to what Sisense shows to users in the UI.
        if formula and context:
            parts = re.findall(r'\[(.*?)]', formula)
            for part in parts:
                part_metadata = context.get(f'[{part}]')
                if not part_metadata:
                    continue
                part_title = part_metadata.get('title')
                if part_title:
                    human_readable_formula = human_readable_formula.replace(
                        part, part_title)

        self._set_string_field(tag, 'formula', human_readable_formula)

        self._set_string_field(tag, 'aggregation', jaql_metadata.get('agg'))

        self._set_string_field(tag, 'server_url', self.__server_address)

        title = jaql_metadata.get('title')
        subcolumn_name = sisense_connector_strings_helper\
            .SisenseConnectorStringsHelper.format_column_name(title)
        tag.column = f'{column_prefix}.{subcolumn_name}'
        tags.append(tag)

        tags.extend(
            self.__make_tags_for_jaql_formula(tag_template, jaql_metadata,
                                              tag.column))

        filter_by_tag = self.__make_tag_for_jaql_filter_by(
            tag_template, jaql_metadata, tag.column)
        if filter_by_tag:
            tags.append(filter_by_tag)

        return tags

    def __make_tags_for_jaql_formula(self, tag_template: TagTemplate,
                                     jaql_metadata: Dict[str, Any],
                                     column_prefix: str) -> List[Tag]:

        tags = []

        formula = jaql_metadata.get(constants.JAQL_FORMULA_FIELD_NAME)
        context = jaql_metadata.get(constants.JAQL_CONTEXT_FIELD_NAME)
        if not (formula and context):
            return tags

        parts = re.findall(r'\[(.*?)]', formula)
        for part in parts:
            tags.extend(
                self.__make_tags_for_jaql(
                    tag_template, context.get(f'[{part}]'),
                    f'{column_prefix}.{constants.ENTRY_COLUMN_FORMULA}'))

        return tags

    def __make_tag_for_jaql_filter_by(self, tag_template: TagTemplate,
                                      jaql_metadata: Dict[str, Any],
                                      column_prefix: str) -> Optional[Tag]:

        jaql_filter = jaql_metadata.get(constants.JAQL_FILTER_FIELD_NAME)
        if not jaql_filter:
            return

        tags = self.__make_tags_for_jaql(
            tag_template, jaql_filter.get(constants.JAQL_FILTER_BY_FIELD_NAME),
            f'{column_prefix}.{constants.ENTRY_COLUMN_FILTER_BY}')

        return tags[0] if tags else None
