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

import logging

from google.cloud import datacatalog
from google.cloud.datacatalog import types

from google.datacatalog_connectors.commons import prepare

from . import constant


class DataCatalogEntryFactory(prepare.BaseEntryFactory):

    def __init__(self, project_id, location_id, entry_group_id,
                 user_specified_system, instance_url):

        self.__project_id = project_id
        self.__location_id = location_id
        self.__entry_group_id = entry_group_id
        self.__user_specified_system = user_specified_system
        self.__instance_url = instance_url

        # Strip schema (http | https) and slashes from the server url.
        self.__server_id = instance_url[instance_url.find('//') + 2:]

    def make_entry_for_dashboard(self, dashboard):
        entry = types.Entry()

        generated_id = self.__format_id(constant.ENTRY_ID_DASHBOARD,
                                        dashboard.id)
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constant.USER_SPECIFIED_TYPE_DASHBOARD

        entry.display_name = self._format_display_name(dashboard.title)

        entry.linked_resource = \
            f'{self.__instance_url}/dashboards/{dashboard.id}'

        if dashboard.created_at:  # LookML dashboards come with None
            created_secs = \
                self.__convert_datetime_to_seconds(dashboard.created_at)
            entry.source_system_timestamps.create_time.seconds = created_secs
            # TODO Evaluate/remove "+ 1" after b/144041881 has been closed.
            entry.source_system_timestamps.update_time.seconds = \
                created_secs + 10
        else:
            logging.info('Dashboard "%s" has no created_at information!',
                         dashboard.id)

        return generated_id, entry

    def make_entry_for_dashboard_element(self, element):
        title = element.title if element.title else element.title_text

        if not title or title == '':
            logging.warning(
                'Dashboard Element "%s" has no title nor title_text'
                ' and will be skipped!', element.id)
            return None, None

        entry = types.Entry()

        generated_id = self.__format_id(constant.ENTRY_ID_DASHBOARD_ELEMENT,
                                        element.id)
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = \
            constant.USER_SPECIFIED_TYPE_DASHBOARD_ELEMENT

        entry.display_name = self._format_display_name(title)

        return generated_id, entry

    def make_entry_for_folder(self, folder):
        entry = types.Entry()

        generated_id = self.__format_id(constant.ENTRY_ID_FOLDER, folder.id)
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constant.USER_SPECIFIED_TYPE_FOLDER

        entry.display_name = self._format_display_name(folder.name)

        entry.linked_resource = f'{self.__instance_url}/folders/{folder.id}'

        return generated_id, entry

    def make_entry_for_look(self, look):
        entry = types.Entry()

        generated_id = self.__format_id(constant.ENTRY_ID_LOOK, look.id)
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constant.USER_SPECIFIED_TYPE_LOOK

        entry.display_name = self._format_display_name(look.title)

        entry.linked_resource = f'{self.__instance_url}/looks/{look.id}'

        created_secs = self.__convert_datetime_to_seconds(look.created_at)
        entry.source_system_timestamps.create_time.seconds = created_secs
        updated_secs = self.__convert_datetime_to_seconds(look.updated_at) \
            if look.updated_at else created_secs
        # TODO Evaluate/remove "+ 1" after b/144041881 has been closed.
        entry.source_system_timestamps.update_time.seconds = updated_secs + 10

        return generated_id, entry

    def make_entry_for_query(self, query):
        entry = types.Entry()

        generated_id = self.__format_id(constant.ENTRY_ID_QUERY, query.id)
        entry.name = datacatalog.DataCatalogClient.entry_path(
            self.__project_id, self.__location_id, self.__entry_group_id,
            generated_id)

        entry.user_specified_system = self.__user_specified_system
        entry.user_specified_type = constant.USER_SPECIFIED_TYPE_QUERY

        entry.display_name = self._format_display_name(
            f'Query {query.id} - model {query.model} - explore {query.view}')

        entry.linked_resource = query.share_url

        return generated_id, entry

    def __format_id(self, source_type_prefix, source_id):
        prefixed_id = f'{constant.ENTRY_ID_PREFIX}' \
                      f'{self.__server_id}_' \
                      f'{source_type_prefix}{source_id}'
        return self._format_id(prefixed_id)

    @classmethod
    def __convert_datetime_to_seconds(cls, datetime_object):
        return int(datetime_object.timestamp())
