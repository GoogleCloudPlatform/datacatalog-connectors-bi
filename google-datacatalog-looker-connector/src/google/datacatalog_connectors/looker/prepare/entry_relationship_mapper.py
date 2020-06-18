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

from google.datacatalog_connectors.commons import prepare

from . import constant


class EntryRelationshipMapper(prepare.BaseEntryRelationshipMapper):
    __DASHBOARD = constant.USER_SPECIFIED_TYPE_DASHBOARD
    __DASHBOARD_ELEMENT = constant.USER_SPECIFIED_TYPE_DASHBOARD_ELEMENT
    __FOLDER = constant.USER_SPECIFIED_TYPE_FOLDER
    __LOOK = constant.USER_SPECIFIED_TYPE_LOOK
    __QUERY = constant.USER_SPECIFIED_TYPE_QUERY

    def fulfill_tag_fields(self, assembled_entries_data):
        resolvers = (self.__resolve_dashboard_mappings,
                     self.__resolve_element_mappings,
                     self.__resolve_folder_mappings,
                     self.__resolve_look_mappings)

        self._fulfill_tag_fields(assembled_entries_data, resolvers)

    @classmethod
    def __resolve_dashboard_mappings(cls, assembled_entries_data,
                                     id_name_pairs):

        for assembled_entry_data in assembled_entries_data:
            entry = assembled_entry_data.entry
            if not entry.user_specified_type == cls.__DASHBOARD:
                continue

            cls._map_related_entry(assembled_entry_data, cls.__FOLDER,
                                   'folder_id', 'folder_entry', id_name_pairs)

    @classmethod
    def __resolve_element_mappings(cls, assembled_entries_data, id_name_pairs):

        for assembled_entry_data in assembled_entries_data:
            entry = assembled_entry_data.entry
            if not entry.user_specified_type == cls.__DASHBOARD_ELEMENT:
                continue

            cls._map_related_entry(assembled_entry_data, cls.__DASHBOARD,
                                   'dashboard_id', 'dashboard_entry',
                                   id_name_pairs)
            cls._map_related_entry(assembled_entry_data, cls.__LOOK, 'look_id',
                                   'look_entry', id_name_pairs)
            cls._map_related_entry(assembled_entry_data, cls.__QUERY,
                                   'query_id', 'query_entry', id_name_pairs)

    @classmethod
    def __resolve_folder_mappings(cls, assembled_entries_data, id_name_pairs):
        for assembled_entry_data in assembled_entries_data:
            entry = assembled_entry_data.entry
            if not entry.user_specified_type == cls.__FOLDER:
                continue

            cls._map_related_entry(assembled_entry_data, cls.__FOLDER,
                                   'parent_id', 'parent_entry', id_name_pairs)

    @classmethod
    def __resolve_look_mappings(cls, assembled_entries_data, id_name_pairs):
        for assembled_entry_data in assembled_entries_data:
            entry = assembled_entry_data.entry
            if not entry.user_specified_type == cls.__LOOK:
                continue

            cls._map_related_entry(assembled_entry_data, cls.__FOLDER,
                                   'folder_id', 'folder_entry', id_name_pairs)
            cls._map_related_entry(assembled_entry_data, cls.__QUERY,
                                   'query_id', 'query_entry', id_name_pairs)
