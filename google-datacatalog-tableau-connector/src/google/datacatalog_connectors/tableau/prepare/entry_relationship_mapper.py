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

from google.datacatalog_connectors.tableau.prepare import constants


class EntryRelationshipMapper(prepare.BaseEntryRelationshipMapper):
    __LUID_FIELD_KEY = 'luid'

    def fulfill_tag_fields(self, assembled_entries_data):
        resolvers = (self.__resolve_dashboard_mappings,
                     self.__resolve_sheet_mappings)

        self._fulfill_tag_fields(assembled_entries_data, resolvers)

    @classmethod
    def __resolve_dashboard_mappings(cls, assembled_entries_data,
                                     id_name_pairs):

        dashboard_entries_data = filter(
            lambda assembled_entry_data: assembled_entry_data.entry.
            user_specified_type == constants.USER_SPECIFIED_TYPE_DASHBOARD,
            assembled_entries_data)

        for dashboard_entry_data in dashboard_entries_data:
            cls._map_related_entry(dashboard_entry_data,
                                   constants.USER_SPECIFIED_TYPE_WORKBOOK,
                                   'workbook_luid', 'workbook_entry',
                                   id_name_pairs)

    @classmethod
    def __resolve_sheet_mappings(cls, assembled_entries_data, id_name_pairs):

        sheet_entries_data = filter(
            lambda assembled_entry_data: assembled_entry_data.entry.
            user_specified_type == constants.USER_SPECIFIED_TYPE_SHEET,
            assembled_entries_data)

        for sheet_entry_data in sheet_entries_data:
            cls._map_related_entry(sheet_entry_data,
                                   constants.USER_SPECIFIED_TYPE_WORKBOOK,
                                   'workbook_luid', 'workbook_entry',
                                   id_name_pairs)

    @classmethod
    def _get_asset_identifier_tag_field_key(cls):
        return cls.__LUID_FIELD_KEY
