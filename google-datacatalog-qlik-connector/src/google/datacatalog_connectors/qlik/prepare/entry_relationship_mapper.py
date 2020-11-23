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

from google.datacatalog_connectors.qlik.prepare import constants


class EntryRelationshipMapper(prepare.BaseEntryRelationshipMapper):
    __APP = constants.USER_SPECIFIED_TYPE_APP
    __STREAM = constants.USER_SPECIFIED_TYPE_STREAM

    def fulfill_tag_fields(self, assembled_entries_data):
        resolvers = (self.__resolve_app_mappings,)

        self._fulfill_tag_fields(assembled_entries_data, resolvers)

    @classmethod
    def __resolve_app_mappings(cls, assembled_entries_data, id_name_pairs):
        for assembled_entry_data in assembled_entries_data:
            entry = assembled_entry_data.entry
            if not entry.user_specified_type == cls.__APP:
                continue

            cls._map_related_entry(assembled_entry_data, cls.__STREAM,
                                   'stream_id', 'stream_entry', id_name_pairs)
