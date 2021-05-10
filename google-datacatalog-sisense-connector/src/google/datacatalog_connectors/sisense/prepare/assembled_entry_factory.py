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

from google.datacatalog_connectors.commons import prepare

from google.datacatalog_connectors.sisense.prepare import \
    datacatalog_entry_factory
from google.datacatalog_connectors.sisense.prepare.datacatalog_entry_factory \
    import DataCatalogEntryFactory


class AssembledEntryFactory:
    __datacatalog_entry_factory: DataCatalogEntryFactory

    def __init__(self, project_id, location_id, entry_group_id,
                 user_specified_system, server_address):

        self.__datacatalog_entry_factory = datacatalog_entry_factory \
            .DataCatalogEntryFactory(
                project_id, location_id, entry_group_id, user_specified_system,
                server_address)

    def make_assembled_entries_for_folder(self, folder_metadata):
        assembled_entries = [
            self.__make_assembled_entry_for_folder(folder_metadata)
        ]

        return assembled_entries

    def __make_assembled_entry_for_folder(self, folder_metadata):
        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_folder(
                folder_metadata)

        tags = []

        return prepare.AssembledEntryData(entry_id, entry, tags)
