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

from typing import Any, Dict, List

from google.cloud.datacatalog import TagTemplate
from google.datacatalog_connectors.commons import prepare
from google.datacatalog_connectors.commons.prepare import AssembledEntryData

from google.datacatalog_connectors.sisense.prepare import constants, \
    datacatalog_entry_factory, datacatalog_tag_factory
from google.datacatalog_connectors.sisense.prepare.datacatalog_entry_factory \
    import DataCatalogEntryFactory


class AssembledEntryFactory:
    __datacatalog_entry_factory: DataCatalogEntryFactory

    def __init__(self, project_id: str, location_id: str, entry_group_id: str,
                 user_specified_system: str, server_address: str):

        self.__datacatalog_entry_factory = datacatalog_entry_factory \
            .DataCatalogEntryFactory(
                project_id, location_id, entry_group_id, user_specified_system,
                server_address)

        self.__datacatalog_tag_factory = \
            datacatalog_tag_factory.DataCatalogTagFactory(server_address)

    def make_assembled_entries_list(
            self, asset_metadata: Dict[str, Any],
            tag_templates: Dict[str, TagTemplate]) -> List[AssembledEntryData]:

        assembled_entries = []
        if constants.SISENSE_ASSET_TYPE_FOLDER == asset_metadata.get('type'):
            assembled_entries.extend(
                self.__make_assembled_entries_for_folder(
                    asset_metadata, tag_templates))

        return assembled_entries

    @classmethod
    def __get_tag_template(
            cls, tag_template_id: str,
            tag_templates: Dict[str, TagTemplate]) -> TagTemplate:

        return tag_templates[tag_template_id] \
            if tag_template_id in tag_templates else None

    def __make_assembled_entries_for_folder(
            self, folder_metadata: Dict[str, Any],
            tag_templates: Dict[str, TagTemplate]) -> List[AssembledEntryData]:

        folder_tag_template = self.__get_tag_template(
            constants.TAG_TEMPLATE_ID_FOLDER, tag_templates)

        assembled_entries = [
            self.__make_assembled_entry_for_folder(folder_metadata,
                                                   folder_tag_template)
        ]

        for child_folder in folder_metadata['folders']:
            assembled_entries.append(
                self.__make_assembled_entry_for_folder(child_folder,
                                                       folder_tag_template))

        return assembled_entries

    def __make_assembled_entry_for_folder(
            self, folder_metadata: Dict[str, Any],
            tag_template: TagTemplate) -> AssembledEntryData:

        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_folder(
                folder_metadata)

        tags = []
        if tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_folder(
                    tag_template, folder_metadata))

        return prepare.AssembledEntryData(entry_id=entry_id,
                                          entry=entry,
                                          tags=tags)
