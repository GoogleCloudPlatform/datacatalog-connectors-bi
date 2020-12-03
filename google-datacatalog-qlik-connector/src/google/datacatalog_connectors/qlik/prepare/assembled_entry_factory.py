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

from google.datacatalog_connectors.qlik.prepare import \
    constants, datacatalog_entry_factory, datacatalog_tag_factory


class AssembledEntryFactory:
    __datacatalog_entry_factory: datacatalog_entry_factory\
        .DataCatalogEntryFactory

    def __init__(self, project_id, location_id, entry_group_id,
                 user_specified_system, site_url):

        self.__datacatalog_entry_factory = datacatalog_entry_factory \
            .DataCatalogEntryFactory(
                project_id, location_id, entry_group_id, user_specified_system,
                site_url)

        self.__app_tag_template = None
        self.__sheet_tag_template = None
        self.__stream_tag_template = None

        self.__datacatalog_tag_factory = \
            datacatalog_tag_factory.DataCatalogTagFactory(site_url)

    def make_assembled_entries_list(self, stream_metadata, tag_templates_dict):
        self.__initialize_tag_templates(tag_templates_dict)

        assembled_entries = [
            self.__make_assembled_entry_for_stream(stream_metadata)
        ]

        assembled_entries.extend(
            self.__make_assembled_entries_for_apps(
                stream_metadata.get('apps')))

        return assembled_entries

    def __initialize_tag_templates(self, tag_templates_dict):
        self.__app_tag_template = \
            tag_templates_dict.get(constants.TAG_TEMPLATE_ID_APP)
        self.__sheet_tag_template = \
            tag_templates_dict.get(constants.TAG_TEMPLATE_ID_SHEET)
        self.__stream_tag_template = \
            tag_templates_dict.get(constants.TAG_TEMPLATE_ID_STREAM)

    def __make_assembled_entry_for_stream(self, stream_metadata):
        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_stream(
                stream_metadata)

        tags = []
        if self.__stream_tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_stream(
                    self.__stream_tag_template, stream_metadata))

        return prepare.AssembledEntryData(entry_id, entry, tags)

    def __make_assembled_entries_for_apps(self, apps_metadata):
        assembled_entries = []

        if not apps_metadata:
            return assembled_entries

        for app_metadata in apps_metadata:
            assembled_entries.append(
                self.__make_assembled_entry_for_app(app_metadata))
            assembled_entries.extend(
                self.__make_assembled_entries_for_sheets(
                    app_metadata.get('sheets')))

        return assembled_entries

    def __make_assembled_entry_for_app(self, app_metadata):
        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_app(app_metadata)

        tags = []
        if self.__app_tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_app(
                    self.__app_tag_template, app_metadata))

        return prepare.AssembledEntryData(entry_id, entry, tags)

    def __make_assembled_entries_for_sheets(self, sheets_metadata):
        return [
            self.__make_assembled_entry_for_sheet(sheet_metadata)
            for sheet_metadata in sheets_metadata
        ] if sheets_metadata else []

    def __make_assembled_entry_for_sheet(self, sheet_metadata):
        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_sheet(
                sheet_metadata)

        tags = []
        if self.__sheet_tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_sheet(
                    self.__sheet_tag_template, sheet_metadata))

        return prepare.AssembledEntryData(entry_id, entry, tags)
