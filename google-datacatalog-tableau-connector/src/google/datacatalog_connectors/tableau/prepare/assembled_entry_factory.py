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

from google.datacatalog_connectors.tableau.prepare import \
    constants, datacatalog_entry_factory, datacatalog_tag_factory


class AssembledEntryFactory:

    def __init__(self, project_id, location_id, entry_group_id,
                 user_specified_system, server_address):

        self.__datacatalog_entry_factory = \
            datacatalog_entry_factory.DataCatalogEntryFactory(
                project_id, location_id, entry_group_id, user_specified_system,
                server_address)

    def make_entries_for_dashboards(self, dashboards_metadata,
                                    tag_templates_dict):

        tag_template_workbook = tag_templates_dict.get(
            constants.TAG_TEMPLATE_ID_WORKBOOK)

        tag_template_dashboard = tag_templates_dict.get(
            constants.TAG_TEMPLATE_ID_DASHBOARD)

        assembled_entries = []
        for dashboard_metadata in dashboards_metadata:
            # Temporary objects to fulfill Data Catalog Entry relationships
            # comprising Dashboards and the Workbooks they belong to.
            # Such objects have incomplete information and must not be ingested
            # into Data Catalog.
            workbook_metadata = dashboard_metadata.get('workbook')
            if workbook_metadata:
                assembled_entries.append(
                    self.__make_entry_for_workbook(workbook_metadata,
                                                   tag_template_workbook))

            assembled_entries.append(
                self.__make_entry_for_dashboard(dashboard_metadata,
                                                tag_template_dashboard))

        return assembled_entries

    def make_entries_for_sites(self, sites_metadata, tag_templates_dict):
        assembled_entries = []
        for site_metadata in sites_metadata:
            workbooks_metadata = site_metadata.get('workbooks')
            assembled_entries.extend(
                self.make_entries_for_workbooks(workbooks_metadata,
                                                tag_templates_dict))

        return assembled_entries

    def make_entries_for_workbooks(self, workbooks_metadata,
                                   tag_templates_dict):

        tag_template_workbook = tag_templates_dict.get(
            constants.TAG_TEMPLATE_ID_WORKBOOK)

        tag_template_sheet = tag_templates_dict.get(
            constants.TAG_TEMPLATE_ID_SHEET)

        assembled_entries = []
        for workbook_metadata in workbooks_metadata:
            assembled_entries.append(
                self.__make_entry_for_workbook(workbook_metadata,
                                               tag_template_workbook))

            sheets_metadata = workbook_metadata.get('sheets')
            assembled_entries.extend(
                self.__make_entries_for_sheets(sheets_metadata,
                                               workbook_metadata,
                                               tag_template_sheet))

        return assembled_entries

    def __make_entry_for_dashboard(self, dashboard_metadata, tag_template):
        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_dashboard(
                dashboard_metadata)

        tags = []
        if tag_template:
            tags.append(
                datacatalog_tag_factory.DataCatalogTagFactory.
                make_tag_for_dashboard(tag_template, dashboard_metadata))

        return prepare.AssembledEntryData(entry_id, entry, tags)

    def __make_entry_for_workbook(self, workbook_metadata, tag_template):
        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_workbook(
                workbook_metadata)

        tags = []
        if tag_template:
            tags.append(
                datacatalog_tag_factory.DataCatalogTagFactory.
                make_tag_for_workbook(tag_template, workbook_metadata))

        return prepare.AssembledEntryData(entry_id, entry, tags)

    def __make_entries_for_sheets(self, sheets_metadata, workbook_metadata,
                                  tag_template):
        entries = []
        for sheet_metadata in sheets_metadata:
            entries.append(
                self.__make_entry_for_sheet(sheet_metadata, workbook_metadata,
                                            tag_template))

        return entries

    def __make_entry_for_sheet(self, sheet_metadata, workbook_metadata,
                               tag_template):
        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_sheet(
                sheet_metadata,
                workbook_metadata)

        tags = []
        if tag_template:
            tags.append(
                datacatalog_tag_factory.DataCatalogTagFactory.
                make_tag_for_sheet(tag_template, sheet_metadata,
                                   workbook_metadata))

        return prepare.AssembledEntryData(entry_id, entry, tags)
