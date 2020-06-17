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
from . import datacatalog_entry_factory, datacatalog_tag_factory


class AssembledEntryFactory:
    __datacatalog_entry_factory: datacatalog_entry_factory\
        .DataCatalogEntryFactory

    def __init__(self, project_id, location_id, entry_group_id,
                 user_specified_system, instance_url):

        self.__datacatalog_entry_factory = datacatalog_entry_factory\
            .DataCatalogEntryFactory(
                project_id, location_id, entry_group_id, user_specified_system,
                instance_url)

        self.__datacatalog_tag_factory = \
            datacatalog_tag_factory.DataCatalogTagFactory(instance_url)

    def make_assembled_entries_list(self, folders, queries,
                                    tag_templates_dict):

        folder_tag_template = self.__get_tag_template(
            constant.TAG_TEMPLATE_ID_FOLDER, tag_templates_dict)
        dashboard_tag_template = self.__get_tag_template(
            constant.TAG_TEMPLATE_ID_DASHBOARD, tag_templates_dict)
        element_tag_template = self.__get_tag_template(
            constant.TAG_TEMPLATE_ID_DASHBOARD_ELEMENT, tag_templates_dict)
        look_tag_template = self.__get_tag_template(
            constant.TAG_TEMPLATE_ID_LOOK, tag_templates_dict)
        query_tag_template = self.__get_tag_template(
            constant.TAG_TEMPLATE_ID_QUERY, tag_templates_dict)

        assembled_entries = []
        for folder in folders:
            assembled_entries.append(
                self.__make_assembled_entry_for_folder(folder,
                                                       folder_tag_template))

            if hasattr(folder, 'dashboards'):
                assembled_entries.extend(
                    self.__make_assembled_entries_for_dashboads(
                        folder.dashboards, dashboard_tag_template,
                        element_tag_template))

            if hasattr(folder, 'looks'):
                assembled_entries.extend(
                    self.__make_assembled_entries_for_looks(
                        folder.looks, look_tag_template))

        assembled_entries.extend([
            self.__make_assembled_entry_for_query(query, query_tag_template)
            for query in queries
        ])

        return assembled_entries

    @classmethod
    def __get_tag_template(cls, tag_template_id, tag_templates_dict):
        return tag_templates_dict[tag_template_id] \
            if tag_template_id in tag_templates_dict else None

    def __make_assembled_entry_for_folder(self, folder, tag_template):
        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_folder(folder)

        tags = []
        if tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_folder(
                    tag_template, folder))

        return prepare.AssembledEntryData(entry_id, entry, tags)

    def __make_assembled_entries_for_dashboads(self, dashboards, tag_template,
                                               element_tag_template):

        assembled_entries = []

        if not dashboards:
            return assembled_entries

        for dashboard in dashboards:
            assembled_entries.append(
                self.__make_assembled_entry_for_dashboard(
                    dashboard, tag_template))
            if hasattr(dashboard, 'dashboard_elements'):
                assembled_entries.extend(
                    self.__make_assembled_entries_for_dashboad_elements(
                        dashboard.dashboard_elements, dashboard,
                        element_tag_template))

        return assembled_entries

    def __make_assembled_entry_for_dashboard(self, dashboard, tag_template):
        entry_id, entry = self.__datacatalog_entry_factory.\
            make_entry_for_dashboard(dashboard)

        tags = []
        if tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_dashboard(
                    tag_template, dashboard))

        return prepare.AssembledEntryData(entry_id, entry, tags)

    def __make_assembled_entries_for_dashboad_elements(self, elements,
                                                       dashboard,
                                                       tag_template):

        assembled_entries = []
        for element in elements:
            entry = self.__make_assembled_entry_for_dashboard_element(
                element, dashboard, tag_template)
            if entry:
                assembled_entries.append(entry)

        return assembled_entries

    def __make_assembled_entry_for_dashboard_element(self, element, dashboard,
                                                     tag_template):

        entry_id, entry = self.__datacatalog_entry_factory.\
            make_entry_for_dashboard_element(element)

        if not (entry_id and entry):
            return

        tags = []
        if tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_dashboard_element(
                    tag_template, element, dashboard))

        return prepare.AssembledEntryData(entry_id, entry, tags)

    def __make_assembled_entries_for_looks(self, looks, tag_template):

        assembled_entries = []

        if not looks:
            return assembled_entries

        assembled_entries.extend([
            self.__make_assembled_entry_for_look(look, tag_template)
            for look in looks
        ])

        return assembled_entries

    def __make_assembled_entry_for_look(self, look, tag_template):
        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_look(look)

        tags = []
        if tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_look(
                    tag_template, look))

        return prepare.AssembledEntryData(entry_id, entry, tags)

    def __make_assembled_entry_for_query(self, assembled_query_metadata,
                                         tag_template):

        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_query(
                assembled_query_metadata.query)

        tags = []
        if tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_query(
                    tag_template, assembled_query_metadata))

        return prepare.AssembledEntryData(entry_id, entry, tags)
