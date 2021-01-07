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

        self.__datacatalog_tag_factory = \
            datacatalog_tag_factory.DataCatalogTagFactory(site_url)

    def make_assembled_entry_for_custom_property_def(
            self, custom_property_def_metadata, tag_template):

        entry_id, entry = self.__datacatalog_entry_factory\
            .make_entry_for_custom_property_definition(
                custom_property_def_metadata)

        tags = []
        if tag_template:
            tags.append(
                self.__datacatalog_tag_factory.
                make_tag_for_custom_property_definition(
                    tag_template, custom_property_def_metadata))

        return prepare.AssembledEntryData(entry_id, entry, tags)

    def make_assembled_entries_for_stream(self, stream_metadata,
                                          tag_templates_dict):

        assembled_entries = [
            self.__make_assembled_entry_for_stream(stream_metadata,
                                                   tag_templates_dict)
        ]

        assembled_entries.extend(
            self.__make_assembled_entries_for_apps(stream_metadata.get('apps'),
                                                   tag_templates_dict))

        return assembled_entries

    def __make_assembled_entry_for_stream(self, stream_metadata,
                                          tag_templates_dict):

        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_stream(
                stream_metadata)

        stream_tag_template = tag_templates_dict.get(
            constants.TAG_TEMPLATE_ID_STREAM)

        tags = []
        if stream_tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_stream(
                    stream_tag_template, stream_metadata))

        tags.extend(
            self.__datacatalog_tag_factory.make_tags_for_custom_properties(
                tag_templates_dict, stream_metadata.get('customProperties')))

        return prepare.AssembledEntryData(entry_id, entry, tags)

    def __make_assembled_entries_for_apps(self, apps_metadata,
                                          tag_templates_dict):

        assembled_entries = []

        if not apps_metadata:
            return assembled_entries

        for app_metadata in apps_metadata:
            assembled_entries.append(
                self.__make_assembled_entry_for_app(app_metadata,
                                                    tag_templates_dict))

            assembled_entries.extend(
                self.__make_assembled_entries_for_dimensions(
                    app_metadata.get('dimensions'), tag_templates_dict))

            assembled_entries.extend(
                self.__make_assembled_entries_for_measures(
                    app_metadata.get('measures'), tag_templates_dict))

            assembled_entries.extend(
                self.__make_assembled_entries_for_visualizations(
                    app_metadata.get('visualizations'), tag_templates_dict))

            assembled_entries.extend(
                self.__make_assembled_entries_for_sheets(
                    app_metadata.get('sheets'), tag_templates_dict))

        return assembled_entries

    def __make_assembled_entry_for_app(self, app_metadata, tag_templates_dict):
        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_app(app_metadata)

        app_tag_template = tag_templates_dict.get(
            constants.TAG_TEMPLATE_ID_APP)

        tags = []
        if app_tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_app(
                    app_tag_template, app_metadata))

        tags.extend(
            self.__datacatalog_tag_factory.make_tags_for_custom_properties(
                tag_templates_dict, app_metadata.get('customProperties')))

        return prepare.AssembledEntryData(entry_id, entry, tags)

    def __make_assembled_entries_for_dimensions(self, dimensions_metadata,
                                                tag_templates_dict):

        return [
            self.__make_assembled_entry_for_dimension(dimension_metadata,
                                                      tag_templates_dict)
            for dimension_metadata in dimensions_metadata
        ] if dimensions_metadata else []

    def __make_assembled_entry_for_dimension(self, dimension_metadata,
                                             tag_templates_dict):

        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_dimension(
                dimension_metadata)

        tag_template = tag_templates_dict.get(
            constants.TAG_TEMPLATE_ID_DIMENSION)

        tags = []
        if tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_dimension(
                    tag_template, dimension_metadata))

        return prepare.AssembledEntryData(entry_id, entry, tags)

    def __make_assembled_entries_for_measures(self, measures_metadata,
                                              tag_templates_dict):

        return [
            self.__make_assembled_entry_for_measure(measure_metadata,
                                                    tag_templates_dict)
            for measure_metadata in measures_metadata
        ] if measures_metadata else []

    def __make_assembled_entry_for_measure(self, measure_metadata,
                                           tag_templates_dict):

        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_measure(
                measure_metadata)

        tag_template = tag_templates_dict.get(
            constants.TAG_TEMPLATE_ID_MEASURE)

        tags = []
        if tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_measure(
                    tag_template, measure_metadata))

        return prepare.AssembledEntryData(entry_id, entry, tags)

    def __make_assembled_entries_for_sheets(self, sheets_metadata,
                                            tag_templates_dict):

        return [
            self.__make_assembled_entry_for_sheet(sheet_metadata,
                                                  tag_templates_dict)
            for sheet_metadata in sheets_metadata
        ] if sheets_metadata else []

    def __make_assembled_entry_for_sheet(self, sheet_metadata,
                                         tag_templates_dict):

        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_sheet(
                sheet_metadata)

        tag_template = tag_templates_dict.get(constants.TAG_TEMPLATE_ID_SHEET)

        tags = []
        if tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_sheet(
                    tag_template, sheet_metadata))

        return prepare.AssembledEntryData(entry_id, entry, tags)

    def __make_assembled_entries_for_visualizations(self,
                                                    visualizations_metadata,
                                                    tag_templates_dict):

        return [
            self.__make_assembled_entry_for_visualization(
                visualization_metadata, tag_templates_dict)
            for visualization_metadata in visualizations_metadata
        ] if visualizations_metadata else []

    def __make_assembled_entry_for_visualization(self, visualization_metadata,
                                                 tag_templates_dict):

        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_visualization(
                visualization_metadata)

        tag_template = tag_templates_dict.get(
            constants.TAG_TEMPLATE_ID_VISUALIZATION)

        tags = []
        if tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_visualization(
                    tag_template, visualization_metadata))

        return prepare.AssembledEntryData(entry_id, entry, tags)
