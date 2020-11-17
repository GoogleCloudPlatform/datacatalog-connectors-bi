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

from . import constants, datacatalog_entry_factory, datacatalog_tag_factory


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

    def make_assembled_entries_list(self, stream_metadata, tag_templates_dict):
        stream_tag_template = self.__get_tag_template(
            constants.TAG_TEMPLATE_ID_STREAM, tag_templates_dict)

        assembled_entries = [
            self.__make_assembled_entry_for_stream(stream_metadata,
                                                   stream_tag_template)
        ]

        return assembled_entries

    @classmethod
    def __get_tag_template(cls, tag_template_id, tag_templates_dict):
        return tag_templates_dict[tag_template_id] \
            if tag_template_id in tag_templates_dict else None

    def __make_assembled_entry_for_stream(self, stream_metadata, tag_template):
        entry_id, entry = \
            self.__datacatalog_entry_factory.make_entry_for_stream(
                stream_metadata)

        tags = []
        if tag_template:
            tags.append(
                self.__datacatalog_tag_factory.make_tag_for_stream(
                    tag_template, stream_metadata))

        return prepare.AssembledEntryData(entry_id, entry, tags)
