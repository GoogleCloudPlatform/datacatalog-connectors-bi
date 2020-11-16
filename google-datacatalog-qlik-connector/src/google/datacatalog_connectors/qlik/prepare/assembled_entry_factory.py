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

    def make_assembled_entries_list(self, streams, tag_templates_dict):
        pass
