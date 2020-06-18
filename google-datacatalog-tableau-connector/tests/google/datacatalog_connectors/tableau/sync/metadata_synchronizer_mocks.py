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
from google.datacatalog_connectors.tableau.sync import metadata_synchronizer


class DictWithAttributeAccess(dict):

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        self[key] = value


class FakeSynchronizer(metadata_synchronizer.MetadataSynchronizer):

    def __init__(self):
        super().__init__(tableau_server_address='test-server',
                         tableau_api_version='test-api-version',
                         tableau_username='test-api-username',
                         tableau_password='test-api-password',
                         tableau_site='test-site',
                         datacatalog_project_id='test-project-id',
                         datacatalog_location_id='test-location-id',
                         assets_type=['test-type'])

    def _scrape_source_system_metadata(self, query_filter=None):
        return []

    def _make_assembled_entries(self, source_system_metadata,
                                tag_templates_dict):

        return [(prepare.AssembledEntryData('test-entry-id-1', {}, []), [])]

    def _make_tag_templates_dict(self):
        tag_template = DictWithAttributeAccess()
        tag_template.name = 'tag_template_name'
        return {'some_tag_template': ('id', tag_template)}


def make_fake_entry_group():
    entry_group = DictWithAttributeAccess()
    entry_group.name = 'test-entry-group'
    return entry_group
