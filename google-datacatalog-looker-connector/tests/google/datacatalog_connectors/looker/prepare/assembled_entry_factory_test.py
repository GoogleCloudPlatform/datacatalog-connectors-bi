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

import json
import unittest
from unittest import mock

from google.cloud.datacatalog import types
from looker_sdk import models
from looker_sdk.rtl import serialize

from google.datacatalog_connectors.looker import entities, prepare


class AssembledEntryFactoryTest(unittest.TestCase):
    __PREPARE_PACKAGE = 'google.datacatalog_connectors.looker.prepare'

    @mock.patch(f'{__PREPARE_PACKAGE}.datacatalog_tag_factory'
                f'.DataCatalogTagFactory')
    @mock.patch(f'{__PREPARE_PACKAGE}.datacatalog_entry_factory'
                f'.DataCatalogEntryFactory')
    def setUp(self, mock_entry_factory, mock_tag_factory):
        self.__assembled_data_factory = prepare.AssembledEntryFactory(
            'project-id', 'location-id', 'entry_group_id', 'user_system',
            'https://test.server.com')

        self.__entry_factory = mock_entry_factory.return_value
        self.__tag_factory = mock_tag_factory.return_value

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__assembled_data_factory.__dict__

        self.assertIsNotNone(
            attrs['_AssembledEntryFactory__datacatalog_entry_factory'])
        self.assertIsNotNone(
            attrs['_AssembledEntryFactory__datacatalog_tag_factory'])

    def test_make_assembled_entries_list_should_process_folders(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_folder = self.__mock_make_entry

        tag_factory = self.__tag_factory
        tag_factory.make_tag_for_folder = self.__mock_make_tag

        tag_templates_dict = {
            'looker_folder_metadata': {
                'name': 'tagTemplates/looker_folder_metadata',
            }
        }

        assembled_entries = \
            self.__assembled_data_factory.make_assembled_entries_list(
                [self.__make_fake_folder()], [], tag_templates_dict)

        self.assertEqual(1, len(assembled_entries))

        assembled_entry = assembled_entries[0]

        self.assertEqual('test_folder', assembled_entry.entry_id)
        self.assertEqual('fake_entries/test_folder',
                         assembled_entry.entry.name)

        tags = assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.assertEqual('tagTemplates/looker_folder_metadata',
                         tags[0].template)

    def test_make_assembled_entries_list_should_process_dashboards(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_folder = self.__mock_make_entry
        entry_factory.make_entry_for_dashboard = self.__mock_make_entry

        tag_factory = self.__tag_factory
        tag_factory.make_tag_for_dashboard = self.__mock_make_tag

        dashboard_data = {
            'id': 'test_dashboard',
        }
        dashboard = serialize.deserialize(json.dumps(dashboard_data),
                                          models.Dashboard)

        tag_templates_dict = {
            'looker_dashboard_metadata': {
                'name': 'tagTemplates/looker_dashboard_metadata',
            }
        }

        assembled_entries = \
            self.__assembled_data_factory.make_assembled_entries_list(
                [self.__make_fake_folder(dashboard=dashboard)], [],
                tag_templates_dict)

        self.assertEqual(2, len(assembled_entries))

        # The first entry refers to the folder.
        dashboard_assembled_entry = assembled_entries[1]

        self.assertEqual('test_dashboard', dashboard_assembled_entry.entry_id)
        self.assertEqual('fake_entries/test_dashboard',
                         dashboard_assembled_entry.entry.name)

        tags = dashboard_assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.assertEqual('tagTemplates/looker_dashboard_metadata',
                         tags[0].template)

    def test_make_assembled_entries_list_should_process_dashboard_tiles(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_folder = self.__mock_make_entry
        entry_factory.make_entry_for_dashboard = self.__mock_make_entry
        entry_factory.make_entry_for_dashboard_element = self.__mock_make_entry

        tag_factory = self.__tag_factory
        tag_factory.make_tag_for_dashboard_element = \
            self.__mock_make_tag_parent_dep

        dashboard_data = {
            'id': 'test_dashboard',
            'dashboard_elements': [{
                'id': 194,
            }],
        }
        dashboard = serialize.deserialize(json.dumps(dashboard_data),
                                          models.Dashboard)
        folder = self.__make_fake_folder()
        folder.dashboards = [dashboard]

        tag_templates_dict = {
            'looker_dashboard_element_metadata': {
                'name': 'tagTemplates/looker_dashboard_element_metadata',
            }
        }

        assembled_entries = \
            self.__assembled_data_factory.make_assembled_entries_list(
                [folder], [], tag_templates_dict)

        self.assertEqual(3, len(assembled_entries))

        # The first entry refers to the folder and the second to the dashboard.
        element_assembled_entry = assembled_entries[2]

        self.assertEqual('194', element_assembled_entry.entry_id)
        self.assertEqual('fake_entries/194',
                         element_assembled_entry.entry.name)

        tags = element_assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.assertEqual('tagTemplates/looker_dashboard_element_metadata',
                         tags[0].template)

    def test_make_assembled_entries_list_should_skip_empty_title(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_folder = self.__mock_make_entry
        entry_factory.make_entry_for_dashboard = self.__mock_make_entry
        entry_factory.make_entry_for_dashboard_element.return_value = \
            None, None

        dashboard_data = {
            'id': 'test_dashboard',
            'dashboard_elements': [{
                'id': 194,
            }],
        }
        dashboard = serialize.deserialize(json.dumps(dashboard_data),
                                          models.Dashboard)
        folder = self.__make_fake_folder()
        folder.dashboards = [dashboard]

        assembled_entries = \
            self.__assembled_data_factory.make_assembled_entries_list(
                [folder], [], {})

        # The first entry refers to the folder and the second to the dashboard.
        self.assertEqual(2, len(assembled_entries))

    def test_make_assembled_entries_list_should_process_looks(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_folder = self.__mock_make_entry
        entry_factory.make_entry_for_look = self.__mock_make_entry

        tag_factory = self.__tag_factory
        tag_factory.make_tag_for_look = self.__mock_make_tag

        look_data = {
            'id': 10,
        }
        look = serialize.deserialize(json.dumps(look_data), models.Look)

        tag_templates_dict = {
            'looker_look_metadata': {
                'name': 'tagTemplates/looker_look_metadata',
            }
        }

        assembled_entries = \
            self.__assembled_data_factory.make_assembled_entries_list(
                [self.__make_fake_folder(look=look)], [],
                tag_templates_dict)

        self.assertEqual(2, len(assembled_entries))

        # The first entry refers to the folder.
        dashboard_assembled_entry = assembled_entries[1]

        self.assertEqual(10, dashboard_assembled_entry.entry_id)
        self.assertEqual('fake_entries/10',
                         dashboard_assembled_entry.entry.name)

        tags = dashboard_assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.assertEqual('tagTemplates/looker_look_metadata', tags[0].template)

    def test_make_assembled_entries_list_should_process_queries(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_query = self.__mock_make_entry

        tag_factory = self.__tag_factory
        tag_factory.make_tag_for_query = self.__mock_make_tag

        tag_templates_dict = {
            'looker_query_metadata': {
                'name': 'tagTemplates/looker_query_metadata',
            }
        }

        query_metadata = entities.AssembledQueryMetadata(
            self.__make_fake_query(), 'select *', {}, {})

        assembled_entries = \
            self.__assembled_data_factory.make_assembled_entries_list(
                [], [query_metadata], tag_templates_dict)

        self.assertEqual(1, len(assembled_entries))

        assembled_entry = assembled_entries[0]

        self.assertEqual(837, assembled_entry.entry_id)
        self.assertEqual('fake_entries/837', assembled_entry.entry.name)

        tags = assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.assertEqual('tagTemplates/looker_query_metadata',
                         tags[0].template)

    @classmethod
    def __make_fake_folder(cls, dashboard=None, look=None):
        dashboard_data = json.loads(
            serialize.serialize(dashboard)) if dashboard else None
        look_data = json.loads(serialize.serialize(look)) if look else None
        folder_data = {
            'id': 'test_folder',
            'name': 'Test folder',
            'parent_id': '',
            'dashboards': [dashboard_data] if dashboard_data else None,
            'looks': [look_data] if look_data else None,
        }
        return serialize.deserialize(json.dumps(folder_data), models.Folder)

    @classmethod
    def __make_fake_query(cls):
        query_data = {
            'id': 837,
            'model': '',
            'view': '',
        }
        return serialize.deserialize(json.dumps(query_data), models.Query)

    @classmethod
    def __mock_make_entry(cls, asset):
        entry = types.Entry()
        entry_id = asset.id
        entry.name = f'fake_entries/{entry_id}'
        return entry_id, entry

    @classmethod
    def __mock_make_tag(cls, tag_template_dict, asset):
        tag = types.Tag()
        tag.template = tag_template_dict['name']
        return tag

    @classmethod
    def __mock_make_tag_parent_dep(cls, tag_template_dict, asset, parent):
        tag = types.Tag()
        tag.template = tag_template_dict['name']
        return tag
