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

import unittest
from unittest import mock

from google.cloud import datacatalog

from google.datacatalog_connectors.qlik import prepare


class AssembledEntryFactoryTest(unittest.TestCase):
    __PREPARE_PACKAGE = 'google.datacatalog_connectors.qlik.prepare'

    @mock.patch(f'{__PREPARE_PACKAGE}.datacatalog_tag_factory'
                f'.DataCatalogTagFactory')
    @mock.patch(f'{__PREPARE_PACKAGE}.datacatalog_entry_factory'
                f'.DataCatalogEntryFactory')
    def setUp(self, mock_entry_factory, mock_tag_factory):
        self.__factory = prepare.AssembledEntryFactory(
            project_id='test-project',
            location_id='test-location',
            entry_group_id='test-entry-group',
            user_specified_system='test-system',
            site_url='https://test.server.com')

        self.__entry_factory = mock_entry_factory.return_value
        self.__tag_factory = mock_tag_factory.return_value

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__factory.__dict__

        self.assertEqual(
            self.__entry_factory,
            attrs['_AssembledEntryFactory__datacatalog_entry_factory'])

    def test_make_assembled_entries_list_should_process_streams(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_stream = self.__mock_make_entry

        tag_factory = self.__tag_factory
        tag_factory.make_tag_for_stream = self.__mock_make_tag

        tag_templates_dict = {
            'qlik_stream_metadata': {
                'name': 'tagTemplates/qlik_stream_metadata',
            }
        }

        assembled_entries = \
            self.__factory.make_assembled_entries_list(
                self.__make_fake_stream(), tag_templates_dict)

        self.assertEqual(1, len(assembled_entries))

        stream_assembled_entry = assembled_entries[0]

        self.assertEqual('test_stream', stream_assembled_entry.entry_id)
        self.assertEqual('fake_entries/test_stream',
                         stream_assembled_entry.entry.name)

        tags = stream_assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.assertEqual('tagTemplates/qlik_stream_metadata', tags[0].template)

    def test_make_assembled_entries_list_should_process_apps(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_stream = self.__mock_make_entry
        entry_factory.make_entry_for_app = self.__mock_make_entry

        tag_factory = self.__tag_factory
        tag_factory.make_tag_for_app = self.__mock_make_tag

        tag_templates_dict = {
            'qlik_app_metadata': {
                'name': 'tagTemplates/qlik_app_metadata',
            }
        }

        assembled_entries = \
            self.__factory.make_assembled_entries_list(
                self.__make_fake_stream_with_apps(), tag_templates_dict)

        self.assertEqual(2, len(assembled_entries))

        app_assembled_entry = assembled_entries[1]

        self.assertEqual('test_app', app_assembled_entry.entry_id)
        self.assertEqual('fake_entries/test_app',
                         app_assembled_entry.entry.name)

        tags = app_assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.assertEqual('tagTemplates/qlik_app_metadata', tags[0].template)

    @classmethod
    def __make_fake_stream(cls):
        return {
            'id': 'test_stream',
            'name': 'Test stream',
        }

    @classmethod
    def __make_fake_stream_with_apps(cls):
        return {
            'id': 'test_stream',
            'name': 'Test stream',
            'apps': [{
                'id': 'test_app',
                'name': 'Test app',
            }]
        }

    @classmethod
    def __mock_make_entry(cls, asset):
        entry = datacatalog.Entry()
        entry_id = asset.get('id')
        entry.name = f'fake_entries/{entry_id}'
        return entry_id, entry

    @classmethod
    def __mock_make_tag(cls, tag_template_dict, asset):
        tag = datacatalog.Tag()
        tag.template = tag_template_dict['name']
        return tag
