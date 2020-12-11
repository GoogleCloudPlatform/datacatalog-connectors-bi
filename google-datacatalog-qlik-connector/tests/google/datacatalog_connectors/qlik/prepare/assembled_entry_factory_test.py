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

    def test_make_assembled_entry_for_custom_property_def_should_process_def(
            self):

        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_custom_property_definition\
            .return_value = ('id', {})

        tag_templates_dict = {
            'qlik_custom_property_definition_metadata': {
                'name': 'tagTemplates/qlik_custom_property_def_metadata',
            }
        }

        assembled_entry = \
            self.__factory.make_assembled_entry_for_custom_property_def(
                {'id': 'test_definition'}, tag_templates_dict)

        entry_factory.make_entry_for_custom_property_definition\
            .assert_called_once()

        tags = assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.__tag_factory.make_tag_for_custom_property_defintion\
            .assert_called_once()

    def test_make_assembled_entries_for_stream_should_process_stream(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_stream.return_value = ('id', {})

        tag_templates_dict = {
            'qlik_stream_metadata': {
                'name': 'tagTemplates/qlik_stream_metadata',
            }
        }

        assembled_entries = \
            self.__factory.make_assembled_entries_for_stream(
                self.__make_fake_stream(), tag_templates_dict)

        self.assertEqual(1, len(assembled_entries))
        entry_factory.make_entry_for_stream.assert_called_once()

        stream_assembled_entry = assembled_entries[0]
        tags = stream_assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.__tag_factory.make_tag_for_stream.assert_called_once()

    def test_make_assembled_entries_for_stream_should_process_apps(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_stream.return_value = ('id', {})
        entry_factory.make_entry_for_app.return_value = ('id', {})

        tag_templates_dict = {
            'qlik_app_metadata': {
                'name': 'tagTemplates/qlik_app_metadata',
            }
        }

        fake_stream = self.__make_fake_stream()
        fake_stream['apps'].append(self.__make_fake_app())
        assembled_entries = \
            self.__factory.make_assembled_entries_for_stream(
                fake_stream, tag_templates_dict)

        self.assertEqual(2, len(assembled_entries))
        entry_factory.make_entry_for_stream.assert_called_once()

        app_assembled_entry = assembled_entries[1]
        tags = app_assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.__tag_factory.make_tag_for_app.assert_called_once()

    def test_make_assembled_entries_for_stream_should_process_sheets(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_stream.return_value = ('id', {})
        entry_factory.make_entry_for_app.return_value = ('id', {})
        entry_factory.make_entry_for_sheet.return_value = ('id', {})

        tag_templates_dict = {
            'qlik_sheet_metadata': {
                'name': 'tagTemplates/qlik_sheet_metadata',
            }
        }

        fake_stream = self.__make_fake_stream()
        fake_app = self.__make_fake_app()
        fake_app['sheets'].append(self.__make_fake_sheet())
        fake_stream['apps'].append(fake_app)
        assembled_entries = \
            self.__factory.make_assembled_entries_for_stream(
                fake_stream, tag_templates_dict)

        self.assertEqual(3, len(assembled_entries))
        entry_factory.make_entry_for_sheet.assert_called_once()

        sheet_assembled_entry = assembled_entries[2]
        tags = sheet_assembled_entry.tags

        self.assertEqual(1, len(tags))
        self.__tag_factory.make_tag_for_sheet.assert_called_once()

    @classmethod
    def __make_fake_stream(cls):
        return {
            'id': 'test_stream',
            'name': 'Test stream',
            'apps': [],
        }

    @classmethod
    def __make_fake_app(cls):
        return {
            'id': 'test_app',
            'name': 'Test app',
            'sheets': [],
        }

    @classmethod
    def __make_fake_sheet(cls):
        return {
            'qInfo': {
                'qId': 'test_sheet',
            },
            'qMeta': {
                'title': 'Test sheet',
            },
        }
