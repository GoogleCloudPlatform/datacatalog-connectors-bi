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

import unittest
from unittest import mock

from google.cloud import datacatalog
from google.datacatalog_connectors.commons import prepare as commons_prepare

from google.datacatalog_connectors.sisense import prepare


class AssembledEntryFactoryTest(unittest.TestCase):
    __PREPARE_PACKAGE = 'google.datacatalog_connectors.sisense.prepare'
    __FACTORY_MODULE = f'{__PREPARE_PACKAGE}.assembled_entry_factory'
    __FACTORY_CLASS = f'{__FACTORY_MODULE}.AssembledEntryFactory'
    __PRIVATE_METHOD_PREFIX = f'{__FACTORY_CLASS}._AssembledEntryFactory'

    @mock.patch(f'{__PREPARE_PACKAGE}.datacatalog_tag_factory'
                f'.DataCatalogTagFactory')
    @mock.patch(f'{__FACTORY_MODULE}.datacatalog_entry_factory'
                f'.DataCatalogEntryFactory')
    def setUp(self, mock_entry_factory, mock_tag_factory):
        self.__factory = prepare.AssembledEntryFactory(
            project_id='test-project',
            location_id='test-location',
            entry_group_id='test-entry-group',
            user_specified_system='test-system',
            server_address='https://test.server.com')

        self.__entry_factory = mock_entry_factory.return_value
        self.__tag_factory = mock_tag_factory.return_value

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__factory.__dict__

        self.assertEqual(
            self.__entry_factory,
            attrs['_AssembledEntryFactory__datacatalog_entry_factory'])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}'
                f'__make_assembled_entries_for_folder')
    def test_make_assembled_entries_list_should_process_folders(
            self, mock_make_assembled_entries_for_folder):

        folder = self.__make_fake_folder()

        mock_make_assembled_entries_for_folder.return_value = \
            [commons_prepare.AssembledEntryData('test-folder', {})]

        assembled_entries = self.__factory.make_assembled_entries_list(
            folder, {})

        self.assertEqual(1, len(assembled_entries))
        mock_make_assembled_entries_for_folder.assert_called_once()

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_assembled_entry_for_folder')
    def test_make_assembled_entries_for_folder_should_process_folder(
            self, mock_make_assembled_entry_for_folder):

        folder = self.__make_fake_folder()

        tag_template = datacatalog.TagTemplate()
        tag_templates_dict = {'sisense_folder_metadata': tag_template}

        mock_make_assembled_entry_for_folder.return_value = \
            commons_prepare.AssembledEntryData('test-folder', {})

        assembled_entries = self.__factory\
            ._AssembledEntryFactory__make_assembled_entries_for_folder(
                folder, tag_templates_dict)

        self.assertEqual(1, len(assembled_entries))
        mock_make_assembled_entry_for_folder.assert_called_once_with(
            folder, tag_template)

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_assembled_entry_for_folder')
    def test_make_assembled_entries_for_folder_should_process_child_folders(
            self, mock_make_assembled_entry_for_folder):

        folder = self.__make_fake_folder_with_children()

        tag_template = datacatalog.TagTemplate()
        tag_templates_dict = {'sisense_folder_metadata': tag_template}

        mock_make_assembled_entry_for_folder.side_effect = [
            commons_prepare.AssembledEntryData('test-parent-folder', {}),
            commons_prepare.AssembledEntryData('test-folder', {}),
        ]

        assembled_entries = self.__factory\
            ._AssembledEntryFactory__make_assembled_entries_for_folder(
                folder, tag_templates_dict)

        self.assertEqual(2, len(assembled_entries))
        self.assertEqual(2, mock_make_assembled_entry_for_folder.call_count)

    def test_make_assembled_entry_for_folder_should_make_entry_and_tags(self):
        folder = self.__make_fake_folder()
        tag_template = datacatalog.TagTemplate()
        tag_template.name = 'tagTemplates/sisense_folder_metadata'

        fake_entry = ('test-folder', {})
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_folder.return_value = fake_entry

        fake_tag = datacatalog.Tag()
        fake_tag.template = 'tagTemplates/sisense_folder_metadata'
        tag_factory = self.__tag_factory
        tag_factory.make_tag_for_folder.return_value = fake_tag

        assembled_entry = self.__factory\
            ._AssembledEntryFactory__make_assembled_entry_for_folder(
                folder, tag_template)

        self.assertEqual('test-folder', assembled_entry.entry_id)
        self.assertEqual({}, assembled_entry.entry)
        entry_factory.make_entry_for_folder.assert_called_once_with(folder)

        tags = assembled_entry.tags
        self.assertEqual(1, len(tags))
        self.assertEqual('tagTemplates/sisense_folder_metadata',
                         tags[0].template)
        tag_factory.make_tag_for_folder.assert_called_once_with(
            tag_template, folder)

    @classmethod
    def __make_fake_folder(cls):
        return {
            'oid': 'test-folder',
            'type': 'folder',
            'name': 'Test folder',
        }

    @classmethod
    def __make_fake_folder_with_children(cls):
        return {
            'oid': 'test-parent-folder',
            'type': 'folder',
            'name': 'Test parent folder',
            'folders': [cls.__make_fake_folder()]
        }
