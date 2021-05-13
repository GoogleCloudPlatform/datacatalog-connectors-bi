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

from google.datacatalog_connectors.sisense import prepare


class AssembledEntryFactoryTest(unittest.TestCase):
    __PREPARE_PACKAGE = 'google.datacatalog_connectors.sisense.prepare'
    __FACTORY_MODULE = f'{__PREPARE_PACKAGE}.assembled_entry_factory'
    __FACTORY_CLASS = f'{__FACTORY_MODULE}.AssembledEntryFactory'
    __PRIVATE_METHOD_PREFIX = f'{__FACTORY_CLASS}._AssembledEntryFactory'

    @mock.patch(f'{__FACTORY_MODULE}.datacatalog_entry_factory'
                f'.DataCatalogEntryFactory')
    def setUp(self, mock_entry_factory):
        self.__factory = prepare.AssembledEntryFactory(
            project_id='test-project',
            location_id='test-location',
            entry_group_id='test-entry-group',
            user_specified_system='test-system',
            server_address='https://test.server.com')

        self.__entry_factory = mock_entry_factory.return_value

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__factory.__dict__

        self.assertEqual(
            self.__entry_factory,
            attrs['_AssembledEntryFactory__datacatalog_entry_factory'])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_assembled_entry_for_folder')
    def test_make_assembled_entries_for_folder_should_process_folder(
            self, make_assembled_entry_for_folder):

        make_assembled_entry_for_folder.return_value = ('id', {})

        assembled_entries = self.__factory\
            ._AssembledEntryFactory__make_assembled_entries_for_folder(
                self.__make_fake_folder())

        self.assertEqual(1, len(assembled_entries))
        make_assembled_entry_for_folder.assert_called_once()

    def test_make_assembled_entry_for_folder_should_process_folder(self):
        fake_entry = ('id', {})

        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_folder.return_value = fake_entry

        assembled_entry = self.__factory\
            ._AssembledEntryFactory__make_assembled_entry_for_folder(
                self.__make_fake_folder())

        self.assertEqual('id', assembled_entry.entry_id)
        entry_factory.make_entry_for_folder.assert_called_once()

    @classmethod
    def __make_fake_folder(cls):
        return {
            '_id': 'test-folder',
            'name': 'Test folder',
        }
