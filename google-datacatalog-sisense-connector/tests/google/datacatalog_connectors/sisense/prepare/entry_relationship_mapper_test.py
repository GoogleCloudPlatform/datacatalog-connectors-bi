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
from google.cloud.datacatalog import Entry, Tag
from google.datacatalog_connectors.commons import \
    prepare as commons_prepare

from google.datacatalog_connectors.sisense import prepare


class EntryRelationshipMapperTest(unittest.TestCase):
    __PREPARE_PACKAGE = 'google.datacatalog_connectors.sisense.prepare'
    __MAPPER_MODULE = f'{__PREPARE_PACKAGE}.entry_relationship_mapper'
    __MAPPER_CLASS = f'{__MAPPER_MODULE}.EntryRelationshipMapper'

    def test_fulfill_tag_fields_should_resolve_dashboard_folder_mapping(self):
        folder_id = 'parent-folder'
        folder_entry = self.__make_fake_entry(folder_id, 'Folder')
        folder_tag = self.__make_fake_tag(string_fields=(('id', folder_id),))

        dashboard_id = 'test-dashboard'
        dashboard_entry = self.__make_fake_entry(dashboard_id, 'Dashboard')
        string_fields = ('id', dashboard_id), ('folder_id', folder_id)
        dashboard_tag = self.__make_fake_tag(string_fields=string_fields)

        folder_assembled_entry = commons_prepare.AssembledEntryData(
            folder_id, folder_entry, [folder_tag])
        dashboard_assembled_entry = commons_prepare.AssembledEntryData(
            dashboard_id, dashboard_entry, [dashboard_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [folder_assembled_entry, dashboard_assembled_entry])

        self.assertEqual(
            f'https://console.cloud.google.com/datacatalog/'
            f'{folder_entry.name}',
            dashboard_tag.fields['folder_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_parent_folder_mapping(self):
        parent_folder_id = 'parent-folder'
        parent_folder_entry = self.__make_fake_entry(parent_folder_id,
                                                     'Folder')
        parent_folder_tag = self.__make_fake_tag(
            string_fields=(('id', parent_folder_id),))

        folder_id = 'test-folder'
        folder_entry = self.__make_fake_entry(folder_id, 'Folder')
        string_fields = ('id', folder_id), ('parent_id', parent_folder_id)
        folder_tag = self.__make_fake_tag(string_fields=string_fields)

        parent_folder_assembled_entry = commons_prepare.AssembledEntryData(
            parent_folder_id, parent_folder_entry, [parent_folder_tag])
        folder_assembled_entry = commons_prepare.AssembledEntryData(
            folder_id, folder_entry, [folder_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [parent_folder_assembled_entry, folder_assembled_entry])

        self.assertEqual(
            f'https://console.cloud.google.com/datacatalog/'
            f'{parent_folder_entry.name}',
            folder_tag.fields['parent_folder_entry'].string_value)

    @mock.patch(f'{__MAPPER_CLASS}._map_related_entry')
    def test_resolve_folder_mappings_should_skip_non_folder_entries(
            self, mock_map_related_entry):

        entry_id = 'test'
        entry = self.__make_fake_entry(entry_id, 'not-a-folder')
        tag = self.__make_fake_tag(string_fields=(('id', entry_id),))

        assembled_entry = commons_prepare.AssembledEntryData(
            entry_id, entry, [tag])

        prepare.EntryRelationshipMapper()\
            ._EntryRelationshipMapper__resolve_folder_mappings(
                [assembled_entry], {})

        mock_map_related_entry.assert_not_called()

    @classmethod
    def __make_fake_entry(cls, entry_id, entry_type) -> Entry:
        entry = datacatalog.Entry()
        entry.name = f'fake_entries/{entry_id}'
        entry.user_specified_type = entry_type
        return entry

    @classmethod
    def __make_fake_tag(cls, string_fields=None, double_fields=None) -> Tag:
        tag = datacatalog.Tag()

        if string_fields:
            for string_field in string_fields:
                tag_field = datacatalog.TagField()
                tag_field.string_value = string_field[1]
                tag.fields[string_field[0]] = tag_field

        if double_fields:
            for double_field in double_fields:
                tag_field = datacatalog.TagField()
                tag_field.double_value = double_field[1]
                tag.fields[double_field[0]] = tag_field

        return tag
