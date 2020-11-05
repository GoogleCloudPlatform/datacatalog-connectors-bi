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

from google.cloud import datacatalog

from google.datacatalog_connectors.commons import \
    prepare as commons_prepare
from google.datacatalog_connectors.looker import prepare


class EntryRelationshipMapperTest(unittest.TestCase):

    def test_fulfill_tag_fields_should_resolve_dashboard_folder_mapping(self):
        folder_id = 'test_folder'
        folder_entry = self.__make_fake_entry(folder_id, 'folder')
        folder_tag = self.__make_fake_tag(string_fields=(('id', folder_id),))

        dashboard_id = 'test_dashboard'
        dashboard_entry = self.__make_fake_entry(dashboard_id, 'dashboard')
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

    def test_fulfill_tag_fields_should_resolve_element_dashboard_mapping(self):
        dashboard_id = 'test_dashboard'
        dashboard_entry = self.__make_fake_entry(dashboard_id, 'dashboard')
        string_fields = ('id', dashboard_id),
        dashboard_tag = self.__make_fake_tag(string_fields=string_fields)

        element_id = 'test_element'
        element_entry = self.__make_fake_entry(element_id, 'dashboard_element')
        string_fields = ('id', element_id), ('dashboard_id', dashboard_id)
        element_tag = self.__make_fake_tag(string_fields=string_fields)

        dashboard_assembled_entry = commons_prepare.AssembledEntryData(
            dashboard_id, dashboard_entry, [dashboard_tag])
        element_assembled_entry = commons_prepare.AssembledEntryData(
            element_id, element_entry, [element_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [dashboard_assembled_entry, element_assembled_entry])

        self.assertEqual(
            f'https://console.cloud.google.com/datacatalog/'
            f'{dashboard_entry.name}',
            element_tag.fields['dashboard_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_element_look_mapping(self):
        look_id = 123
        look_entry = self.__make_fake_entry(look_id, 'look')
        look_tag = self.__make_fake_tag(double_fields=(('id', look_id),))

        element_id = 'test_element'
        element_entry = self.__make_fake_entry(element_id, 'dashboard_element')
        double_fields = ('look_id', look_id),
        element_tag = self.__make_fake_tag(string_fields=(('id', element_id),),
                                           double_fields=double_fields)

        look_assembled_entry = commons_prepare.AssembledEntryData(
            look_id, look_entry, [look_tag])
        element_assembled_entry = commons_prepare.AssembledEntryData(
            element_id, element_entry, [element_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [look_assembled_entry, element_assembled_entry])

        self.assertEqual(
            f'https://console.cloud.google.com/datacatalog/{look_entry.name}',
            element_tag.fields['look_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_element_query_mapping(self):
        query_id = 1080
        query_entry = self.__make_fake_entry(query_id, 'query')
        query_tag = self.__make_fake_tag(double_fields=(('id', query_id),))

        element_id = 'test_element'
        element_entry = self.__make_fake_entry(element_id, 'dashboard_element')
        double_fields = ('query_id', query_id),
        element_tag = self.__make_fake_tag(string_fields=(('id', element_id),),
                                           double_fields=double_fields)

        query_assembled_entry = commons_prepare.AssembledEntryData(
            query_id, query_entry, [query_tag])
        element_assembled_entry = commons_prepare.AssembledEntryData(
            element_id, element_entry, [element_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [query_assembled_entry, element_assembled_entry])

        self.assertEqual(
            f'https://console.cloud.google.com/datacatalog/'
            f'{query_entry.name}',
            element_tag.fields['query_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_folder_parent_mapping(self):
        parent_id = 'test_parent'
        parent_entry = self.__make_fake_entry(parent_id, 'folder')
        parent_tag = self.__make_fake_tag(string_fields=(('id', parent_id),))

        folder_id = 'test_folder'
        folder_entry = self.__make_fake_entry(folder_id, 'folder')
        string_fields = ('id', folder_id), ('parent_id', parent_id)
        folder_tag = self.__make_fake_tag(string_fields=string_fields)

        parent_assembled_entry = commons_prepare.AssembledEntryData(
            parent_id, parent_entry, [parent_tag])
        folder_assembled_entry = commons_prepare.AssembledEntryData(
            folder_id, folder_entry, [folder_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [parent_assembled_entry, folder_assembled_entry])

        self.assertEqual(
            f'https://console.cloud.google.com/datacatalog/'
            f'{parent_entry.name}',
            folder_tag.fields['parent_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_look_folder_mapping(self):
        folder_id = 'test_folder'
        folder_entry = self.__make_fake_entry(folder_id, 'folder')
        folder_tag = self.__make_fake_tag(string_fields=(('id', folder_id),))

        look_id = 123
        look_entry = self.__make_fake_entry(look_id, 'look')
        string_fields = ('folder_id', folder_id),
        look_tag = self.__make_fake_tag(string_fields=string_fields,
                                        double_fields=(('id', look_id),))

        folder_assembled_entry = commons_prepare.AssembledEntryData(
            folder_id, folder_entry, [folder_tag])
        look_assembled_entry = commons_prepare.AssembledEntryData(
            look_id, look_entry, [look_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [folder_assembled_entry, look_assembled_entry])

        self.assertEqual(
            f'https://console.cloud.google.com/datacatalog/'
            f'{folder_entry.name}',
            look_tag.fields['folder_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_look_query_mapping(self):
        query_id = 1080
        query_entry = self.__make_fake_entry(query_id, 'query')
        query_tag = self.__make_fake_tag(double_fields=(('id', query_id),))

        look_id = 123
        look_entry = self.__make_fake_entry(look_id, 'look')
        double_fields = ('id', look_id), ('query_id', query_id)
        look_tag = self.__make_fake_tag(double_fields=double_fields)

        query_assembled_entry = commons_prepare.AssembledEntryData(
            query_id, query_entry, [query_tag])
        look_assembled_entry = commons_prepare.AssembledEntryData(
            look_id, look_entry, [look_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [look_assembled_entry, query_assembled_entry])

        self.assertEqual(
            f'https://console.cloud.google.com/datacatalog/'
            f'{query_entry.name}', look_tag.fields['query_entry'].string_value)

    @classmethod
    def __make_fake_entry(cls, entry_id, entry_type):
        entry = datacatalog.Entry()
        entry.name = f'fake_entries/{entry_id}'
        entry.user_specified_type = entry_type
        return entry

    @classmethod
    def __make_fake_tag(cls, string_fields=None, double_fields=None):
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
