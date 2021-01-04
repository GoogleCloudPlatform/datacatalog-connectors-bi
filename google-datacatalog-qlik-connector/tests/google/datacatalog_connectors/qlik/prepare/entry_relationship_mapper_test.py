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

from google.datacatalog_connectors.qlik import prepare


class EntryRelationshipMapperTest(unittest.TestCase):

    def test_fulfill_tag_fields_should_resolve_app_custom_prop_def_mapping(
            self):

        definition_id = 'test_definition'
        definition_entry = self.__make_fake_entry(
            definition_id, 'custom_property_definition')
        definition_tag = self.__make_fake_tag(string_fields=(('id',
                                                              definition_id),))

        app_id = 'test_app'
        app_entry = self.__make_fake_entry(app_id, 'app')
        string_fields = ('id', app_id), ('property_definition_id',
                                         definition_id)
        app_tag = self.__make_fake_tag(string_fields=string_fields)

        definition_assembled_entry = commons_prepare.AssembledEntryData(
            definition_id, definition_entry, [definition_tag])
        app_assembled_entry = commons_prepare.AssembledEntryData(
            app_id, app_entry, [app_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [definition_assembled_entry, app_assembled_entry])

        self.assertEqual(
            'https://console.cloud.google.com/datacatalog/'
            'fake_entries/test_definition',
            app_tag.fields['property_definition_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_app_stream_mapping(self):
        stream_id = 'test_stream'
        stream_entry = self.__make_fake_entry(stream_id, 'stream')
        stream_tag = self.__make_fake_tag(string_fields=(('id', stream_id),))

        app_id = 'test_app'
        app_entry = self.__make_fake_entry(app_id, 'app')
        string_fields = ('id', app_id), ('stream_id', stream_id)
        app_tag = self.__make_fake_tag(string_fields=string_fields)

        stream_assembled_entry = commons_prepare.AssembledEntryData(
            stream_id, stream_entry, [stream_tag])
        app_assembled_entry = commons_prepare.AssembledEntryData(
            app_id, app_entry, [app_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [stream_assembled_entry, app_assembled_entry])

        self.assertEqual(
            'https://console.cloud.google.com/datacatalog/'
            'fake_entries/test_stream',
            app_tag.fields['stream_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_dimension_app_mapping(self):
        app_id = 'test_app'
        app_entry = self.__make_fake_entry(app_id, 'app')
        app_tag = self.__make_fake_tag(string_fields=(('id', app_id),))

        dimension_id = 'test_dimension'
        dimension_entry = self.__make_fake_entry(dimension_id, 'dimension')
        string_fields = ('id', dimension_id), ('app_id', app_id)
        dimension_tag = self.__make_fake_tag(string_fields=string_fields)

        app_assembled_entry = commons_prepare.AssembledEntryData(
            app_id, app_entry, [app_tag])
        sheet_assembled_entry = commons_prepare.AssembledEntryData(
            dimension_id, dimension_entry, [dimension_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [app_assembled_entry, sheet_assembled_entry])

        self.assertEqual(
            'https://console.cloud.google.com/datacatalog/'
            'fake_entries/test_app',
            dimension_tag.fields['app_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_sheet_app_mapping(self):
        app_id = 'test_app'
        app_entry = self.__make_fake_entry(app_id, 'app')
        app_tag = self.__make_fake_tag(string_fields=(('id', app_id),))

        sheet_id = 'test_sheet'
        sheet_entry = self.__make_fake_entry(sheet_id, 'sheet')
        string_fields = ('id', sheet_id), ('app_id', app_id)
        sheet_tag = self.__make_fake_tag(string_fields=string_fields)

        app_assembled_entry = commons_prepare.AssembledEntryData(
            app_id, app_entry, [app_tag])
        sheet_assembled_entry = commons_prepare.AssembledEntryData(
            sheet_id, sheet_entry, [sheet_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [app_assembled_entry, sheet_assembled_entry])

        self.assertEqual(
            'https://console.cloud.google.com/datacatalog/'
            'fake_entries/test_app',
            sheet_tag.fields['app_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_stream_custom_prop_def_mapping(
            self):

        definition_id = 'test_definition'
        definition_entry = self.__make_fake_entry(
            definition_id, 'custom_property_definition')
        definition_tag = self.__make_fake_tag(string_fields=(('id',
                                                              definition_id),))

        stream_id = 'test_stream'
        stream_entry = self.__make_fake_entry(stream_id, 'stream')
        string_fields = ('id', stream_id), ('property_definition_id',
                                            definition_id)
        stream_tag = self.__make_fake_tag(string_fields=string_fields)

        definition_assembled_entry = commons_prepare.AssembledEntryData(
            definition_id, definition_entry, [definition_tag])
        stream_assembled_entry = commons_prepare.AssembledEntryData(
            stream_id, stream_entry, [stream_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [definition_assembled_entry, stream_assembled_entry])

        self.assertEqual(
            'https://console.cloud.google.com/datacatalog/'
            'fake_entries/test_definition',
            stream_tag.fields['property_definition_entry'].string_value)

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
