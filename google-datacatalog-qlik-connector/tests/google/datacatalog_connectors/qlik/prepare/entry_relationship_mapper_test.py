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

from google.cloud import datacatalog
from google.datacatalog_connectors.commons import \
    prepare as commons_prepare

from google.datacatalog_connectors.qlik import prepare


class EntryRelationshipMapperTest(unittest.TestCase):

    def test_fulfill_tag_fields_should_resolve_app_custom_prop_def_mapping(
            self):

        definition_id = 'test-definition'
        definition_entry = self.__make_fake_entry(
            definition_id, 'custom_property_definition')
        definition_tag = self.__make_fake_tag(string_fields=(('id',
                                                              definition_id),))

        app_id = 'test-app'
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
            'fake_entries/test-definition',
            app_tag.fields['property_definition_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_app_stream_mapping(self):
        stream_id = 'test-stream'
        stream_entry = self.__make_fake_entry(stream_id, 'stream')
        stream_tag = self.__make_fake_tag(string_fields=(('id', stream_id),))

        app_id = 'test-app'
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
            'fake_entries/test-stream',
            app_tag.fields['stream_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_dimension_app_mapping(self):
        app_id = 'test-app'
        app_entry = self.__make_fake_entry(app_id, 'app')
        app_tag = self.__make_fake_tag(string_fields=(('id', app_id),))

        dimension_id = 'test-dimension'
        dimension_entry = self.__make_fake_entry(dimension_id, 'dimension')
        string_fields = ('id', dimension_id), ('app_id', app_id)
        dimension_tag = self.__make_fake_tag(string_fields=string_fields)

        app_assembled_entry = commons_prepare.AssembledEntryData(
            app_id, app_entry, [app_tag])
        dimension_assembled_entry = commons_prepare.AssembledEntryData(
            dimension_id, dimension_entry, [dimension_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [app_assembled_entry, dimension_assembled_entry])

        self.assertEqual(
            'https://console.cloud.google.com/datacatalog/'
            'fake_entries/test-app',
            dimension_tag.fields['app_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_measure_app_mapping(self):
        app_id = 'test-app'
        app_entry = self.__make_fake_entry(app_id, 'app')
        app_tag = self.__make_fake_tag(string_fields=(('id', app_id),))

        measure_id = 'test-measure'
        measure_entry = self.__make_fake_entry(measure_id, 'measure')
        string_fields = ('id', measure_id), ('app_id', app_id)
        measure_tag = self.__make_fake_tag(string_fields=string_fields)

        app_assembled_entry = commons_prepare.AssembledEntryData(
            app_id, app_entry, [app_tag])
        measure_assembled_entry = commons_prepare.AssembledEntryData(
            measure_id, measure_entry, [measure_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [app_assembled_entry, measure_assembled_entry])

        self.assertEqual(
            'https://console.cloud.google.com/datacatalog/'
            'fake_entries/test-app',
            measure_tag.fields['app_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_sheet_app_mapping(self):
        app_id = 'test-app'
        app_entry = self.__make_fake_entry(app_id, 'app')
        app_tag = self.__make_fake_tag(string_fields=(('id', app_id),))

        sheet_id = 'test-sheet'
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
            'fake_entries/test-app',
            sheet_tag.fields['app_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_stream_custom_prop_def_mapping(
            self):

        definition_id = 'test-definition'
        definition_entry = self.__make_fake_entry(
            definition_id, 'custom_property_definition')
        definition_tag = self.__make_fake_tag(string_fields=(('id',
                                                              definition_id),))

        stream_id = 'test-stream'
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
            'fake_entries/test-definition',
            stream_tag.fields['property_definition_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_visualization_app_mapping(self):
        app_id = 'test-app'
        app_entry = self.__make_fake_entry(app_id, 'app')
        app_tag = self.__make_fake_tag(string_fields=(('id', app_id),))

        visualization_id = 'test-measure'
        visualization_entry = self.__make_fake_entry(visualization_id,
                                                     'visualization')
        string_fields = ('id', visualization_id), ('app_id', app_id)
        visualization_tag = self.__make_fake_tag(string_fields=string_fields)

        app_assembled_entry = commons_prepare.AssembledEntryData(
            app_id, app_entry, [app_tag])
        measure_assembled_entry = commons_prepare.AssembledEntryData(
            visualization_id, visualization_entry, [visualization_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [app_assembled_entry, measure_assembled_entry])

        self.assertEqual(
            'https://console.cloud.google.com/datacatalog/'
            'fake_entries/test-app',
            visualization_tag.fields['app_entry'].string_value)

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
