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

from google.cloud.datacatalog import types

from google.datacatalog_connectors.commons import \
    prepare as commons_prepare
from google.datacatalog_connectors.tableau import prepare


class EntryRelationshipMapperTest(unittest.TestCase):

    def test_fulfill_tag_fields_should_resolve_dashboard_workbook_mapping(
            self):  # noqa: E125
        workbook_luid = 'test_workbook'
        workbook_entry = self.__make_fake_entry(workbook_luid, 'workbook')
        workbook_tag = self.__make_fake_tag(string_fields=(('luid',
                                                            workbook_luid),))

        dashboard_luid = 'test_dashboard'
        dashboard_entry = self.__make_fake_entry(dashboard_luid, 'dashboard')
        string_fields = ('luid', dashboard_luid), ('workbook_luid',
                                                   workbook_luid)
        dashboard_tag = self.__make_fake_tag(string_fields=string_fields)

        workbook_assembled_entry = commons_prepare.AssembledEntryData(
            workbook_luid, workbook_entry, [workbook_tag])
        dashboard_assembled_entry = commons_prepare.AssembledEntryData(
            dashboard_luid, dashboard_entry, [dashboard_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [workbook_assembled_entry, dashboard_assembled_entry])

        self.assertEqual(
            f'https://console.cloud.google.com/datacatalog/'
            f'{workbook_entry.name}',
            dashboard_tag.fields['workbook_entry'].string_value)

    def test_fulfill_tag_fields_should_resolve_sheet_workbook_mapping(self):
        workbook_luid = 'test_workbook'
        workbook_entry = self.__make_fake_entry(workbook_luid, 'workbook')
        workbook_tag = self.__make_fake_tag(string_fields=(('luid',
                                                            workbook_luid),))

        sheet_luid = 'test_dashboard'
        sheet_entry = self.__make_fake_entry(sheet_luid, 'sheet')
        string_fields = ('luid', sheet_luid), ('workbook_luid', workbook_luid)
        sheet_tag = self.__make_fake_tag(string_fields=string_fields)

        workbook_assembled_entry = commons_prepare.AssembledEntryData(
            workbook_luid, workbook_entry, [workbook_tag])
        sheet_assembled_entry = commons_prepare.AssembledEntryData(
            sheet_luid, sheet_entry, [sheet_tag])

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            [workbook_assembled_entry, sheet_assembled_entry])

        self.assertEqual(
            f'https://console.cloud.google.com/datacatalog/'
            f'{workbook_entry.name}',
            sheet_tag.fields['workbook_entry'].string_value)

    @classmethod
    def __make_fake_entry(cls, entry_id, entry_type):
        entry = types.Entry()
        entry.name = f'fake_entries/{entry_id}'
        entry.user_specified_type = entry_type
        return entry

    @classmethod
    def __make_fake_tag(cls, string_fields=None, double_fields=None):
        tag = types.Tag()

        if string_fields:
            for field in string_fields:
                tag.fields[field[0]].string_value = field[1]

        if double_fields:
            for field in double_fields:
                tag.fields[field[0]].double_value = field[1]

        return tag
