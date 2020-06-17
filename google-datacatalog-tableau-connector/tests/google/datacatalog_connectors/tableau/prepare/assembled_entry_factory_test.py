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

from google.datacatalog_connectors.tableau import prepare

_PREPARE_PACKAGE = 'google.datacatalog_connectors.tableau.prepare'


@mock.patch(f'{_PREPARE_PACKAGE}.datacatalog_tag_factory'
            f'.DataCatalogTagFactory', mock.MagicMock())
class AssembledEntryFactoryTest(unittest.TestCase):

    @mock.patch(f'{_PREPARE_PACKAGE}.datacatalog_entry_factory'
                f'.DataCatalogEntryFactory')
    def setUp(self, mock_entry_factory):
        self.__factory = prepare.AssembledEntryFactory(
            project_id='test-project',
            location_id='test-location',
            entry_group_id='test-entry-group',
            user_specified_system='test-system',
            server_address='test-server')

        self.__entry_factory = mock_entry_factory.return_value

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__factory.__dict__

        self.assertEqual(
            self.__entry_factory,
            attrs['_AssembledEntryFactory__datacatalog_entry_factory'])

    def test_make_entries_for_dashboards_should_create_entries(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_dashboard.return_value = 'd_entry_id', {}
        entry_factory.make_entry_for_workbook.return_value = 'w_entry_id', {}

        metadata = [{'luid': 'a123-b456', 'workbook': {'luid': 'c234-d567'}}]

        assembled_entries = self.__factory.make_entries_for_dashboards(
            metadata, {})

        self.assertEqual(2, len(assembled_entries))
        self.assertEqual(0, len(assembled_entries[0].tags))
        self.assertEqual(0, len(assembled_entries[1].tags))

    def test_make_entries_for_dashboards_should_create_tags_if_templates_given(
            self):  # noqa: E125

        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_dashboard.return_value = 'entry_id', {}

        metadata = [{'luid': 'a123-b456'}]
        tag_templates_dict = {
            'tableau_dashboard_metadata': {
                'name': 'Fake template'
            }
        }

        assembled_entries = self.__factory.make_entries_for_dashboards(
            metadata, tag_templates_dict)

        self.assertEqual(1, len(assembled_entries))
        self.assertEqual(1, len(assembled_entries[0].tags))

    def test_make_entries_for_sites_should_create_entries(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_workbook.return_value = 'entry_id', {}

        metadata = [{'workbooks': [{'luid': 'a123-b456', 'sheets': []}]}]

        assembled_entries = self.__factory.make_entries_for_sites(metadata, {})

        self.assertEqual(1, len(assembled_entries))
        self.assertEqual(0, len(assembled_entries[0].tags))

    def test_make_entries_for_workbooks_should_create_entries(self):
        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_workbook.return_value = 'entry_id', {}
        entry_factory.make_entry_for_sheet.return_value = 'entry_id', {}

        metadata = [{'luid': 'a123-b456', 'sheets': [{'luid': 'c234-d567'}]}]

        assembled_entries = self.__factory.make_entries_for_workbooks(
            metadata, {})

        self.assertEqual(2, len(assembled_entries))

        assembled_workbook_entry = assembled_entries[0]
        assembled_sheet_entry = assembled_entries[1]
        self.assertEqual(0, len(assembled_workbook_entry.tags))
        self.assertEqual(0, len(assembled_sheet_entry.tags))

    def test_make_entries_for_workbooks_should_create_tags_if_templates_given(
            self):  # noqa: E125

        entry_factory = self.__entry_factory
        entry_factory.make_entry_for_workbook.return_value = 'entry_id', {}
        entry_factory.make_entry_for_sheet.return_value = 'entry_id', {}

        metadata = [{'luid': 'a123-b456', 'sheets': [{'luid': 'c234-d567'}]}]
        tag_templates_dict = {
            'tableau_sheet_metadata': {
                'name': 'Fake sheet template'
            },
            'tableau_workbook_metadata': {
                'name': 'Fake workbook template'
            }
        }

        assembled_entries = self.__factory.make_entries_for_workbooks(
            metadata, tag_templates_dict)

        assembled_workbook_entry = assembled_entries[0]
        assembled_sheet_entry = assembled_entries[1]
        self.assertEqual(1, len(assembled_workbook_entry.tags))
        self.assertEqual(1, len(assembled_sheet_entry.tags))
