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
from typing import Any, Dict

from google.cloud import datacatalog
from google.cloud.datacatalog import Entry

from google.datacatalog_connectors.sisense import sync


class MetadataSynchronizerTest(unittest.TestCase):
    __SYNC_PACKAGE = 'google.datacatalog_connectors.sisense.sync'
    __SYNCR_MODULE = f'{__SYNC_PACKAGE}.metadata_synchronizer'
    __SYNCR_CLASS = f'{__SYNCR_MODULE}.MetadataSynchronizer'
    __PRIVATE_METHOD_PREFIX = f'{__SYNCR_CLASS}._MetadataSynchronizer'

    @mock.patch(f'{__SYNCR_MODULE}.prepare.AssembledEntryFactory')
    @mock.patch(f'{__SYNCR_MODULE}.prepare.DataCatalogTagTemplateFactory')
    @mock.patch(f'{__SYNCR_MODULE}.scrape.MetadataScraper')
    def setUp(self, mock_metadata_scraper, mock_tag_template_factory,
              mock_assembled_entry_factory):

        self.__synchronizer = sync.MetadataSynchronizer(
            sisense_server_address='test-server',
            sisense_username='test-username',
            sisense_password='test-password',
            datacatalog_project_id='test-project-id',
            datacatalog_location_id='test-location-id')

        self.__mock_metadata_scraper = mock_metadata_scraper.return_value
        self.__mock_tag_template_factory = \
            mock_tag_template_factory.return_value
        self.__mock_assembled_entry_factory = \
            mock_assembled_entry_factory.return_value

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__synchronizer.__dict__

        self.assertEqual('test-server',
                         attrs['_MetadataSynchronizer__server_address'])
        self.assertEqual('test-username',
                         attrs['_MetadataSynchronizer__username'])
        self.assertEqual('test-password',
                         attrs['_MetadataSynchronizer__password'])

        self.assertEqual(self.__mock_metadata_scraper,
                         attrs['_MetadataSynchronizer__metadata_scraper'])

        self.assertEqual('test-project-id',
                         attrs['_MetadataSynchronizer__project_id'])
        self.assertEqual('test-location-id',
                         attrs['_MetadataSynchronizer__location_id'])

        self.assertEqual(self.__mock_tag_template_factory,
                         attrs['_MetadataSynchronizer__tag_template_factory'])

        self.assertEqual(
            self.__mock_assembled_entry_factory,
            attrs['_MetadataSynchronizer__assembled_entry_factory'])

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__ingest_metadata')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__delete_obsolete_entries')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__map_datacatalog_relationships')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_assembled_entries_dict')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__make_tag_templates_dict')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__assemble_sisense_assets')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__scrape_dashboards')
    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__scrape_folders')
    def test_run_should_traverse_all_workflow_steps(
            self, mock_scrape_folders, mock_scrape_dashboards,
            mock_assemble_sisense_assets, mock_make_tag_templates_dict,
            mock_make_assembled_entries_dict,
            mock_map_datacatalog_relationships, mock_delete_obsolete_entries,
            mock_ingest_metadata):

        self.__synchronizer.run()

        mock_scrape_folders.assert_called_once()
        mock_scrape_dashboards.assert_called_once()
        mock_assemble_sisense_assets.assert_called_once()
        mock_make_tag_templates_dict.assert_called_once()
        mock_make_assembled_entries_dict.assert_called_once()
        mock_map_datacatalog_relationships.assert_called_once()
        mock_delete_obsolete_entries.assert_called_once()
        mock_ingest_metadata.assert_called_once()

    def test_scrape_folders_should_scrape_all_folders(self):
        mock_scraper = self.__mock_metadata_scraper
        self.__synchronizer._MetadataSynchronizer__scrape_folders()
        mock_scraper.scrape_all_folders.assert_called_once()

    def test_scrape_folders_should_add_owner_data_when_folder_has_owner(self):
        folder = self.__make_fake_folder()

        mock_scraper = self.__mock_metadata_scraper
        mock_scraper.scrape_all_folders.return_value = [folder]

        folders = self.__synchronizer._MetadataSynchronizer__scrape_folders()

        self.assertIsNotNone(folders[0]['ownerData'])
        mock_scraper.scrape_user.assert_called_once()

    def test_scrape_folders_should_skip_adding_owner_data_if_not_available(
            self):

        folder = self.__make_fake_folder()

        mock_scraper = self.__mock_metadata_scraper
        mock_scraper.scrape_all_folders.return_value = [folder]
        mock_scraper.scrape_user.return_value = None

        folders = self.__synchronizer._MetadataSynchronizer__scrape_folders()

        self.assertEqual(folder, folders[0])
        mock_scraper.scrape_user.assert_called_once()

    def test_scrape_dashboards_should_scrape_all_dashboards(self):
        mock_scraper = self.__mock_metadata_scraper
        self.__synchronizer._MetadataSynchronizer__scrape_dashboards([])
        mock_scraper.scrape_all_dashboards.assert_called_once()

    def test_scrape_dashboards_should_add_owner_data_when_dashboard_has_owner(
            self):

        dashboard = self.__make_fake_dashboard()

        mock_scraper = self.__mock_metadata_scraper
        mock_scraper.scrape_all_dashboards.return_value = [dashboard]

        dashboards = \
            self.__synchronizer._MetadataSynchronizer__scrape_dashboards([])

        self.assertIsNotNone(dashboards[0]['ownerData'])
        mock_scraper.scrape_user.assert_called_once()

    def test_scrape_dashboards_should_skip_adding_owner_data_if_not_available(
            self):

        dashboard = self.__make_fake_dashboard()

        mock_scraper = self.__mock_metadata_scraper
        mock_scraper.scrape_all_dashboards.return_value = [dashboard]
        mock_scraper.scrape_user.return_value = None

        dashboards = \
            self.__synchronizer._MetadataSynchronizer__scrape_dashboards([])

        self.assertEqual(dashboard, dashboards[0])
        mock_scraper.scrape_user.assert_called_once()

    def test_scrape_dashboards_should_add_folder_data_when_dashboard_has_parent(  # noqa: E501
            self):

        folder = self.__make_fake_folder()
        dashboard = self.__make_fake_dashboard()
        dashboard['parentFolder'] = 'test-folder'

        mock_scraper = self.__mock_metadata_scraper
        mock_scraper.scrape_all_dashboards.return_value = [dashboard]

        dashboards = self.__synchronizer\
            ._MetadataSynchronizer__scrape_dashboards([folder])

        self.assertEqual(folder, dashboards[0]['folderData'])

    def test_scrape_dashboards_should_raise_on_missing_parent_data(self):
        dashboard = self.__make_fake_dashboard()
        dashboard['parentFolder'] = 'test-folder'

        mock_scraper = self.__mock_metadata_scraper
        mock_scraper.scrape_all_dashboards.return_value = [dashboard]

        self.assertRaises(
            StopIteration,
            self.__synchronizer._MetadataSynchronizer__scrape_dashboards, [])

    def test_scrape_user_should_handle_exception(self):
        mock_scraper = self.__mock_metadata_scraper
        mock_scraper.scrape_user.side_effect = Exception()

        user = self.__synchronizer._MetadataSynchronizer__scrape_user(
            'user-id')

        self.assertIsNone(user)
        mock_scraper.scrape_user.assert_called_once()

    @mock.patch(f'{__PRIVATE_METHOD_PREFIX}__assemble_folder_from_flat_lists')
    def test_assemble_sisense_assets_should_assemble_folder_from_flat_lists(
            self, mock_assemble_folder_from_flat_lists):

        child_folder = self.__make_fake_folder()
        child_folder['parentId'] = 'test-parent-folder'

        parent_folder = self.__make_fake_folder('test-parent-folder')
        parent_folder['folders'] = [child_folder]

        flat_folders = [parent_folder, child_folder]

        mock_assemble_folder_from_flat_lists.return_value = \
            'test-parent-folder', parent_folder

        top_level_assets_dict = \
            self.__synchronizer._MetadataSynchronizer__assemble_sisense_assets(
                flat_folders, [])

        self.assertEqual(1, len(top_level_assets_dict))
        mock_assemble_folder_from_flat_lists.assert_called_once_with(
            parent_folder, flat_folders, [])

    def test_assemble_sisense_assets_should_assemble_dashboard_from_flat_lists(
            self):

        dashboard = self.__make_fake_dashboard()
        flat_dashboards = [dashboard]

        top_level_assets_dict = \
            self.__synchronizer._MetadataSynchronizer__assemble_sisense_assets(
                [], flat_dashboards)

        self.assertEqual(1, len(top_level_assets_dict))
        self.assertEqual(dashboard, top_level_assets_dict['test-dashboard'])

    def test_assemble_folder_from_flat_lists_should_build_parent_child_hierarchy(  # noqa: E501
            self):

        child_folder_1_1 = self.__make_fake_folder('test-child-folder-1.1')
        child_folder_1_1['parentId'] = 'test-child-folder-1'

        child_folder_1 = self.__make_fake_folder('test-child-folder-1')
        child_folder_1['parentId'] = 'test-top-level-folder'

        top_level_folder = self.__make_fake_folder('test-top-level-folder')

        flat_folders = [child_folder_1_1, child_folder_1, top_level_folder]

        folder_id, assembled_folder = self.__synchronizer\
            ._MetadataSynchronizer__assemble_folder_from_flat_lists(
                top_level_folder, flat_folders, [])

        self.assertEqual('test-top-level-folder', folder_id)
        self.assertEqual(child_folder_1, assembled_folder['folders'][0])
        self.assertEqual(child_folder_1_1,
                         assembled_folder['folders'][0]['folders'][0])

    def test_assemble_folder_from_flat_lists_should_build_folder_dashboards_hierarchy(  # noqa: E501
            self):

        folder = self.__make_fake_folder()
        dashboard = self.__make_fake_dashboard()
        dashboard['parentFolder'] = 'test-folder'

        folder_id, assembled_folder = self.__synchronizer\
            ._MetadataSynchronizer__assemble_folder_from_flat_lists(
                folder, [folder], [dashboard])

        self.assertEqual('test-folder', folder_id)
        self.assertEqual(dashboard, assembled_folder['dashboards'][0])

    def test_make_tag_templates_dict_should_make_template_for_folder(self):
        mock_template_factory = self.__mock_tag_template_factory

        tag_templates_dict = self.__synchronizer\
            ._MetadataSynchronizer__make_tag_templates_dict()

        self.assertIsNotNone(tag_templates_dict['sisense_folder_metadata'])
        mock_template_factory.make_tag_template_for_folder.assert_called_once()

    def test_make_assembled_entries_dict_should_process_all_assets(self):
        assembled_metadata = {
            'test-folder-1': self.__make_fake_folder('test-folder-1'),
            'test-folder-2': self.__make_fake_folder('test-folder-2'),
        }

        mock_assembled_entry_factory = self.__mock_assembled_entry_factory

        assembled_entries_dict = self.__synchronizer\
            ._MetadataSynchronizer__make_assembled_entries_dict(
                assembled_metadata, {})

        self.assertEqual(2, len(assembled_entries_dict))
        self.assertEqual(
            2, mock_assembled_entry_factory.make_assembled_entries_list.
            call_count)

    @mock.patch(f'{__SYNCR_MODULE}.prepare.EntryRelationshipMapper')
    def test_map_datacatalog_relationships_should_process_all_entries(
            self, mock_entry_relationship_mapper):

        entry = self.__make_fake_entry('Folder')

        assembled_entries = {
            'test-folder-1': [entry],
            'test-folder-2': [entry, entry],
        }

        self.__synchronizer\
            ._MetadataSynchronizer__map_datacatalog_relationships(
                assembled_entries)

        mock_fulfill_tag_fields = \
            mock_entry_relationship_mapper.return_value.fulfill_tag_fields

        self.assertEqual(1, mock_fulfill_tag_fields.call_count)
        mock_fulfill_tag_fields.assert_called_with([entry, entry, entry])

    @mock.patch(f'{__SYNCR_MODULE}.cleanup.DataCatalogMetadataCleaner')
    def test_delete_obsolete_entries_should_delegate_to_metadata_cleaner(
            self, mock_datacatalog_metadata_cleaner):

        entry = self.__make_fake_entry('Folder')

        assembled_entries = {
            'test-folder-1': [entry],
            'test-folder-2': [entry, entry],
        }

        self.__synchronizer\
            ._MetadataSynchronizer__delete_obsolete_entries(
                assembled_entries)

        mock_delete_obsolete_metadata = mock_datacatalog_metadata_cleaner\
            .return_value.delete_obsolete_metadata

        self.assertEqual(1, mock_delete_obsolete_metadata.call_count)
        mock_delete_obsolete_metadata.assert_called_with(
            [entry, entry, entry], 'system=Sisense tag:server_url:test-server')

    @mock.patch(f'{__SYNCR_MODULE}.ingest.DataCatalogMetadataIngestor')
    def test_ingest_metadata_should_delegate_to_metadata_ingestor(
            self, mock_datacatalog_metadata_ingestor):

        entry = self.__make_fake_entry('folder')

        assembled_entries = {
            'test-folder-1': [entry],
            'test-folder-2': [entry, entry],
        }

        self.__synchronizer._MetadataSynchronizer__ingest_metadata(
            assembled_entries, {})

        mock_ingest_metadata = mock_datacatalog_metadata_ingestor\
            .return_value.ingest_metadata

        self.assertEqual(2, mock_ingest_metadata.call_count)

        calls = [mock.call([entry], {}), mock.call([entry, entry], {})]
        mock_ingest_metadata.assert_has_calls(calls)

    @classmethod
    def __make_fake_folder(cls, oid='test-folder') -> Dict[str, Any]:
        return {
            'oid': oid,
            'owner': 'test-owner',
        }

    @classmethod
    def __make_fake_dashboard(cls, oid='test-dashboard') -> Dict[str, Any]:
        return {
            'oid': oid,
            'owner': 'test-owner',
        }

    @classmethod
    def __make_fake_entry(cls, user_specified_type: str) -> Entry:
        entry = datacatalog.Entry()
        entry.user_specified_type = user_specified_type
        return entry
