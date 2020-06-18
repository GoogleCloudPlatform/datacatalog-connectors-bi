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

import io
import json
import unittest
from unittest import mock

from looker_sdk import error, models
from looker_sdk.rtl import serialize

from google.datacatalog_connectors.looker import sync

_PREPARE_PACKAGE = 'google.datacatalog_connectors.looker.prepare'
__SYNC_PACKAGE = 'google.datacatalog_connectors.looker.sync'
_SYNC_MODULE = '{}.metadata_synchronizer'.format(__SYNC_PACKAGE)


@mock.patch(f'{_SYNC_MODULE}.ingest.DataCatalogMetadataIngestor')
@mock.patch(f'{_SYNC_MODULE}.cleanup.DataCatalogMetadataCleaner')
@mock.patch(f'{_PREPARE_PACKAGE}.EntryRelationshipMapper')
class MetadataSynchronizerTest(unittest.TestCase):

    @mock.patch(f'{_SYNC_MODULE}.prepare.AssembledEntryFactory')
    @mock.patch(f'{_SYNC_MODULE}.configparser.open',
                new_callable=mock.mock_open())
    @mock.patch(f'{_SYNC_MODULE}.scrape.MetadataScraper')
    def setUp(self, mock_scraper, mock_open, mock_assembled_entry_factory):
        mock_open.return_value = io.StringIO(
            '[Looker]\n'
            'base_url=https://test-instance.com:123\n')

        self.__synchronizer = sync.MetadataSynchronizer(
            'test-project', 'test-location', 'looker-credentials.ini')

    def test_constructor_should_set_instance_attributes(
            self, mock_mapper, mock_cleaner, mock_ingestor):  # noqa: E125

        attrs = self.__synchronizer.__dict__

        self.assertEqual('test-project',
                         attrs['_MetadataSynchronizer__project_id'])
        self.assertEqual('test-location',
                         attrs['_MetadataSynchronizer__location_id'])
        self.assertIsNotNone(attrs['_MetadataSynchronizer__metadata_scraper'])
        self.assertIsNotNone(
            attrs['_MetadataSynchronizer__tag_template_factory'])
        self.assertEqual('https://test-instance.com',
                         attrs['_MetadataSynchronizer__instance_url'])
        self.assertIsNotNone(
            attrs['_MetadataSynchronizer__assembled_entry_factory'])

    def test_run_no_metadata_should_succeed(self, mock_mapper, mock_cleaner,
                                            mock_ingestor):
        scraper = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__metadata_scraper']

        scraper.scrape_folder.return_value = None  # LookML folder

        self.__synchronizer.run()

        scraper.scrape_all_folders.assert_called_once()
        scraper.scrape_all_dashboards.assert_called_once()
        scraper.scrape_all_looks.assert_called_once()
        scraper.scrape_folder.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_not_called()

    def test_run_top_level_folder_should_succeed(self, mock_mapper,
                                                 mock_cleaner, mock_ingestor):

        scraper = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__metadata_scraper']

        top_level_folder = self.__make_fake_folder()
        scraper.scrape_all_folders.return_value = [top_level_folder]
        scraper.scrape_folder.return_value = None  # LookML folder

        self.__synchronizer.run()

        mapper = mock_mapper.return_value
        mapper.fulfill_tag_fields.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_called_once()

    def test_run_child_folder_should_succeed(self, mock_mapper, mock_cleaner,
                                             mock_ingestor):

        scraper = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__metadata_scraper']

        top_level_folder = self.__make_fake_folder()
        child_folder = self.__make_fake_folder(top_level_folder)

        scraper.scrape_all_folders.return_value = [
            top_level_folder, child_folder
        ]
        scraper.scrape_folder.return_value = None  # LookML folder

        self.__synchronizer.run()

        mapper = mock_mapper.return_value
        mapper.fulfill_tag_fields.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_called_once()

    def test_run_lookml_folder_should_succeed(self, mock_mapper, mock_cleaner,
                                              mock_ingestor):

        scraper = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__metadata_scraper']

        scraper.scrape_folder.return_value = \
            self.__make_fake_folder()  # LookML

        self.__synchronizer.run()

        mapper = mock_mapper.return_value
        mapper.fulfill_tag_fields.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_called_once()

    def test_run_recursive_dashboard_should_succeed(self, mock_mapper,
                                                    mock_cleaner,
                                                    mock_ingestor):

        scraper = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__metadata_scraper']

        lookml_folder = self.__make_fake_folder()
        lookml_folder.dashboards = [self.__make_fake_dashboard(lookml_folder)]
        scraper.scrape_folder.return_value = lookml_folder

        self.__synchronizer.run()

        scraper.scrape_dashboard.assert_called_once()

        mapper = mock_mapper.return_value
        mapper.fulfill_tag_fields.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_called_once()

    def test_run_recursive_dashboard_should_handle_sdk_error(
            self, mock_mapper, mock_cleaner, mock_ingestor):  # noqa: E125

        scraper = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__metadata_scraper']

        lookml_folder = self.__make_fake_folder()
        lookml_folder.dashboards = [self.__make_fake_dashboard(lookml_folder)]
        scraper.scrape_folder.return_value = lookml_folder
        scraper.scrape_dashboard.side_effect = error.SDKError('SDK error')

        self.__synchronizer.run()

        scraper.scrape_dashboard.assert_called_once()

        mapper = mock_mapper.return_value
        mapper.fulfill_tag_fields.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_called_once()

    def test_run_recursive_look_should_succeed(self, mock_mapper, mock_cleaner,
                                               mock_ingestor):

        scraper = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__metadata_scraper']

        lookml_folder = self.__make_fake_folder()
        lookml_folder.looks = [self.__make_fake_look(lookml_folder)]
        scraper.scrape_folder.return_value = lookml_folder

        self.__synchronizer.run()

        scraper.scrape_look.assert_called_once()

        mapper = mock_mapper.return_value
        mapper.fulfill_tag_fields.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_called_once()

    def test_run_recursive_child_folder_should_succeed(self, mock_mapper,
                                                       mock_cleaner,
                                                       mock_ingestor):

        scraper = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__metadata_scraper']

        lookml_folder = self.__make_fake_folder()
        child_folder = self.__make_fake_folder(lookml_folder)
        scraper.scrape_folder.return_value = lookml_folder
        scraper.scrape_child_folders.side_effect = [[child_folder], []]

        self.__synchronizer.run()

        self.assertEqual(2, scraper.scrape_child_folders.call_count)

        mapper = mock_mapper.return_value
        mapper.fulfill_tag_fields.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_called_once()

    def test_run_lookml_model_explore_should_handle_sdk_error(
            self, mock_mapper, mock_cleaner, mock_ingestor):  # noqa: E125

        scraper = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__metadata_scraper']

        lookml_folder = self.__make_fake_folder()
        lookml_folder.looks = [self.__make_fake_look(lookml_folder)]
        scraper.scrape_folder.return_value = lookml_folder
        scraper.scrape_lookml_model_explore.side_effect = error.SDKError(
            'SDK error')

        self.__synchronizer.run()

        scraper.scrape_lookml_model_explore.assert_called_once()

        mapper = mock_mapper.return_value
        mapper.fulfill_tag_fields.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_called_once()

    @classmethod
    def __make_fake_folder(cls, parent=None):
        parent_data = json.loads(
            serialize.serialize(parent)) if parent else None
        folder_data = {
            'id': 'test_child_folder' if parent else 'test_folder',
            'name': 'Test child folder' if parent else 'Test folder',
            'parent_id': parent_data['id'] if parent_data else '',
            'child_count': 1 if parent else 0,
        }
        return serialize.deserialize(json.dumps(folder_data), models.Folder)

    @classmethod
    def __make_fake_dashboard(cls, folder):
        dashboard_data = {
            'id': 'test_dashboard',
            'space': json.loads(serialize.serialize(folder)),
            'dashboard_elements': [],
        }
        return serialize.deserialize(json.dumps(dashboard_data),
                                     models.Dashboard)

    @classmethod
    def __make_fake_look(cls, folder):
        look_data = {
            'id': 123,
            'space': json.loads(serialize.serialize(folder)),
        }
        return serialize.deserialize(json.dumps(look_data), models.Look)
