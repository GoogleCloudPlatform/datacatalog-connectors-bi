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

from google.datacatalog_connectors.qlik import sync

__SYNC_PACKAGE = 'google.datacatalog_connectors.qlik.sync'
_SYNCR_MODULE = f'{__SYNC_PACKAGE}.metadata_synchronizer'


@mock.patch(f'{_SYNCR_MODULE}.ingest.DataCatalogMetadataIngestor')
@mock.patch(f'{_SYNCR_MODULE}.cleanup.DataCatalogMetadataCleaner')
@mock.patch(f'{_SYNCR_MODULE}.prepare.EntryRelationshipMapper')
class MetadataSynchronizerTest(unittest.TestCase):

    @mock.patch(f'{_SYNCR_MODULE}.prepare.AssembledEntryFactory')
    @mock.patch(f'{_SYNCR_MODULE}.scrape.MetadataScraper')
    def setUp(self, mock_scraper, mock_assembled_entry_factory):
        self.__synchronizer = sync.MetadataSynchronizer(
            qlik_server_address='test-server',
            qlik_ad_domain='test-domain',
            qlik_username='test-username',
            qlik_password='test-password',
            datacatalog_project_id='test-project-id',
            datacatalog_location_id='test-location-id')

    def test_constructor_should_set_instance_attributes(
            self, mock_mapper, mock_cleaner, mock_ingestor):  # noqa: E125

        attrs = self.__synchronizer.__dict__

        self.assertEqual('test-project-id',
                         attrs['_MetadataSynchronizer__project_id'])
        self.assertEqual('test-location-id',
                         attrs['_MetadataSynchronizer__location_id'])
        self.assertIsNotNone(attrs['_MetadataSynchronizer__metadata_scraper'])
        self.assertIsNotNone(
            attrs['_MetadataSynchronizer__tag_template_factory'])
        self.assertEqual('test-server',
                         attrs['_MetadataSynchronizer__site_url'])
        self.assertIsNotNone(
            attrs['_MetadataSynchronizer__assembled_entry_factory'])

    def test_run_no_metadata_should_succeed(self, mock_mapper, mock_cleaner,
                                            mock_ingestor):

        scraper = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__metadata_scraper']

        self.__synchronizer.run()

        scraper.scrape_all_streams.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_not_called()

    def test_run_stream_metadata_should_succeed(self, mock_mapper,
                                                mock_cleaner, mock_ingestor):

        scraper = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {'id': 'test_stream'}

        make_assembled_entries_args = \
            assembled_entry_factory.make_assembled_entries_list.call_args[0]
        self.assertEqual(expected_make_assembled_entries_call_arg,
                         make_assembled_entries_args[0])

        mapper = mock_mapper.return_value
        mapper.fulfill_tag_fields.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_called_once()

    def test_run_published_app_metadata_should_succeed(self, mock_mapper,
                                                       mock_cleaner,
                                                       mock_ingestor):

        scraper = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        scraper.scrape_all_apps.return_value = \
            [self.__make_fake_published_app()]

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {
            'id': 'test_stream',
            'apps': [{
                'id': 'test_app',
                'stream': {
                    'id': 'test_stream'
                }
            }]
        }

        make_assembled_entries_args = \
            assembled_entry_factory.make_assembled_entries_list.call_args[0]
        self.assertEqual(expected_make_assembled_entries_call_arg,
                         make_assembled_entries_args[0])

        mapper = mock_mapper.return_value
        mapper.fulfill_tag_fields.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_called_once()

    def test_run_wip_app_metadata_should_succeed(self, mock_mapper,
                                                 mock_cleaner, mock_ingestor):

        scraper = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__metadata_scraper']
        assembled_entry_factory = self.__synchronizer.__dict__[
            '_MetadataSynchronizer__assembled_entry_factory']

        scraper.scrape_all_streams.return_value = [self.__make_fake_stream()]
        scraper.scrape_all_apps.return_value = [self.__make_fake_wip_app()]

        self.__synchronizer.run()

        expected_make_assembled_entries_call_arg = {'id': 'test_stream'}

        make_assembled_entries_args = \
            assembled_entry_factory.make_assembled_entries_list.call_args[0]
        self.assertEqual(expected_make_assembled_entries_call_arg,
                         make_assembled_entries_args[0])

        mapper = mock_mapper.return_value
        mapper.fulfill_tag_fields.assert_called_once()

        cleaner = mock_cleaner.return_value
        cleaner.delete_obsolete_metadata.assert_called_once()

        ingestor = mock_ingestor.return_value
        ingestor.ingest_metadata.assert_called_once()

    @classmethod
    def __make_fake_stream(cls):
        return {
            'id': 'test_stream',
        }

    @classmethod
    def __make_fake_published_app(cls):
        return {'id': 'test_app', 'stream': cls.__make_fake_stream()}

    @classmethod
    def __make_fake_wip_app(cls):
        return {'id': 'test_app', 'stream': None}
