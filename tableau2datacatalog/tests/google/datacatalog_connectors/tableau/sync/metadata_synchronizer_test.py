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

from . import metadata_synchronizer_mocks

__COMMONS_PACKAGE = 'google.datacatalog_connectors.commons'
__PREPARE_PACKAGE = 'google.datacatalog_connectors.tableau.prepare'


@mock.patch(f'{__COMMONS_PACKAGE}.ingest.DataCatalogMetadataIngestor')
@mock.patch(f'{__COMMONS_PACKAGE}.cleanup.DataCatalogMetadataCleaner')
@mock.patch(f'{__PREPARE_PACKAGE}.EntryRelationshipMapper')
class MetadataSynchronizerTest(unittest.TestCase):

    def setUp(self):
        self.__synchronizer = metadata_synchronizer_mocks.FakeSynchronizer()

    def test_constructor_should_set_instance_attributes(
            self, mock_mapper, mock_cleaner, mock_ingestor):  # noqa: E125

        attrs = self.__synchronizer.__dict__

        self.assertEqual('test-project-id',
                         attrs['_MetadataSynchronizer__project_id'])
        self.assertEqual('test-location-id',
                         attrs['_MetadataSynchronizer__location_id'])
        self.assertEqual(['test-type'],
                         attrs['_MetadataSynchronizer__assets_type'])
        self.assertEqual('test-site', attrs['_MetadataSynchronizer__site'])

        self.assertIsNotNone(attrs['_metadata_scraper'])
        self.assertIsNotNone(attrs['_entry_factory'])

    def test_run_full_sync_should_process_scrape_prepare_ingest_workflow(
            self, mock_mapper, mock_cleaner, mock_ingestor):  # noqa: E125

        self.__synchronizer.run()

        mock_mapper.return_value.fulfill_tag_fields.assert_called_once()
        mock_cleaner.return_value.delete_obsolete_metadata.assert_called_once()
        mock_ingestor.return_value.ingest_metadata.assert_called_once()

    def test_run_partial_sync_should_process_scrape_prepare_ingest_workflow(
            self, mock_mapper, mock_cleaner, mock_ingestor):  # noqa: E125

        self.__synchronizer.run(query_filter={})

        mock_mapper.return_value.fulfill_tag_fields.assert_called_once()
        mock_cleaner.return_value.delete_obsolete_metadata.assert_not_called()
        mock_ingestor.return_value.ingest_metadata.assert_called_once()
