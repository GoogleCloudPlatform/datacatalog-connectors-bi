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

from google.datacatalog_connectors.commons import prepare
from google.datacatalog_connectors.tableau.sync import \
    workbooks_synchronizer


class WorkbooksSynchronizerTest(unittest.TestCase):
    __COMMONS_PACKAGE = 'google.datacatalog_connectors.commons'
    __PREPARE_PACKAGE = 'google.datacatalog_connectors.tableau.prepare'
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.tableau.scrape'

    @mock.patch(f'{__COMMONS_PACKAGE}.ingest.DataCatalogMetadataIngestor')
    @mock.patch(f'{__COMMONS_PACKAGE}.cleanup.DataCatalogMetadataCleaner')
    @mock.patch(f'{__PREPARE_PACKAGE}.EntryRelationshipMapper')
    @mock.patch(f'{__PREPARE_PACKAGE}.AssembledEntryFactory')
    @mock.patch(f'{__SCRAPE_PACKAGE}.MetadataScraper')
    def test_run_should_process_scrape_prepare_ingest_workflow(
            self, mock_scraper, mock_assembled_entry_factory, mock_mapper,
            mock_cleaner, mock_ingestor):

        assembled_entry_factory = mock_assembled_entry_factory.return_value
        assembled_entry_factory.make_entries_for_workbooks.return_value = \
            [(prepare.AssembledEntryData('test-entry-id-1', {}, []))]

        workbooks_synchronizer.WorkbooksSynchronizer(
            tableau_server_address='test-server',
            tableau_api_version='test-api-version',
            tableau_username='test-api-username',
            tableau_password='test-api-password',
            tableau_site='test-site',
            datacatalog_project_id='test-project-id',
            datacatalog_location_id='test-location-id').run()

        scraper = mock_scraper.return_value
        scraper.scrape_workbooks.assert_called_once()

        assembled_entry_factory.make_entries_for_workbooks.assert_called_once()
        mock_mapper.return_value.fulfill_tag_fields.assert_called_once()
        mock_cleaner.return_value.delete_obsolete_metadata.assert_called_once()
        mock_ingestor.return_value.ingest_metadata.assert_called_once()
