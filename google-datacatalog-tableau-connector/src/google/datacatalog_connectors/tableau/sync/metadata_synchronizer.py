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

import abc
from abc import abstractmethod
import logging

from google.datacatalog_connectors.commons import cleanup, ingest

from google.datacatalog_connectors.tableau import prepare, scrape


class MetadataSynchronizer(abc.ABC):
    __ENTRY_GROUP_ID = 'tableau'
    __SPECIFIED_SYSTEM = 'tableau'

    def __init__(self,
                 tableau_server_address,
                 tableau_api_version,
                 tableau_username,
                 tableau_password,
                 datacatalog_project_id,
                 datacatalog_location_id,
                 assets_type,
                 tableau_site=None):

        super().__init__()

        self.__project_id = datacatalog_project_id
        self.__location_id = datacatalog_location_id
        self.__assets_type = assets_type
        self.__site = tableau_site

        self._metadata_scraper = scrape.MetadataScraper(
            server_address=tableau_server_address,
            api_version=tableau_api_version,
            username=tableau_username,
            password=tableau_password,
            site=tableau_site)

        self._entry_factory = \
            prepare.AssembledEntryFactory(
                project_id=datacatalog_project_id,
                location_id=datacatalog_location_id,
                entry_group_id=self.__ENTRY_GROUP_ID,
                user_specified_system=self.__SPECIFIED_SYSTEM,
                server_address=tableau_server_address)

        self._tag_template_factory = \
            prepare.DataCatalogTagTemplateFactory(
                project_id=datacatalog_project_id,
                location_id=datacatalog_location_id)

    def run(self, query_filter=None):
        """Template method for a scrape > prepare > ingest process."""

        is_partial_sync = query_filter is not None
        if is_partial_sync:
            logging.info('!!! THIS IS A PARTIAL SYNC !!!')

        # Scrape metadata from Tableau server.
        logging.info('')
        logging.info(f'===> Scraping Tableau metadata'
                     f' [site: {self.__site if self.__site else "all"},'
                     f' type: {self.__assets_type}]...')

        source_system_metadata = self._scrape_source_system_metadata(
            query_filter)
        logging.info('==== DONE =======================================')
        self._log_scraping_results(source_system_metadata)

        # Prepare: convert Tableau metadata into Data Catalog entities model.
        logging.info('')
        logging.info('===> Converting Tableau metadata into '
                     'Data Catalog entities model...')

        tag_templates_dict = self._make_tag_templates_dict()

        assembled_entries = self._make_assembled_entries(
            source_system_metadata, tag_templates_dict)
        logging.info('==== DONE =======================================')

        # Data Catalog entries relationship mapping.
        logging.info('')
        logging.info('===> Mapping Data Catalog entries relationships...')

        prepare.EntryRelationshipMapper().fulfill_tag_fields(assembled_entries)
        logging.info('==== DONE ========================================')

        # Temporary assembled entries removal.
        logging.info('')
        logging.info('===> Removing temporary assembled entries...')

        assembled_entries = self._filter_ingestable_assembled_entries(
            assembled_entries)
        logging.info('==== DONE ========================================')

        # Data Catalog clean up: delete obsolete data.
        logging.info('')
        logging.info('===> Deleting Data Catalog obsolete metadata...')

        # Since we can't rely on search returning the ingested entries,
        # we clean up the obsolete entries before ingesting.
        if not is_partial_sync:  # TODO add clean up logic for partial sync
            cleaner = cleanup.DataCatalogMetadataCleaner(
                self.__project_id, self.__location_id, self.__ENTRY_GROUP_ID)

            # TODO b/140021727, search results are
            #  returning wrong linked_resource
            # We can't ensure entries belong to a given site yet.
            cleaner.delete_obsolete_metadata(assembled_entries,
                                             self.__get_search_query())
        logging.info('==== DONE =======================================')

        # Ingest metadata into Data Catalog.
        logging.info('')
        logging.info('===> Synchronizing Tableau :: Data Catalog metadata...')

        ingestor = ingest.DataCatalogMetadataIngestor(self.__project_id,
                                                      self.__location_id,
                                                      self.__ENTRY_GROUP_ID)

        ingestor.ingest_metadata(assembled_entries, tag_templates_dict)
        logging.info('==== DONE =======================================')

    @abstractmethod
    def _scrape_source_system_metadata(self, query_filter=None):
        pass

    def _log_scraping_results(self, metadata):
        logging.info(f'==== {len(metadata)} assets found')

    @abstractmethod
    def _make_tag_templates_dict(self):
        pass

    @abstractmethod
    def _make_assembled_entries(self, source_system_metadata,
                                tag_templates_dict):
        pass

    @classmethod
    def _filter_ingestable_assembled_entries(cls, assembled_entries):
        return assembled_entries

    def __get_search_query(self):
        search_query = f'system={self.__SPECIFIED_SYSTEM} ('
        for asset_type in self.__assets_type:
            search_query += f'type={asset_type} or '
        search_query = search_query[:-4]
        search_query += ')'
        return search_query
