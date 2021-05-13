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

import logging
import re
from typing import Any, Dict, List

from google.cloud.datacatalog import TagTemplate
from google.datacatalog_connectors.commons import cleanup, ingest
from google.datacatalog_connectors.commons.prepare import AssembledEntryData

from google.datacatalog_connectors.sisense import prepare, scrape
from google.datacatalog_connectors.sisense.prepare import constants


class MetadataSynchronizer:
    # Data Catalog constants
    __ENTRY_GROUP_ID = 'sisense'
    __SPECIFIED_SYSTEM = 'sisense'
    # Sisense constants
    __SISENSE_API_VERSION = 'v1'

    def __init__(self, sisense_server_address, sisense_username,
                 sisense_password, datacatalog_project_id,
                 datacatalog_location_id):

        self.__server_address = sisense_server_address
        self.__username = sisense_username
        self.__password = sisense_password

        self.__metadata_scraper = scrape.MetadataScraper(
            self.__server_address, self.__SISENSE_API_VERSION, self.__username,
            self.__password)

        self.__project_id = datacatalog_project_id
        self.__location_id = datacatalog_location_id

        self.__assembled_entry_factory = prepare.AssembledEntryFactory(
            project_id=datacatalog_project_id,
            location_id=datacatalog_location_id,
            entry_group_id=self.__ENTRY_GROUP_ID,
            user_specified_system=self.__SPECIFIED_SYSTEM,
            server_address=sisense_server_address)

        self.__tag_template_factory = prepare.DataCatalogTagTemplateFactory(
            project_id=datacatalog_project_id,
            location_id=datacatalog_location_id)

    def run(self) -> None:
        """Coordinate a full scrape > prepare > ingest process."""

        # Scrape metadata from the Sisense server.
        logging.info('')
        logging.info('===> Scraping Sisense metadata...')

        logging.info('')
        logging.info('Objects to be scraped: Folders')
        folders = self.__scrape_folders()
        logging.info('==== DONE ========================================')

        # Prepare: convert Sisense metadata into Data Catalog entities model.
        logging.info('')
        logging.info('===> Converting Sisense metadata'
                     ' into Data Catalog entities model...')

        assembled_assets = self.__assemble_sisense_assets(folders)
        tag_templates_dict = self.__make_tag_templates_dict()

        assembled_entries_dict = self.__make_assembled_entries_dict(
            assembled_assets, tag_templates_dict)
        logging.info('==== DONE ========================================')

        # Ingest metadata into Data Catalog.
        logging.info('')
        logging.info('===> Synchronizing Sisense :: Data Catalog metadata...')

        self.__ingest_metadata(assembled_entries_dict, tag_templates_dict)
        logging.info('==== DONE ========================================')

    def __scrape_folders(self) -> List[Dict[str, Any]]:
        """Scrape metadata from all Folders the current user has access to. A
        custom ``owner`` field is added to the Folder metadata if the
        authenticated user is allowed to scrape users' metadata.

        Returns:
            A ``list``.
        """
        all_folders = self.__metadata_scraper.scrape_all_folders()
        folders_with_owner = [
            folder for folder in all_folders if folder.get('userId')
        ]
        for folder in folders_with_owner:
            try:
                folder['user'] = self.__scrape_user(folder.get('userId'))
            except:  # noqa E722
                logging.warning("error on __scrape_folders:", exc_info=True)

        return all_folders

    def __scrape_user(self, user_id: str) -> Dict[str, Any]:
        """Scrape metadata from a specific user.

        Returns:
             A User metadata object.
        """
        return self.__metadata_scraper.scrape_user(user_id)

    def __assemble_sisense_assets(
            self, folders: List[Dict[str,
                                     Any]]) -> Dict[str, List[Dict[str, Any]]]:
        """
        Assemble metadata from all assets the current user has access to.
        Folders metadata include nested objects such as sub-folders,
        dashboards, widgets, and fields.

        Returns:
            A ``dict`` in which keys are top-level asset IDs and values are
            lists containing all metadata gathered for those assets in a
            deep-first hierarchy.
        """
        top_level_folders = [
            folder for folder in folders if not folder.get('parentId')
        ]

        top_level_assets_dict = {}
        for folder in top_level_folders:
            # The root folder does not have an ``_id`` field.
            folder_id = folder.get('_id') or folder.get('name')
            top_level_assets_dict[
                folder_id] = self.__assemble_folder_from_flat_lists(
                    folder, folders)

        return top_level_assets_dict

    def __assemble_folder_from_flat_lists(
            self, folder: Dict[str, Any],
            all_folders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Assemble folders metadata from the given flat asset lists.
        """
        folders = [folder]

        # The root folder does not have an ``_id`` field.
        folder_id = folder.get('_id') or folder.get('name')
        child_folders = [
            child for child in all_folders
            if child.get('parentId') == folder_id
        ]

        # TODO Check whether the already used folders, dashboards, widgets, and
        #  fields can be removed from the given lists to improve next
        #  iterations performance and memory usage.

        for child in child_folders:
            folders.extend(
                self.__assemble_folder_from_flat_lists(child, all_folders))

        return folders

    def __make_tag_templates_dict(self) -> Dict[str, TagTemplate]:
        return {
            constants.TAG_TEMPLATE_ID_FOLDER:
                self.__tag_template_factory.make_tag_template_for_folder(),
        }

    def __make_assembled_entries_dict(
        self, assembled_metadata: Dict[str, List[Dict[str, Any]]],
        tag_templates: Dict[str, TagTemplate]
    ) -> Dict[str, List[AssembledEntryData]]:
        """Make Data Catalog entries and tags for the Sisense assets the
        current user has access to.

        Returns:
            A ``dict`` in which keys are the top level asset ids and values are
            flat lists containing those assets and their nested ones, with all
            related entries and tags.
        """
        assembled_entries = {}

        for asset_id, assets in assembled_metadata.items():
            assembled_entries[asset_id] = self.__assembled_entry_factory \
                    .make_assembled_entries_list(assets, tag_templates)

        return assembled_entries

    def __ingest_metadata(self,
                          assembled_entries: Dict[str,
                                                  List[AssembledEntryData]],
                          tag_templates: Dict[str, TagTemplate]) -> None:

        metadata_ingestor = ingest.DataCatalogMetadataIngestor(
            self.__project_id, self.__location_id, self.__ENTRY_GROUP_ID)

        entry_count = sum(
            len(entries) for entries in assembled_entries.values())
        logging.info('==== %d entries to be synchronized!', entry_count)

        synced_entry_count = 0
        for asset_id, assembled_entries in assembled_entries.items():
            asset_entry_count = len(assembled_entries)

            logging.info('')
            logging.info('==== The asset identified by %s has %d entries.',
                         asset_id, asset_entry_count)
            metadata_ingestor.ingest_metadata(assembled_entries, tag_templates)
            synced_entry_count += asset_entry_count

        logging.info('')
        logging.info('==== %d of %d entries successfully synchronized!',
                     synced_entry_count, entry_count)
