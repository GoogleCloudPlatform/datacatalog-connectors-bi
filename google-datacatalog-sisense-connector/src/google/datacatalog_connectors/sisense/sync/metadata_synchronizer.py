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

from google.datacatalog_connectors.commons import cleanup, ingest

from google.datacatalog_connectors.sisense import scrape


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

    def run(self) -> None:
        """Coordinates a full scrape > prepare > ingest process."""

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

        # tag_templates_dict = self.__make_tag_templates_dict()

        assembled_entries_dict = self.__make_assembled_entries_dict(folders)
        logging.info('==== DONE ========================================')

    def __scrape_folders(self) -> List[Dict[str, Any]]:
        """Scrapes metadata from all the Folders the user has access to.

        Returns:
            A ``list``.
        """
        return self.__metadata_scraper.scrape_all_folders()

    def __scrape_user(self, user_id) -> Dict[str, Any]:
        """Scrapes metadata from a specific user.

        Returns:
             A user metadata object.
        """
        return self.__metadata_scraper.scrape_user(user_id)

    def __make_assembled_entries_dict(self, folders_metadata) -> Dict:
        """Makes Data Catalog entries and tags for the Sisense assets the
        current user has access to.
        Returns:
            A ``dict`` in which keys are the top level asset ids and values are
            flat lists containing those assets and their nested ones, with all
            related entries and tags.
        """
        assembled_entries = {}

        for folder_metadata in folders_metadata:
            # The root folder does not have an ``_id`` field.
            folder_id = folder_metadata['_id'] if folder_metadata.get(
                '_id') else folder_metadata.get('name')

            assembled_entries[folder_id] = \
                self.__assembled_entry_factory \
                    .make_assembled_entries_for_folder(folder_metadata)

        return assembled_entries
