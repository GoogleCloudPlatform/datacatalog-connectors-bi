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
from typing import Dict, List

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

        user = self.__scrape_user(folders[0]['owner'])

    def __scrape_folders(self) -> List[Dict]:
        """Scrapes metadata from all the Folders the user has access to.

        Returns:
            A ``list``.
        """
        return self.__metadata_scraper.scrape_all_folders()

    def __scrape_user(self, user_id) -> Dict:
        """Scrapes metadata from a specific user.

        Returns:
             A user metadata object.
        """
        return self.__metadata_scraper.scrape_user(user_id)
