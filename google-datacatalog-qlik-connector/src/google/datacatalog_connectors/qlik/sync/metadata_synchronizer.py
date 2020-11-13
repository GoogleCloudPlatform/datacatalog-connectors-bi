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

import logging

from .. import scrape


class MetadataSynchronizer:
    __ENTRY_GROUP_ID = 'qlik'
    __SPECIFIED_SYSTEM = 'qlik'

    def __init__(self, qlik_server_address, qlik_domain, qlik_username,
                 qlik_password, datacatalog_project_id,
                 datacatalog_location_id):

        self.__project_id = datacatalog_project_id
        self.__location_id = datacatalog_location_id

        self.__metadata_scraper = scrape.MetadataScraper(
            qlik_server_address, qlik_domain, qlik_username, qlik_password)

    def run(self):
        """Coordinates a full scrape > prepare > ingest process."""

        # Scrape metadata from Looker server.
        logging.info('')
        logging.info('===> Scraping Qlik Sense metadata...')

        logging.info('')
        logging.info('Streams...')
        streams = self.__metadata_scraper.scrape_streams()
        logging.info('==== DONE ========================================')

        print(streams)
