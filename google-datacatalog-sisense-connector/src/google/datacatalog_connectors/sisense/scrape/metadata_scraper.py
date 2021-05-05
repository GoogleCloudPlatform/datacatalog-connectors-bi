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
from typing import Any, Dict, List

from google.datacatalog_connectors.sisense.scrape import rest_api_helper


class MetadataScraper:

    def __init__(self, server_address: str, api_version: str, username: str,
                 password: str):

        self.__api_helper = rest_api_helper.RESTAPIHelper(
            server_address, api_version, username, password)

    def scrape_all_folders(self) -> List[Dict[str, Any]]:
        self.__log_scrape_start('Scraping all Folders...')
        folders = self.__api_helper.get_all_folders()

        logging.info('')
        logging.info('  %s Folders found:', len(folders))
        for folder in folders:
            name = folder.get('name')
            folder_id = folder.get('_id')
            logging.info('    - %s [%s]', name, folder_id)

        return folders

    def scrape_user(self, user_id) -> Dict:
        self.__log_scrape_start(f'Scraping user "{user_id}" metadata...')
        user = self.__api_helper.get_user(user_id)

        if user:
            first_name = user.get('firstName')
            last_name = user.get('lastName')
            logging.info('  User found: %s %s', first_name, last_name)
        else:
            logging.info('  User NOT found!')

        return user

    @classmethod
    def __log_scrape_start(cls, message: str, *args: Any) -> None:
        logging.info('')
        logging.info(message, *args)
        logging.info('-------------------------------------------------')
