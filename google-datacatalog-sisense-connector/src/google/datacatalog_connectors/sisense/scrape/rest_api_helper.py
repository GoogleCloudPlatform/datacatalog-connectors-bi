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

import requests
from typing import Dict, List

from google.datacatalog_connectors.sisense.scrape import \
    authenticator, constants


class RESTAPIHelper:

    def __init__(self, server_address: str, api_version: str, username: str,
                 password: str):

        self.__server_address = server_address
        self.__api_version = api_version
        self.__username = username
        self.__password = password

        self.__base_api_endpoint = f'{server_address}/api/{api_version}'
        self.__common_headers = {
            'Content-Type': constants.JSON_CONTENT_TYPE,
            'Accept': constants.JSON_CONTENT_TYPE
        }

        self.__auth_credentials = None

    def get_all_folders(self) -> List[Dict]:
        """Get all Folders the user has access to on a given server.

        Returns:
            A ``list``.
        """
        self.__set_up_auth()

        # Sisense can return the folders in flat (default) or tree structures.
        # We decided for flat because it simplifies further processing.
        url = f'{self.__base_api_endpoint}/folders?structure=flat'
        return requests.get(url=url, headers=self.__common_headers).json()

    def __set_up_auth(self) -> None:
        if self.__auth_credentials:
            return

        self.__auth_credentials = \
            authenticator.Authenticator.authenticate(
                self.__server_address,
                self.__api_version,
                self.__username,
                self.__password)

        self.__common_headers[constants.AUTHORIZATION_HEADER_NAME] = \
            f'{constants.BEARER_TOKEN_PREFIX}' \
            f' {self.__auth_credentials["access_token"]}'
