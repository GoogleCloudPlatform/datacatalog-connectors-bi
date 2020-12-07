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

import requests

from google.datacatalog_connectors.tableau.scrape import \
    authenticator, constants


class RestAPIHelper:

    def __init__(self,
                 server_address,
                 api_version,
                 username,
                 password,
                 site_content_url=None):

        self.__server_address = server_address
        self.__api_version = api_version
        self.__username = username
        self.__password = password
        self.__site_content_url = site_content_url

        self.__base_api_endpoint = f'{server_address}/api/{api_version}'
        self.__common_headers = {
            'Content-Type': constants.JSON_CONTENT_TYPE,
            'Accept': constants.JSON_CONTENT_TYPE
        }

        self.__auth_credentials = None

    def get_all_sites_for_server(self):
        self.__set_up_auth_credentials()

        url = f'{self.__base_api_endpoint}/sites'

        headers = self.__common_headers.copy()
        headers[constants.X_TABLEAU_AUTH_HEADER_NAME] = \
            self.__auth_credentials['token']

        response = requests.get(url=url, headers=headers).json()

        return response['sites']['site'] \
            if response and response.get('sites') \
            and 'site' in response['sites'] \
            else []

    def __set_up_auth_credentials(self):
        if self.__auth_credentials:
            return

        self.__auth_credentials = \
            authenticator.Authenticator.authenticate(
                self.__server_address,
                self.__api_version,
                self.__username,
                self.__password,
                self.__site_content_url)
