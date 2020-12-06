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

from google.datacatalog_connectors.tableau.scrape import constants


class RestAPIHelper:

    def __init__(self, server_address, api_version):
        self.__base_api_endpoint = f'{server_address}/api/{api_version}'
        self.__common_headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def get_all_sites_on_server(self, auth_credentials):
        url = f'{self.__base_api_endpoint}/sites'

        headers = self.__common_headers.copy()
        headers[
            constants.X_TABLEAU_AUTH_HEADER_NAME] = auth_credentials['token']

        response = requests.get(url=url, headers=headers).json()

        return response['sites']['site'] \
            if response and 'sites' in response and response['sites'] \
            and 'site' in response['sites'] \
            else []

    def get_site(self, auth_credentials):
        url = f'{self.__base_api_endpoint}/sites' \
              f'/{auth_credentials["site"]["id"]}'

        headers = self.__common_headers.copy()
        headers[
            constants.X_TABLEAU_AUTH_HEADER_NAME] = auth_credentials['token']

        response = requests.get(url=url, headers=headers).json()

        return response.get('site')
