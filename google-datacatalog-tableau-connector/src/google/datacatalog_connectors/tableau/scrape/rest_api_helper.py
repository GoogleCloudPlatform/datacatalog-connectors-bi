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


class RestAPIHelper:

    def __init__(self, server_address, api_version, auth_credentials):
        self.__base_api_endpoint = f'{server_address}/api/{api_version}'
        self.__headers = {
            'X-Tableau-Auth': auth_credentials['token'],
            'Content-Type': 'application/json',
            'Accept': 'application/json'
        }

    def get_all_sites_on_server(self):
        url = f'{self.__base_api_endpoint}/sites'

        result = requests.get(url=url, headers=self.__headers).json()

        return result['sites']['site'] \
            if result and 'sites' in result and result['sites'] \
            and 'site' in result['sites'] \
            else []
