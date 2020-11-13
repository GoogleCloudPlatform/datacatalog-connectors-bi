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

from . import constants


class RepositoryServicesAPIHelper:

    def __init__(self, server_address):
        self.__base_api_endpoint = f'{server_address}/qrs'
        self.__headers = {
            'Content-Type': 'application/json',
            'Accept': 'application/json',
            'X-Qlik-Xrfkey': constants.XRFKEY,
            'User-Agent': 'Windows'
        }

    def get_windows_authentication_url(self):
        """Get a Windows Authentication url.

        This method sends an unauthenticated request to a well known endpoint
        of the Qlik Sense Repository Service API. The expected response has a
        302 status code and a `Location` header, which is the Windows
        Authentication url.

        Returns:
            A string.
        """
        url = f'{self.__base_api_endpoint}/about?Xrfkey={constants.XRFKEY}'

        response = requests.get(url=url,
                                headers=self.__headers,
                                allow_redirects=False)

        return response.headers.get('Location')

    def scrape_streams(self, session):
        """Read streams metadata from a given server.

        Returns:
            A list of stream metadata objects.
        """
        url = f'{self.__base_api_endpoint}' \
              f'/stream/full?Xrfkey={constants.XRFKEY}'

        return session.get(url=url,
                           headers=self.__headers,
                           cookies=requests.session().cookies).json()
