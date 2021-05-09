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
import requests
from typing import Dict, Optional

from google.datacatalog_connectors.sisense.scrape import constants


class Authenticator:

    @classmethod
    def authenticate(cls, server_address: str, api_version: str, username: str,
                     password: str) -> Optional[Dict]:

        url = f'{server_address}/api/{api_version}/authentication/login'

        headers = {
            'Content-Type': constants.X_WWW_FORM_URLENCODED_CONTENT_TYPE,
            'Accept': constants.JSON_CONTENT_TYPE
        }

        credentials = {'username': username, 'password': password}

        response = requests.post(url=url, headers=headers,
                                 data=credentials).json()

        if not response.get('success'):
            logging.info(response)
            return

        return response
