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

import requests
from requests_ntlm import HttpNtlmAuth

from google.datacatalog_connectors.qlik.scrape import constants


class Authenticator:

    @classmethod
    def get_qps_session_cookie_windows_auth(cls, ad_domain, username, password,
                                            auth_url):

        # Set up user credentials
        user_auth = HttpNtlmAuth(username=f'{ad_domain}\\{username}',
                                 password=password)

        headers = {constants.XRFKEY_HEADER_NAME: constants.XRFKEY}

        response = requests.get(url=auth_url, auth=user_auth, headers=headers)

        if not response.cookies:
            cls.__log_authentication_failure()
            return

        try:
            return next(
                cookie for cookie in response.cookies
                if cookie.name.startswith(constants.QPS_SESSION_COOKIE_PREFIX))
        except StopIteration:
            cls.__log_authentication_failure()

    @classmethod
    def __log_authentication_failure(cls):
        logging.warning(
            'Authentication failed! %s cookie not found in the response.',
            constants.QPS_SESSION_COOKIE_PREFIX)
