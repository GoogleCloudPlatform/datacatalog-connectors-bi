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
from requests_ntlm import HttpNtlmAuth


class Authenticator:
    __QPS_SESSION_COOKIE_PREFIX = 'X-Qlik-Session'

    @classmethod
    def get_qps_session_cookie_windows_auth(cls, domain, username, password,
                                            auth_url):

        # Set up user credentials
        user_auth = HttpNtlmAuth(username=f'{domain}\\{username}',
                                 password=password)

        headers = {'x-Qlik-Xrfkey': 'application/json'}

        response = requests.get(url=auth_url, auth=user_auth, headers=headers)

        if not response.cookies:
            return

        try:
            return next(
                cookie for cookie in response.cookies
                if cookie.name.startswith(cls.__QPS_SESSION_COOKIE_PREFIX))
        except StopIteration:
            pass
