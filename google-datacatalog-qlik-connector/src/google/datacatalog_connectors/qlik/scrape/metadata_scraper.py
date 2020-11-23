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

from requests import sessions

from google.datacatalog_connectors.qlik.scrape import \
    authenticator, constants, repository_services_api_helper


class MetadataScraper:

    def __init__(self, server_address, ad_domain, username, password):
        self.__server_address = server_address
        self.__ad_domain = ad_domain
        self.__username = username
        self.__password = password
        self.__session = sessions.Session()
        self.__qrs_api_helper = \
            repository_services_api_helper.RepositoryServicesAPIHelper(
                server_address)

    def scrape_all_apps(self):
        self.__set_qps_session_cookie()
        return self.__qrs_api_helper.get_full_app_list(self.__session)

    def scrape_all_streams(self):
        self.__set_qps_session_cookie()
        return self.__qrs_api_helper.get_full_stream_list(self.__session)

    def __set_qps_session_cookie(self):
        cookie = None

        if self.__session.cookies:
            cookie = next(
                cookie for cookie in self.__session.cookies
                if cookie.name.startswith(constants.QPS_SESSION_COOKIE_PREFIX))

        if not cookie:
            windows_auth_url = \
                self.__qrs_api_helper.get_windows_authentication_url(
                    self.__session)
            qps_session_cookie = authenticator.Authenticator\
                .get_qps_session_cookie_windows_auth(
                    ad_domain=self.__ad_domain,
                    username=self.__username,
                    password=self.__password,
                    auth_url=windows_auth_url)
            self.__session.cookies.set_cookie(qps_session_cookie)
            logging.debug('New QPS session cookie set: %s', qps_session_cookie)
