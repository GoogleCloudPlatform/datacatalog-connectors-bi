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
    authenticator, engine_api_helper, repository_services_api_helper


class MetadataScraper:
    """A Facade that provides a simplified interface for the Qlik services,
    comprising the interactions between Qlik Sense Proxy Service (QPS), Qlik
    Sense Repository Service (QRS), and Qlik Engine JSON API.

    Attributes:
        __qrs_api_session: An HTTP session for the QRS RESP API calls.
        __engine_api_auth_cookie: A cookie to authenticate the provided user
          in the Qlik Engine JSON API.

    """

    def __init__(self, server_address, ad_domain, username, password):
        self.__server_address = server_address
        self.__ad_domain = ad_domain
        self.__username = username
        self.__password = password

        self.__qrs_api_helper = \
            repository_services_api_helper.RepositoryServicesAPIHelper(
                server_address)
        self.__engine_api_helper = \
            engine_api_helper.EngineAPIHelper(server_address)

        self.__qrs_api_session = None
        self.__engine_api_auth_cookie = None

    def scrape_all_apps(self):
        self.__initialize_qrs_api_session()
        return self.__qrs_api_helper.get_full_app_list(self.__qrs_api_session)

    def scrape_all_streams(self):
        self.__initialize_qrs_api_session()
        return self.__qrs_api_helper.get_full_stream_list(
            self.__qrs_api_session)

    def __initialize_qrs_api_session(self):
        if self.__qrs_api_session:
            return

        self.__qrs_api_session = sessions.Session()

        windows_auth_url = \
            self.__qrs_api_helper.get_windows_authentication_url(
                self.__qrs_api_session)
        qps_session_cookie = authenticator.Authenticator \
            .get_qps_session_cookie_windows_auth(
                ad_domain=self.__ad_domain,
                username=self.__username,
                password=self.__password,
                auth_url=windows_auth_url)

        self.__qrs_api_session.cookies.set_cookie(qps_session_cookie)
        logging.debug('QPS session cookie issued for the QRS API: %s',
                      qps_session_cookie)

    def scrape_sheets(self, app):
        self.__initialize_engine_api_auth_cookie()
        app_id = app.get('id')
        return self.__engine_api_helper.get_sheets(
            app_id, self.__engine_api_auth_cookie)

    def __initialize_engine_api_auth_cookie(self):
        if self.__engine_api_auth_cookie:
            return

        windows_auth_url = \
            self.__engine_api_helper.get_windows_authentication_url()
        self.__engine_api_auth_cookie = authenticator.Authenticator\
            .get_qps_session_cookie_windows_auth(
                ad_domain=self.__ad_domain,
                username=self.__username,
                password=self.__password,
                auth_url=windows_auth_url)
        logging.debug('QPS session cookie issued for the Engine API: %s',
                      self.__engine_api_auth_cookie)
