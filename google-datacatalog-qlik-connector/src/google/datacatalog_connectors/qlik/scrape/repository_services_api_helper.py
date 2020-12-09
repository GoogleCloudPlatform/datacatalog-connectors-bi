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
from requests import sessions

from google.datacatalog_connectors.qlik.scrape import authenticator, constants


class RepositoryServicesAPIHelper:
    """Wraps requests to the Qlik Sense Repository Service (QRS).

    The Qlik Sense Repository Service (QRS) manages persistence and
    synchronization of Qlik Sense apps, licensing, security, and service
    configuration data.

    QRS returns full objects when /full is included in the request path;
    otherwise, it returns condensed objects. Full objects are preferred
    because they allow the connector to read a richer metadata set (see [Full
    versus condensed objects](https://help.qlik.com/en-US/sense-developer/September2020/Subsystems/RepositoryServiceAPI/Content/Sense_RepositoryServiceAPI/RepositoryServiceAPI-Connect-API-Full-vs-Condensed-Objects.htm)  # noqa E501
    for more information).

    Attributes:
        __http_session: An HTTP session for the QRS RESP API calls.

    """

    def __init__(self, server_address, ad_domain, username, password):
        self.__server_address = server_address
        self.__ad_domain = ad_domain
        self.__username = username
        self.__password = password

        self.__base_api_endpoint = f'{server_address}/qrs'
        self.__common_headers = {
            'Content-Type': constants.JSON_CONTENT_TYPE,
            'Accept': constants.JSON_CONTENT_TYPE,
            constants.XRFKEY_HEADER_NAME: constants.XRFKEY,
        }

        self.__http_session = None

    def get_full_app_list(self):
        """Get the list of all Apps that can be opened by the current user,
        via the current proxy.

        Returns:
            A list of full App metadata objects.
        """
        return self.__execute_api_call('app/hublist/full')

    def get_full_custom_property_definition_list(self):
        """Get the list of Custom Property Definitions with full metadata from
        a given server.

        Returns:
            A list of full Custom Property Definition metadata objects.
        """
        return self.__execute_api_call('custompropertydefinition/full')

    def get_full_stream_list(self):
        """Get the list of Streams with full metadata from a given server.

        Returns:
            A list of full Stream metadata objects.
        """
        return self.__execute_api_call('stream/full')

    def __execute_api_call(self, resource_path):
        self.__set_up_http_session()

        url = f'{self.__base_api_endpoint}/{resource_path}' \
              f'?Xrfkey={constants.XRFKEY}'

        return self.__http_session.get(url=url,
                                       headers=self.__common_headers).json()

    def __set_up_http_session(self):
        if self.__http_session:
            return

        self.__http_session = sessions.Session()

        windows_auth_url = self.__get_windows_authentication_url(
            self.__http_session)
        qps_session_cookie = authenticator.Authenticator \
            .get_qps_session_cookie_windows_auth(
                ad_domain=self.__ad_domain,
                username=self.__username,
                password=self.__password,
                auth_url=windows_auth_url)

        self.__http_session.cookies.set_cookie(qps_session_cookie)
        logging.debug('QPS session cookie issued for the QRS API: %s',
                      qps_session_cookie)

    def __get_windows_authentication_url(self, session):
        """Get a Windows Authentication url.

        This method sends an unauthenticated request to a well known endpoint
        of the Qlik Sense Repository Service API. The expected response has a
        302 status code and a `Location` header, which is the Windows
        Authentication url.

        Returns:
            A string.
        """
        url = f'{self.__base_api_endpoint}/about?Xrfkey={constants.XRFKEY}'

        # Sets the User-Agent to Windows temporarily to get a Windows
        # Authentication URL that is required by the NTLM authentication flow.
        headers = self.__common_headers.copy()
        headers['User-Agent'] = constants.WINDOWS_USER_AGENT

        response = requests.get(url=url,
                                headers=headers,
                                allow_redirects=False)

        return session.get_redirect_target(response)
