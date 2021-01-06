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

import asyncio
import json
import logging

from urllib.parse import urlparse
import websockets

from google.datacatalog_connectors.qlik.scrape import \
    authenticator, constants, engine_api_dimensions_helper, \
    engine_api_measures_helper, engine_api_sheets_helper, \
    engine_api_visualizations_helper


class EngineAPIScraper:
    """Wraps requests to the Qlik Engine JSON API.

    The Qlik Engine JSON API is a WebSocket protocol that uses JSON to pass
    information between the Qlik associative engine and the clients. This API
    consists of a set of objects representing apps, lists, and so on. These
    objects are organized in a hierarchical structure.

    Websockets use an asynchronous communication channel, but the public
    methods from this class are intended to be called synchronously to keep
    consistency with the overall scrape > prepare > ingest  workflow. The
    public methods take care of handling the async API calls for their clients.

    Most private coroutines (async def) rely on 'async with' statements.
    They work with an asynchronous context manager and the connection is closed
    when exiting the context.

    Attributes:
        __auth_cookie: An HTTP cookie used to authorize the requests.
    """

    def __init__(self, server_address, ad_domain, username, password):
        self.__server_address = server_address
        self.__ad_domain = ad_domain
        self.__username = username
        self.__password = password

        # The server address starts with an http/https scheme. The below
        # statement replaces the original scheme with 'wss', which is used for
        # secure websockets communication.
        self.__base_api_endpoint = f'wss://{urlparse(server_address).hostname}'
        self.__common_headers = {
            constants.XRFKEY_HEADER_NAME: constants.XRFKEY,
        }

        self.__auth_cookie = None

    def get_dimensions(self, app_id):
        """Gets the Dimensions (Master Items) set up to a given App.

        Returns:
            A list of [GenericDimensionProperties](https://help.qlik.com/en-US/sense-developer/September2020/APIs/EngineAPI/definitions-GenericDimensionProperties.html).  # noqa E501
        """
        self.__set_up_auth_cookie()
        return engine_api_dimensions_helper.EngineAPIDimensionsHelper(
            self.__server_address, self.__auth_cookie).get_dimensions(app_id)

    def get_measures(self, app_id):
        """Gets the Measures (Master Items) set up to a given App.

        Returns:
            A list of [GenericMeasureProperties](https://help.qlik.com/en-US/sense-developer/September2020/APIs/EngineAPI/definitions-GenericMeasureProperties.html).  # noqa E501
        """
        self.__set_up_auth_cookie()
        return engine_api_measures_helper.EngineAPIMeasuresHelper(
            self.__server_address, self.__auth_cookie).get_measures(app_id)

    def get_sheets(self, app_id):
        """Gets the Sheets that belong to the given App.

        Returns:
            A list of [NxContainerEntry](https://help.qlik.com/en-US/sense-developer/September2020/APIs/EngineAPI/definitions-NxContainerEntry.html).  # noqa E501
        """
        self.__set_up_auth_cookie()
        return engine_api_sheets_helper.EngineAPISheetsHelper(
            self.__server_address, self.__auth_cookie).get_sheets(app_id)

    def get_visualizations(self, app_id):
        """Gets the Visualizations (Master Items) set up to a given App.

        Returns:
            A list of [GenericObjectProperties](https://help.qlik.com/en-US/sense-developer/September2020/APIs/EngineAPI/definitions-GenericObjectProperties.html).  # noqa E501
        """
        self.__set_up_auth_cookie()
        return engine_api_visualizations_helper.EngineAPIVisualizationsHelper(
            self.__server_address,
            self.__auth_cookie).get_visualizations(app_id)

    def __set_up_auth_cookie(self):
        if self.__auth_cookie:
            return

        windows_auth_url = asyncio.get_event_loop().run_until_complete(
            self.__get_windows_authentication_url())
        self.__auth_cookie = authenticator.Authenticator\
            .get_qps_session_cookie_windows_auth(
                ad_domain=self.__ad_domain,
                username=self.__username,
                password=self.__password,
                auth_url=windows_auth_url)
        logging.debug('QPS session cookie issued for the Engine API: %s',
                      self.__auth_cookie)

    async def __get_windows_authentication_url(self):
        """Gets a Windows Authentication url.

        This method sends an unauthenticated request to a well known endpoint
        of the Qlik Engine JSON API. The expected response has a `loginUri`
        param, which is the Windows Authentication url.
        P.S. The endpoint was manually captured from the Engine API Explorer's
        Execution Logs (https://<qlik-site>/dev-hub/engine-api-explorer).

        Returns:
            A string.
        """
        uri = f'{self.__base_api_endpoint}/app/?transient=' \
              f'?Xrfkey={constants.XRFKEY}' \
              f'&reloadUri={self.__server_address}/dev-hub/engine-api-explorer'

        # Sets the User-Agent to Windows temporarily to get a Windows
        # Authentication URL that is required by the NTLM authentication flow.
        headers = self.__common_headers.copy()
        headers['User-Agent'] = constants.WINDOWS_USER_AGENT

        async with websockets.connect(uri=uri,
                                      extra_headers=headers) as websocket:

            async for message in websocket:
                json_message = json.loads(message)
                params = json_message.get('params')
                if params:
                    return params.get('loginUri')
