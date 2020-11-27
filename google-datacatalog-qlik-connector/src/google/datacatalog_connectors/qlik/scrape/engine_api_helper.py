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

import asyncio
import json
import random

from urllib.parse import urlparse
import websockets

from google.datacatalog_connectors.qlik.scrape import constants


class EngineAPIHelper:
    """Wraps requests to the Qlik Qlik Engine JSON API.

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

    """

    def __init__(self, server_address):
        self.__server_address = server_address
        # The server address starts with an http/https scheme. The below
        # statement replaces the original scheme with 'wss', which is used for
        # secure websockets communication.
        self.__base_api_endpoint = f'wss://{urlparse(server_address).hostname}'
        self.__common_headers = {
            constants.XRFKEY_HEADER_NAME: constants.XRFKEY,
        }

    def get_sheets(self, app_id, auth_cookie):
        """Get the list of Sheets that belong to a given App.

        Returns:
            A list of sheets.
        """
        return self.__run_until_complete(self.__get_sheets(
            app_id, auth_cookie))

    async def __get_sheets(self, app_id, auth_cookie):
        async with self.__connect_websocket(app_id, auth_cookie) as websocket:
            open_doc_return = await self.__open_doc(app_id, websocket)

            request_id = self.__generate_request_id()
            await websocket.send(
                json.dumps({
                    'handle': open_doc_return.get('qHandle'),
                    'method': 'GetObjects',
                    'params': {
                        'qOptions': {
                            'qTypes': ['sheet'],
                        },
                    },
                    'id': request_id,
                }))

            async for message in websocket:
                json_message = json.loads(message)
                response_id = json_message.get('id')
                if request_id == response_id:
                    return json_message.get('result').get('qList')

    def __connect_websocket(self, app_id, auth_cookie):
        """Open websocket connection.

        Args:
            app_id:
              An App ID to be appended to the Engine API URLs when making
              websocket connections and then get an isolated Qlik Engine
              session for each Qlik App (see [Connecting to the Qlik Engine
              JSON API](https://help.qlik.com/en-US/sense-developer/November2020/Subsystems/EngineAPI/Content/Sense_EngineAPI/GettingStarted/connecting-to-engine-api.htm)).  # noqa E510
            auth_cookie:
              An HTTP cookie used to authorize the requests.

        Returns:
            An awaiting function that yields a :class:`WebSocketClientProtocol`
            which can then be used to send and receive messages.
        """
        uri = f'{self.__base_api_endpoint}/app/{app_id}' \
              f'?Xrfkey={constants.XRFKEY}'

        headers = self.__common_headers.copy()
        # Format the header value as <key>=<value> string.
        headers['Cookie'] = f'{auth_cookie.name}={auth_cookie.value}'

        return websockets.connect(uri=uri, extra_headers=headers)

    @classmethod
    async def __open_doc(cls, app_id, websocket):
        """Open an App (aka Doc) to enable calling subsequent methods of it,
        e.g. `GetObjects`.

        Returns:
            An [ObjectInterface](https://help.qlik.com/en-US/sense-developer/November2020/APIs/EngineAPI/definitions-ObjectInterface.html).  # noqa E501
        """
        request_id = cls.__generate_request_id()
        await websocket.send(
            json.dumps({
                'handle': -1,
                'method': 'OpenDoc',
                'params': [app_id],
                'id': request_id
            }))

        async for message in websocket:
            json_message = json.loads(message)
            response_id = json_message.get('id')
            if request_id == response_id:
                return json_message.get('result').get('qReturn')

    def get_windows_authentication_url(self):
        return self.__run_until_complete(
            self.__get_windows_authentication_url())

    async def __get_windows_authentication_url(self):
        """Get a Windows Authentication url.

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

    @classmethod
    def __generate_request_id(cls):
        return random.randint(1, 9999)

    @classmethod
    def __run_until_complete(cls, async_method_call):
        loop = asyncio.get_event_loop()
        return loop.run_until_complete(async_method_call)
