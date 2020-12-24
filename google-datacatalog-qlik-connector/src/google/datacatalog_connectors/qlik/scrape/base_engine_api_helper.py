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

import abc
import asyncio
import json
import logging
import threading

from urllib.parse import urlparse
import websockets

from google.datacatalog_connectors.qlik.scrape import constants


class BaseEngineAPIHelper(abc.ABC):
    """The base class for all Engine API Helpers."""

    def __init__(self, server_address, auth_cookie):
        # The server address starts with an http/https scheme. The below
        # statement replaces the original scheme with 'wss', which is used for
        # secure websockets communication.
        self.__base_api_endpoint = f'wss://{urlparse(server_address).hostname}'
        self.__auth_cookie = auth_cookie
        self.__common_headers = {
            constants.XRFKEY_HEADER_NAME: constants.XRFKEY,
        }
        self.__requests_counter = 0

    def _connect_websocket(self, app_id):
        """Opens a websocket connection.

        Args:
            app_id:
              An App ID to be appended to the Engine API URLs when making
              websocket connections and then get an isolated Qlik Engine
              session for each Qlik App (see [Connecting to the Qlik Engine
              JSON API](https://help.qlik.com/en-US/sense-developer/September2020/Subsystems/EngineAPI/Content/Sense_EngineAPI/GettingStarted/connecting-to-engine-api.htm)).  # noqa E510

        Returns:
            An awaiting function that yields a :class:`WebSocketClientProtocol`
            which can then be used to send and receive messages.
        """
        uri = f'{self.__base_api_endpoint}/app/{app_id}' \
              f'?Xrfkey={constants.XRFKEY}'

        auth_cookie = self.__auth_cookie
        headers = self.__common_headers.copy()
        # Format the header value as <key>=<value> string.
        headers['Cookie'] = f'{auth_cookie.name}={auth_cookie.value}'

        return websockets.connect(uri=uri, extra_headers=headers)

    def _generate_request_id(self):
        self.__requests_counter += 1
        return self.__requests_counter

    @classmethod
    def _run_until_complete(cls, future, max_seconds_to_wait):
        loop = asyncio.get_event_loop()
        timeout_controller = threading.Timer(
            interval=max_seconds_to_wait,
            function=cls._handle_event_loop_exec_timeout,
            args=(loop,))
        timeout_controller.start()
        result = loop.run_until_complete(future)
        timeout_controller.cancel()
        return result

    @classmethod
    def _handle_event_loop_exec_timeout(cls, event_loop):
        logging.warning(
            'Timeout reached during the websocket communication session.')
        event_loop.stop()

    @classmethod
    async def _handle_websocket_communication(cls, consumer_future,
                                              producer_future):

        # The 'results' array is expected to have two elements. The first one
        # stores the result of the consumer, which means the object to be
        # returned on a successfull execution. The second one stores the result
        # of the producer and can be ignored.
        results = await asyncio.gather(*[consumer_future, producer_future])
        return results[0]

    async def _send_open_doc_interface_request(self, app_id, websocket):
        """Sends a Open Doc (aka App) Interface request.

        Returns:
            The request id.
        """
        request_id = self._generate_request_id()
        await websocket.send(
            json.dumps({
                'handle': -1,
                'method': 'OpenDoc',
                'params': [app_id],
                'id': request_id
            }))

        logging.debug('Open Doc Interface request sent: %d', request_id)
        return request_id

    async def _send_get_all_infos_request(self, doc_handle, websocket):
        """Sends a Get All Infos request.

        Returns:
            The request id.
        """
        request_id = self._generate_request_id()
        await websocket.send(
            json.dumps({
                'handle': doc_handle,
                'method': 'GetAllInfos',
                'params': {},
                'id': request_id,
            }))

        logging.debug('Get All Infos request sent: %d', request_id)
        return request_id

    @classmethod
    def _handle_generic_api_response(cls, response):
        cls._handle_error_api_response(response)

    @classmethod
    def _handle_error_api_response(cls, response):
        method = response.get('method')
        if 'OnMaxParallelSessionsExceeded' == method:
            message = response.get('params').get('message')
            logging.warning(message)
            raise Exception(message)
