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

import jmespath
from urllib.parse import urlparse
import websockets

from google.datacatalog_connectors.qlik.scrape import constants


class BaseEngineAPIHelper(abc.ABC):
    """The base class for all Engine API Helpers."""

    # Methods to be used in the requests.
    _GET_ALL_INFOS = 'GetAllInfos'
    _OPEN_DOC = 'OpenDoc'

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
        self.__requests_counter_thread_lock = threading.Lock()

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
        with self.__requests_counter_thread_lock:
            self.__requests_counter += 1
            return self.__requests_counter

    @classmethod
    def _run_until_complete(cls, future):
        event_loop = asyncio.new_event_loop()
        try:
            return event_loop.run_until_complete(future)
        except asyncio.TimeoutError:
            cls.__handle_event_loop_exec_timeout(event_loop)
            raise

    @classmethod
    def __handle_event_loop_exec_timeout(cls, event_loop):
        logging.warning(
            'Timeout reached during the websocket communication session.')
        for task in asyncio.Task.all_tasks(loop=event_loop):
            task.cancel()
        event_loop.stop()

    async def _start_websocket_communication(self, websocket, app_id,
                                             responses_manager):

        request_id = \
            await self.__send_open_doc_request(websocket, app_id)
        responses_manager.add_pending_id(request_id, self._OPEN_DOC)

    @classmethod
    async def _consume_messages(cls, websocket, responses_manager,
                                result_method, result_path):

        results = []
        async for message in websocket:
            response = json.loads(message)
            response_id = response.get('id')
            if not response_id:
                cls.__handle_generic_api_response(response)
                continue

            logging.debug('Response received: %d', response_id)
            if responses_manager.is_pending(response_id, result_method):
                result = jmespath.search(result_path, response)
                if isinstance(result, list):
                    results.extend(result)
                else:
                    results.append(result)
            else:
                responses_manager.add_unhandled(response)

            responses_manager.remove_pending_id(response_id)
            responses_manager.notify_new_response()

        return results

    @classmethod
    def __handle_generic_api_response(cls, response):
        cls.__handle_error_api_response(response)

    @classmethod
    def __handle_error_api_response(cls, response):
        method = response.get('method')
        if 'OnMaxParallelSessionsExceeded' == method:
            message = response.get('params').get('message')
            logging.warning(message)
            raise Exception(message)

    @classmethod
    async def _produce_messages(cls, websocket, responses_manager, producer):
        while not responses_manager.were_all_precessed():
            if not responses_manager.is_there_response_notification():
                await responses_manager.wait_for_responses()
                responses_manager.clear_response_notifications()
            for response in responses_manager.get_all_unhandled():
                await producer(websocket, responses_manager, response)

        # Closes the websocket when there is no further response to process.
        await websocket.close()

    @classmethod
    async def _handle_websocket_communication(cls, consumer_future,
                                              producer_future):

        # The 'results' array is expected to have two elements. The first one
        # stores the result of the consumer, which means the object to be
        # returned on a successfull execution. The second one stores the result
        # of the producer and can be ignored.
        results = await asyncio.gather(*[consumer_future, producer_future])
        return results[0]

    async def __send_open_doc_request(self, websocket, app_id):
        """Sends a Open Doc (aka App) Interface request.

        Returns:
            The request id.
        """
        request_id = self._generate_request_id()
        await websocket.send(
            json.dumps({
                'handle': -1,
                'method': self._OPEN_DOC,
                'params': [app_id],
                'id': request_id
            }))

        logging.debug('Open Doc Interface request sent: %d', request_id)
        return request_id

    async def _send_get_all_infos_request(self, websocket, doc_handle):
        """Sends a Get All Infos request.

        Returns:
            The request id.
        """
        request_id = self._generate_request_id()
        await websocket.send(
            json.dumps({
                'handle': doc_handle,
                'method': self._GET_ALL_INFOS,
                'params': {},
                'id': request_id,
            }))

        logging.debug('Get All Infos request sent: %d', request_id)
        return request_id
