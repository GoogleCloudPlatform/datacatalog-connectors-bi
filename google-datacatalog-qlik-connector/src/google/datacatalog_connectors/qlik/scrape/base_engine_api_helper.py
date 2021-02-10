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
        self.__messages_counter = 0
        self.__messages_counter_thread_lock = threading.Lock()

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

    @classmethod
    def _run_until_complete(cls, future):
        event_loop = asyncio.new_event_loop()
        try:
            return event_loop.run_until_complete(future)
        except Exception:
            logging.warning(
                'Something wrong happened while running the event loop.')
            cls.__cancel_all_tasks(event_loop)
            raise
        finally:
            event_loop.close()

    @classmethod
    def __cancel_all_tasks(cls, event_loop):
        logging.info('All tasks will be canceled...')
        for task in asyncio.Task.all_tasks(loop=event_loop):
            task.cancel()

    async def _start_websocket_communication(self, websocket, app_id,
                                             replies_helper):

        request_id = \
            await self.__send_open_doc_message(websocket, app_id)
        replies_helper.add_pending_id(request_id, self._OPEN_DOC)

    @classmethod
    async def _hold_websocket_communication(cls, msg_sender, msg_receiver):
        """Holds a websocket communication session until the awaitable message
        sender and receiver are done.

        Args:
            msg_sender: A coroutine or future that sends messages.
            msg_receiver: A coroutine or future that receives messages.

        Returns:
            The result of the receiver.
        """
        # The ``results`` list is expected to have two elements. The first one
        # stores the message sender's result and can be ignored. The second one
        # stores the receiver's result, which means the object to be returned
        # on successful execution.
        results = await asyncio.gather(*[msg_sender, msg_receiver])
        return results[1]

    @classmethod
    async def _receive_messages(cls, websocket, replies_helper, result_method,
                                result_path):

        results = []
        async for message in websocket:
            message_json = json.loads(message)
            message_id = message_json.get('id')
            if not message_id:
                cls.__handle_generic_api_message(message_json)
                continue

            logging.debug('Reply received: %d', message_id)
            if replies_helper.is_pending(message_id, result_method):
                result = jmespath.search(result_path, message_json)
                if isinstance(result, list):
                    results.extend(result)
                else:
                    results.append(result)
            else:
                replies_helper.add_unhandled(message_json)

            replies_helper.remove_pending_id(message_id)
            replies_helper.notify_new_reply()

        return results

    @classmethod
    def __handle_generic_api_message(cls, message):
        cls.__handle_error_api_message(message)

    @classmethod
    def __handle_error_api_message(cls, message):
        method = message.get('method')
        if 'OnMaxParallelSessionsExceeded' == method:
            error_message = message.get('params').get('message')
            logging.warning(error_message)
            raise Exception(error_message)

    @classmethod
    async def _send_messages(cls, websocket, replies_helper, sender):
        while not replies_helper.were_all_processed():
            if not replies_helper.is_there_reply_notification():
                await replies_helper.wait_for_replies()
                replies_helper.clear_reply_notifications()
            for reply in replies_helper.get_all_unhandled():
                await sender(websocket, replies_helper, reply)

        # Closes the websocket when there are no more replies to be processed.
        await websocket.close()

    async def __send_open_doc_message(self, websocket, app_id):
        """Sends a Open Doc (aka App) Interface message.

        Returns:
            The message id.
        """
        message_id = self._generate_message_id()
        await websocket.send(
            json.dumps({
                'handle': -1,
                'method': self._OPEN_DOC,
                'params': [app_id],
                'id': message_id
            }))

        logging.debug('Open Doc Interface message sent: %d', message_id)
        return message_id

    async def _send_get_all_infos_message(self, websocket, doc_handle):
        """Sends a Get All Infos message.

        Returns:
            The message id.
        """
        message_id = self._generate_message_id()
        await websocket.send(
            json.dumps({
                'handle': doc_handle,
                'method': self._GET_ALL_INFOS,
                'params': {},
                'id': message_id,
            }))

        logging.debug('Get All Infos message sent: %d', message_id)
        return message_id

    def _generate_message_id(self):
        with self.__messages_counter_thread_lock:
            self.__messages_counter += 1
            return self.__messages_counter
