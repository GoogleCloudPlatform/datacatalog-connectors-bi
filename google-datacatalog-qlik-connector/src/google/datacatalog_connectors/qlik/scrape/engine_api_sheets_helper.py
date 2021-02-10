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

from google.datacatalog_connectors.qlik.scrape import \
    base_engine_api_helper, websocket_replies_helper


class EngineAPISheetsHelper(base_engine_api_helper.BaseEngineAPIHelper):
    # Methods to be used in the requests.
    __GET_OBJECTS = 'GetObjects'

    def get_sheets(self, app_id, timeout=60):
        try:
            return self._run_until_complete(self.__get_sheets(app_id, timeout))
        except Exception as e:
            logging.warning("error on get_sheets:", exc_info=True)
            if isinstance(e, asyncio.TimeoutError):
                return []
            else:
                raise

    async def __get_sheets(self, app_id, timeout):
        async with self._connect_websocket(app_id) as websocket:
            replies_helper = \
                websocket_replies_helper.WebsocketRepliesHelper()

            await self._start_websocket_communication(websocket, app_id,
                                                      replies_helper)

            sender = self.__send_get_sheets_msg
            receiver = self.__receive_get_sheets_msg
            return await asyncio.wait_for(
                self._hold_websocket_communication(
                    sender(websocket, replies_helper),
                    receiver(websocket, replies_helper)), timeout)

    async def __receive_get_sheets_msg(self, websocket, replies_helper):
        return await self._receive_messages(websocket, replies_helper,
                                            self.__GET_OBJECTS, 'result.qList')

    async def __send_get_sheets_msg(self, websocket, replies_helper):
        return await self._send_messages(websocket, replies_helper,
                                         self.__send_follow_up_msg_get_sheets)

    async def __send_follow_up_msg_get_sheets(self, websocket, replies_helper,
                                              response):

        response_id = response.get('id')
        if replies_helper.is_method(response_id, self._OPEN_DOC):
            await self.__handle_open_doc_reply(websocket, replies_helper,
                                               response)
            replies_helper.remove_unhandled(response)

    async def __handle_open_doc_reply(self, websocket, replies_helper,
                                      response):

        doc_handle = response.get('result').get('qReturn').get('qHandle')
        follow_up_req_id = await self.__send_get_sheets_message(
            websocket, doc_handle)
        replies_helper.add_pending_id(follow_up_req_id, self.__GET_OBJECTS)

    async def __send_get_sheets_message(self, websocket, doc_handle):
        """Sends a Get Objects message for the sheet type.

        Returns:
            The message id.
        """
        message_id = self._generate_message_id()
        await websocket.send(
            json.dumps({
                'handle': doc_handle,
                'method': self.__GET_OBJECTS,
                'params': {
                    'qOptions': {
                        'qTypes': ['sheet'],
                    },
                },
                'id': message_id,
            }))

        logging.debug('Get Objects (type=sheet) message sent: %d', message_id)
        return message_id
