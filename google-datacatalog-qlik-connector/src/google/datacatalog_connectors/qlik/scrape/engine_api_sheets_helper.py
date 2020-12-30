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
import logging

from google.datacatalog_connectors.qlik.scrape import \
    base_engine_api_helper, websocket_responses_manager


class EngineAPISheetsHelper(base_engine_api_helper.BaseEngineAPIHelper):
    # Methods to be used in the requests.
    __GET_OBJECTS = 'GetObjects'

    def get_sheets(self, app_id, timeout=60):
        try:
            return self._run_until_complete(self.__get_sheets(app_id, timeout))
        except Exception:
            logging.getLogger().warning("error on get_sheets:", exc_info=True)
            return []

    async def __get_sheets(self, app_id, timeout):
        async with self._connect_websocket(app_id) as websocket:
            responses_manager = \
                websocket_responses_manager.WebsocketResponsesManager()

            await self._start_websocket_communication(websocket, app_id,
                                                      responses_manager)

            consumer = self.__get_sheets_msg_consumer
            producer = self.__get_sheets_msg_producer
            return await asyncio.wait_for(
                self._handle_websocket_communication(
                    consumer(websocket, responses_manager),
                    producer(websocket, responses_manager)), timeout)

    async def __get_sheets_msg_consumer(self, websocket, responses_manager):
        sheets = []
        async for message in websocket:
            response = json.loads(message)
            response_id = response.get('id')
            if not response_id:
                self._handle_generic_api_response(response)
                continue

            logging.debug('Response received: %d', response_id)
            if responses_manager.is_pending(response_id, self.__GET_OBJECTS):
                sheets.extend(response.get('result').get('qList'))
            else:
                responses_manager.add_unhandled(response)

            responses_manager.notify_new_response()
            responses_manager.remove_pending_id(response_id)

        return sheets

    async def __get_sheets_msg_producer(self, websocket, responses_manager):
        while not responses_manager.were_all_precessed():
            if not responses_manager.is_there_response_notification():
                await responses_manager.wait_for_responses()
                responses_manager.clear_response_notifications()
            for response in responses_manager.get_all_unhandled():
                await self.__send_follow_up_msg_get_sheets(
                    websocket, responses_manager, response)

        await websocket.close()

    async def __send_follow_up_msg_get_sheets(self, websocket,
                                              responses_manager, response):

        response_id = response.get('id')
        if responses_manager.is_method(response_id, self._OPEN_DOC):
            await self.__handle_open_doc_response(websocket, responses_manager,
                                                  response)
            responses_manager.remove_unhandled(response)

    async def __handle_open_doc_response(self, websocket, responses_manager,
                                         response):

        doc_handle = response.get('result').get('qReturn').get('qHandle')
        follow_up_req_id = await self.__send_get_sheets_request(
            websocket, doc_handle)
        responses_manager.add_pending_id(follow_up_req_id, self.__GET_OBJECTS)

    async def __send_get_sheets_request(self, websocket, doc_handle):
        """Sends a Get Objects request for the sheet type.

        Returns:
            The request id.
        """
        request_id = self._generate_request_id()
        await websocket.send(
            json.dumps({
                'handle': doc_handle,
                'method': self.__GET_OBJECTS,
                'params': {
                    'qOptions': {
                        'qTypes': ['sheet'],
                    },
                },
                'id': request_id,
            }))

        logging.debug('Get Objects (type=sheet) request sent: %d', request_id)
        return request_id
