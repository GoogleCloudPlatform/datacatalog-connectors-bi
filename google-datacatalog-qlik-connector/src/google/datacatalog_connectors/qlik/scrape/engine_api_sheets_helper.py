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

import json
import logging

from google.datacatalog_connectors.qlik.scrape import \
    base_engine_api_helper, stream_responses_manager


class EngineAPISheetsHelper(base_engine_api_helper.BaseEngineAPIHelper):
    # Keys to be used in the pending reponse ids lists.
    __DOC_INTERFACES = 'doc_interfaces'
    __SHEETS = 'sheets'

    def get_sheets(self, app_id):
        return self._run_until_complete(self.__get_sheets(app_id))

    async def __get_sheets(self, app_id):
        async with self._connect_websocket(app_id) as websocket:
            responses_manager = \
                stream_responses_manager.StreamResponsesManager()
            self.__init_pending_response_ids_lists(responses_manager)

            await self.__start_stream(app_id, websocket, responses_manager)

            return await self._handle_stream(
                self.__get_sheets_stream_consumer(websocket,
                                                  responses_manager),
                self.__get_sheets_stream_producer(websocket,
                                                  responses_manager))

    async def __get_sheets_stream_consumer(self, websocket, responses_manager):
        sheets = []
        async for message in websocket:
            response = json.loads(message)
            response_id = response.get('id')
            if not response_id:
                self._handle_generic_api_response(response)
                continue

            logging.debug('Response received: %d', response_id)
            if responses_manager.is_pending(response_id, self.__SHEETS):
                sheets.extend(response.get('result').get('qList'))
                responses_manager.remove_pending_id(response_id)
            else:
                responses_manager.add_unhandled(response)

            responses_manager.notify_new_response()
            if responses_manager.were_all_received():
                return sheets

    async def __get_sheets_stream_producer(self, websocket, responses_manager):
        while not responses_manager.were_all_received():
            if not responses_manager.is_there_response_notification():
                await responses_manager.wait_for_responses()
                responses_manager.clear_response_notifications()
            for response in responses_manager.get_all_unhandled():
                follow_up_req_id = \
                    await self.__send_follow_up_get_sheets_stream_request(
                        websocket, responses_manager, response)
                if follow_up_req_id:
                    responses_manager.remove_unhandled(response)

    async def __send_follow_up_get_sheets_stream_request(
            self, websocket, responses_manager, response):

        response_id = response.get('id')
        follow_up_req_id = None
        if responses_manager.is_pending(response_id, self.__DOC_INTERFACES):
            doc_handle = response.get('result').get('qReturn').get('qHandle')
            follow_up_req_id = await self.__send_get_sheets_request(
                websocket, doc_handle)
            responses_manager.add_pending_id(follow_up_req_id, self.__SHEETS)
            responses_manager.remove_pending_id(response_id)

        return follow_up_req_id

    async def __send_get_sheets_request(self, websocket, doc_handle):
        """Sends a Get Objects request for the sheet type.

        Returns:
            The request id.
        """
        request_id = self._generate_request_id()
        await websocket.send(
            json.dumps({
                'handle': doc_handle,
                'method': 'GetObjects',
                'params': {
                    'qOptions': {
                        'qTypes': ['sheet'],
                    },
                },
                'id': request_id,
            }))

        logging.debug('Get Objects (type=sheet) request sent: %d', request_id)
        return request_id

    @classmethod
    def __init_pending_response_ids_lists(cls, responses_manager):
        responses_manager.add_pending_ids_lists(
            [cls.__DOC_INTERFACES, cls.__SHEETS])

    async def __start_stream(self, app_id, websocket, responses_manager):
        request_id = await self._send_open_doc_interface_request(
            app_id, websocket)
        responses_manager.add_pending_id(request_id, self.__DOC_INTERFACES)
