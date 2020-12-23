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

from google.datacatalog_connectors.qlik.scrape import base_engine_api_helper


class EngineAPISheetsHelper(base_engine_api_helper.BaseEngineAPIHelper):
    # Keys to be used in the pending reponse ids dict.
    __DOC_INTERFACES = 'doc_interfaces'
    __SHEETS = 'sheets'

    def __init__(self, server_address, auth_cookie):
        super().__init__(server_address, auth_cookie)
        self.__responses_queue = []

    def get_sheets(self, app_id):
        return self._run_until_complete(self.__get_sheets(app_id))

    async def __get_sheets(self, app_id):
        async with self._connect_websocket(app_id) as websocket:
            pending_resp_ids = self.__init_pending_response_ids_dict()
            await self.__start_stream(app_id, websocket, pending_resp_ids)
            return await self.__get_sheets_stream_handler(
                websocket, pending_resp_ids)

    async def __get_sheets_stream_handler(self, websocket, pending_resp_ids):
        # Used by the consumer handler to notify the producer on new responses,
        # so the producer can take actions such as sending follow up requests.
        new_response_event = asyncio.Event()

        # The 'results' array is expected to have two elements. The first one
        # stores the result of the consumer handler, which means the sheets to
        # be returned on a successfull scraping execution. The second one
        # stores the result of the producer handler and can be ignored.
        results = await asyncio.gather(*[
            self.__get_sheets_consumer_handler(websocket, pending_resp_ids,
                                               new_response_event),
            self.__get_sheets_producer_handler(websocket, pending_resp_ids,
                                               new_response_event)
        ])
        return results[0]

    async def __get_sheets_consumer_handler(self, websocket, pending_resp_ids,
                                            new_response_event):

        sheets = []
        async for message in websocket:
            response = json.loads(message)
            response_id = response.get('id')
            if not response_id:
                self._handle_generic_api_response(response)
                continue

            logging.debug('Response received: %d', response_id)
            if response_id in pending_resp_ids[self.__SHEETS]:
                sheets.extend(response.get('result').get('qList'))
                pending_resp_ids[self.__SHEETS].remove(response_id)
            else:
                self.__responses_queue.append(response)

            new_response_event.set()

            if self._all_responses_received(pending_resp_ids):
                return sheets

    async def __get_sheets_producer_handler(self, websocket, pending_resp_ids,
                                            new_response_event):

        response_queue = self.__responses_queue
        while not self._all_responses_received(pending_resp_ids):
            if not new_response_event.is_set():
                await new_response_event.wait()
                new_response_event.clear()
            for response in response_queue:
                follow_up_req_id = \
                    await self.__handle_get_sheets_stream_response(
                        websocket, pending_resp_ids, response)
                if follow_up_req_id:
                    response_queue.remove(response)

    async def __handle_get_sheets_stream_response(self, websocket,
                                                  pending_resp_ids, response):

        response_id = response.get('id')
        follow_up_req_id = None
        if response_id in pending_resp_ids[self.__DOC_INTERFACES]:
            doc_handle = response.get('result').get('qReturn').get('qHandle')
            follow_up_req_id = await self.__send_get_sheets_request(
                websocket, doc_handle)
            pending_resp_ids[self.__SHEETS].append(follow_up_req_id)
            pending_resp_ids[self.__DOC_INTERFACES].remove(response_id)

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
    def __init_pending_response_ids_dict(cls):
        return {
            cls.__DOC_INTERFACES: [],
            cls.__SHEETS: [],
        }

    async def __start_stream(self, app_id, websocket, pending_resp_ids):
        request_id = await self._send_open_doc_interface_request(
            app_id, websocket)
        pending_resp_ids[self.__DOC_INTERFACES].append(request_id)
