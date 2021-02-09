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


class EngineAPIMeasuresHelper(base_engine_api_helper.BaseEngineAPIHelper):
    # Keys used to identify the handles.
    __DOC_HANDLE = 'doc-handle'

    # Methods to be used in the requests.
    __GET_MEASURE = 'GetMeasure'
    __GET_PROPERTIES = 'GetProperties'

    def get_measures(self, app_id, timeout=60):
        try:
            return self._run_until_complete(
                self.__get_measures(app_id, timeout))
        except Exception as e:
            logging.warning("error on get_measures:", exc_info=True)
            if isinstance(e, asyncio.TimeoutError):
                return []
            else:
                raise

    async def __get_measures(self, app_id, timeout):
        async with self._connect_websocket(app_id) as websocket:
            replies_helper = websocket_replies_helper.WebsocketRepliesHelper()

            await self._start_websocket_communication(websocket, app_id,
                                                      replies_helper)

            sender = self.__send_get_measures_msg
            receiver = self.__receive_get_measures_msg
            return await asyncio.wait_for(
                self._hold_websocket_communication(
                    sender(websocket, replies_helper),
                    receiver(websocket, replies_helper)), timeout)

    async def __receive_get_measures_msg(self, websocket, replies_helper):
        return await self._receive_messages(websocket, replies_helper,
                                            self.__GET_PROPERTIES,
                                            'result.qProp')

    async def __send_get_measures_msg(self, websocket, replies_helper):
        return await self._send_messages(
            websocket, replies_helper, self.__send_follow_up_msg_get_measures)

    async def __send_follow_up_msg_get_measures(self, websocket,
                                                replies_helper, response):

        response_id = response.get('id')
        if replies_helper.is_method(response_id, self._OPEN_DOC):
            await self.__handle_open_doc_reply(websocket, replies_helper,
                                               response)
            replies_helper.remove_unhandled(response)
        elif replies_helper.is_method(response_id, self._GET_ALL_INFOS):
            await self.__handle_get_all_infos_reply(websocket, replies_helper,
                                                    response)
            replies_helper.remove_unhandled(response)
        elif replies_helper.is_method(response_id, self.__GET_MEASURE):
            await self.__handle_get_measure_reply(websocket, replies_helper,
                                                  response)
            replies_helper.remove_unhandled(response)

    async def __handle_open_doc_reply(self, websocket, replies_helper,
                                      response):

        doc_handle = response.get('result').get('qReturn').get('qHandle')
        replies_helper.set_handle(doc_handle, self.__DOC_HANDLE)
        follow_up_req_id = await self._send_get_all_infos_message(
            websocket, doc_handle)
        replies_helper.add_pending_id(follow_up_req_id, self._GET_ALL_INFOS)

    async def __handle_get_all_infos_reply(self, websocket, replies_helper,
                                           response):

        all_infos = response.get('result').get('qInfos')
        doc_handle = replies_helper.get_handle(self.__DOC_HANDLE)
        measure_ids = [
            info.get('qId')
            for info in all_infos
            if 'measure' == info.get('qType')
        ]
        follow_up_req_ids = await asyncio.gather(*[
            self.__send_get_measure_message(websocket, doc_handle, measure_id)
            for measure_id in measure_ids
        ])
        replies_helper.add_pending_ids(follow_up_req_ids, self.__GET_MEASURE)

    async def __handle_get_measure_reply(self, websocket, replies_helper,
                                         response):

        measure_handle = response.get('result').get('qReturn').get('qHandle')
        follow_up_req_id = await self.__send_get_properties_message(
            websocket, measure_handle)
        replies_helper.add_pending_id(follow_up_req_id, self.__GET_PROPERTIES)

    async def __send_get_measure_message(self, websocket, doc_handle,
                                         measure_id):
        """Sends a Get Measure Interface message.

        Returns:
            The message id.
        """
        message_id = self._generate_message_id()
        await websocket.send(
            json.dumps({
                'handle': doc_handle,
                'method': self.__GET_MEASURE,
                'params': {
                    'qId': measure_id,
                },
                'id': message_id,
            }))

        logging.debug('Get Measure Interface message sent: %d', message_id)
        return message_id

    async def __send_get_properties_message(self, websocket, measure_handle):
        """Sends a Get Measure Properties message.

        Returns:
            The message id.
        """
        message_id = self._generate_message_id()
        await websocket.send(
            json.dumps({
                'handle': measure_handle,
                'method': self.__GET_PROPERTIES,
                'params': {},
                'id': message_id,
            }))

        logging.debug('Get Measure Properties message sent: %d', message_id)
        return message_id
