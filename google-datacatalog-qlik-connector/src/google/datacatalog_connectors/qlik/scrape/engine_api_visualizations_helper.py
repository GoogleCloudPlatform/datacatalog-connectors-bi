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
    base_engine_api_helper as base_helper, websocket_responses_manager


class EngineAPIVisualizationsHelper(base_helper.BaseEngineAPIHelper):
    # Keys used to identify the handles.
    __DOC_HANDLE = 'doc-handle'

    # Methods to be used in the requests.
    __GET_OBJECT = 'GetObject'
    __GET_PROPERTIES = 'GetProperties'

    def get_visualizations(self, app_id, timeout=60):
        try:
            return self._run_until_complete(
                self.__get_visualizations(app_id, timeout))
        except Exception as e:
            logging.warning("error on get_visualizations:", exc_info=True)
            if isinstance(e, asyncio.TimeoutError):
                return []
            else:
                raise

    async def __get_visualizations(self, app_id, timeout):
        async with self._connect_websocket(app_id) as websocket:
            responses_manager = \
                websocket_responses_manager.WebsocketResponsesManager()

            await self._start_websocket_communication(websocket, app_id,
                                                      responses_manager)

            sender = self.__send_get_visualizations_msg
            receiver = self.__receive_get_visualizations_msg
            return await asyncio.wait_for(
                self._hold_websocket_communication(
                    sender(websocket, responses_manager),
                    receiver(websocket, responses_manager)), timeout)

    async def __receive_get_visualizations_msg(self, websocket,
                                               responses_manager):

        return await self._receive_messages(websocket, responses_manager,
                                            self.__GET_PROPERTIES,
                                            'result.qProp')

    async def __send_get_visualizations_msg(self, websocket,
                                            responses_manager):

        return await self._send_messages(
            websocket, responses_manager,
            self.__send_follow_up_msg_get_visualizations)

    async def __send_follow_up_msg_get_visualizations(self, websocket,
                                                      responses_manager,
                                                      response):

        response_id = response.get('id')
        if responses_manager.is_method(response_id, self._OPEN_DOC):
            await self.__handle_open_doc_reply(websocket, responses_manager,
                                               response)
            responses_manager.remove_unhandled(response)
        elif responses_manager.is_method(response_id, self._GET_ALL_INFOS):
            await self.__handle_get_all_infos_reply(websocket,
                                                    responses_manager,
                                                    response)
            responses_manager.remove_unhandled(response)
        elif responses_manager.is_method(response_id, self.__GET_OBJECT):
            await self.__handle_get_object_reply(websocket, responses_manager,
                                                 response)
            responses_manager.remove_unhandled(response)

    async def __handle_open_doc_reply(self, websocket, responses_manager,
                                      response):

        doc_handle = response.get('result').get('qReturn').get('qHandle')
        responses_manager.set_handle(doc_handle, self.__DOC_HANDLE)
        follow_up_req_id = await self._send_get_all_infos_message(
            websocket, doc_handle)
        responses_manager.add_pending_id(follow_up_req_id, self._GET_ALL_INFOS)

    async def __handle_get_all_infos_reply(self, websocket, responses_manager,
                                           response):

        all_infos = response.get('result').get('qInfos')
        doc_handle = responses_manager.get_handle(self.__DOC_HANDLE)
        master_obj_ids = [
            info.get('qId')
            for info in all_infos
            if 'masterobject' == info.get('qType')
        ]
        follow_up_req_ids = await asyncio.gather(*[
            self.__send_get_object_message(websocket, doc_handle,
                                           master_obj_id)
            for master_obj_id in master_obj_ids
        ])
        responses_manager.add_pending_ids(follow_up_req_ids, self.__GET_OBJECT)

    async def __handle_get_object_reply(self, websocket, responses_manager,
                                        response):

        object_handle = response.get('result').get('qReturn').get('qHandle')
        follow_up_req_id = await self.__send_get_properties_message(
            websocket, object_handle)
        responses_manager.add_pending_id(follow_up_req_id,
                                         self.__GET_PROPERTIES)

    async def __send_get_object_message(self, websocket, doc_handle,
                                        object_id):
        """Sends a Get Object Interface message.

        Returns:
            The message id.
        """
        message_id = self._generate_message_id()
        await websocket.send(
            json.dumps({
                'handle': doc_handle,
                'method': self.__GET_OBJECT,
                'params': {
                    'qId': object_id,
                },
                'id': message_id,
            }))

        logging.debug('Get Object Interface message sent: %d', message_id)
        return message_id

    async def __send_get_properties_message(self, websocket, object_handle):
        """Sends a Get Object Properties message.

        Returns:
            The message id.
        """
        message_id = self._generate_message_id()
        await websocket.send(
            json.dumps({
                'handle': object_handle,
                'method': self.__GET_PROPERTIES,
                'params': {},
                'id': message_id,
            }))

        logging.debug('Get Object Properties message sent: %d', message_id)
        return message_id
