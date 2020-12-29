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


class EngineAPIDimensionsHelper(base_engine_api_helper.BaseEngineAPIHelper):
    # Keys to be used in the handles dict.
    __DOC_HANDLE = 'doc_handle'

    # Keys to be used in the pending reponse ids dict.
    __ALL_INFOS = 'all_infos'
    __DIM_INTERFACES = 'dim_interfaces'
    __DIM_PROPERTIES = 'dim_properties'
    __DOC_INTERFACES = 'doc_interfaces'

    def get_dimensions(self, app_id, timeout=60):
        try:
            return self._run_until_complete(
                self.__get_dimensions(app_id, timeout))
        except Exception:
            logging.getLogger().warning("error on get_dimensions:",
                                        exc_info=True)
            return []

    async def __get_dimensions(self, app_id, timeout):
        async with self._connect_websocket(app_id) as websocket:
            responses_manager = \
                websocket_responses_manager.WebsocketResponsesManager()
            self.__init_pending_response_ids_holders(responses_manager)

            await self._start_websocket_communication(websocket, app_id,
                                                      responses_manager)

            consumer = self.__get_dimensions_msg_consumer
            producer = self.__get_dimensions_msg_producer
            return await asyncio.wait_for(
                self._handle_websocket_communication(
                    consumer(websocket, responses_manager),
                    producer(websocket, responses_manager)), timeout)

    async def __get_dimensions_msg_consumer(self, websocket,
                                            responses_manager):

        dimensions = []
        async for message in websocket:
            response = json.loads(message)
            response_id = response.get('id')
            if not response_id:
                self._handle_generic_api_response(response)
                continue

            logging.debug('Response received: %d', response_id)
            if responses_manager.is_pending(response_id,
                                            self.__DIM_PROPERTIES):
                dimensions.append(response.get('result').get('qProp'))
                responses_manager.remove_pending_id(response_id)
            else:
                responses_manager.add_unhandled(response)

            responses_manager.notify_new_response()

        return dimensions

    async def __get_dimensions_msg_producer(self, websocket,
                                            responses_manager):

        while not responses_manager.were_all_received():
            if not responses_manager.is_there_response_notification():
                await responses_manager.wait_for_responses()
                responses_manager.clear_response_notifications()
            for response in responses_manager.get_all_unhandled():
                await self.__send_follow_up_msg_get_dimensions(
                    websocket, responses_manager, response)

        await websocket.close()

    async def __send_follow_up_msg_get_dimensions(self, websocket,
                                                  responses_manager, response):

        response_id = response.get('id')
        response_handled = False
        if responses_manager.is_pending(response_id, self.__DOC_INTERFACES):
            await self.__handle_open_doc_response(websocket, responses_manager,
                                                  response)
            response_handled = True
        elif responses_manager.is_pending(response_id, self.__ALL_INFOS):
            await self.__handle_get_all_infos_response(websocket,
                                                       responses_manager,
                                                       response)
            response_handled = True
        elif responses_manager.is_pending(response_id, self.__DIM_INTERFACES):
            await self.__handle_open_dimension_response(
                websocket, responses_manager, response)
            response_handled = True

        if response_handled:
            responses_manager.remove_pending_id(response_id)
            responses_manager.remove_unhandled(response)

    async def __handle_open_doc_response(self, websocket, responses_manager,
                                         response):

        doc_handle = response.get('result').get('qReturn').get('qHandle')
        responses_manager.set_handle(doc_handle, self.__DOC_HANDLE)
        follow_up_req_id = await self._send_get_all_infos_request(
            websocket, doc_handle)
        responses_manager.add_pending_id(follow_up_req_id, self.__ALL_INFOS)

    async def __handle_get_all_infos_response(self, websocket,
                                              responses_manager, response):

        all_infos = response.get('result').get('qInfos')
        doc_handle = responses_manager.get_handle(self.__DOC_HANDLE)
        dim_ids = [
            info.get('qId')
            for info in all_infos
            if 'dimension' == info.get('qType')
        ]
        follow_up_req_ids = await asyncio.gather(*[
            self.__send_get_dimension_interface_request(
                websocket, doc_handle, dim_id) for dim_id in dim_ids
        ])
        responses_manager.add_pending_ids(follow_up_req_ids,
                                          self.__DIM_INTERFACES)

    async def __handle_open_dimension_response(self, websocket,
                                               responses_manager, response):

        dim_handle = response.get('result').get('qReturn').get('qHandle')
        follow_up_req_id = await self.__send_get_dimension_properties_request(
            websocket, dim_handle)
        responses_manager.add_pending_id(follow_up_req_id,
                                         self.__DIM_PROPERTIES)

    async def __send_get_dimension_interface_request(self, websocket,
                                                     doc_handle, dimension_id):
        """Sends a Get Dimension Interface request.

        Returns:
            The request id.
        """
        request_id = self._generate_request_id()
        await websocket.send(
            json.dumps({
                'handle': doc_handle,
                'method': 'GetDimension',
                'params': {
                    'qId': dimension_id,
                },
                'id': request_id,
            }))

        logging.debug('Get Dimension Interface request sent: %d', request_id)
        return request_id

    async def __send_get_dimension_properties_request(self, websocket,
                                                      dimension_handle):
        """Sends a Get Dimension Properties request.

        Returns:
            The request id.
        """
        request_id = self._generate_request_id()
        await websocket.send(
            json.dumps({
                'handle': dimension_handle,
                'method': 'GetProperties',
                'params': {},
                'id': request_id,
            }))

        logging.debug('Get Dimension Properties request sent: %d', request_id)
        return request_id

    @classmethod
    def __init_pending_response_ids_holders(cls, responses_manager):
        responses_manager.init_pending_ids_holders([
            cls.__DOC_INTERFACES, cls.__ALL_INFOS, cls.__DIM_INTERFACES,
            cls.__DIM_PROPERTIES
        ])
