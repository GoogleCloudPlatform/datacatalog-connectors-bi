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


class WebsocketRepliesHelper:
    """Utility class with common features for replies handling over websocket
    communication sessions.

    Attributes:
        __messages_history:
            A ``dict`` containing a full history of the messages known by a
            given ``WebsocketRepliesHelper`` instance, represented as
            ``message-id: method`` items. It is automatically fulfilled when
            pending ids are added to ``__pending_ids``.
        __pending_ids:
            A ``list`` containing the ids of the pending replies, which means
            the messages identified by them were sent but not answered yet.
        __unhandled_replies:
            A ``list`` containing all reply objects that were received but not
            handled yet.
        __new_reply_event:
            A signal used by the receiver to notify the sender on the arrival
            of new replies, so the sender can take actions such as sending
            follow-up messages.
        __interface_handles:
            A ``dict`` containing the keys and values of the interface handles
            required by a given communication session.
    """

    def __init__(self):
        self.__messages_history = {}
        self.__pending_ids = []
        self.__unhandled_replies = []
        self.__new_reply_event = asyncio.Event()
        self.__interface_handles = {}

    def add_pending_id(self, message_id, method):
        self.__pending_ids.append(message_id)
        self.__messages_history[message_id] = method

    def add_pending_ids(self, message_ids, method):
        for response_id in message_ids:
            self.add_pending_id(response_id, method)

    def remove_pending_id(self, message_id):
        self.__pending_ids.remove(message_id)

    def is_pending(self, message_id, method):
        return message_id in self.__pending_ids and self.is_method(
            message_id, method)

    def is_method(self, message_id, method):
        return method == self.__messages_history.get(message_id)

    def add_unhandled(self, reply):
        self.__unhandled_replies.append(reply)

    def remove_unhandled(self, reply):
        self.__unhandled_replies.remove(reply)

    def get_all_unhandled(self):
        return self.__unhandled_replies

    def were_all_processed(self):
        return not self.__pending_ids and not self.__unhandled_replies

    def notify_new_reply(self):
        self.__new_reply_event.set()

    async def wait_for_replies(self):
        return await self.__new_reply_event.wait()

    def is_there_reply_notification(self):
        return self.__new_reply_event.is_set()

    def clear_reply_notifications(self):
        self.__new_reply_event.clear()

    def set_handle(self, value, handle_key):
        self.__interface_handles[handle_key] = value

    def get_handle(self, handle_key):
        return self.__interface_handles[handle_key]
