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


class FakeIgnoredCookie:

    def __init__(self):
        self.name = 'X-Ignored-Cookie'


class FakeQPSSessionCookie:

    def __init__(self):
        self.name = 'X-Qlik-Session'
        self.value = 'Test cookie'
        self.domain = 'test-domain'
        self.path = '/'


class FakeResponseWithCookies:

    def __init__(self):
        self.cookies = [FakeQPSSessionCookie()]


class FakeResponseWithIgnoredCookies:

    def __init__(self):
        self.cookies = [FakeIgnoredCookie()]


class FakeResponseWithNoCookies:

    def __init__(self):
        self.cookies = []


class FakeSessionWithCookies:

    def __init__(self):
        self.cookies = [FakeQPSSessionCookie()]
