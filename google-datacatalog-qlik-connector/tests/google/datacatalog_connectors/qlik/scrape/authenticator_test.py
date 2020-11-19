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

import unittest
from unittest import mock

from google.datacatalog_connectors.qlik.scrape import authenticator


class AuthenticatorTest(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.qlik.scrape'

    @mock.patch(f'{__SCRAPE_PACKAGE}.authenticator.requests')
    def test_get_qps_cookie_win_auth_should_return_cookie_on_success(
            self, mock_requests):
        mock_requests.get.return_value = FakeResponseWithCookies()

        cookie = authenticator.Authenticator\
            .get_qps_session_cookie_windows_auth(
                'test-domain', 'test-username', 'test-password', 'test-url')

        self.assertIsNotNone(cookie)
        mock_requests.get.assert_called_once()

    @mock.patch(f'{__SCRAPE_PACKAGE}.authenticator.requests')
    def test_get_qps_cookie_win_auth_should_return_none_on_no_cookies(
            self, mock_requests):
        mock_requests.get.return_value = FakeResponseWithNoCookies()

        cookie = authenticator.Authenticator \
            .get_qps_session_cookie_windows_auth(
                'test-domain', 'test-username', 'test-password', 'test-url')

        self.assertIsNone(cookie)
        mock_requests.get.assert_called_once()

    @mock.patch(f'{__SCRAPE_PACKAGE}.authenticator.requests')
    def test_get_qps_cookie_win_auth_should_return_none_on_cookie_not_found(
            self, mock_requests):
        mock_requests.get.return_value = FakeResponseWithIgnoredCookies()

        cookie = authenticator.Authenticator \
            .get_qps_session_cookie_windows_auth(
                'test-domain', 'test-username', 'test-password', 'test-url')

        self.assertIsNone(cookie)
        mock_requests.get.assert_called_once()


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
