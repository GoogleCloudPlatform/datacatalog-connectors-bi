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
import unittest
from unittest import mock

from google.datacatalog_connectors.qlik.scrape import \
    constants, engine_api_scraper

from . import scrape_ops_mocks

_SCRAPE_PACKAGE = 'google.datacatalog_connectors.qlik.scrape'
_SCRAPER_MODULE = f'{_SCRAPE_PACKAGE}.engine_api_scraper'


@mock.patch(f'{_SCRAPER_MODULE}'
            f'.engine_api_sheets_helper.EngineAPISheetsHelper.get_sheets',
            lambda *args: None)
class EngineAPIScraperTest(unittest.TestCase):
    __SCRAPER_CLASS = f'{_SCRAPER_MODULE}.EngineAPIScraper'

    def setUp(self):
        self.__scraper = engine_api_scraper.EngineAPIScraper(
            server_address='https://test-server',
            ad_domain='test-domain',
            username='test-username',
            password='test-password')

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__scraper.__dict__

        self.assertEqual('https://test-server',
                         attrs['_EngineAPIScraper__server_address'])
        self.assertEqual('test-domain', attrs['_EngineAPIScraper__ad_domain'])
        self.assertEqual('test-username', attrs['_EngineAPIScraper__username'])
        self.assertEqual('test-password', attrs['_EngineAPIScraper__password'])

        self.assertEqual('wss://test-server',
                         attrs['_EngineAPIScraper__base_api_endpoint'])
        self.assertIsNotNone(attrs['_EngineAPIScraper__common_headers'])

    def test_constructor_should_not_set_auth_related_attributes(self):
        attrs = self.__scraper.__dict__
        self.assertIsNone(attrs['_EngineAPIScraper__auth_cookie'])

    @mock.patch(f'{__SCRAPER_CLASS}._EngineAPIScraper__set_up_auth_cookie')
    def test_get_dimensions_should_authenticate_user_beforehand(
            self, mock_set_up_cookie):

        self.__scraper.get_dimensions('app-id')
        mock_set_up_cookie.assert_called_once()

    @mock.patch(f'{__SCRAPER_CLASS}._EngineAPIScraper__set_up_auth_cookie')
    def test_get_measures_should_authenticate_user_beforehand(
            self, mock_set_up_cookie):

        self.__scraper.get_measures('app-id')
        mock_set_up_cookie.assert_called_once()

    @mock.patch(f'{__SCRAPER_CLASS}._EngineAPIScraper__set_up_auth_cookie')
    def test_get_sheets_should_authenticate_user_beforehand(
            self, mock_set_up_cookie):

        self.__scraper.get_sheets('app-id')
        mock_set_up_cookie.assert_called_once()

    @mock.patch(f'{_SCRAPER_MODULE}.authenticator.Authenticator'
                f'.get_qps_session_cookie_windows_auth')
    @mock.patch(f'{__SCRAPER_CLASS}'
                f'._EngineAPIScraper__get_windows_authentication_url')
    def test_set_up_auth_cookie_should_authenticate_and_set_instance_cookie(
            self, mock_get_auth_url, mock_get_qps_session_cookie):

        mock_get_auth_url.return_value = asyncio.sleep(0)

        fake_cookie = scrape_ops_mocks.FakeQPSSessionCookie()
        mock_get_qps_session_cookie.return_value = fake_cookie

        # Call a public method to trigger the authentication workflow.
        self.__scraper.get_sheets('app-id')

        mock_get_qps_session_cookie.assert_called_once()

        attrs = self.__scraper.__dict__
        self.assertEqual(fake_cookie, attrs['_EngineAPIScraper__auth_cookie'])

    @mock.patch(f'{_SCRAPER_MODULE}.authenticator.Authenticator'
                f'.get_qps_session_cookie_windows_auth')
    @mock.patch(f'{__SCRAPER_CLASS}'
                f'._EngineAPIScraper__get_windows_authentication_url')
    def test_set_up_auth_cookie_should_skip_authentication_on_available_cookie(
            self, mock_get_auth_url, mock_get_qps_session_cookie):

        mock_get_auth_url.return_value = asyncio.sleep(0)

        attrs = self.__scraper.__dict__
        attrs['_EngineAPIScraper__auth_cookie'] = \
            scrape_ops_mocks.FakeQPSSessionCookie()

        # Call a public method to trigger the authentication workflow.
        self.__scraper.get_sheets('app-id')

        mock_get_qps_session_cookie.assert_not_called()

    @mock.patch(f'{_SCRAPER_MODULE}.authenticator.Authenticator'
                f'.get_qps_session_cookie_windows_auth',
                lambda *args, **kwargs: None)
    @mock.patch(f'{_SCRAPE_PACKAGE}.engine_api_scraper.websockets.connect',
                new_callable=scrape_ops_mocks.AsyncContextManager)
    def test_get_windows_authentication_url_should_pass_appropriate_auth_args(
            self, mock_websocket):

        mock_websocket.return_value.__enter__.return_value.set_data(
            [], stop_itr_on_no_data=True)

        # Call a public method to trigger the authentication workflow.
        self.__scraper.get_sheets('app-id')

        attrs = self.__scraper.__dict__
        extra_headers = attrs['_EngineAPIScraper__common_headers'].copy()
        extra_headers['User-Agent'] = 'Windows'

        expected_ws_call_args = mock.call(
            uri=f'wss://test-server/app/?transient=?Xrfkey={constants.XRFKEY}'
            f'&reloadUri=https://test-server/dev-hub/engine-api-explorer',
            extra_headers=extra_headers)
        actual_ws_call_args = mock_websocket.call_args_list[0]
        self.assertEqual(expected_ws_call_args, actual_ws_call_args)

    @mock.patch(f'{_SCRAPER_MODULE}.authenticator.Authenticator'
                f'.get_qps_session_cookie_windows_auth')
    @mock.patch(f'{_SCRAPE_PACKAGE}.engine_api_scraper.websockets.connect',
                new_callable=scrape_ops_mocks.AsyncContextManager)
    def test_get_windows_authentication_url_should_return_login_uri(
            self, mock_websocket, mock_get_qps_session_cookie):

        mock_websocket.return_value.__enter__.return_value.set_data([
            {
                'params': {
                    'loginUri': 'redirect-url'
                }
            },
        ])

        # Call a public method to trigger the authentication workflow.
        self.__scraper.get_sheets('app-id')

        mock_get_qps_session_cookie.assert_called_once_with(
            ad_domain='test-domain',
            username='test-username',
            password='test-password',
            auth_url='redirect-url')
