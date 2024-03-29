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

import unittest
from unittest import mock

from google.datacatalog_connectors.sisense import scrape


class MetadataScraperTest(unittest.TestCase):
    __SCRAPE_PACKAGE = 'google.datacatalog_connectors.sisense.scrape'
    __SCRAPER_MODULE = f'{__SCRAPE_PACKAGE}.metadata_scraper'

    @mock.patch(f'{__SCRAPER_MODULE}.rest_api_helper.RESTAPIHelper')
    def setUp(self, mock_rest_api_helper):
        self.__scraper = scrape.MetadataScraper(server_address='test-server',
                                                api_version='test-version',
                                                username='test-username',
                                                password='test-password')

        self.__mock_api_helper = mock_rest_api_helper.return_value

    def test_constructor_should_set_instance_attributes(self):
        attrs = self.__scraper.__dict__
        self.assertEqual(self.__mock_api_helper,
                         attrs['_MetadataScraper__api_helper'])

    def test_scrape_all_dashboards_should_delegate_to_api_helper(self):
        dashboards_metadata = [{
            'oid': 'dashboard-id',
        }]

        api_helper = self.__mock_api_helper
        api_helper.get_all_dashboards.return_value = dashboards_metadata

        dashboards = self.__scraper.scrape_all_dashboards()

        self.assertEqual(1, len(dashboards))
        self.assertEqual('dashboard-id', dashboards[0]['oid'])
        api_helper.get_all_dashboards.assert_called_once()

    def test_scrape_all_folders_should_delegate_to_api_helper(self):
        folders_metadata = [{
            'oid': 'folder-id',
        }]

        api_helper = self.__mock_api_helper
        api_helper.get_all_folders.return_value = folders_metadata

        folders = self.__scraper.scrape_all_folders()

        self.assertEqual(1, len(folders))
        self.assertEqual('folder-id', folders[0]['oid'])
        api_helper.get_all_folders.assert_called_once()

    def test_scrape_user_should_delegate_to_api_helper(self):
        user_metadata = {
            '_id': 'user-id',
            'firstName': 'Jane',
            'lastName': 'Doe'
        }

        api_helper = self.__mock_api_helper
        api_helper.get_user.return_value = user_metadata

        user = self.__scraper.scrape_user('user-id')

        self.assertEqual('user-id', user['_id'])
        api_helper.get_user.assert_called_once()

    def test_scrape_widgets_should_delegate_to_api_helper(self):
        widgets_metadata = [{
            'oid': 'widget-id',
        }]

        api_helper = self.__mock_api_helper
        api_helper.get_widgets.return_value = widgets_metadata

        widgets = self.__scraper.scrape_widgets({'oid': 'dashboard-id'})

        self.assertEqual(1, len(widgets))
        self.assertEqual('widget-id', widgets[0]['oid'])
        api_helper.get_widgets.assert_called_once_with('dashboard-id')
