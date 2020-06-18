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

from google.datacatalog_connectors.tableau import sync

__SYNC_PACKAGE = 'google.datacatalog_connectors.tableau.sync'


@mock.patch(f'{__SYNC_PACKAGE}.workbooks_synchronizer'
            f'.WorkbooksSynchronizer.run')
@mock.patch(f'{__SYNC_PACKAGE}.sites_synchronizer.SitesSynchronizer.run')
@mock.patch(f'{__SYNC_PACKAGE}.dashboards_synchronizer'
            f'.DashboardsSynchronizer.run')
class DataCatalogSynchronizerTest(unittest.TestCase):

    def setUp(self):
        self.__synchronizer = sync.DataCatalogSynchronizer(
            tableau_server_address='test-server',
            tableau_api_version='test-api-version',
            tableau_username='test-api-username',
            tableau_password='test-api-password',
            tableau_site='test-site',
            datacatalog_project_id='test-project-id',
            datacatalog_location_id='test-location-id')

    def test_constructor_should_set_instance_attributes(
            self, mock_dashboards_sync, mock_sites_sync,
            mock_workbooks_sync):  # noqa: E125

        attrs = self.__synchronizer.__dict__
        self.assertIsNotNone(
            attrs['_DataCatalogSynchronizer__dashboards_synchronizer'])
        self.assertIsNotNone(
            attrs['_DataCatalogSynchronizer__sites_synchronizer'])
        self.assertIsNotNone(
            attrs['_DataCatalogSynchronizer__workbooks_synchronizer'])

    def test_run_should_synchronize_all_asset_types_full_sync(
            self, mock_dashboards_sync, mock_sites_sync,
            mock_workbooks_sync):  # noqa: E125

        self.__synchronizer.run()

        mock_sites_sync.assert_called_once()
        mock_workbooks_sync.assert_not_called()
        mock_dashboards_sync.assert_called_once()

    def test_run_should_synchronize_specific_asset_types_partial_sync(
            self, mock_dashboards_sync, mock_sites_sync,
            mock_workbooks_sync):  # noqa: E125

        query_filters = {'workbooks': {'luid': '123456789'}}

        self.__synchronizer.run(query_filters=query_filters)

        mock_sites_sync.assert_not_called()
        mock_workbooks_sync.assert_called_once()
        mock_dashboards_sync.assert_called_once()
