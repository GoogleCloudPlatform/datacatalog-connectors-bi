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

from google.datacatalog_connectors.tableau.sync import \
    dashboards_synchronizer, sites_synchronizer, workbooks_synchronizer


class DataCatalogSynchronizer:

    def __init__(self,
                 tableau_server_address,
                 tableau_api_version,
                 tableau_username,
                 tableau_password,
                 datacatalog_project_id,
                 datacatalog_location_id,
                 tableau_site=None):

        self.__dashboards_synchronizer = \
            dashboards_synchronizer.DashboardsSynchronizer(
                tableau_server_address=tableau_server_address,
                tableau_api_version=tableau_api_version,
                tableau_username=tableau_username,
                tableau_password=tableau_password,
                tableau_site=tableau_site,
                datacatalog_project_id=datacatalog_project_id,
                datacatalog_location_id=datacatalog_location_id)

        self.__sites_synchronizer = \
            sites_synchronizer.SitesSynchronizer(
                tableau_server_address=tableau_server_address,
                tableau_api_version=tableau_api_version,
                tableau_username=tableau_username,
                tableau_password=tableau_password,
                tableau_site=tableau_site,
                datacatalog_project_id=datacatalog_project_id,
                datacatalog_location_id=datacatalog_location_id)

        self.__workbooks_synchronizer = \
            workbooks_synchronizer.WorkbooksSynchronizer(
                tableau_server_address=tableau_server_address,
                tableau_api_version=tableau_api_version,
                tableau_username=tableau_username,
                tableau_password=tableau_password,
                tableau_site=tableau_site,
                datacatalog_project_id=datacatalog_project_id,
                datacatalog_location_id=datacatalog_location_id)

    def run(self, query_filters=None):
        if not query_filters:
            self.__run_full_sync()
        else:
            # Processing a server event (triggered by a web hook) is a typical
            # scenario for this use case.
            self.__run_partial_sync(query_filters)

    def __run_full_sync(self):
        self.__sites_synchronizer.run()
        self.__dashboards_synchronizer.run()

    def __run_partial_sync(self, query_filters):
        workbooks_filter = None
        if 'workbooks' in query_filters:
            workbooks_filter = query_filters['workbooks']

        if workbooks_filter:
            self.__workbooks_synchronizer.run(query_filter=workbooks_filter)

            # All dashboards need to be synchronized on workbooks partial sync
            # because until Metadata API 3.6 (Tableau 2020.1) there's no way to
            # filter only the dashboards belonging to a given workbook.
            # A second reason is that, as of Metadata API 3.6 (Tableau 2020.1),
            # the "Workbook updated" event contains only the workbook luid,
            # even when a dashboard is deleted.
            self.__dashboards_synchronizer.run()
