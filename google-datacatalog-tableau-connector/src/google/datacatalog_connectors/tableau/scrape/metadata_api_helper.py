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

import requests

from google.datacatalog_connectors.tableau.scrape import metadata_api_constants


class MetadataAPIHelper:

    def __init__(self, server_address, auth_credentials):
        self.__url = f'{server_address}/relationship-service-war/graphql'
        self.__headers = {'X-Tableau-Auth': auth_credentials['token']}

    def fetch_dashboards(self, query_filter=None):
        """
        Read dashboards metadata from a given server.

        Returns:
            dashboards: A list of dashboards metadata
        """
        body = {'query': metadata_api_constants.FETCH_DASHBOARDS_QUERY}
        result = requests.post(url=self.__url,
                               headers=self.__headers,
                               json=body).json()

        return result['data']['dashboards'] \
            if result and 'data' in result and result['data'] \
            and 'dashboards' in result['data'] \
            else []

    def fetch_sites(self, query_filter=None):
        """
        Read sites metadata from a given server.

        Returns:
            sites: A list of sites metadata
        """
        body = {'query': metadata_api_constants.FETCH_SITES_QUERY}
        result = requests.post(url=self.__url,
                               headers=self.__headers,
                               json=body).json()

        return result['data']['tableauSites'] \
            if result and 'data' in result and result['data'] \
            and 'tableauSites' in result['data'] \
            else []

    def fetch_workbooks(self, query_filter=None):
        """
        Read workbooks metadata from a given server.

        Args:
            query_filter (dict): Filter fields and values

        Returns:
            workbooks: A list of workbooks metadata
        """
        body = {'query': metadata_api_constants.FETCH_WORKBOOKS_QUERY}
        if query_filter:
            variables = metadata_api_constants.FETCH_WORKBOOKS_FILTER_TEMPLATE
            for key, value in query_filter.items():
                variables = variables.replace(f'${key}', value)
            body['variables'] = variables

        result = requests.post(url=self.__url,
                               headers=self.__headers,
                               json=body).json()

        return result['data']['workbooks'] \
            if result and 'data' in result and result['data'] \
            and 'workbooks' in result['data'] \
            else []
