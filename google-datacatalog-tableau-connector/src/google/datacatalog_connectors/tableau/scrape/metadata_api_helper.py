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

from google.datacatalog_connectors.tableau.scrape import \
    authenticator, constants, metadata_api_constants


class MetadataAPIHelper:

    def __init__(self,
                 server_address,
                 api_version,
                 username,
                 password,
                 site_content_url=None):

        self.__server_address = server_address
        self.__api_version = api_version
        self.__username = username
        self.__password = password
        self.__site_content_url = site_content_url

        self.__api_endpoint = f'{server_address}' \
                              f'/relationship-service-war/graphql'

        self.__auth_credentials = None

    def fetch_dashboards(self, query_filter=None):
        """
        Read dashboards metadata from a given server.

        Args:
            query_filter (dict): Filter fields and values

        Returns:
            dashboards: A list of dashboards metadata
        """
        self.__set_up_auth_credentials()

        body = {'query': metadata_api_constants.FETCH_DASHBOARDS_QUERY}
        headers = {
            constants.X_TABLEAU_AUTH_HEADER_NAME:
                self.__auth_credentials['token']
        }

        response = requests.post(url=self.__api_endpoint,
                                 headers=headers,
                                 json=body).json()

        dashboards = response['data']['dashboards'] \
            if response and response.get('data') \
            and 'dashboards' in response['data'] \
            else []

        # Site contentUrl handling
        for dashboard in dashboards:
            if dashboard.get('workbook') and 'site' in dashboard['workbook']:
                self.__add_site_content_url_field(
                    dashboard['workbook']['site'])

        return dashboards

    def fetch_sites(self, query_filter=None):
        """
        Read sites metadata from a given server.

        Args:
            query_filter (dict): Filter fields and values

        Returns:
            sites: A list of sites metadata
        """
        self.__set_up_auth_credentials()

        body = {'query': metadata_api_constants.FETCH_SITES_QUERY}
        headers = {
            constants.X_TABLEAU_AUTH_HEADER_NAME:
                self.__auth_credentials['token']
        }

        response = requests.post(url=self.__api_endpoint,
                                 headers=headers,
                                 json=body).json()

        sites = response['data']['tableauSites'] \
            if response and response.get('data') \
            and 'tableauSites' in response['data'] \
            else []

        # Site contentUrl handling
        for site in sites:
            self.__add_site_content_url_field(site)
            workbooks = site.get('workbooks') or []
            for workbook in workbooks:
                self.__add_site_content_url_field(workbook['site'])

        return sites

    def fetch_workbooks(self, query_filter=None):
        """
        Read workbooks metadata from a given server.

        Args:
            query_filter (dict): Filter fields and values

        Returns:
            workbooks: A list of workbooks metadata
        """
        self.__set_up_auth_credentials()

        body = {'query': metadata_api_constants.FETCH_WORKBOOKS_QUERY}
        if query_filter:
            variables = metadata_api_constants.FETCH_WORKBOOKS_FILTER_TEMPLATE
            for key, value in query_filter.items():
                variables = variables.replace(f'${key}', value)
            body['variables'] = variables
        headers = {
            constants.X_TABLEAU_AUTH_HEADER_NAME:
                self.__auth_credentials['token']
        }

        response = requests.post(url=self.__api_endpoint,
                                 headers=headers,
                                 json=body).json()

        workbooks = response['data']['workbooks'] \
            if response and response.get('data') \
            and 'workbooks' in response['data'] \
            else []

        # Site contentUrl handling
        for workbook in workbooks:
            if 'site' in workbook:
                self.__add_site_content_url_field(workbook['site'])

        return workbooks

    def __set_up_auth_credentials(self):
        if self.__auth_credentials:
            return

        self.__auth_credentials = \
            authenticator.Authenticator.authenticate(
                self.__server_address,
                self.__api_version,
                self.__username,
                self.__password,
                self.__site_content_url)

    def __add_site_content_url_field(self, original_site_metadata):
        """The `contentUrl` field is not available in the original
        `TableauSite` objects returned by the Metadata API but it is required
        in the prepare stage. So, it is injected into the returned objects to
        make further processing more efficient.

        Args:
            original_site_metadata: The object returned by the Metadata API
        """
        original_site_metadata['contentUrl'] = self.__site_content_url
