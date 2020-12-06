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
    constants, metadata_api_constants


class MetadataAPIHelper:

    def __init__(self, server_address):
        self.__url = f'{server_address}/relationship-service-war/graphql'

    def fetch_dashboards(self,
                         current_site,
                         auth_credentials,
                         query_filter=None):
        """
        Read dashboards metadata from a given server.

        Args:
            current_site (dict): Metadata for the current site
            auth_credentials (dict): Credentials to authenticate the request
            query_filter (dict): Filter fields and values

        Returns:
            dashboards: A list of dashboards metadata
        """
        body = {'query': metadata_api_constants.FETCH_DASHBOARDS_QUERY}
        headers = {
            constants.X_TABLEAU_AUTH_HEADER_NAME: auth_credentials['token']
        }
        response = requests.post(url=self.__url, headers=headers,
                                 json=body).json()

        dashboards = response['data']['dashboards'] \
            if response and response.get('data') \
            and 'dashboards' in response['data'] \
            else []

        # Site contentUrl handling
        for dashboard in dashboards:
            if dashboard.get('workbook') and 'site' in dashboard['workbook']:
                self.__add_site_content_url_field(
                    dashboard['workbook']['site'], current_site)

        return dashboards

    def fetch_sites(self, current_site, auth_credentials, query_filter=None):
        """
        Read sites metadata from a given server.

        Args:
            current_site (dict): Metadata for the current site
            auth_credentials (dict): Credentials to authenticate the request
            query_filter (dict): Filter fields and values

        Returns:
            sites: A list of sites metadata
        """
        body = {'query': metadata_api_constants.FETCH_SITES_QUERY}
        headers = {
            constants.X_TABLEAU_AUTH_HEADER_NAME: auth_credentials['token']
        }
        response = requests.post(url=self.__url, headers=headers,
                                 json=body).json()

        sites = response['data']['tableauSites'] \
            if response and response.get('data') \
            and 'tableauSites' in response['data'] \
            else []

        # Site contentUrl handling
        for site in sites:
            self.__add_site_content_url_field(site, current_site)
            workbooks = site.get('workbooks') or []
            for workbook in workbooks:
                self.__add_site_content_url_field(workbook['site'],
                                                  current_site)

        return sites

    def fetch_workbooks(self,
                        current_site,
                        auth_credentials,
                        query_filter=None):
        """
        Read workbooks metadata from a given server.

        Args:
            current_site (dict): Metadata for the current site
            auth_credentials (dict): Credentials to authenticate the request
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
        headers = {
            constants.X_TABLEAU_AUTH_HEADER_NAME: auth_credentials['token']
        }
        response = requests.post(url=self.__url, headers=headers,
                                 json=body).json()

        workbooks = response['data']['workbooks'] \
            if response and response.get('data') \
            and 'workbooks' in response['data'] \
            else []

        # Site contentUrl handling
        for workbook in workbooks:
            if workbook.get('site'):
                self.__add_site_content_url_field(workbook['site'],
                                                  current_site)

        return workbooks

    @classmethod
    def __add_site_content_url_field(cls, original_site_metadata,
                                     current_site):
        """The `contentUrl` field is not available in the original
        `TableauSite` objects returned by the Metadata API but it is required
        in the prepare stage. So, it is injected into the returned objects to
        make further processing more efficient.

        Args:
            original_site_metadata: The object returned by the Metadata API
            current_site: The site which is being scraped
        """
        original_site_metadata['contentUrl'] = current_site.get('contentUrl')
