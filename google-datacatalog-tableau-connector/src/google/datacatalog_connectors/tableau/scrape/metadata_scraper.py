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

from google.datacatalog_connectors.tableau.scrape import \
    authenticator, metadata_api_helper, rest_api_helper


class MetadataScraper:

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

        self.__auth_credentials = {}

    def scrape_dashboards(self, query_filter=None):
        return self.__scrape_metadata(self.__scrape_dashboards, query_filter)

    def scrape_sites(self, query_filter=None):
        return self.__scrape_metadata(self.__scrape_sites, query_filter)

    def scrape_workbooks(self, query_filter=None):
        return self.__scrape_metadata(self.__scrape_workbooks, query_filter)

    def __scrape_metadata(self, reader_method, query_filter):
        metadata = []

        sites = self.__get_sites()
        if not sites:
            return metadata

        api_helper = metadata_api_helper.MetadataAPIHelper(
            self.__server_address)

        for site in sites:
            site_content_url = site.get('contentUrl')
            auth_credentials = self.__auth_credentials.get(site_content_url)
            if not auth_credentials:
                auth_credentials = authenticator.Authenticator.authenticate(
                    self.__server_address, self.__api_version, self.__username,
                    self.__password, site_content_url)
                self.__auth_credentials[site_content_url] = auth_credentials
            metadata.extend(
                reader_method(api_helper, site, auth_credentials,
                              query_filter))

        return metadata

    @classmethod
    def __scrape_dashboards(cls, api_helper, current_site, auth_credentials,
                            query_filter):

        return api_helper.fetch_dashboards(current_site, auth_credentials,
                                           query_filter)

    @classmethod
    def __scrape_sites(cls, api_helper, current_site, auth_credentials,
                       query_filter):

        return api_helper.fetch_sites(current_site, auth_credentials,
                                      query_filter)

    @classmethod
    def __scrape_workbooks(cls, api_helper, current_site, auth_credentials,
                           query_filter):

        return api_helper.fetch_workbooks(current_site, auth_credentials,
                                          query_filter)

    def __get_sites(self):
        api_helper = rest_api_helper.RestAPIHelper(
            server_address=self.__server_address,
            api_version=self.__api_version)

        if self.__site_content_url:
            site_credentials = \
                authenticator.Authenticator.authenticate(
                    self.__server_address,
                    self.__api_version,
                    self.__username,
                    self.__password,
                    site_content_url=self.__site_content_url)
            self.__auth_credentials[self.__site_content_url] = site_credentials
            return [api_helper.get_site(site_credentials)]

        default_site_credentials = \
            authenticator.Authenticator.authenticate(
                self.__server_address,
                self.__api_version,
                self.__username,
                self.__password)
        self.__auth_credentials['default'] = default_site_credentials
        return api_helper.get_all_sites_on_server(default_site_credentials)
