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

from functools import lru_cache
import logging
import requests
from requests import Response
from typing import Any, Dict, List, Union

from google.datacatalog_connectors.sisense.scrape import \
    authenticator, constants


class RESTAPIHelper:

    def __init__(self, server_address: str, api_version: str, username: str,
                 password: str):

        self.__server_address = server_address
        self.__api_version = api_version
        self.__username = username
        self.__password = password

        self.__base_api_endpoint = f'{server_address}/api/{api_version}'
        self.__common_headers = {
            'Content-Type': constants.JSON_CONTENT_TYPE,
            'Accept': constants.JSON_CONTENT_TYPE
        }

        self.__auth_credentials = None

    def get_all_dashboards(self) -> List[Dict[str, Any]]:
        """Get all Dashboards the user has access to on a given server.

        Returns:
            A ``list``.

        Raises:
            Exception: If the API returns any status code than ``200``.
        """
        self.__set_up_auth()

        url = f'{self.__base_api_endpoint}/dashboards'
        response = requests.get(url=url, headers=self.__common_headers)
        # ``GET /dashboards`` currently does not support pagination.
        all_dashboards = self.__get_response_body_or_raise(response)

        # ``GET /dashboards`` returns an array containing a mix of summary
        # objects for dashboards that are shared with the current user and full
        # objects for dashboards that are owned by the user. Such summary
        # objects contain only the ``oid`` and ``lastPublish`` fields.
        #
        # So, we check the presence of ``_id`` to determine whether an object
        # refers to a shared dashboard or not. If ``_id`` is absent, we need to
        # perform extra API calls to ``GET dashboards/{id}``, passing ``oid``
        # as parameter, in order to get detailed metadata for each shared
        # dashboard.
        shared_dashboards = [
            dashboard for dashboard in all_dashboards
            if not dashboard.get('_id')
        ]
        for shared_dashboard in shared_dashboards:
            all_dashboards.remove(shared_dashboard)
        all_dashboards.extend([
            self.get_dashboard(dashboard.get('oid'))
            for dashboard in shared_dashboards
        ])

        return all_dashboards

    def get_dashboard(self, dashboard_id: str) -> Dict[str, Any]:
        """Get a specific Dashboard.

        Returns:
            A Dashboard object.

        Raises:
            Exception: If the API returns any status code than ``200``.
        """
        self.__set_up_auth()
        url = f'{self.__base_api_endpoint}/dashboards/{dashboard_id}'
        response = requests.get(url=url, headers=self.__common_headers)
        return self.__get_response_body_or_raise(response)

    def get_all_folders(self) -> List[Dict[str, Any]]:
        """Get all Folders the user has access to on a given server.

        Returns:
            A ``list``.

        Raises:
            Exception: If the API returns any status code than ``200``.
        """
        self.__set_up_auth()

        # Sisense can return the folders in flat (default) or tree structures.
        # We decided for flat because it supports standard pagination handling
        # (paginating a tree structure would require extra programming effort).
        url = f'{self.__base_api_endpoint}/folders?structure=flat'
        return self.__get_list_using_pagination(base_url=url,
                                                results_per_page=50)

    @lru_cache(maxsize=128)
    def get_user(self, user_id: str) -> Dict[str, Any]:
        """Get a specific User.

        Returns:
            A User object.

        Raises:
            Exception: If the API returns any status code than ``200``.
        """
        self.__set_up_auth()
        url = f'{self.__base_api_endpoint}/users/{user_id}'
        response = requests.get(url=url, headers=self.__common_headers)
        return self.__get_response_body_or_raise(response)

    def get_widgets(self, dashboard_id) -> List[Dict[str, Any]]:
        """Get all Widgets belonging to a given Dashboard.

        Returns:
            A ``list``.

        Raises:
            Exception: If the API returns any status code than ``200``.
        """
        self.__set_up_auth()

        url = f'{self.__base_api_endpoint}/dashboards/{dashboard_id}/widgets'
        return self.__get_list_using_pagination(base_url=url,
                                                results_per_page=50)

    def __set_up_auth(self) -> None:
        if self.__auth_credentials:
            return

        self.__auth_credentials = \
            authenticator.Authenticator.authenticate(
                self.__server_address,
                self.__api_version,
                self.__username,
                self.__password)

        self.__common_headers[constants.AUTHORIZATION_HEADER_NAME] = \
            f'{constants.BEARER_TOKEN_PREFIX}' \
            f' {self.__auth_credentials["access_token"]}'

    def __get_list_using_pagination(self,
                                    base_url: str,
                                    results_per_page: int,
                                    id_field='oid') -> List[Dict[str, Any]]:
        """Get a ``List`` using pagination.

        Args:
            base_url: The url to which the ``skip`` and ``limit`` query
              parameters will be appended to handle pagination accordingly.
            results_per_page: The number of results per page. Must be greater
              than 1.

        Returns:
            A ``list``.
        """
        if results_per_page <= 1:
            raise ValueError('results_per_page must be greater than 1')

        results = []
        query_param_prefix = '?' if '?' not in base_url else '&'

        page_count = 0
        page_results = []
        # Sisense APIs may add more results to some pages than specified by the
        # ``results_per_page`` argument. It happens in the ``GET /folders``
        # pages, for instance: all pages include the ``rootFolder``, resulting
        # in ``results_per_page + 1`` folders in all pages but the last.
        # Given this behavior, we need ``results_per_page`` to be greater than
        # ``1`` and to use the ``>=`` operator to decide between executing the
        # ``while`` loop or not, to avoid issues.
        while page_count == 0 or len(page_results) >= results_per_page:
            offset = page_count * results_per_page
            url = f'{base_url}{query_param_prefix}skip={offset}' \
                  f'&limit={results_per_page}'
            response = requests.get(url=url, headers=self.__common_headers)
            page_results = self.__get_response_body_or_raise(response)
            results.extend(page_results)
            page_count += 1
            logging.info(f'page {page_count}: {len(page_results)} results')

        logging.info('')
        logging.info('Removing potential duplicates...')
        # Set comprehension is used to remove potential duplicates after
        # merging the results, e.g.: the ``rootFolder`` that comes in all
        # ``GET /folders`` pages.
        unique_ids = {result.get(id_field) for result in results}

        unique_results = []
        for unique_id in unique_ids:
            unique_results.append(
                next(result for result in results
                     if result.get(id_field) == unique_id))

        return unique_results

    @classmethod
    def __get_response_body_or_raise(
            cls,
            response: Response) -> Union[Dict[str, Any], List[Dict[str, Any]]]:
        """Get the response body on successful API call or raise an exception.

        Returns:
            A ``Dict`` or ``List``.

        Raises:
            Exception: If the API returns any status code than ``200``.
        """

        status_code = response.status_code

        if status_code == 200:
            return response.json()

        # Raise exception for all status codes other than ``200`` to let the
        # caller know what went wrong.
        error = response.json().get('error') or {}
        logging.warning('error on API call: %s', error)
        raise Exception(error.get('message'))
