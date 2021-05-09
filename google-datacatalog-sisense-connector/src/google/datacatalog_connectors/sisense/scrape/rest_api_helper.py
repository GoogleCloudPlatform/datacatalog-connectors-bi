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

import logging
import requests
from typing import Any, Dict, List

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

    def get_all_folders(self) -> List[Dict[str, Any]]:
        """Get all Folders the user has access to on a given server.

        Returns:
            A ``list``.
        """
        self.__set_up_auth()

        # Sisense can return the folders in flat (default) or tree structures.
        # We decided for flat because it simplifies further processing.
        url = f'{self.__base_api_endpoint}/folders?structure=flat'
        return self.__get_list_using_pagination(base_url=url,
                                                results_per_page=50)

    def get_user(self, user_id: str) -> Dict[str, Any]:
        """Get a specific User.

        Returns:
            A user object.

        Raises:
            Exception: If the API returns any status code than ``200``.
        """
        self.__set_up_auth()
        url = f'{self.__base_api_endpoint}/users/{user_id}'
        response = requests.get(url=url, headers=self.__common_headers)
        status_code = response.status_code

        if status_code == 200:
            return response.json()

        # The ``GET /users/{id}`` endpoint needs admin license rights in the
        # API version we are using as reference, ``Windows 8.2.5.11026 v1``.
        # A ``403 Forbidden`` is returned when the authenticated user does not
        # have access to the endpoint.
        #
        # A ``404 Not Found`` is returned when there is no user with the
        # provided ``user_id``.
        #
        # For all status codes other than ``200`` we raise an exception to let
        # the caller know what went wrong.
        error = response.json().get('error') or {}
        logging.warning('error on get_user: %s', error)
        raise Exception(error.get('message'))

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

    def __get_list_using_pagination(
            self, base_url: str,
            results_per_page: int) -> List[Dict[str, Any]]:
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
            page_results = requests.get(url=url,
                                        headers=self.__common_headers).json()
            results.extend(page_results)
            page_count += 1
            logging.info(f'page {page_count}: {len(page_results)} results')

        logging.info('')
        logging.info('Removing potential duplicates...')
        # Set comprehension is used to remove potential duplicates after
        # merging the results, e.g.: the ``rootFolder`` that comes in all
        # ``GET /folders`` pages.
        results_set = {tuple(result.items()) for result in results}

        return [dict(result_tuple) for result_tuple in results_set]
