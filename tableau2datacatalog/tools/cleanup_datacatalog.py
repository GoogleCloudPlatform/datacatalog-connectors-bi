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

import argparse
import re

from google.cloud import datacatalog
from google.cloud.datacatalog import types

datacatalog = datacatalog.DataCatalogClient()

__DATACATALOG_LOCATION_ID = 'us-central1'


def __delete_entries_and_groups(project_ids):
    entry_name_pattern = '(?P<entry_group_name>.+?)/entries/(.+?)'

    query = f'system=tableau'

    scope = types.SearchCatalogRequest.Scope()
    scope.include_project_ids.extend(project_ids)

    # TODO Replace "search entries" by "list entries by group"
    #  when/if it becomes available.
    search_results = [
        result for result in datacatalog.search_catalog(
            scope=scope, query=query, order_by='relevance', page_size=1000)
    ]

    entry_group_names = []
    for result in search_results:
        try:
            datacatalog.delete_entry(result.relative_resource_name)
            print('Entry deleted: {}'.format(result.relative_resource_name))
            entry_group_name = re.match(
                pattern=entry_name_pattern,
                string=result.relative_resource_name).group('entry_group_name')
            entry_group_names.append(entry_group_name)
        except:
            print('Exception deleting entry')

    # Delete any pre-existing Entry Groups.
    for entry_group_name in set(entry_group_names):
        try:
            datacatalog.delete_entry_group(entry_group_name)
            print('--> Entry Group deleted: {}'.format(entry_group_name))
        except:
            print('Exception deleting entry group')


def __delete_tag_templates(project_id, location_id):
    tag_template_id = 'tableau_sheet_metadata'

    try:
        datacatalog.delete_tag_template(
            datacatalog.DataCatalogClient.tag_template_path(
                project=project_id,
                location=location_id,
                tag_template=tag_template_id),
            force=True)
        print('--> Tag Template deleted: {}'.format(tag_template_id))
    except:
        print('Exception deleting Tag Template')

    tag_template_id = 'tableau_dashboard_metadata'

    try:
        datacatalog.delete_tag_template(
            name=datacatalog.DataCatalogClient.tag_template_path(
                project=project_id,
                location=location_id,
                tag_template=tag_template_id),
            force=True)
        print('--> Tag Template deleted: {}'.format(tag_template_id))
    except:
        print('Exception deleting Tag Template')

    tag_template_id = 'tableau_workbook_metadata'

    try:
        datacatalog.delete_tag_template(
            name=datacatalog.DataCatalogClient.tag_template_path(
                project=project_id,
                location=location_id,
                tag_template=tag_template_id),
            force=True)
        print('--> Tag Template deleted: {}'.format(tag_template_id))
    except:
        print('Exception deleting Tag Template')


def __parse_args():
    parser = argparse.ArgumentParser(
        description='Command line to clean up Tableau metadata on Datacatalog')

    parser.add_argument(
        '--datacatalog-project-ids',
        help='List of Google Cloud project IDs split by comma, '
        'At least one must be specified',
        required=True)
    parser.add_argument(
        '--datacatalog-location-id',
        help='Location id which is the Region that your Datacatalog resides',
        default=__DATACATALOG_LOCATION_ID)
    return parser.parse_args()


if __name__ == "__main__":
    args = __parse_args()

    # Split multiple values separated by comma.
    datacatalog_project_ids = [
        item for item in args.datacatalog_project_ids.split(',')
    ]

    __delete_entries_and_groups(datacatalog_project_ids)
    for datacatalog_project_id in datacatalog_project_ids:
        __delete_tag_templates(datacatalog_project_id,
                               args.datacatalog_location_id)
