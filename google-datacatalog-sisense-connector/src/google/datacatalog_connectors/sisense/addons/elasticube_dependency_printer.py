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

from typing import Dict, List, Optional, Tuple

from google.cloud.datacatalog import Entry, Tag
import tabulate

from google.datacatalog_connectors.sisense.addons import \
    elasticube_dependency_finder as base_finder


class ElastiCubeDependencyPrinter:

    @classmethod
    def print_dependency_finder_results(
            cls, results: Dict[str, Tuple[Entry, List[Tag]]]) -> None:

        if not results:
            return

        print('\nResults:')

        human_readable_index = 0
        output_table_headers = [
            'Matching field or filter                ', 'Table', 'Column'
        ]
        dashboard_title_tag_field = 'dashboard_title'
        datasource_tag_field = 'datasource'
        table_tag_field = 'table'
        column_tag_field = 'column'

        for entry_name, metadata in results.items():
            entry = metadata[0]
            tags = metadata[1]
            dashboard = cls.__get_asset_metadata_value(
                tags, dashboard_title_tag_field)
            datasource = cls.__get_asset_metadata_value(
                tags, datasource_tag_field)
            jaql_tags = base_finder.ElastiCubeDependencyFinder\
                .filter_jaql_tags(tags)

            human_readable_index += 1
            print(f'\n\n{human_readable_index}')
            print(f'Entry       : {entry.name}')
            print(f'Type        : {entry.user_specified_type}')
            print(f'Title       : {entry.display_name}')
            if dashboard:
                print(f'Dashboard   : {dashboard}')
            print(f'Data source : {datasource if datasource else ""}')
            print(f'URL         : {entry.linked_resource}\n')

            if not jaql_tags:
                continue

            output_table_data = []
            for tag in jaql_tags:
                if table_tag_field in tag.fields \
                        and column_tag_field in tag.fields:
                    output_table_data.append([
                        tag.column, tag.fields[table_tag_field].string_value,
                        tag.fields[column_tag_field].string_value
                    ])
            if output_table_data:
                print(
                    tabulate.tabulate(output_table_data,
                                      headers=output_table_headers,
                                      tablefmt='presto'))
        print()

    @classmethod
    def __get_asset_metadata_value(cls, tags: List[Tag],
                                   field_name: str) -> Optional[str]:

        asset_metadata_tag = base_finder.ElastiCubeDependencyFinder\
            .filter_asset_metadata_tag(tags)

        if asset_metadata_tag and field_name in asset_metadata_tag.fields:
            return asset_metadata_tag.fields[field_name].string_value
