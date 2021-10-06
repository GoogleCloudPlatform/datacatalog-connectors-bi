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

from google.datacatalog_connectors.sisense import addons


class ElastiCubeDependencyPrinter:

    @classmethod
    def print_dependency_finder_results(
            cls, results: Dict[str, Tuple[Entry, List[Tag]]]) -> None:

        if not results:
            return

        print('\nResults:')

        human_readable_index = 0
        for entry_name, metadata in results.items():
            entry = metadata[0]
            tags = metadata[1]

            dashboard_title = cls.__get_asset_metadata_value(
                tags, 'dashboard_title')
            datasource = cls.__get_asset_metadata_value(tags, 'datasource')
            jaql_tags = addons.ElastiCubeDependencyFinder.filter_jaql_tags(
                tags)

            human_readable_index += 1
            print(f'\n{human_readable_index}')
            print(f'Entry       : {entry.name}')
            print(f'Type        : {entry.user_specified_type}')
            print(f'Title       : {entry.display_name}')
            if dashboard_title:
                print(f'Dashboard   : {dashboard_title}')
            print(f'Data source : {datasource if datasource else ""}')
            print(f'URL         : {entry.linked_resource}\n')
            if jaql_tags:
                print('Matching fields and filters:')
                for tag in jaql_tags:
                    print()
                    print(f'  Component : {tag.column}')
                    print(f'  Table     : {tag.fields["table"].string_value}')
                    print(f'  Column    : {tag.fields["column"].string_value}')
            print('--------------------------------------------------')
        print()

    @classmethod
    def __get_asset_metadata_value(cls, tags: List[Tag],
                                   field_name: str) -> Optional[str]:

        asset_metadata_tag = addons.ElastiCubeDependencyFinder\
            .filter_asset_metadata_tag(tags)

        if field_name in asset_metadata_tag.fields:
            return asset_metadata_tag.fields[field_name].string_value
