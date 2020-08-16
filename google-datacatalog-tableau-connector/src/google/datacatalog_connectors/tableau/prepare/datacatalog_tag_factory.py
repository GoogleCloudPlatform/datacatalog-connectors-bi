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

from google.cloud.datacatalog import types

from google.datacatalog_connectors.commons import prepare


class DataCatalogTagFactory(prepare.BaseTagFactory):

    @classmethod
    def make_tags_for_dashboards(cls, tag_template, dashboards_metadata):
        return [
            cls.make_tag_for_dashboard(tag_template, metadata)
            for metadata in dashboards_metadata
        ]

    @classmethod
    def make_tags_for_sheets(cls, tag_template, sheets_metadata,
                             workbook_metadata):
        return [
            cls.make_tag_for_sheet(tag_template, metadata, workbook_metadata)
            for metadata in sheets_metadata
        ]

    @classmethod
    def make_tags_for_workbooks(cls, tag_template, workbooks_metadata):
        return [
            cls.make_tag_for_workbook(tag_template, metadata)
            for metadata in workbooks_metadata
        ]

    @classmethod
    def make_tag_for_dashboard(cls, tag_template, dashboard_metadata):
        tag = types.Tag()

        tag.template = tag_template.name

        cls._set_string_field(tag, 'id', dashboard_metadata.get('id'))
        cls._set_string_field(tag, 'luid', dashboard_metadata.get('luid'))
        cls._set_string_field(tag, 'workbook_luid',
                              dashboard_metadata.get('workbook').get('luid'))
        cls._set_string_field(tag, 'workbook_name',
                              dashboard_metadata.get('workbook').get('name'))
        cls._set_string_field(
            tag, 'site_name',
            dashboard_metadata.get('workbook').get('site').get('name'))
        cls._set_bool_field(
            tag, 'has_external_url', 'path' in dashboard_metadata and
            not dashboard_metadata.get('path') == '')

        return tag

    @classmethod
    def make_tag_for_sheet(cls, tag_template, sheet_metadata,
                           workbook_metadata):

        tag = types.Tag()

        tag.template = tag_template.name

        cls._set_string_field(tag, 'id', sheet_metadata.get('id'))
        cls._set_string_field(tag, 'luid', sheet_metadata.get('luid'))
        cls._set_string_field(tag, 'workbook_luid',
                              workbook_metadata.get('luid'))
        cls._set_string_field(tag, 'workbook_name',
                              workbook_metadata.get('name'))
        cls._set_string_field(tag, 'site_name',
                              workbook_metadata.get('site').get('name'))
        cls._set_bool_field(
            tag, 'has_external_url', 'path' in sheet_metadata and
            not sheet_metadata.get('path') == '')

        return tag

    @classmethod
    def make_tag_for_workbook(cls, tag_template, workbook_metadata):
        tag = types.Tag()

        tag.template = tag_template.name

        cls._set_string_field(tag, 'luid', workbook_metadata.get('luid'))

        site = workbook_metadata.get('site')
        if site:
            cls._set_string_field(tag, 'site_name', site.get('name'))

        cls._set_string_field(tag, 'project_name',
                              workbook_metadata.get('projectName'))

        owner = workbook_metadata.get('owner')
        if owner:
            cls._set_string_field(tag, 'owner_username', owner.get('username'))
            cls._set_string_field(tag, 'owner_name', owner.get('name'))

        upstream_tables = cls.make_upstream_tables_field_value(
            workbook_metadata)
        if upstream_tables:
            cls._set_string_field(
                tag, 'upstream_table_definition',
                'DATABASE NAME (CONNECTION TYPE) / TABLE NAME')
            cls._set_string_field(tag, 'upstream_tables',
                                  ', '.join(upstream_tables))

        return tag

    @classmethod
    def make_upstream_tables_field_value(cls, metadata):
        upstream_tables = set()

        if 'upstreamTables' not in metadata:
            return

        for table_metadata in metadata['upstreamTables']:
            database = table_metadata.get('database')
            database_luid = database.get('luid') if database else None
            databases_metadata = metadata.get('upstreamDatabases')
            database_info = cls\
                .make_database_field_value(databases_metadata, database_luid) \
                if databases_metadata and database_luid else None
            upstream_tables.add(f'{database_info if database_info else ""}'
                                f'/{table_metadata["fullName"]}')

        return upstream_tables

    @classmethod
    def make_database_field_value(cls, databases_metadata, database_luid):
        for database_metadata in databases_metadata:
            if database_luid == database_metadata['luid']:
                return f'{database_metadata.get("name")}' \
                       f' ({database_metadata.get("connectionType")})'
