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

import unittest

from google.datacatalog_connectors.tableau import prepare
from google.datacatalog_connectors.tableau.prepare import \
    datacatalog_tag_factory


class DataCatalogTagFactoryTest(unittest.TestCase):

    def test_make_tag_for_dashboard_should_set_all_available_fields(self):
        tag_template_factory = prepare.DataCatalogTagTemplateFactory(
            'project_id', 'us_central_1')
        template_id, template = tag_template_factory\
            .make_tag_template_for_dashboard()

        metadata = [{
            'id': 'a123-b456',
            'luid': 'c234-d567',
            'path': 'test/dashboard',
            'workbook': {
                'luid': 'e345-f678',
                'name': 'Test Workbook Name',
                'site': {
                    'name': 'test-site'
                }
            }
        }]

        tags = datacatalog_tag_factory.DataCatalogTagFactory\
            .make_tags_for_dashboards(template, metadata)
        tag = tags[0]
        self.assertEqual('a123-b456', tag.fields['id'].string_value)
        self.assertEqual('c234-d567', tag.fields['luid'].string_value)
        self.assertEqual('e345-f678', tag.fields['workbook_luid'].string_value)
        self.assertEqual('Test Workbook Name',
                         tag.fields['workbook_name'].string_value)
        self.assertEqual('test-site', tag.fields['site_name'].string_value)
        self.assertEqual(True, tag.fields['has_external_url'].bool_value)

    def test_make_tag_for_dashboard_no_ext_url_should_skip_field(self):
        tag_template_factory = prepare.DataCatalogTagTemplateFactory(
            'project_id', 'us_central_1')
        template_id, template = tag_template_factory\
            .make_tag_template_for_dashboard()

        metadata = [{'path': '', 'workbook': {'site': {}}}]

        tags = datacatalog_tag_factory.DataCatalogTagFactory\
            .make_tags_for_dashboards(template, metadata)

        tag = tags[0]
        self.assertEqual(False, tag.fields['has_external_url'].bool_value)

    def test_make_tag_for_sheet_should_set_all_available_fields(self):
        tag_template_factory = prepare.DataCatalogTagTemplateFactory(
            'project_id', 'us_central_1')
        template_id, template = tag_template_factory\
            .make_tag_template_for_sheet()

        metadata = [{'id': 'a123-b456', 'path': 'test/sheet'}]

        workbook_metadata = {
            'name': 'Test Workbook Name',
            'site': {
                'name': 'test-site'
            }
        }

        tags = datacatalog_tag_factory.DataCatalogTagFactory\
            .make_tags_for_sheets(template, metadata, workbook_metadata)

        tag = tags[0]
        self.assertEqual('Test Workbook Name',
                         tag.fields['workbook_name'].string_value)
        self.assertEqual('test-site', tag.fields['site_name'].string_value)
        self.assertEqual(True, tag.fields['has_external_url'].bool_value)

    def test_make_tag_for_sheet_no_external_url_should_set_all_fields(self):
        tag_template_factory = prepare.DataCatalogTagTemplateFactory(
            'project_id', 'us_central_1')
        template_id, template = tag_template_factory\
            .make_tag_template_for_sheet()

        metadata = [{'path': ''}]

        workbook_metadata = {'site': {}}

        tags = datacatalog_tag_factory.DataCatalogTagFactory\
            .make_tags_for_sheets(template, metadata, workbook_metadata)

        tag = tags[0]
        self.assertEqual(False, tag.fields['has_external_url'].bool_value)

    def test_make_tag_for_workbook_should_set_all_available_fields(self):
        tag_template_factory = prepare.DataCatalogTagTemplateFactory(
            'project_id', 'us_central_1')
        template_id, template = tag_template_factory\
            .make_tag_template_for_workbook()

        metadata = [{
            'luid':
                'c234-d567',
            'site': {
                'name': 'test-site'
            },
            'projectName':
                'Test Project Name',
            'owner': {
                'username': 'test-username',
                'name': 'Test User Name'
            },
            'upstreamTables': [{
                'fullName': '[Test schema].[Test table]',
                'database': {
                    'luid': 'b234-c567'
                }
            }],
            'upstreamDatabases': [{
                'luid': 'b234-c567',
                'name': 'Test db',
                'connectionType': 'Test conn'
            }]
        }]

        tags = datacatalog_tag_factory.DataCatalogTagFactory\
            .make_tags_for_workbooks(template, metadata)

        tag = tags[0]
        self.assertEqual('c234-d567', tag.fields['luid'].string_value)
        self.assertEqual('test-site', tag.fields['site_name'].string_value)
        self.assertEqual('Test Project Name',
                         tag.fields['project_name'].string_value)
        self.assertEqual('Test User Name',
                         tag.fields['owner_name'].string_value)
        self.assertEqual('test-username',
                         tag.fields['owner_username'].string_value)
        self.assertEqual('DATABASE NAME (CONNECTION TYPE) / TABLE NAME',
                         tag.fields['upstream_table_definition'].string_value)
        self.assertEqual('Test db (Test conn)/[Test schema].[Test table]',
                         tag.fields['upstream_tables'].string_value)

    def test_make_tag_for_workbook_no_upstream_databases_should_succeed(self):

        tag_template_factory = prepare.DataCatalogTagTemplateFactory(
            'project_id', 'us_central_1')
        template_id, template = tag_template_factory\
            .make_tag_template_for_workbook()

        metadata = [{
            'site': {},
            'upstreamTables': [{
                'fullName': '[Test schema].[Test table]',
                'database': {
                    'luid': 'b234-c567'
                }
            }]
        }]

        tags = datacatalog_tag_factory.DataCatalogTagFactory\
            .make_tags_for_workbooks(template, metadata)

        tag = tags[0]
        self.assertEqual('/[Test schema].[Test table]',
                         tag.fields['upstream_tables'].string_value)

    def test_make_tag_for_workbook_no_upstream_tables_should_skip_field(self):
        tag_template_factory = prepare.DataCatalogTagTemplateFactory(
            'project_id', 'us_central_1')
        template_id, template = tag_template_factory\
            .make_tag_template_for_workbook()

        metadata = [{'site': {}}]

        tags = datacatalog_tag_factory.DataCatalogTagFactory\
            .make_tags_for_workbooks(template, metadata)

        tag = tags[0]
        self.assertFalse('upstream_tables' in tag.fields)
