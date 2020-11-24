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

import logging

from google.datacatalog_connectors.tableau import prepare
from google.datacatalog_connectors.tableau.sync import metadata_synchronizer


class WorkbooksSynchronizer(metadata_synchronizer.MetadataSynchronizer):

    def __init__(self,
                 tableau_server_address,
                 tableau_api_version,
                 tableau_username,
                 tableau_password,
                 datacatalog_project_id,
                 datacatalog_location_id,
                 tableau_site=None):

        super().__init__(tableau_server_address, tableau_api_version,
                         tableau_username, tableau_password,
                         datacatalog_project_id, datacatalog_location_id, [
                             prepare.constants.USER_SPECIFIED_TYPE_WORKBOOK,
                             prepare.constants.USER_SPECIFIED_TYPE_SHEET
                         ], tableau_site)

    def _scrape_source_system_metadata(self, query_filter=None):
        return self._metadata_scraper.scrape_workbooks(query_filter)

    def _log_scraping_results(self, metadata):
        workbooks_count = len(metadata)
        sheets_count = sum([
            len(workbook_metadata['sheets']) for workbook_metadata in metadata
        ])

        assets_count = sum([workbooks_count, sheets_count])
        assets_count_str_len = len(str(assets_count))

        logging.info('')
        logging.info('==== %s assets scraped!', assets_count)
        spaces_count = assets_count_str_len - len(str(workbooks_count))
        logging.info('   > %s%s workbooks', " " * spaces_count,
                     workbooks_count)
        spaces_count = assets_count_str_len - len(str(sheets_count))
        logging.info('   > %s%s sheets', " " * spaces_count, sheets_count)

    def _make_tag_templates_dict(self):
        workbook_tag_template_id, workbook_tag_template = \
            self._tag_template_factory.make_tag_template_for_workbook()

        sheet_tag_template_id, sheet_tag_template = \
            self._tag_template_factory.make_tag_template_for_sheet()

        return {
            workbook_tag_template_id: workbook_tag_template,
            sheet_tag_template_id: sheet_tag_template
        }

    def _make_assembled_entries(self, metadata, tag_templates_dict):
        return self._entry_factory.make_entries_for_workbooks(
            metadata, tag_templates_dict)
