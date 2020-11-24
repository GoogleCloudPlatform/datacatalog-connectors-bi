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

from google.datacatalog_connectors.tableau.prepare import constants
from google.datacatalog_connectors.tableau.sync import metadata_synchronizer


class DashboardsSynchronizer(metadata_synchronizer.MetadataSynchronizer):

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
                         datacatalog_project_id, datacatalog_location_id,
                         [constants.USER_SPECIFIED_TYPE_DASHBOARD],
                         tableau_site)

    def _scrape_source_system_metadata(self, query_filter=None):
        return self._metadata_scraper.scrape_dashboards(query_filter)

    def _make_tag_templates_dict(self):
        # Used to fulfill Data Catalog Entry relationships
        # comprising Dashboards and the Workbooks they belong to.
        workbook_tag_template_id, workbook_tag_template = \
            self._tag_template_factory.make_tag_template_for_workbook()

        dashboard_tag_template_id, dashboard_tag_template = \
            self._tag_template_factory.make_tag_template_for_dashboard()

        return {
            workbook_tag_template_id: workbook_tag_template,
            dashboard_tag_template_id: dashboard_tag_template
        }

    def _make_assembled_entries(self, metadata, tag_templates_dict):
        return self._entry_factory.make_entries_for_dashboards(
            metadata, tag_templates_dict)

    @classmethod
    def _filter_ingestable_assembled_entries(cls, assembled_entries):
        return [
            assembled_entry for assembled_entry in assembled_entries
            if assembled_entry.entry.user_specified_type ==
            constants.USER_SPECIFIED_TYPE_DASHBOARD
        ]
