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
from typing import Any, Dict, List, Optional, Tuple

from google.cloud.datacatalog import TagTemplate
from google.datacatalog_connectors.commons import cleanup, ingest
from google.datacatalog_connectors.commons.prepare import AssembledEntryData

from google.datacatalog_connectors.sisense import prepare, scrape
from google.datacatalog_connectors.sisense.prepare import constants


class MetadataSynchronizer:
    # Data Catalog constants
    __ENTRY_GROUP_ID = 'sisense'
    __SPECIFIED_SYSTEM = 'Sisense'
    # Sisense constants
    __SISENSE_API_VERSION = 'v1'

    def __init__(self, sisense_server_address, sisense_username,
                 sisense_password, datacatalog_project_id,
                 datacatalog_location_id):

        self.__server_address = sisense_server_address
        self.__username = sisense_username
        self.__password = sisense_password

        self.__metadata_scraper = scrape.MetadataScraper(
            self.__server_address, self.__SISENSE_API_VERSION, self.__username,
            self.__password)

        self.__project_id = datacatalog_project_id
        self.__location_id = datacatalog_location_id

        self.__tag_template_factory = prepare.DataCatalogTagTemplateFactory(
            project_id=datacatalog_project_id,
            location_id=datacatalog_location_id)

        self.__assembled_entry_factory = prepare.AssembledEntryFactory(
            project_id=datacatalog_project_id,
            location_id=datacatalog_location_id,
            entry_group_id=self.__ENTRY_GROUP_ID,
            user_specified_system=self.__SPECIFIED_SYSTEM,
            server_address=sisense_server_address)

    def run(self) -> None:
        """Coordinate a full scrape > prepare > ingest process."""

        # Scrape metadata from the Sisense server.
        logging.info('')
        logging.info('===> Scraping Sisense metadata...')

        logging.info('')
        logging.info('Objects to be scraped: Folders, Dashboards, and Widgets')
        folders = self.__scrape_folders()
        dashboards = self.__scrape_dashboards(folders)
        logging.info('==== DONE ========================================')

        # Prepare: convert Sisense metadata into Data Catalog entities model.
        logging.info('')
        logging.info('===> Converting Sisense metadata'
                     ' into Data Catalog entities model...')

        assembled_assets_dict = self.__assemble_sisense_assets(
            folders, dashboards)
        tag_templates_dict = self.__make_tag_templates_dict()
        assembled_entries_dict = self.__make_assembled_entries_dict(
            assembled_assets_dict, tag_templates_dict)
        logging.info('==== DONE ========================================')

        # Data Catalog entry relationships mapping.
        logging.info('')
        logging.info('===> Mapping Data Catalog entry relationships...')

        self.__map_datacatalog_relationships(assembled_entries_dict)
        logging.info('==== DONE ========================================')

        # Data Catalog clean up: delete obsolete data.
        logging.info('')
        logging.info('===> Deleting Data Catalog obsolete metadata...')

        self.__delete_obsolete_entries(assembled_entries_dict)
        logging.info('==== DONE ========================================')

        # Ingest metadata into Data Catalog.
        logging.info('')
        logging.info('===> Synchronizing Sisense :: Data Catalog metadata...')

        self.__ingest_metadata(assembled_entries_dict, tag_templates_dict)
        logging.info('==== DONE ========================================')

    def __scrape_folders(self) -> List[Dict[str, Any]]:
        """Scrape metadata from all Folders the current user has access to.

        The outgoing Folder objects may be enriched with additional fields in
        order to expedite/improve the metadata synchronization process:
        - ``ownerData``: added when the authenticated user is allowed to read
        users' information; intended to provide ownership-related metadata to
        the Data Catalog Tags created for the Folder.
        - ``parentFolderData``: added when the Folder has a parent.

        Returns:
            A ``list``.
        """
        all_folders = self.__metadata_scraper.scrape_all_folders()

        for folder in all_folders:
            owner_id = folder.get('owner')
            # The ``rootFolder`` does not have an owner, for instance.
            owner_data = self.__scrape_user(owner_id) if owner_id else None
            if owner_data:
                folder['ownerData'] = owner_data
            parent_id = folder.get('parentId')
            if parent_id:
                folder['parentFolderData'] = next(
                    fdr for fdr in all_folders if fdr.get('oid') == parent_id)

        return all_folders

    def __scrape_dashboards(
            self, folders: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """Scrape metadata from all Dashboards the current user has access to.

        The outgoing Dashboard objects are enriched with additional fields
        in order to expedite/improve the metadata synchronization process:
        - ``ownerData``: added when the authenticated user is allowed to read
        users' information; intended to provide ownership-related metadata to
        the Data Catalog Tags created for the Dashboard.
        - ``folderData``: added when the Dashboard has a parent Folder.
        - ``widgets``: always added.

        Returns:
            A ``list``.
        """
        all_dashboards = self.__metadata_scraper.scrape_all_dashboards()

        for dashboard in all_dashboards:
            owner_data = self.__scrape_user(dashboard.get('owner'))
            if owner_data:
                dashboard['ownerData'] = owner_data
            folder_id = dashboard.get('parentFolder')
            if folder_id:
                dashboard['folderData'] = next(
                    folder for folder in folders
                    if folder.get('oid') == folder_id)
            dashboard['widgets'] = self.__scrape_widgets(dashboard)

        return all_dashboards

    def __scrape_widgets(self, dashboard: Dict[str,
                                               Any]) -> List[Dict[str, Any]]:
        """Scrape metadata from all Widgets the current user has access to for
        a given Dashboard.

        The outgoing Widget objects are enriched with additional fields in
        order to expedite/improve the metadata synchronization process:
        - ``ownerData``: added when the authenticated user is allowed to read
        users' information; intended to provide ownership-related metadata to
        the Data Catalog Tags created for the Widget.
        - ``dashboardData``: always added.

        Returns:
            A ``list``.
        """
        widgets = self.__metadata_scraper.scrape_widgets(dashboard)

        for widget in widgets:
            owner_data = self.__scrape_user(widget.get('owner'))
            if owner_data:
                widget['ownerData'] = owner_data
            widget['dashboardData'] = dashboard

        return widgets

    def __scrape_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Scrape metadata from a given User.

        Returns:
            A ``dict`` if the current user has access to the API endpoint or
            ``None`` if not.
        """
        try:
            return self.__metadata_scraper.scrape_user(user_id)
        except:  # noqa E722
            logging.warning("error on __scrape_user:", exc_info=True)

    def __assemble_sisense_assets(
            self, folders: List[Dict[str, Any]],
            dashboards: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
        """
        Assemble metadata concerning all assets the current user has access
        to. Folder metadata include nested objects such as sub-folders,
        dashboards, widgets, and fields.

        Returns:
            A ``dict``. Keys are top-level asset ids and values are  dicts
            containing all metadata gathered for those assets in a hierarchical
            structure.
        """
        top_level_folders = [
            folder for folder in folders if not folder.get('parentId')
        ]
        top_level_dashboards = [
            dashboard for dashboard in dashboards
            if not dashboard.get('parentFolder')
        ]

        top_level_assets_dict = {}
        for folder in top_level_folders:
            assembled_folder = self.__assemble_folder_from_flat_lists(
                folder, folders, dashboards)
            top_level_assets_dict[assembled_folder[0]] = assembled_folder[1]

        for dashboard in top_level_dashboards:
            top_level_assets_dict[dashboard.get('oid')] = dashboard

        return top_level_assets_dict

    @classmethod
    def __assemble_folder_from_flat_lists(
            cls, folder: Dict[str, Any], all_folders: List[Dict[str, Any]],
            all_dashboards: List[Dict[str,
                                      Any]]) -> Tuple[str, Dict[str, Any]]:
        """
        Assemble Folder metadata from the given flat asset lists.

        The outgoing Folder object may be enriched with additional fields in
        order to expedite/improve the metadata synchronization process:
        - ``folders``: stores its child Folders.
        - ``dashboards``: stores its child Dashboards.

        Returns:
            A ``tuple``. The first item is the Folder id and the second is a
            ``dict`` containing all metadata gathered for the Folder in a
            hierarchical structure.
        """
        # The root folder's ``oid`` field is not fulfilled.
        folder_id = folder.get('oid') or folder.get('name')
        folder['folders'] = [
            child_folder for child_folder in all_folders
            if child_folder.get('parentId') == folder_id
        ]
        folder['dashboards'] = [
            dashboard for dashboard in all_dashboards
            if dashboard.get('parentFolder') == folder_id
        ]
        for child_folder in folder['folders']:
            cls.__assemble_folder_from_flat_lists(child_folder, all_folders,
                                                  all_dashboards)

        # TODO Check whether the already used Folders and Dashboards can be
        #  removed from the provided lists to improve next iterations
        #  performance and decrease memory usage.

        return folder_id, folder

    def __make_tag_templates_dict(self) -> Dict[str, TagTemplate]:
        return {
            constants.TAG_TEMPLATE_ID_FOLDER:
                self.__tag_template_factory.make_tag_template_for_folder(),
            constants.TAG_TEMPLATE_ID_DASHBOARD:
                self.__tag_template_factory.make_tag_template_for_dashboard(),
            constants.TAG_TEMPLATE_ID_JAQL:
                self.__tag_template_factory.make_tag_template_for_jaql(),
            constants.TAG_TEMPLATE_ID_WIDGET:
                self.__tag_template_factory.make_tag_template_for_widget(),
        }

    def __make_assembled_entries_dict(
        self, assembled_metadata: Dict[str, Dict[str, Any]],
        tag_templates: Dict[str, TagTemplate]
    ) -> Dict[str, List[AssembledEntryData]]:
        """Make Data Catalog entries and tags for the Sisense assets the
        current user has access to.

        Returns:
            A ``dict``. Keys are the top level asset ids and values are flat
            lists containing those assets and their nested ones, with all
            related entries and tags.
        """
        assembled_entries = {}

        for asset_id, assets in assembled_metadata.items():
            assembled_entries[asset_id] = self.__assembled_entry_factory \
                    .make_assembled_entries_list(assets, tag_templates)

        return assembled_entries

    @classmethod
    def __map_datacatalog_relationships(
            cls,
            assembled_entries_dict: Dict[str,
                                         List[AssembledEntryData]]) -> None:

        all_assembled_entries = []
        for assembled_entries_data in assembled_entries_dict.values():
            all_assembled_entries.extend(assembled_entries_data)

        prepare.EntryRelationshipMapper().fulfill_tag_fields(
            all_assembled_entries)

    def __delete_obsolete_entries(
        self,
        new_assembled_entries_dict: Dict[str,
                                         List[AssembledEntryData]]) -> None:

        all_assembled_entries = []
        for assembled_entry_data in new_assembled_entries_dict.values():
            all_assembled_entries.extend(assembled_entry_data)

        cleanup.DataCatalogMetadataCleaner(
            self.__project_id, self.__location_id, self.__ENTRY_GROUP_ID). \
            delete_obsolete_metadata(
            all_assembled_entries,
            f'system={self.__SPECIFIED_SYSTEM}'
            f' tag:server_url:{self.__server_address}')

    def __ingest_metadata(self,
                          assembled_entries: Dict[str,
                                                  List[AssembledEntryData]],
                          tag_templates: Dict[str, TagTemplate]) -> None:

        metadata_ingestor = ingest.DataCatalogMetadataIngestor(
            self.__project_id, self.__location_id, self.__ENTRY_GROUP_ID)

        entry_count = sum(
            len(entries) for entries in assembled_entries.values())
        logging.info('==== %d entries to be synchronized!', entry_count)

        synced_entry_count = 0
        for asset_id, assembled_entries in assembled_entries.items():
            asset_entry_count = len(assembled_entries)

            logging.info('')
            logging.info('==== The asset identified by %s has %d entries.',
                         asset_id, asset_entry_count)
            metadata_ingestor.ingest_metadata(assembled_entries, tag_templates)
            synced_entry_count += asset_entry_count

        logging.info('')
        logging.info('==== %d of %d entries successfully synchronized!',
                     synced_entry_count, entry_count)
