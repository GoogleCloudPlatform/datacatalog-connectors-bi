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

__TABLEAU_USER_FIELDS = """
    username
    name
"""

__DATABASE_FIELDS = """
    luid
    name
    connectionType
"""

__DATABASE_TABLE_FIELDS = """
    fullName
    database {
        luid
    }
"""

__DATASOURCE_FIELDS = f"""
    name
    upstreamTables {{
        {__DATABASE_TABLE_FIELDS}
    }}
    upstreamDatabases {{
        {__DATABASE_FIELDS}
    }}
"""

# Id to be used as fallback when luid is not available.
__DASHBOARD_FIELDS = """
    id
    luid
    name
    path
    createdAt
    updatedAt
    workbook {
        luid
        name
        site {
            name
        }
        description
        vizportalUrlId
        createdAt
        updatedAt
    }
"""

__PUBLISHED_DATASOURCE_FIELDS = f"""
    luid
    {__DATASOURCE_FIELDS}
    site {{
        luid
        name
    }}
    projectName
    owner {{
        {__TABLEAU_USER_FIELDS}
    }}
    isCertified
    certifierDisplayName
    certificationNote
    description
    vizportalUrlId
"""

# Id to be used as fallback when luid is not available.
__SHEETS_FIELDS = """
    id
    luid
    name
    path
    createdAt
    updatedAt
"""

__WORKBOOK_FIELDS = f"""
    luid
    name
    site {{
        luid
        name
    }}
    projectName
    owner {{
        {__TABLEAU_USER_FIELDS}
    }}
    sheets {{
        {__SHEETS_FIELDS}
    }}
    description
    vizportalUrlId
    createdAt
    updatedAt
    upstreamTables {{
        {__DATABASE_TABLE_FIELDS}
    }}
    upstreamDatabases {{
        {__DATABASE_FIELDS}
    }}
"""

__SITE_FIELDS = f"""
    luid
    uri
    name
    publishedDatasources {{
        {__PUBLISHED_DATASOURCE_FIELDS}
    }}
    workbooks {{
        {__WORKBOOK_FIELDS}
    }}
"""

FETCH_DASHBOARDS_QUERY = f"""
query getDashboards($filter: Dashboard_Filter) {{
    dashboards(filter: $filter) {{
        {__DASHBOARD_FIELDS}
    }}
}}
"""

FETCH_SITES_QUERY = f"""
query getSites {{
    tableauSites {{
        {__SITE_FIELDS}
    }}
}}
"""

FETCH_WORKBOOKS_FILTER_TEMPLATE = """
{
    "filter": {
        "luid": "$luid"
    }
}
"""

FETCH_WORKBOOKS_QUERY = f"""
query getWorkbooks($filter: Workbook_Filter) {{
    workbooks(filter: $filter) {{
        {__WORKBOOK_FIELDS}
    }}
}}
"""
