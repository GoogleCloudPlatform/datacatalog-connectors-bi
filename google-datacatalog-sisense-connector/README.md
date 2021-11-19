# google-datacatalog-sisense-connector

Package for ingesting [Sisense](https://www.sisense.com) metadata into Google
Cloud Data Catalog, currently supporting the below assets:
- Folder
- Dashboard
- Widget

This sample connector creates Data Catalog tags to enable a data lineage
mechanism that allows users to search the catalog to find where/which
ElastiCube Table fields are used in Widgets or Dashboards. To do so, it
currently processes JAQL query metadata from:
- Dashboard filters
- Widgets fields and filters
- Nested [formulas](https://sisense.dev/reference/jaql/#formulas)
- Nested `filter.by` properties, used by [top/bottom filters](https://sisense.dev/reference/jaql/#top-bottom-filters)

**Disclaimer: This is not an officially supported Google product.**

> This connector is a work in progress!

<!--
  ⚠️ DO NOT UPDATE THE TABLE OF CONTENTS MANUALLY ️️⚠️
  run `npx markdown-toc -i README.md`.

  Please stick to 80-character line wraps as much as you can.
-->

## Table of Contents

<!-- toc -->

- [1. Installation](#1-installation)
  * [1.1. Mac/Linux](#11-maclinux)
  * [1.2. Windows](#12-windows)
  * [1.3. Install from source](#13-install-from-source)
    + [1.3.1. Get the code](#131-get-the-code)
    + [1.3.2. Create and activate a *virtualenv*](#132-create-and-activate-a-virtualenv)
    + [1.3.3. Install the library](#133-install-the-library)
- [2. Environment setup](#2-environment-setup)
  * [2.1. Auth credentials](#21-auth-credentials)
    + [2.1.1. Create a GCP Service Account and grant it below roles](#211-create-a-gcp-service-account-and-grant-it-below-roles)
    + [2.1.2. Download a JSON key and save it as](#212-download-a-json-key-and-save-it-as)
  * [2.2. Set environment variables](#22-set-environment-variables)
- [3. Running the connector](#3-running-the-connector)
  * [3.1. sync-catalog](#31-sync-catalog)
    + [3.1.1. Python entry point](#311-python-entry-point)
    + [3.1.2. Docker entry point](#312-docker-entry-point)
  * [3.2. find-elasticube-deps](#32-find-elasticube-deps)
    + [3.2.1. Python entry point](#321-python-entry-point)
    + [3.2.2. Docker entry point](#322-docker-entry-point)
  * [3.3. list-elasticube-deps](#33-list-elasticube-deps)
    + [3.3.1. Python entry point](#331-python-entry-point)
    + [3.3.2. Docker entry point](#332-docker-entry-point)
- [4. Developer environment](#4-developer-environment)
  * [4.1. Install and run the YAPF formatter](#41-install-and-run-the-yapf-formatter)
  * [4.2. Install and run the Flake8 linter](#42-install-and-run-the-flake8-linter)
  * [4.3. Run Tests](#43-run-tests)
  * [4.4. Additional resources](#44-additional-resources)
- [5. Templates, Tags, and Data Lineage](#5-templates-tags-and-data-lineage)
- [6. Troubleshooting](#6-troubleshooting)
  * [6.1. Sisense APIs compatibility](#61-sisense-apis-compatibility)
  * [6.2. Data Catalog quota](#62-data-catalog-quota)

<!-- tocstop -->

---

## 1. Installation

Install this library in a [virtualenv][1] using pip. [Virtualenv][1] is a tool
to create isolated Python environments. The basic problem it addresses is one
of dependencies and versions, and indirectly permissions.

With [virtualenv][1], it's possible to install this library without needing
system install permissions, and without clashing with the installed system
dependencies. Make sure you use Python 3.6+.

### 1.1. Mac/Linux

```shell script
pip3 install virtualenv
virtualenv --python python3.6 <your-env>
source <your-env>/bin/activate
<your-env>/bin/pip install google-datacatalog-sisense-connector
```

### 1.2. Windows

```shell script
pip3 install virtualenv
virtualenv --python python3.6 <your-env>
<your-env>\Scripts\activate
<your-env>\Scripts\pip.exe install google-datacatalog-sisense-connector
```

### 1.3. Install from source

#### 1.3.1. Get the code

````shell script
git clone https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi/
cd datacatalog-connectors-bi/google-datacatalog-sisense-connector
````

#### 1.3.2. Create and activate a *virtualenv*

```shell script
pip3 install virtualenv
virtualenv --python python3.6 <your-env>
source <your-env>/bin/activate
```

#### 1.3.3. Install the library

```shell script
pip install .
```

## 2. Environment setup

### 2.1. Auth credentials

#### 2.1.1. Create a GCP Service Account and grant it below roles

- Data Catalog Admin

#### 2.1.2. Download a JSON key and save it as
- `<YOUR-CREDENTIALS_FILES_FOLDER>/sisense2dc-datacatalog-credentials.json`

> Please notice this folder and file will be required in next steps.

### 2.2. Set environment variables

Replace below values according to your environment:

```shell script
export GOOGLE_APPLICATION_CREDENTIALS=datacatalog_credentials_file

export SISENSE2DC_SISENSE_SERVER=sisense_server
export SISENSE2DC_SISENSE_USERNAME=sisense_username
export SISENSE2DC_SISENSE_PASSWORD=sisense_password
export SISENSE2DC_DATACATALOG_PROJECT_ID=google_cloud_project_id
export SISENSE2DC_DATACATALOG_LOCATION_ID=google_cloud_location_id
```

> Replace above values according to your environment. The Data Catalog
> credentials file was saved in [step
> 2.1.2](#212-download-a-json-key-and-save-it-as).

## 3. Running the connector

### 3.1. sync-catalog

Synchronizes Google Data Catalog with a given Sisense server.

- The `--datacatalog-location-id` argument is optional and defaults to `us`.

#### 3.1.1. Python entry point

- Virtualenv

```shell script
google-datacatalog-sisense-connector sync-catalog \
  --sisense-server $SISENSE2DC_SISENSE_SERVER \
  --sisense-username $SISENSE2DC_SISENSE_USERNAME \
  --sisense-password $SISENSE2DC_SISENSE_PASSWORD \
  --datacatalog-project-id $SISENSE2DC_DATACATALOG_PROJECT_ID \
  [--datacatalog-location-id $SISENSE2DC_DATACATALOG_LOCATION_ID]
```

#### 3.1.2. Docker entry point

```shell script
docker build --rm --tag sisense2datacatalog .
docker run --rm --tty -v YOUR-CREDENTIALS_FILES_FOLDER:/data \
  sisense2datacatalog sync-catalog \
  --sisense-server $SISENSE2DC_SISENSE_SERVER \
  --sisense-username $SISENSE2DC_SISENSE_USERNAME \
  --sisense-password $SISENSE2DC_SISENSE_PASSWORD \
  --datacatalog-project-id $SISENSE2DC_DATACATALOG_PROJECT_ID \
  [--datacatalog-location-id $SISENSE2DC_DATACATALOG_LOCATION_ID]
```

### 3.2. find-elasticube-deps

Finds ElastiCube dependencies through catalog search and prints them in the
console.

#### 3.2.1. Python entry point

- Virtualenv

```shell script
google-datacatalog-sisense-connector find-elasticube-deps \
  --datasource <datasource> \
  --table <table> \
  --column <column> \
  --datacatalog-project-id $SISENSE2DC_DATACATALOG_PROJECT_ID
```

#### 3.2.2. Docker entry point

```shell script
docker build --rm --tag sisense2datacatalog .
docker run --rm --tty -v YOUR-CREDENTIALS_FILES_FOLDER:/data \
  sisense2datacatalog find-elasticube-deps \
  --datasource <datasource> \
  --table <table> \
  --column <column> \
  --datacatalog-project-id $SISENSE2DC_DATACATALOG_PROJECT_ID
```

### 3.3. list-elasticube-deps

Lists ElastiCube dependencies for a given Sisense Dashboard or Widget and
prints them in the console.

#### 3.3.1. Python entry point

- Virtualenv

```shell script
google-datacatalog-sisense-connector list-elasticube-deps \
  --asset-url <asset-url> \
  --datacatalog-project-id $SISENSE2DC_DATACATALOG_PROJECT_ID
```

#### 3.3.2. Docker entry point

```shell script
docker build --rm --tag sisense2datacatalog .
docker run --rm --tty -v YOUR-CREDENTIALS_FILES_FOLDER:/data \
  sisense2datacatalog list-elasticube-deps \
  --asset-url <asset-url> \
  --datacatalog-project-id $SISENSE2DC_DATACATALOG_PROJECT_ID
```

## 4. Developer environment

### 4.1. Install and run the YAPF formatter

```shell script
pip install --upgrade yapf

# Auto update files
yapf --in-place --recursive src tests

# Show diff
yapf --diff --recursive src tests

# Set up pre-commit hook
# From the root of your git project.
curl -o pre-commit.sh https://raw.githubusercontent.com/google/yapf/master/plugins/pre-commit.sh
chmod a+x pre-commit.sh
mv pre-commit.sh .git/hooks/pre-commit
```

### 4.2. Install and run the Flake8 linter

```shell script
pip install --upgrade flake8
flake8 src tests
```

### 4.3. Run Tests

```shell script
python setup.py test
```

### 4.4. Additional resources

Please refer to the [Developer Resources
documentation](docs/developer-resources).

## 5. Templates, Tags, and Data Lineage

The Data Catalog Tag Templates created by this connector and their usage
scenarios are described below:

| TAG TEMPLATE                                      | FIELDS                                                                                                                                                                                                                                                                        | USAGE                                                                                                        |
| ------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------ |
| Folder Metadata (`sisense_folder_metadata`)       | <ul><li>Id</li><li>Owner username</li><li>Owner name</li><li>Id of Parent</li><li>Parent Folder</li><li>Data Catalog Entry for the parent Folder</li><li>Has children</li><li>Child count</li><li>Has dashboards</li><li>Dashboard count</li><li>Sisense Server Url</li></ul> | Store additional metadata for Folder-related Entries.                                                        |
| Dashboard Metadata (`sisense_dashboard_metadata`) | <ul><li>Id</li><li>Owner username</li><li>Owner name</li><li>Folder Id</li><li>Folder Name</li><li>Data Catalog Entry for the Folder</li><li>Data Source</li><li>Time it was last published</li><li>Time it was last opened</li><li>Sisense Server Url</li></ul>              | Store additional metadata for Dashboard-related Entries.                                                     |
| Widget Metadata (`sisense_widget_metadata`)       | <ul><li>Id</li><li>Type</li><li>Subtype</li><li>Owner username</li><li>Owner name</li><li>Dashboard Id</li><li>Dashboard Title</li><li>Data Catalog Entry for the Dashboard</li><li>Data Source</li><li>Sisense Server Url</li></ul>                                          | Store additional metadata for Widget-related Entries.                                                        |
| JAQL Metadata (`sisense_jaql_metadata`)           | <ul><li>Table</li><li>Column</li><li>Dimension</li><li>Formula</li><li>Aggregation</li><li>Sisense Server Url</li></ul>                                                                                                                                                       | Store JAQL metadata for ElasticCube-dependent entities such as Dashboard filters, Widget fields and filters. |

Please notice the connector creates Data Catalog Tags for most of the Dashboard
and Widget properties that depend on JAQL queries, e.g., fields, filters,
nested formulas, and top/bottom filters. Such tags, created from the **JAQL
Metadata** template, are quite simple: ~4 fields each. The connector uses lots
of them to enable column-level lineage tracking for a given Sisense server.

## 6. Troubleshooting

### 6.1. Sisense APIs compatibility

The connector may fail during the scrape stage if the Sisense API do not return
metadata in the expected format. As a reference, the below versions were
already validated:

- Sisense REST API v1.0

| VERSION       | RESULT  |
| ------------- | :-----: |
| Windows 8.2.5 | SUCCESS |

### 6.2. Data Catalog quota

In case a connector execution hits Data Catalog quota limit, an error will be
raised and logged with the following details, depending on the performed
operation (READ/WRITE/SEARCH): 

```
status = StatusCode.RESOURCE_EXHAUSTED
details = "Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute' of service 'datacatalog.googleapis.com' for consumer 'project_number:1111111111111'."
debug_error_string = 
"{"created":"@1587396969.506556000", "description":"Error received from peer ipv4:172.217.29.42:443","file":"src/core/lib/surface/call.cc","file_line":1056,"grpc_message":"Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute' of service 'datacatalog.googleapis.com' for consumer 'project_number:1111111111111'.","grpc_status":8}"
```

For more information on Data Catalog quota, please refer to the [product
documentation][2].

[1]: https://virtualenv.pypa.io/en/latest/
[2]: https://cloud.google.com/data-catalog/docs/resources/quotas
