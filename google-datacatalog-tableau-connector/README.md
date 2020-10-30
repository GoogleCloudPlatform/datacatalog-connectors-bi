# google-datacatalog-tableau-connector

Package for ingesting Tableau metadata into Google Cloud Data Catalog,
currently supporting below asset types:
- Workbook
- Sheet
- Dashboard

[![Python package][3]][3] [![PyPi][4]][5] [![License][6]][6] [![Issues][7]][8]

**Disclaimer: This is not an officially supported Google product.**

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
    + [2.1.1. Create a service account and grant it below roles](#211-create-a-service-account-and-grant-it-below-roles)
    + [2.1.2. Download a JSON key and save it as](#212-download-a-json-key-and-save-it-as)
  * [2.2. Set environment variables](#22-set-environment-variables)
- [3. Run entry point](#3-run-entry-point)
  * [3.1. Run Python entry point](#31-run-python-entry-point)
  * [3.2. Run Docker entry point](#32-run-docker-entry-point)
- [4. Set up a Tableau demo server](#4-set-up-a-tableau-demo-server)
- [5. Developer environment](#5-developer-environment)
  * [5.1. Install and run Yapf formatter](#51-install-and-run-yapf-formatter)
  * [6.2. Install and run Flake8 linter](#62-install-and-run-flake8-linter)
  * [6.3. Run Tests](#63-run-tests)
  * [6.4. Additional resources](#64-additional-resources)
- [7. Troubleshooting](#7-troubleshooting)

<!-- tocstop -->

-----

## 1. Installation

Install this library in a [virtualenv][2] using pip. [virtualenv][2] is a tool to
create isolated Python environments. The basic problem it addresses is one of
dependencies and versions, and indirectly permissions.

With [virtualenv][2], it's possible to install this library without needing system
install permissions, and without clashing with the installed system
dependencies. Make sure you use Python 3.6+.


### 1.1. Mac/Linux

```bash
pip3 install virtualenv
virtualenv --python python3.6 <your-env>
source <your-env>/bin/activate
<your-env>/bin/pip install google-datacatalog-tableau-connector
```

### 1.2. Windows

```bash
pip3 install virtualenv
virtualenv --python python3.6 <your-env>
<your-env>\Scripts\activate
<your-env>\Scripts\pip.exe install google-datacatalog-tableau-connector
```

### 1.3. Install from source

#### 1.3.1. Get the code

````bash
git clone https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi/
cd datacatalog-connectors-bi/google-datacatalog-tableau-connector
````

#### 1.3.2. Create and activate a *virtualenv*

```bash
pip3 install virtualenv
virtualenv --python python3.6 <your-env> 
source <your-env>/bin/activate
```

#### 1.3.3. Install the library

```bash
pip install .
```

## 2. Environment setup

### 2.1. Auth credentials

#### 2.1.1. Create a service account and grant it below roles

- Data Catalog Admin

#### 2.1.2. Download a JSON key and save it as
- `<YOUR-CREDENTIALS_FILES_FOLDER>/tableau2dc-datacatalog-credentials.json`

> Please notice this folder and file will be required in next steps.

### 2.2. Set environment variables

Replace below values according to your environment:

```bash
export GOOGLE_APPLICATION_CREDENTIALS=data_catalog_credentials_file

export TABLEAU2DC_TABLEAU_SERVER=tableau_server
export TABLEAU2DC_TABLEAU_API_VERSION=tableau_api_version
export TABLEAU2DC_TABLEAU_USERNAME=tableau_username
export TABLEAU2DC_TABLEAU_PASSWORD=tableau_password
export TABLEAU2DC_TABLEAU_SITE=tableau_site (optional)
export TABLEAU2DC_DATACATALOG_PROJECT_ID=google_cloud_project_id
```

## 3. Run entry point

### 3.1. Run Python entry point

- Virtualenv

```bash
google-datacatalog-tableau-connector \
  --tableau-server $TABLEAU2DC_TABLEAU_SERVER \
  --tableau-api-version $TABLEAU2DC_TABLEAU_API_VERSION \
  --tableau-username $TABLEAU2DC_TABLEAU_USERNAME \
  --tableau-password $TABLEAU2DC_TABLEAU_PASSWORD \
  [--tableau-site $TABLEAU2DC_TABLEAU_SITE \]
  --datacatalog-project-id $TABLEAU2DC_DATACATALOG_PROJECT_ID
```

### 3.2. Run Docker entry point

```bash
docker build --rm --tag tableau2datacatalog .
docker run --rm --tty -v YOUR-CREDENTIALS_FILES_FOLDER:/data \
  tableau2datacatalog \
  --tableau-server $TABLEAU2DC_TABLEAU_SERVER \ 
  --tableau-api-version $TABLEAU2DC_TABLEAU_API_VERSION \
  --tableau-username $TABLEAU2DC_TABLEAU_USERNAME \
  --tableau-password $TABLEAU2DC_TABLEAU_PASSWORD \
  [--tableau-site] $TABLEAU2DC_TABLEAU_SITE \]
  --datacatalog-project-id $TABLEAU2DC_DATACATALOG_PROJECT_ID
```

## 4. Set up a Tableau demo server

To quickly set up a Tableau demo server, please visit
https://www.tableau.com/developer.

Click on SIGN UP FOR THE TABLEAU DEVELOPER PROGRAM. Once you have signed up you
will receive an e-mail with subject _Tableau Online Developer - Activate your
Site_.

In the e-mail contents click on: Activate My Developer Site. Once you've done
that you will receive another e-mail with subject _You've Successfully Created
Your Site_.

Then you will be able to use your Tableau Online dev server.

## 5. Developer environment

### 5.1. Install and run Yapf formatter

```bash
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

### 6.2. Install and run Flake8 linter

```bash
pip install --upgrade flake8
flake8 src tests
```

### 6.3. Run Tests

```bash
python setup.py test
```

### 6.4. Additional resources

Please refer to the [Developer Resources
documentation](docs/developer-resources).

## 7. Troubleshooting

In the case a connector execution hits Data Catalog quota limit, an error will
be raised and logged with the following detailment, depending on the performed
operation READ/WRITE/SEARCH: 

```
status = StatusCode.RESOURCE_EXHAUSTED
details = "Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute' of service 'datacatalog.googleapis.com' for consumer 'project_number:1111111111111'."
debug_error_string = 
"{"created":"@1587396969.506556000", "description":"Error received from peer ipv4:172.217.29.42:443","file":"src/core/lib/surface/call.cc","file_line":1056,"grpc_message":"Quota exceeded for quota metric 'Read requests' and limit 'Read requests per minute' of service 'datacatalog.googleapis.com' for consumer 'project_number:1111111111111'.","grpc_status":8}"
```

For more information on Data Catalog quota, please refer to: [Data Catalog quota docs][1]

[1]: https://cloud.google.com/data-catalog/docs/resources/quotas
[2]: https://virtualenv.pypa.io/en/latest/
[3]: https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi/workflows/Python%20package/badge.svg?branch=master
[4]: https://img.shields.io/pypi/v/google-datacatalog-tableau-connector.svg
[5]: https://pypi.org/project/google-datacatalog-tableau-connector/
[6]: https://img.shields.io/github/license/GoogleCloudPlatform/datacatalog-connectors-bi.svg
[7]: https://img.shields.io/github/issues/GoogleCloudPlatform/datacatalog-connectors-bi.svg
[8]: https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi/issues
