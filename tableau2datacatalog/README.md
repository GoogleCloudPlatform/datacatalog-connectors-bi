# tableau2datacatalog

Package for ingesting Tableau metadata into Google Cloud Data Catalog,
currently supporting below asset types:
- Workbook
- Sheet
- Dashboard

**Disclaimer: This is not an officially supported Google product.**

<!--
  ⚠️ DO NOT UPDATE THE TABLE OF CONTENTS MANUALLY ️️⚠️
  run `npx markdown-toc -i README.md`.

  Please stick to 80-character line wraps as much as you can.
-->

## Table of Contents

<!-- toc -->

- [1. Environment setup](#1-environment-setup)
  * [1.1. Get the code](#11-get-the-code)
  * [1.2. Auth credentials](#12-auth-credentials)
      - [1.2.1. Create a service account and grant it below roles](#121-create-a-service-account-and-grant-it-below-roles)
      - [1.2.2. Download a JSON key and save it as](#122-download-a-json-key-and-save-it-as)
  * [1.3. Virtualenv](#13-virtualenv)
      - [1.3.1. Install Python 3.6+](#131-install-python-36)
      - [1.3.2. Create and activate a *virtualenv*](#132-create-and-activate-a-virtualenv)
      - [1.3.3. Install the dependencies](#133-install-the-dependencies)
      - [1.3.4. Set environment variables](#134-set-environment-variables)
  * [1.4. Docker](#14-docker)
- [2. Sample application entry point](#2-sample-application-entry-point)
  * [2.1. Run the tableau2datacatalog script](#21-run-the-tableau2datacatalog-script)
- [3. Set up a Tableau demo server](#3-set-up-a-tableau-demo-server)
- [4. Developer environment](#4-developer-environment)
  * [4.1. Install and run Yapf formatter](#41-install-and-run-yapf-formatter)
  * [4.2. Install and run Flake8 linter](#42-install-and-run-flake8-linter)
  * [4.3. Run Tests](#43-run-tests)
- [5. Troubleshooting](#5-troubleshooting)

<!-- tocstop -->

-----

## 1. Environment setup

### 1.1. Get the code

````bash
git clone https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi.git
cd datacatalog-connectors-bi/tableau2datacatalog
````

### 1.2. Auth credentials

##### 1.2.1. Create a service account and grant it below roles

- Data Catalog Admin

##### 1.2.2. Download a JSON key and save it as
- `<YOUR-CREDENTIALS_FILES_FOLDER>/tableau2dc-datacatalog-credentials.json`

> Please notice this folder and file will be required in next steps.

### 1.3. Virtualenv

Using *virtualenv* is optional, but strongly recommended unless you use Docker
or a PEX file.

##### 1.3.1. Install Python 3.6+

##### 1.3.2. Create and activate a *virtualenv*

```bash
pip install --upgrade virtualenv
python3 -m virtualenv --python python3 env
source ./env/bin/activate
```

##### 1.3.3. Install the dependencies

```bash
pip install ./lib/datacatalog_connectors_commons-1.0.0-py2.py3-none-any.whl
pip install --editable .
```

##### 1.3.4. Set environment variables

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

### 1.4. Docker

See instructions below.

## 2. Sample application entry point

### 2.1. Run the tableau2datacatalog script

- Virtualenv

```bash
tableau2datacatalog \
  --tableau-server $TABLEAU2DC_TABLEAU_SERVER \
  --tableau-api-version $TABLEAU2DC_TABLEAU_API_VERSION \
  --tableau-username $TABLEAU2DC_TABLEAU_USERNAME \
  --tableau-password $TABLEAU2DC_TABLEAU_PASSWORD \
  [--tableau-site $TABLEAU2DC_TABLEAU_SITE \]
  --datacatalog-project-id $TABLEAU2DC_DATACATALOG_PROJECT_ID
```

- Or using Docker

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

## 3. Set up a Tableau demo server

To quickly set up a Tableau demo server, please visit
https://www.tableau.com/developer.

Click on SIGN UP FOR THE TABLEAU DEVELOPER PROGRAM. Once you have signed up you
will receive an e-mail with subject _Tableau Online Developer - Activate your
Site_.

In the e-mail contents click on: Activate My Developer Site. Once you've done
that you will receive another e-mail with subject _You've Successfully Created
Your Site_.

Then you will be able to use your Tableau Online dev server.

## 4. Developer environment

### 4.1. Install and run Yapf formatter

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

### 4.2. Install and run Flake8 linter

```bash
pip install --upgrade flake8
flake8 src tests
```

### 4.3. Run Tests

```bash
python setup.py test
```
## 5. Troubleshooting

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
