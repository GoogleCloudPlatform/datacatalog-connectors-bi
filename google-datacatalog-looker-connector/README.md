# google-datacatalog-looker-connector

Package for ingesting Looker metadata into Google Cloud Data Catalog, currently
supporting below asset types:
- Folder
- Look
- Dashboard
- Dashboard Element (aka Tile)
- Query

[![Python package][4]][4] [![PyPi][5]][6] [![License][7]][7] [![Issues][8]][9]

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
    + [2.1.1. Create a GCP Service Account and grant it below roles](#211-create-a-gcp-service-account-and-grant-it-below-roles)
    + [2.1.2. Download a JSON key and save it as](#212-download-a-json-key-and-save-it-as)
    + [2.1.3. Create Looker API3 credentials](#213-create-looker-api3-credentials)
    + [2.1.4. Create a Looker configuration file](#214-create-a-looker-configuration-file)
  * [2.2. Set environment variables](#22-set-environment-variables)
- [3. Run entry point](#3-run-entry-point)
  * [3.1. Run Python entry point](#31-run-python-entry-point)
  * [3.2. Run Docker entry point](#32-run-docker-entry-point)
- [4. Developer environment](#4-developer-environment)
  * [4.1. Install and run Yapf formatter](#41-install-and-run-yapf-formatter)
  * [4.2. Install and run Flake8 linter](#42-install-and-run-flake8-linter)
  * [4.3. Run Tests](#43-run-tests)
  * [4.4. Additional resources](#44-additional-resources)
- [5. Troubleshooting](#5-troubleshooting)

<!-- tocstop -->

-----

## 1. Installation

Install this library in a [virtualenv][3] using pip. [virtualenv][3] is a tool to
create isolated Python environments. The basic problem it addresses is one of
dependencies and versions, and indirectly permissions.

With [virtualenv][3], it's possible to install this library without needing system
install permissions, and without clashing with the installed system
dependencies. Make sure you use Python 3.7+.


### 1.1. Mac/Linux

```bash
pip3 install virtualenv
virtualenv --python python3.7 <your-env>
source <your-env>/bin/activate
<your-env>/bin/pip install google-datacatalog-looker-connector
```

### 1.2. Windows

```bash
pip3 install virtualenv
virtualenv --python python3.7 <your-env>
<your-env>\Scripts\activate
<your-env>\Scripts\pip.exe install google-datacatalog-looker-connector
```

### 1.3. Install from source

#### 1.3.1. Get the code

````bash
git clone https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi/
cd datacatalog-connectors-bi/google-datacatalog-looker-connector
````

#### 1.3.2. Create and activate a *virtualenv*

```bash
pip3 install virtualenv
virtualenv --python python3.7 <your-env>
source <your-env>/bin/activate
```

#### 1.3.3. Install the library

```bash
pip install .
```

## 2. Environment setup

### 2.1. Auth credentials

#### 2.1.1. Create a GCP Service Account and grant it below roles

- Data Catalog Admin

#### 2.1.2. Download a JSON key and save it as
- `<YOUR-CREDENTIALS_FILES_FOLDER>/looker2dc-datacatalog-credentials.json`

#### 2.1.3. Create Looker API3 credentials

The credentials required for API access must be obtained by creating an
API3 key on a user account in the Looker Admin console. The API3 key consists
of a public `client_id` and a private `client_secret`.

The shortcut for Looker Admin console is
https://<YOUR-LOOKER-ENDPOINT>/admin/users/api3_key/<YOUR-USER-ID>

#### 2.1.4. Create a Looker configuration file

File content is described in [Looker SDK documentation][1].
Save the file as
`<YOUR-CREDENTIALS_FILES_FOLDER>/looker2dc-looker-credentials.ini`

> Please notice this folder and files will be required in next steps.

### 2.2. Set environment variables

```bash
export GOOGLE_APPLICATION_CREDENTIALS=datacatalog_credentials_file
```

> Replace above values according to your environment. The Data Catalog
> credentials file was saved in step 1.2.2.

## 3. Run entry point

### 3.1. Run Python entry point

- Virtualenv

```bash
google-datacatalog-looker-connector \
  --datacatalog-project-id <YOUR-DATACATALOG-PROJECT-ID> \
  --looker-credentials-file looker_credentials_ini_file
```

### 3.2. Run Docker entry point

```bash
docker build --rm --tag looker2datacatalog .
docker run --rm --tty -v <YOUR-CREDENTIALS_FILES_FOLDER>:/data \
  looker2datacatalog \ 
  --datacatalog-project-id <YOUR-DATACATALOG-PROJECT-ID> \
  --looker-credentials-file /data/looker2dc-looker-credentials.ini
```

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

### 4.4. Additional resources

Please refer to the [Developer Resources
documentation](docs/developer-resources).

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

For more information on Data Catalog quota, please refer to: [Data Catalog quota docs][2].

[1]: https://github.com/looker-open-source/sdk-codegen/blob/master/looker-sample.ini
[2]: https://cloud.google.com/data-catalog/docs/resources/quotas
[3]: https://virtualenv.pypa.io/en/latest/
[4]: https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi/workflows/Python%20package/badge.svg?branch=master
[5]: https://img.shields.io/pypi/v/google-datacatalog-looker-connector.svg
[6]: https://pypi.org/project/google-datacatalog-looker-connector/
[7]: https://img.shields.io/github/license/GoogleCloudPlatform/datacatalog-connectors-bi.svg
[8]: https://img.shields.io/github/issues/GoogleCloudPlatform/datacatalog-connectors-bi.svg
[9]: https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi/issues