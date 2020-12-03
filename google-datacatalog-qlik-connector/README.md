# google-datacatalog-qlik-connector

Package for ingesting Qlik Sense metadata into Google Cloud Data Catalog,
currently supporting below asset types:
- Stream
- App (only the published ones)
- Sheet (only the published ones)

**Disclaimer: This is not an officially supported Google product.**

> **WORK IN PROGRESS**: This connector is under active development and breaking
> changes are expected in the coming weeks!

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
  * [3.1. Python entry point](#31-python-entry-point)
  * [3.2. Docker entry point](#32-docker-entry-point)
- [4. Developer environment](#4-developer-environment)
  * [4.1. Install and run Yapf formatter](#41-install-and-run-yapf-formatter)
  * [4.2. Install and run Flake8 linter](#42-install-and-run-flake8-linter)
  * [4.3. Run Tests](#43-run-tests)
  * [4.4. Additional resources](#44-additional-resources)
- [5. Troubleshooting](#5-troubleshooting)

<!-- tocstop -->

---

## 1. Installation

Install this library in a [virtualenv][1] using pip. [virtualenv][1] is a tool
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
<your-env>/bin/pip install google-datacatalog-qlik-connector
```

### 1.2. Windows

```shell script
pip3 install virtualenv
virtualenv --python python3.6 <your-env>
<your-env>\Scripts\activate
<your-env>\Scripts\pip.exe install google-datacatalog-qlik-connector
```

### 1.3. Install from source

#### 1.3.1. Get the code

````shell script
git clone https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi/
cd datacatalog-connectors-bi/google-datacatalog-qlik-connector
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
- `<YOUR-CREDENTIALS_FILES_FOLDER>/qlik2dc-datacatalog-credentials.json`

> Please notice this folder and file will be required in next steps.

### 2.2. Set environment variables

The connector uses Windows-based NTLM authentication, which requires the
username to be provided in the format of `<ẃindows-ad-domain>\<username>`. When
fulfilling the below environment variables, set `QLIK2DC_QLIK_AD_DOMAIN` with
the Windows Active Directory domain your user belongs to and
`QLIK2DC_QLIK_USERNAME` with the username (no backslash in both).

```shell script
export GOOGLE_APPLICATION_CREDENTIALS=datacatalog_credentials_file

export QLIK2DC_QLIK_SERVER=qlik_server
export QLIK2DC_QLIK_AD_DOMAIN=qlik_ad_domain
export QLIK2DC_QLIK_USERNAME=qlik_username
export QLIK2DC_QLIK_PASSWORD=qlik_password
export QLIK2DC_DATACATALOG_PROJECT_ID=google_cloud_project_id
export QLIK2DC_DATACATALOG_LOCATION_ID=google_cloud_location_id
```

> Replace above values according to your environment. The Data Catalog
> credentials file was saved in [step
> 2.1.2](#212-download-a-json-key-and-save-it-as).

## 3. Running the connector

- The `--qlik-ad-domain` argument is optional and defaults to `.`.
- The `--datacatalog-location-id` argument is optional and defaults to `us`.

### 3.1. Python entry point

- Virtualenv

```shell script
google-datacatalog-qlik-connector \
  --qlik-server $QLIK2DC_QLIK_SERVER \
  [--qlik-ad-domain $QLIK2DC_QLIK_AD_DOMAIN \]
  --qlik-username $QLIK2DC_QLIK_USERNAME \
  --qlik-password $QLIK2DC_QLIK_PASSWORD \
  --datacatalog-project-id $QLIK2DC_DATACATALOG_PROJECT_ID \
  [--datacatalog-location-id $QLIK2DC_DATACATALOG_LOCATION_ID]
```

### 3.2. Docker entry point

```shell script
docker build --rm --tag qlik2datacatalog .
docker run --rm --tty -v YOUR-CREDENTIALS_FILES_FOLDER:/data \
  qlik2datacatalog \
  --qlik-server $QLIK2DC_QLIK_SERVER \
  [--qlik-ad-domain $QLIK2DC_QLIK_AD_DOMAIN \]
  --qlik-username $QLIK2DC_QLIK_USERNAME \
  --qlik-password $QLIK2DC_QLIK_PASSWORD \
  --datacatalog-project-id $QLIK2DC_DATACATALOG_PROJECT_ID \
  [--datacatalog-location-id $QLIK2DC_DATACATALOG_LOCATION_ID]
```

## 4. Developer environment

### 4.1. Install and run Yapf formatter

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

### 4.2. Install and run Flake8 linter

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

For more information on Data Catalog quota, please refer to: [Data Catalog
quota docs][2].

[1]: https://virtualenv.pypa.io/en/latest/
[2]: https://cloud.google.com/data-catalog/docs/resources/quotas
