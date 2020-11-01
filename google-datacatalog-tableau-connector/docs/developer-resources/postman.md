# Postman

Developers may leverage [Postman](https://www.postman.com/) to validate API
requests. The available collections cover both Tableau GraphQL and REST APIs.  

## Collection for the Tableau GraphQL API

1. File: [tools/postman/Tableau GraphQL API.postman_collection.json](../../tools/postman/Tableau%20GraphQL%20API.postman_collection.json)

1. Postman Environment Variables:

   | NAME          | DESCRIPTION                                                             |
   | ------------- | ----------------------------------------------------------------------- |
   | `PROTOCOL`    | `http` or `https`                                                       |
   | `SERVER`      | URL of the Tableau server, e.g. `10ax.online.tableau.com`               |
   | `API_VERSION` | Version of the Tableau REST API (for user authentication*), e.g. `3.6`) |
   | `USERNAME`    | Username to authenticate* the API calls                                 |
   | `PASSWORD`    | Password to authenticate* the API calls                                 |
   | `SITE_URL`    | Name of the site to authenticate* (a single server can host many sites) |
   
   \* Authentication is always performed through the REST API.

## Collection for the Tableau REST API

1. File: [tools/postman/Tableau REST API.postman_collection.json](../../tools/postman/Tableau%20REST%20API.postman_collection.json)

1. Postman Environment Variables:

   | NAME          | DESCRIPTION                                                            |
   | ------------- | ---------------------------------------------------------------------- |
   | `PROTOCOL`    | `http` or `https`                                                      |
   | `SERVER`      | URL of the Tableau server, e.g. `10ax.online.tableau.com`              |
   | `API_VERSION` | Version of the Tableau REST API, e.g. `3.6`                            |
   | `USERNAME`    | Username to authenticate the API calls                                 |
   | `PASSWORD`    | Password to authenticate the API calls                                 |
   | `SITE_URL`    | Name of the site to authenticate (a single server can host many sites) |
