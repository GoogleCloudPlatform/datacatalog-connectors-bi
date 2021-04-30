# Postman

Developers may leverage [Postman](https://www.postman.com/) to validate API
requests. The available collection covers the [Qlik Sense Repository Service
API](https://help.qlik.com/en-US/sense-developer/September2020/Subsystems/RepositoryServiceAPI/Content/Sense_RepositoryServiceAPI/RepositoryServiceAPI-Introduction.htm).

## Collection for the Qlik Sense Repository Service API

1. File: [tools/postman/Qlik Sense Repository Service API.postman_collection.json](../../tools/postman/Qlik%20Sense%20Repository%20Service%20API.postman_collection.json)

1. Postman Environment Variables:

   | NAME       | DESCRIPTION                                                                               |
   | ---------- | ----------------------------------------------------------------------------------------- |
   | `SCHEME`   | The URI scheme: `http` or `https`                                                                         |
   | `SERVER`   | URL of the Qlik Sense server                                                              |
   | `DOMAIN`   | AD domain for the user                                                                    |
   | `USERNAME` | Username to authenticate the API calls                                                    |
   | `PASSWORD` | Password to authenticate the API calls                                                    |
   | `XRFKEY`   | 16 arbitrary characters to fulfill the `Xrfkey` parameter and `x-Qlik-Xrfkey` HTTP header |

1. Sample environment file: [tools/postman/qliksense-sample-environment.postman_environment.json](../../tools/postman/qliksense-sample-environment.postman_environment.json)

---

Back to the [Developer Resources index](..)
