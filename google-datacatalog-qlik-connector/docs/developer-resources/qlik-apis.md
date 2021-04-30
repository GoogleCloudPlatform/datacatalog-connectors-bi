# Qlik APIs

The below API docs were used as a reference to develop this connector:

1. [Communicating with Qlik Sense](https://help.qlik.com/en-US/sense-developer/September2020/Subsystems/Platform/Content/Sense_PlatformOverview/Integration/expose-qlik-sense.htm):
   an overview on how to interact with Qlik Sense using authenticated
   communication.
   
1. Qlik Sense Repository Service API (QRS):
   contains all data and configuration information for a Qlik Sense site.
   - [Overview](https://help.qlik.com/en-US/sense-developer/September2020/Subsystems/RepositoryServiceAPI/Content/Sense_RepositoryServiceAPI/RepositoryServiceAPI-Introduction.htm)
   - [Specification](https://help.qlik.com/en-US/sense-developer/September2020/APIs/RepositoryServiceAPI/index.html?page=0)
   
1. Qlik Engine JSON API:
   this API is a WebSocket protocol that uses JSON to pass information between
   the Qlik associative engine and the clients. It consists of a set of objects
   representing apps, lists, and so on. These objects are organized in a 
   hierarchical structure. When you send requests to the API, you perform
   actions on these objects.
   - [Overview](https://help.qlik.com/en-US/sense-developer/September2020/Subsystems/EngineAPI/Content/Sense_EngineAPI/introducing-engine-API.htm)
   - [Specification](https://help.qlik.com/en-US/sense-developer/September2020/APIs/EngineAPI/index.html)
   - API explorer tool: `https://<your-qlik-site>/dev-hub/engine-api-explorer`

---

Back to the [Developer Resources index](..)
