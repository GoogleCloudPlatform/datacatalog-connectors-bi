# Design decisions

## Tag Templates for Custom Property Choice Values

The current implementation creates a Tag Template for each Custom Property 
Choice Value assigned to Streams or Apps in the provided Qlik Sense site. The
rationale behind this decision comprises allowing the connector to synchronize
all metadata scraped from each Custom Property, at the same time it enables
Qlik assets to be easily found by their custom properties — using query strings
such as `tag:property_name:"<PROPERTY-NAME>"` and `tag:value:"<SOME-VALUE>"`.

Data Catalog accepts attaching only one Tag per Template to a given Entry, so
there could be metadata loss if the Tag Templates were created in different
ways, e.g. on a Custom Property Definition basis.

Lastly, this approach may lead to the creation of several Tag Templates if
there are many Custom Property Values in use in your Qlik Sense site. In case
you would like to suggest a different approach to tackle this problem, please
[file a feature
request](https://github.com/GoogleCloudPlatform/datacatalog-connectors-bi/issues).
We will be happy to discuss alternative solutions! 

---

Back to the [Documentation index](README.md)
