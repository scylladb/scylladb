# Scylla RESTful API V2
The Scylla has a RESTful API from its first version. The first API version was confusing, it will gradually be replaced by its successor V2.

The API definition is in the [Swagger 2.0 format](https://github.com/OAI/OpenAPI-Specification/blob/master/versions/2.0.md).


You can get the swagger definition file by pointing your browser to:
http://localhost:10000/v2

As an alternative, you can use the swagger ui, which is a javascript GUI based on swagger.


You can use the swagger-ui with Scylla by pointing your browser to http://localhost:10000/ui

make sure the URL in the URL box is: http://localhost:10000/v2

The API is split into sections.

## Config
By expending the config section you can see all of the available configuration.

The values returned by the API are the current values in the system (regardless if it is the default value, taken from a config file or from a command line parameter)