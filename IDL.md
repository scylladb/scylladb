#IDL definition
The schema we use similar to c++ schema.
Use class or struct similar to the object you need the serializer for.
Use namespace when applicable.

##keywords
* class/struct - a class or a struct like C++
  class/struct can have final or stub marker
* namespace - has the same C++ meaning
* enum class - has the same C++ meaning
* final modifier for class - when a class mark as final it will not contain a size parameter. Note that final class cannot be extended by future version, so use with care
* stub class - when a class is mark as stub, it means that no code will be generated for this class and it is only there as a documentation.
* version attributes - mark with [[version id ]] mark that a field is available from a specific version
* template - A template class definition like C++
##Syntax

###Namespace
```
namespace ns_name { namespace-body }
```
* ns_name: either a previously unused identifier, in which case this is original-namespace-definition or the name of a namespace, in which case this is extension-namespace-definition
* namespace-body: possibly empty sequence of declarations of any kind (including class and struct definitions as well as nested namespaces)

###class/struct
`
class-key  class-name final(optional) stub(optional) { member-specification } ;(optional)
`
* class-key: one of class or struct.
* class-name: the name of the class that's being defined. optionally followed by keyword final, optionally followed by keyword stub
* final: when a class mark as final, it means it can not be extended and there is no need to serialize its size, use with care.
* stub: when a class is mark as stub, it means no code will generate for it and it is added for documentation only.
* member-specification: list of access specifiers, and public member accessor see class member below.
* to be compatible with C++ a class definition can be followed by a semicolon.
###enum
`enum-key identifier enum-base { enumerator-list(optional) }`
* enum-key: only enum class is supported
* identifier: the name of the enumeration that's being declared.
* enum-base: colon (:), followed by a type-specifier-seq that names an integral type (see the C++ standard for the full list of all possible integral types).
* enumerator-list: comma-separated list of enumerator definitions, each of which is either simply an identifier, which becomes the name of the enumerator, or an identifier with an initializer: identifier = integral value.
Note that though C++ allows constexpr as an initialize value, it makes the documentation less readable, hence is not permitted.

###class member
`type member-access attributes(optional) default-value(optional);`
* type: Any valid C++ type, following the C++ notation. note that there should be a serializer for the type, but deceleration order is not mandatory
* member-access: is the way the member can be access. If the member is public it can be the name itself. if not it could be a getter function that should be followed by braces. Note that getter can (and probably should) be const methods.
* attributes: Attributes define by square brackets. Currently are use to mark a version in which a specific member was added [ [ version version-number] ] would mark that the specific member was added in the given version number.

###template
`template < parameter-list > class-declaration`
* parameter-list - a non-empty comma-separated list of the template parameters. 
* class-decleration - (See class section) The class name declared become a template name.

##IDL example
Forward slashes comments are ignored until the end of the line.
```
namespace utils {
// An example of a stub class
class UUID stub {
    int64_t most_sig_bits;
    int64_t least_sig_bits;
}
}

namespace gms {
//an enum example
enum class application_state:int {STATUS = 0,
        LOAD,
        SCHEMA,
        DC};

// example of final class
class versioned_value final {
// getter and setter as public member
    int version;
    sstring value;
}

class heart_beat_state {
//getter as function
    int32_t get_generation();
//default value example
    int32_t get_heart_beat_version() = 1;
}

class endpoint_state {
    heart_beat_state get_heart_beat_state();
    std::map<application_state, versioned_value> get_application_state_map();
}

class gossip_digest {
    inet_address get_endpoint();
    int32_t get_generation();
//mark that a field was added on a specific version
    int32_t get_max_version() [ [version 0.14.2] ];
}

class gossip_digest_ack {
    std::vector<gossip_digest> digests();
    std::map<inet_address, gms::endpoint_state> get_endpoint_state_map();
}
}
```

