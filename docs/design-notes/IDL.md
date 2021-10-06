# IDL compiler

IDL compiler is a tool written in Python, that generates utility serialization/de-serialization code (specializations
for `ser::serializer<T>` class) for C++ classes and enums. It takes an IDL definition file as input and generates two
files from it: `<mod_name>.dist.hh`, which is declarations part, and `<mod_name>.dist.impl.hh`, which contains generated
code definitions (private part).

The syntax of IDL is similar to C++ to some degree (e.g. contains `struct/class` and `enum class` constructs), but is
also extended to support more complex things in the context of RPC serialization: RPC messages (also called
"RPC verbs").

By default, all generated code for Scylla is created under `ser` namespace.

## File syntax description

As noted above, the syntax of IDL definitions file resembles C++ by providing similar-looking code constructs for class
and enum serializers.

The IDL file contains a sequence of entity declarations, most of which correspond directly to those from the actual
C++ code:

- Namespaces (`namespace`)
- Classes / structures (`class` / `struct`)
- Enums (`enum class`)
- RPC verbs (`verb`)

Classes can be templated, supporting the ordinary `template <typename T>`-style notation at the beginning of the class
declaration, similar to C++.

Some places (e.g. classes, class members and RPC verbs) also support modifying generation behavior by providing
attribute sequences (via C++ syntax of `[[attr]]` form).

### Namespaces

Namespaces in IDL act the same way as in C++ or any other programming language, and can have arbitrary nesting depth.
Syntax:

```
namespace <ns-name> { <namespace-body> }
```

Where:

- `ns-name` — namespace identifier. Directly corresponds to the namespace from C++, defining a new namespace or
  extending an existing one.
- `namespace-body` — a sequence of 0+ nested entities of any kind: classes, enums, RPC verbs or other nested
  namespaces.

Example:

```
namespace ns {

class my_class {
    int a;
    bool b;
    std::vector<int> c;
};

enum class e {
    E1,
    E2
};

// ...

} // namespace ns
```

### Classes

Class declaration creates a `ser::serializer<T>` specialization для for a class with a given name, along with an
implementation of `write`, `read` and `skip` methods. Syntax:

```
template <template-parameter-list> (optional)
class-key <class-name> final(optional) attributes_seq(optional) stub(optional) { <nested-class> | <member-specification> } ;(optional)
```

Where:

- `template-parameter-list` - the list of template arguments in case a class is a template.
- `class-key` - either `class`, or `struct`.
- `class-name` - class identifier, for which `ser::serializer<T>` specialization is to be created.
- `final` specifier — an optimization option, which denotes that a class is final, i.e. cannot be extended in the
  future, in which case the size is not serialized. Should be used with care.
- `stub` specifier — skip generating serialization code for a class. Initially designed for documentation within IDL
  (there is one exception to that with `[[writable]]` attribute, though. More on that below).
- `attributes_seq` - optional sequence of C++-style attributes. Only `[[writable]]` attribute is supported at the
  moment, which means that: if specified, the writers and serialization  views will also be generated for a class
  (inside the private `<mod_name>.dist.impl.hh` part). Other attributes are ignored.
- `nested-class` - nested class definition following the same syntax.
- `member-specification` — data members and getter functions declarations (more details on the syntax below). Getter
  functions should be used in cases where access to private class fields is needed. Both kinds of field accessors can 
  also be marked with `[[version id]]` version attribute, which denotes, that a field is accessible starting from
  version `id`.

A class declaration can optionally include a semicolon at the end to more closely resemble C++ syntax.

If a class contains both `stub` specifier and `[[writable]]` attribute at the same time, the `ser::serializer` code is
not generated but the serialization views (classes with `_view` suffix in the name, that support reading and writing
data in the stream according to some fixed data layout) are created, nonetheless.

Class members are declared the following way:

```
<type> name <getter-marker>(optional) [[version version-id]](optional) <default-value>(optional);
```

Where:

- `type` - any valid C++ type, following the regular C++ syntax. Naturally, a serializer specialization for this
  type should exist in order to serialize/deserialize it.
- `name` - accessor name. For ordinary data fields it is just a C++ name of a class field. In case it's a getter 
  function, it also should contain a "getter marker", which is denoted as empty `()` braces sequence right after the
  name. As noted above, getter functions should be used if a field is not accessible (i.e. is private), otherwise a
  regular data field can be used. Note, that getter functions can (and probably should) be const methods.
- version attribute — specify that a field is accessible starting from version `version-id` and above.
- `default-value` — an optional clause to specify default value for a field accessor. The syntax is: `= <value>`.

### Enums

Analogous to classes, `ser::serializer<T>` specializations can also be generated for enums. Declaration syntax:

```
enum class identifier enum-base { enumerator-list } ;(optional)
```

Where:

- `identifier` - the name of C++ enum class.
- `enum-base` - mandatory specification of C++ underlying type for the enum, following the regular C++ syntax: `: integer-type`
- `enumerator-list` - a list of enum cases or initializers of the following form: `name = integer-value`, where
  `integer-value` is a plain integer literal value.

Note that although C++ allows `constexpr` as an initializer value, it makes the documentation less readable, hence is
not permitted.

### RPC Verbs

IDL can also contain declarations of RPC messages with a given signature (also called: "RPC verbs"). It allows to
automatically generate boilerplate code for message handlers registration and message passing code via an instance of
`netw::messaging_service` class. Declaration syntax:

```
verb id [[attributes...]](optional) (parameters...) (-> return-type)(optional) ;(optional)
```

There should be a corresponding upper-cased enumerator `ID` inside the `netw::messaging_verb` enum for a verb with name
`id`. For example, for a `my_verb` declaration there should be a corresponding `netw::messaging_verb::MY_VERB` constant
to specify an id for the RPC client.

The parameters of the verb declarations will also act as parameters for the handler functions and corresponding `send`
functions. In case `[[with_timeout]]` attribute is set, the argument list is extended with a `time_point` argument at the
beginning of the parameter list to specify an RPC timeout when sending or handling the message.

The return value type is calculated as `future<return_type>` and is used as return type for message handler and `send`
function. If the `-> return type` clause is omitted, the return type is assumed to be `future<>`. If `[[one_way]]`
attribute is specified, handler function return type is fixed to `future<rpc::no_wait_type>`, and the return type
clause should not be used, otherwise an exception will be thrown during  IDL generation process.

If `[[with_client_info]]` attribute is used then the handler will contain a const reference to  an `rpc::client_info` as
its first argument.

`[[with_client_info]]`, `[[with_timeout]]` and `[[one_way]]` attributes can be combined.

For an RPC verb with the definition of `verb x (arg1_t, arg2_t) -> ret_t;` , which is defined in some `my_mod.idl.hh`
module, the following `my_mod_rpc_verbs` class will be generated (approximately):

```cpp
// my_mod.dist.hh

namespace ser {

struct my_mod_rpc_verbs {
	static void register_x(netw::messaging_service* ms, std::function<future<ret_t> (const rpc::client_info&, arg1_t, arg2_t>&&);
	static future<> unregister_x(netw::messaging_service* ms);
	static future<ret_t> send_x(netw::messaging_service* ms, netw::msg_addr id, arg1_t, arg2_t);

	// calls unregister_x, and the same for other verbs, if there are any,
  // and waits for all of them to resolve.
	static future<> unregister(netw::messaging_service* ms);
};

} // namespace ser
```

Each parameter can optionally have a name, otherwise a placeholder name of form `_N` will be used, where `N` is the
index within the RPC verb parameters list. Also, each argument can be annotated with `[[version id]]` attribute, which
will cause it to be accepted as a `rpc::optional<>` in the handler function signature.

## IDL example
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

