# In-Memory Representation

## Introduction

Instances of all C++ objects need to have their size fixed and known at compile
time. This is a significant restriction when it comes to minimising the memory
footprint of objects that are kept alive for a long amount of time.

Introducing a serialisation format that allows objects to be of a variable size
enables a range of possible optimisations, but manually writing serialisation
and deserialisation code for all required types would not only be tedious, but
also quite quickly result in an unmaintainable code.

IMR attempts to solve this problem by providing a general infrastructure for
serialisation and deserialisation with a goal of imposing as little limitations
as possible and generating as efficient code as possible.

## Basic concepts

### Contexts

The IMR expects that in many cases multiple objects will share certain
properties in a very predictable way. This can be taken advantage of by not
storing all the information necessary to correctly read IMR objects inside them,
but moving the shared part to an external state.

Fundamental and compound IMR types are designed with this in mind and many of
them require an external context to provide additional information necessary
during deserialisation.

*For example, `imr::buffer<Tag>` doesn't store its size anywhere, but requires
to be provided at deserialisation with a context object that implements
`size_of<Tag>()` member function.*

The IMR itself doesn't care about the source of the information the context
provide. It can come either from an external state that describes properties of
multiple instances of a particular IMR type or the context logic may read parts
of an IMR object itself. It is also worth noting that objects are not tied to
contexts in any way. The user code could use different context object (with
different internal logic) as long as the decisions it makes are the same and
sufficient for that particular use.

*For example, in a structure a `imr::buffer<Tag>` could be preceded by an
integer which determines its size. When a context is created it takes a pointer
to the structure, reads the size field and returns it when asked by the buffer
deserialiser.*

#### Context factories

In many cases a context is going to combine some external state and data stored
in an IMR object to provide information necessary to deserialise it. In order
to make creation of context more convenient context factories were introduced
which are objects that keep the external state and create an actual context
instance when given a pointer to an IMR object.

```c++
context_factory<ContextType, ExternalState> factory(external_state);
auto context = factory.create(pointer_to_an_imr_object);
```

### Views & mutable views

The interface for deserialising an IMR object is built around views. An IMR type
given context and a pointer to an IMR object returns a view object that
provides methods specific for that type. For fundamental types these functions
will usually provide a way of creating an equivalent C++ type. In case of
compound types their views will allow accessing members in a checked or
unchecked manner.

Some IMR types can be updated in place. A mutable view of their instances can
be created which aside from providing an interface for reading the object will
also allow it to be updated.

### Serialisers

IMR objects are serialised in three phases:
1. Computing the object sizes and recording any necessary memory allocations.
2. Allocating memory. This is the only phase that can fail.
3. Writing serialised data.

It is imperative that the decisions made in the first (sizing) and the third
(writing) phase are exactly the same. In order to make that easier to guarantee
the serialisation interfaces of many compound types accept a lambda template
which is first invoked with a sizer and later with a writer helper objects.

#### Placeholders

If all instances of an IMR type are guaranteed to be the same size it is
possible to defer their serialisation. The serialisation function can be given
a placeholder object which would later be set to an actual value.

#### Continuation hooks

Compound types can be nested, e.g. a member of a structure can be another
structure. However, as mentioned in the [Serialisers](#serialisers) section the
basic interface for serialisation is to accept lambda objects. In case of
nested structures this would require creating a nested lambdas which can be
inconvenient and result in a code that is hard to follow.

The solution is to introduce continuation hooks to serialisation helpers. The
outer structure serialisation helper provides a `serialize_nested()` member
function for serialising the inner one. The function returns a serialisation
helper for the nested structure which behaves as the stand-alone one except
that when `done()` is called it returns back a serialisation helper for the
outer structure.

```c++
return serializer
    .serialize(...)
    .serialize_nested()
        .serialize(...)
        .done()
    .serialize(...)
    .done();
```

### IMR types

#### Tags

IMR extensively uses tags to name members of a compound type or match an
instance of a type with an appropriate member function of the deserialisation
context.

Any type can be a tag, but it is customary to use incomplete classes with a
meaningful names for this purpose.

Reusing tags should be avoided in order to avoid clashes.

#### Type concept

Most IMR types follow a similar interface for serialisation and
deserialisation. There may be, however, some differences specific to a
particular type and its purpose.

```c++
struct imr_type {
    static auto make_view(const uint_t* imr_object) noexcept;
    static auto make_view(uint_t* imr_object) noexcept;

    // Returns the size of a serialized IMR object
    template<typename Context>
    static size_t serialized_object_size(const uint8_t* imr_object, const Context& context) noexcept;

    // Returns the size of a serialized object without actually serializing it.
    // Arguments depend on the IMR type.
    static size_t size_when_serialized(...) noexcept;

    // Returns the size of a serialized object. Arguments depend on the actual
    // IMR type.
    static size_t serialize(uint8_t* out, ...) noexcept;
};
```

## Fundamental types

 Type                 | Fixed size | Needs context | In-place update            | Placeholders |
----------------------|:----------:|:-------------:|:--------------------------:|:------------:|
 `flags`              | Yes        | No            | Yes                        | Yes          |
 `pod`                | Yes        | No            | Yes                        | Yes          |
 `buffer`             | No         | Yes           | Yes (length cannot change) | No           |

### `flags`

```c++
template<typename Tag>
struct imr::set_flag {
    set_flag(bool v = true) noexcept;
};

template<typename Tags...>
struct imr::flags {
    struct view {
        template<typename Tag>
        bool get() const noexcept;

        template<typename Tag>
        bool set(bool value = true) noexcept;
    };

    // Sets flags which tags are present in Tags1, the remaining flags are
    // cleared.
    template<typename... Tags1>
    void serialize(imr::set_flag<Tags1>...) noexcept;
};
```

`flags` is a bitfield of named flags. Each declared flag uses exactly one bit
and the whole bitfield uses the smalles number of bytes possible to fit all
required bits.

`flags` is a fixed-size type that can be updated in place and doesn't require
and external context for deserialisation.

### `pod`

```c++
template<typename PODType>
struct imr::pod {
    struct view {
        PODType load() const noexcept;
        void store(PODType object) noexcept;
    };

    void serialize(PODType object) noexcept;
};
```

`pod` is a POD object using the same representation as that defined by the C++
ABI including all the internal padding in case of compound types. It is
therefore recommended that `pod` is used exclusively to represent C++
fundamental types.

`pod` is a fixed-size type which can be updated in place.
It does not require an external context for deserialisation.

### `buffer`

```c++
template<typename Tag>
struct imr::buffer {
    using view = bytes_view;

    void serialize(bytes_view) noexcept;
};
```

`buffer` is a type representing an array of bytes of an arbitrary length.

`buffer` is a variable-size type which can be updated in-place as long as the
length of the buffer remains unchanged.

#### Context requirements

Deserialisation of `buffer` objects requires an external context providing the
following member:

```c++
size_t size_of<Tag>() const noexcept;
```

Where `Tag` is the same type as the one used as a template parameter for
`buffer`. `size_of()` is must return the length of the buffer.


## Compound types

### `tagged_type`

```c++
template<typename Tag, typename IMRType>
struct imr::tagged_type : IMRType { };
```

`tagged_type` is a thin wrapper around an IMR type that allows annotating it
with a tag without changing its views or serialisers in any way.

The main purpose of adding tags to the type name is to allow defining custom
methods for only some usage of a particular IMR type (see section
[Methods](#methods)).

### `optional`

```c++
template<typename Tag, typename IMRType>
struct imr::optional {
    struct view {
        template<typename Context>
        IMRType::view get(const Context&);
    };

    template<typename... Args>
    void serialize(Args&&...) noexcept;
};
```

`optional` represents an object that may not be present in some circumstances.
Information whether it does is not stored in any sort of metadata, but the
deserialisation context is expected to provide it.

#### Context requirements

```c++
bool is_present<Tag>() const noexcept;
```

Optionals require the deserialisation context to provide information whether
the underlying object is present.

### `variant`

```c++
template<typename Tag, typename IMRType>
struct imr::alternative;

template<typename Tag, typename... Alternatives>
struct imr::variant {
    struct view {
        template<typename AlternativeTag>
        auto as() const noexcept;

        template<typename Visitor>
        decltype(auto) accept(Visitor&&) const;
    };

    template<typename AlternativeTag, typename... Args>
    void serialize(Args&&...) noexcept;
};
```

`variant` is a compound type that has multiple alternatives, each being an
IMR type. At any time there is exactly one active alternative and it is the
only one that can be read. Variants do not store information about the active
alternative by themselves but rely on the external context to provide that
information.

#### Context requirements

```c++
imr::variant::alternative_index active_alternative_of<Tag>() const noexcept;

template<typename AlternativeTag>
auto context_for() const noexcept;
```

Variant requires deserialisation context to provide information which one of
its alternatives is active as well as context for deserialisation of that
alternative.

### `structure`

```c++
template<typename Tag, typename IMRType>
struct imr::member;

template<typename Tag, typename Members>
struct imr::structure {
    struct view {
        template<typename MemberTag>
        auto get() noexcept;

        template<typename MemberTag>
        size_t offset_of() const noexcept;
    };

    template<typename Writer, typename... Args>
    void serialize(Writer&&, Args&&...) noexcept;

    template<typename Tag, typename Context>
    auto get_member(uint8_t*, const Context&) const noexcept;
};

struct imr::structure_writer {
    // For imr::optional:
    template<typename... Args>
    auto serialize(Args&&...);

    auto skip();

    // For imr::variant:
    template<typename AlternativeTag, typename... Args>
    auto serialize_as(Args&&...);

    // For other types:
    template<typename... Args>
    auto serialize(Args&&...);

    // After all members:
    auto done() noexcept;
};
```

`structure` is a compound type that stores possibly multiple member objects
one after another. All members are always present and their IMR type is fixed.

The interface for serialising structures is based on serialisation helpers.
Methods `serialize()` and `size_when_serialized()` accept a lambda that would
be given a serialisation helper object. That object expects its member function
`serialize()` (there are slightly different ones for optional and variant
structure members) to be invoked with an arguments that would normally be passed
to methods `structure()` and `size_when_serialized()` of the type of the member
that is being serialised (in the order they appear in the structure definition).
After the last member has been serialised `done()` is to be called and its
return value returned from the lambda.

#### Context requirements

```c++
template<typename AlternativeTag>
auto context_for() const noexcept;
```

## Methods

IMR types can define destructor and mover methods. Their goal is to provide
similar functionality to C++ destructors and move constructors and thus enable
owning references inside IMR objects. Movers are also essential in ensuring
that all references are properly updated when LSA moves objects around.

The default mover and destructor for fundamental types does nothing. The
default methods for compound types and containers invokes that method for
all present elements in an unspecified order.

User can define mover and destructor methods for any IMR type which would
override any default methods that this type might have.

Neither movers nor destructors are allowed to fail.

```c++
template<>
struct imr::mover<IMRType> {
    static void run(uint8_t* ptr, const Context& ctx) noexcept {
        // user-defined mover action
    }
};

template<>
struct imr::destructor<IMRType> {
    static void run(const uint8_t* ptr, const Context& ctx) noexcept {
        // user-defined destructor action
    }
};

```

### Mover

When an IMR object is being moved its serialised form is copied to the new
place in memory and then a mover method is called with the address of the new
location passed as an argument.

*Note that, unlike C++, IMR does not call the destructor of the object moved
from. In other words, while moving in C++ involves creating a new object that
steals internal state from another, IMR just copies the object to the new
location and invokes a mover to give it a chance to do necessary updates, it is
however still considered to be the same object as the original one.*

### Destructor

The destructor of an IMR object is called before the storage it occupies is
freed. The address of the object passed to the destructor is either a location
at which the object was originally created or the address that was given to the
latest invocation of the object mover method.

## Memory allocations

IMR objects are allowed to own memory and it is therefore expected that during
serialisation there will be a need to perform allocation. A convenience class
`imr::alloc::object_allocator` exists to aid this process.

The object allocator provides serialisation helpers for both sizing and writing
phases, which act similarily to the structure serialisation helpers and return
a pointer to the owned object. After the sizing phase member function
`allocate_all()` needs to be called which allocates all the reserved buffers.

*Note that in case there is a chain of owned objects (e.g. a list) placeholders
can be used for pointers to avoid recursion.*

```c++
using imr_type1 = ...;
using imr_type2 = ...;

imr::alloc::object_allocator allocator;

auto writer = [&] (auto serializer, auto& allocator) noexcept {
    imr::placeholder<imr::pod<void*>> pointer;
    auto ret = serializer
        .serialize(pointer)
        .done();

    auto pointer_to_owned_object = allocator.allocate_nested<imr_type2>()
        .serialize(...)
        .serialize(...)
        .done();

    pointer.serialize(pointer_to_owned_object);

    return ret;
};

auto size = imr_type1::size_when_serialized(writer , allocator.get_size());

auto obj = std::make_unique<uint8_t[]>(size.total_storage());
allocator.allocate_all();

imr_type1::serialize(obj.get(), writer, allocator.get_serializer());
```

### Log-Structured Allocator support

LSA relies on migrator objects to provide logic necessary for moving objects
and obtaining their size. To do these action the information about the IMR type
of the object needs to be preserved as well as at least minimal external
context sufficient for calling the mover.

A helper class template is provided which takes the IMR type and (context
factory)[#context_factories] type as template parameters as well as actual
context factory in constructor and implements `migrate_fn_type` interface.

```c++
imr::alloc::lsa_migrate_fn<IMRType, ContextFactory> migrator(context_factory);
auto ptr = current_allocator().alloc(migrator, size, 1);
```
