# WASM support for user-defined functions

This document describes the details of WASM language support in user-defined functions (UDF). The language `wasm` is one of the possible languages to use, besides Lua, to implement these functions.

## Experimental status

Before the design of WebAssembly integration and ABI is finalized, it's only available in experimental mode.
User-defined functions are already experimental at the time of this writing, but in order to be ready
for backward incompatible changes, the language accepted by CQL is currently named "xwasm".
Once the ABI is set in stone, it should be changed to "wasm".

## ABI versions

Different programming languages may require different ABIs. To support that, the wasm program is required to
export the symbol "_scylla_abi", that is a WebAssembly global with a 32-bit value of the offset in memory,
where the version number can be read (that's the only method of exporting a constant in Rust).

Currently, the only available ABI versions are 1 and 2. Both of them use the same protocol for passing
parameters and returning values, but they differ in approaches to memory management.

## Memory management

The memory management differs depending on the used ABI verison:
 - version 1 - There are no requirements of the usage of memory by the user. The host grows memory for each of parameters and does not free the memory in any way.
 - version 2 - The user program is required to export "_scylla_malloc" and "_scylla_free" methods, which
 are then used by the host for allocating memory for parameters and freeing memory for the returned value. The user is required to free the memory allocated for parameters (this can be achieved by using
 the provided helper libraries).
 The "_scylla_malloc" and "_scylla_free" methods may be simple wrappers of "malloc" and "free" methods
 implemented by default when compiling with WASI.

## Supported types

Due to the limitations imposed by WebAssembly specification, the following types can be natively supported with CQL:
 - int
 - bigint
 - smallint
 - tinyint
 - bool
 - float
 - double

The rest of CQL types (text, date, timestamp, etc.) are implemented by putting their serialized representation into wasm module memory
and passing for each parameter a 64-bit value, of which top 32 bits are its size and its bottom
32 bits are a pointer to its serialized representation, like below:
```c
{
    int32_t size;
    int32_t ptr;
    int64_t param = ((int64_t)size << 32) | ptr;
}
```

## Support for NULL values

Native WebAssembly types can only be represented directly if the function does not operate on NULL values. Fortunately, user-defined functions
explicitly specify whether they accept NULL or not.

If the function is specified not to accept NULL, all parameters are represented
as in the description above.

If the function is specified to accept NULL, each parameter should be represented in WebAssembly by 
size and pointer to its serialized form in memory, as in the paragraph above.

the important distinction is that size equal to -1 (minus one)indicates that the value is NULL and should not be parsed.

## Return values

NOTE: ABI for return values is experimental and subject to change. It can (and should) be redesigned
after implementing helper libraries for a few popular languages (including C++, C, Rust).

Natively supported types are returned as is. All the other types are returned via memory, similarly to the way
they are passed as parameters: the wasm function should return a single 64-bit integer, representing the size and the offset of a serialized form of the returned value

Currently, returning NULL values is possible only for functions declared to be `CALLED ON NULL INPUT`.
For such functions, the return value is always expected to be presented in the serialized form (which
allows representing nulls), even for types natively supported by WebAssembly.
The decision is experimental and it was done in order to allow returning some values as native WebAssembly types
without having to allocate memory for them and serialize them first.
Alternative ways of expressing whether a function can **return** null should be considered - perhaps
as CQL syntax extension.

## How to generate a correct wasm UDF source code

Scylla accepts UDF's source code in WebAssembly text format - also known as `wat`. The source can use and define whatever's needed for execution, including multiple helper functions and symbols. The only requirement for it to be accepted as correct UDF source is that the WebAssembly module exports a symbol with the same name as the function, and this symbol's type is indeed a function with correct signature.

UDF's source code can be, naturally, simply coded by hand in wat. It is not often very convenient to program directly in assembly, so here are a few tips.

### Compiling to wasm

#### AssemblyScript

AssemblyScript is a TypeScript-like language that compiles to WebAsembly.

Install via npm:
```
npm install -g assemblyscript
```

Example source code:
```assemblyscript
export const _scylla_abi = [1]
export function fib(n: i32): i32 {
  if (n < 2) {
    return n
  }
  return fib(n - 1) + fib(n - 2)
}
```

Compile directly to WebAssembly Text Format with:
```
asc fib.ts --textFile fib.wat --optimize
```

#### C

Clang is capable of compiling C source code to wasm and it also supports useful built-ins
for using wasm-specific interfaces, like `__builtin_wasm_memory_size` and `__builtin_wasm_memory_grow`
for memory management.

Example source code:

```c
#include<stdlib.h>

const int WASM_PAGE_SIZE = 64 * 1024;

const int _scylla_abi = 2;

void* _scylla_malloc(int size) {
    return malloc(size);
}

void _scylla_free(void* ptr) {
    free(ptr);
}

static long long swap_int64(long long val) {
    val = ((val << 8) & 0xFF00FF00FF00FF00ULL ) | ((val >> 8) & 0x00FF00FF00FF00FFULL );
    val = ((val << 16) & 0xFFFF0000FFFF0000ULL ) | ((val >> 16) & 0x0000FFFF0000FFFFULL );
    return (val << 32) | ((val >> 32) & 0xFFFFFFFFULL);
}

long long fib_aux(long long n) {
    if (n < 2) {
        return n;
    }
    return fib_aux(n-1) + fib_aux(n-2);
}

long long fib(long long p) {
    int size = p >> 32;
    long long* p_val = (long long*)(p & 0xffffffff);
    // Initialize memory for the return value
    long long* ret_val = _scylla_malloc(sizeof(long long));
    if (size == -1) {
        *ret_val = swap_int64(42);
    } else {
        *ret_val = swap_int64(fib_aux(swap_int64(*p_val)));
    }
    _scylla_free(p_val);
    // 8 is the size of a bigint
    return (long long)(8ll << 32) | (long long)ret_val;
}
// using wasi in c/c++ requires adding a main function to the program
int main(){}
```

Compilation instructions:
```bash
 /path/to/wasm/supporting/c/compiler --sysroot=/path/to/wasi/sysroot -O2  --target=wasm32-wasi -Wl,--export=fib -Wl,--export=_scylla_abi -Wl,--export=_scylla_malloc -Wl,--export=_scylla_free -Wl,--no-entry fibnull.c -o fibnull.wasm
 wasm2wat fibnull.wasm > fibnull.wat
```

#### Rust

Rust ecosystem exposes a rather convenient way of generating WebAssembly, with the help of `cargo wasi` and `wasm_bindgen`.

As a short example, here's a sample Rust code which can be compiled to WebAssembly:
```rust
use wasm_bindgen::prelude::*;

#[no_mangle]
pub static _scylla_abi: u32 = 1;

#[wasm_bindgen]
pub fn fib(n: i32) -> i32 {
    if n < 2 {
        n
    } else {
        fib(n - 1) + fib(n - 2)
    }
}
```

A more detailed guide and examples can be found here:
https://bytecodealliance.github.io/cargo-wasi/hello-world.html
https://rustwasm.github.io/wasm-bindgen/

### Generating wat from wasm

For those who want to use precompiled WASM modules, it's enough to translate WASM bytecode to `wat` representation. On Linux, it can be achieved by a `wasm2wat` tool, available in most distributions in the `wabt` package.

## Example

Here's how a `wasm` function can be declared:

```cql
CREATE FUNCTION ks.fib (input bigint) RETURNS NULL ON NULL INPUT RETURNS bigint LANGUAGE xwasm
AS '(module
  (func $fib (param $n i64) (result i64)
    (if
      (i64.lt_s (local.get $n) (i64.const 2))
      (return (local.get $n))
    )
    (i64.add
      (call $fib (i64.sub (local.get $n) (i64.const 1)))
      (call $fib (i64.sub (local.get $n) (i64.const 2)))
    )
  )
  (export "fib" (func $fib))
  (global (;0;) i32 (i32.const 1024))
  (export "_scylla_abi" (global 0))
  (data $.rodata (i32.const 1024) "\\01")
)'
```

and it can be invoked just like a regular UDF:
```cql
scylla@cqlsh:ks> CREATE TABLE t(id int, n bigint, PRIMARY KEY(id,n));
scylla@cqlsh:ks> INSERT INTO t(id, n) VALUES (0, 0);
scylla@cqlsh:ks> INSERT INTO t(id, n) VALUES (0, 1);
scylla@cqlsh:ks> INSERT INTO t(id, n) VALUES (0, 2);
scylla@cqlsh:ks> INSERT INTO t(id, n) VALUES (0, 3);
scylla@cqlsh:ks> INSERT INTO t(id, n) VALUES (0, 4);
scylla@cqlsh:ks> INSERT INTO t(id, n) VALUES (0, 5);
scylla@cqlsh:ks> INSERT INTO t(id, n) VALUES (0, 6);
scylla@cqlsh:ks> INSERT INTO t(id, n) VALUES (0, 7);
scylla@cqlsh:ks> INSERT INTO t(id, n) VALUES (0, 8);
scylla@cqlsh:ks> INSERT INTO t(id, n) VALUES (0, 9);
scylla@cqlsh:ks> INSERT INTO t(id, n) VALUES (0, 10);
scylla@cqlsh:ks> SELECT n, ks.fib(n) FROM t;

 n  | ks.fib(n)
----+-----------
  0 |         0
  1 |         1
  2 |         1
  3 |         2
  4 |         3
  5 |         5
  6 |         8
  7 |        13
  8 |        21
  9 |        34
 10 |        55

(11 rows)
```

