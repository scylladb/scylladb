File level encryption in scylla enterprise
==========================================

File encryption in scylla enterprise is done by "block-level" encryption via a `file_impl` implementation that transparently wraps file IO transforming data
to/from encrypted state. Refer to `encrypted_file_impl` in `ent/encryption/encrypted_file_impl.cc`.

Encryption is algorithm-agnostic in that the wrapper supports any symmetric-key algorithm (block cipher) that is available in the OpenSSL EVP (envelope)
library.

The wrapper uses a user-provided symmetric key coupled with ESSIV block initialization vector calculation. *NOTE*: the data file itself does *not* keep track of
the key used to encrypt data, thus an external meta data provider is required to map files to their keys, and is solely the user's responsibility.

File block encryption does not use padding, since it relies on input data size and output data size being identical.

The file is divided in `N` blocks of 4096 bytes size. Each 4KB block is encrypted with the provided key `K`, configured block cipher `B`, and block cipher
operating mode (usually CBC). Because 4KB is an integral multiple of any considerable block cipher's block size, no padding is necessary within any 4KB file
block.

The initialization vector (IV) for the block cipher operating mode (usually CBC) of each 4KB file block is derived via
[ESSIV](https://en.wikipedia.org/wiki/Disk_encryption_theory#Encrypted_salt-sector_initialization_vector_(ESSIV)):

- The user-provided data encryption key `K` is hashed with SHA256 to value `h` (32 bytes = 256 bits).

- An AES256 block cipher is keyed with `h`.

- For the particular file block number, a byte array is populated with 8 `NUL` bytes, followed by the little-endian representation of the `uint64_t` block
  number (16 bytes = 128 bits).

- The 16 byte array is encrypted with a single round (i.e., one ECB mode application) of the AES256 block cipher, to value `c` (16 bytes = 128 bits).

- `c` is the IV of the block cipher `B` (truncated or zero-padded as required by the block size of `B`).

```
h := SHA256(K)
IV_B(block_number) := AES256_h(uint64_t(0) ‖ block_number_le64)
```


Padding/truncation
==================

All encryption is done unpadded. To handle file sizes we use a simplified padding scheme:

Since all writes are assumed to be done by us, and must be aligned (scylla requirement), we can assume in turn that any resizing should be made by truncation.

If a file is truncated to a size that is not a whole multiple of the `B` block cipher's block size (which is typically 16 bytes = 128 bits), then we increment
the actual truncation size by `B`'s block size.


```
                     +----------- 16 bytes ----------+
                     |                               |
               +----------- 16 bytes ----------+     |
               |     |                         |     |
               v     v                         v     v
+--------------+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
| n * 16 bytes |  T  |            P            | T'  |
+--------------+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+-+
                     ^                               ^
                     |                               |
requested truncation offset: n * 16 + 3              |
                                                     |
            actual truncation offset: (n + 1) * 16 + 3

```

- This preserves, in its entirety, for decryption's sake, the ciphertext block (`T ‖ P`) that the user expects to be truncated.

- It records the useful byte count (`size(T)`) of the final ciphertext block (`T ‖ P`) through the trailing misalignment (`size(T')`). The contents of `T'` are
  irrelevant (it's a partial cipher block, so it cannot be decrypted); only its size matters.

When reading an encrypted file, we check the file size. If we're reading from a file with an unaliged size (i.e., `size(T')` is nonzero), we know that the size
of the padding at the end is `B`'s block size. `T'` is discarded; the last complete cipher block (`T ‖ P`) is decrypted. After decryption, `P` is discarded as
well. File size query methods adjust the returned values accordingly.

Non-empty files that are shorter than `B`'s block size are invalid -- they can never be created by the above-described padding scheme.
