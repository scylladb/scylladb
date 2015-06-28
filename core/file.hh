/*
 * This file is open source software, licensed to you under the terms
 * of the Apache License, Version 2.0 (the "License").  See the NOTICE file
 * distributed with this work for additional information regarding copyright
 * ownership.  You may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Copyright 2015 Cloudius Systems
 */

#ifndef FILE_HH_
#define FILE_HH_

#include "stream.hh"
#include "sstring.hh"
#include "core/shared_ptr.hh"
#include "core/align.hh"
#include "core/future-util.hh"
#include <experimental/optional>
#include <system_error>
#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/fs.h>
#include <sys/uio.h>
#include <unistd.h>

/// \addtogroup fileio-module
/// @{

/// Enumeration describing the type of a directory entry being listed.
///
/// \see file::list_directory()
enum class directory_entry_type {
    block_device,
    char_device,
    directory,
    fifo,
    link,
    regular,
    socket,
};

/// A directory entry being listed.
struct directory_entry {
    /// Name of the file in a directory entry.  Will never be "." or "..".  Only the last component is included.
    sstring name;
    /// Type of the directory entry, if known.
    std::experimental::optional<directory_entry_type> type;
};

/// \cond internal
class file_impl {
public:
    virtual ~file_impl() {}

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len) = 0;
    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov) = 0;
    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len) = 0;
    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov) = 0;
    virtual future<> flush(void) = 0;
    virtual future<struct stat> stat(void) = 0;
    virtual future<> truncate(uint64_t length) = 0;
    virtual future<> discard(uint64_t offset, uint64_t length) = 0;
    virtual future<> allocate(uint64_t position, uint64_t length) = 0;
    virtual future<size_t> size(void) = 0;
    virtual future<> close() = 0;
    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) = 0;

    friend class reactor;
};

class posix_file_impl : public file_impl {
public:
    int _fd;
    posix_file_impl(int fd) : _fd(fd) {}
    virtual ~posix_file_impl() override;
    future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len);
    future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov);
    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len);
    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov);
    future<> flush(void);
    future<struct stat> stat(void);
    future<> truncate(uint64_t length);
    future<> discard(uint64_t offset, uint64_t length);
    virtual future<> allocate(uint64_t position, uint64_t length) override;
    future<size_t> size(void);
    virtual future<> close() override;
    virtual subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) override;
};

class blockdev_file_impl : public posix_file_impl {
public:
    blockdev_file_impl(int fd) : posix_file_impl(fd) {}
    future<> truncate(uint64_t length) override;
    future<> discard(uint64_t offset, uint64_t length) override;
    future<size_t> size(void) override;
    virtual future<> allocate(uint64_t position, uint64_t length) override;
};

inline
std::unique_ptr<file_impl>
make_file_impl(int fd) {
    auto r = ::ioctl(fd, BLKGETSIZE);
    if (r != -1) {
        return std::unique_ptr<file_impl>(new blockdev_file_impl(fd));
    } else {
        return std::unique_ptr<file_impl>(new posix_file_impl(fd));
    }
}

/// \endcond

/// A data file on persistent storage.
///
/// File objects represent uncached, unbuffered files.  As such great care
/// must be taken to cache data at the application layer; neither seastar
/// nor the OS will cache these file.
///
/// Data is transferred using direct memory access (DMA).  This imposes
/// restrictions on file offsets and data pointers.  The former must be aligned
/// on a 4096 byte boundary, while a 512 byte boundary suffices for the latter.
class file {
    std::unique_ptr<file_impl> _file_impl;
private:
    explicit file(int fd) : _file_impl(make_file_impl(fd)) {}

public:
    /// Moves a file object.
    file(file&& x) : _file_impl(std::move(x._file_impl)) {}
    /// Moves assigns a file object.
    file& operator=(file&& x) noexcept = default;

    // O_DIRECT reading requires that buffer, offset, and read length, are
    // all aligned. Alignment of 4096 was necessary in the past, but no longer
    // is - 512 is usually enough; But we'll need to use BLKSSZGET ioctl to
    // be sure it is really enough on this filesystem. 4096 is always safe.
    // In addition, if we start reading in things outside page boundaries,
    // we will end up with various pages around, some of them with
    // overlapping ranges. Those would be very challenging to cache.

    /// Alignment requirement for file offsets
    static constexpr uint64_t dma_alignment = 4096;

    // Make sure alignment is a power of 2
    static_assert(dma_alignment > 0 && !(dma_alignment & (dma_alignment - 1)),
                  "dma_alignment must be a power of 2");


    /**
     * Perform a single DMA read operation.
     *
     * @param aligned_pos offset to begin reading at (should be aligned)
     * @param aligned_buffer output buffer (should be aligned)
     * @param aligned_len number of bytes to read (should be aligned)
     *
     * Alignment is HW dependent but use 4KB alignment to be on the safe side as
     * explained above.
     *
     * @return number of bytes actually read
     * @throw exception in case of I/O error
     */
    template <typename CharType>
    future<size_t>
    dma_read(uint64_t aligned_pos, CharType* aligned_buffer, size_t aligned_len) {
        return _file_impl->read_dma(aligned_pos, aligned_buffer, aligned_len);
    }

    /**
     * Read the requested amount of bytes starting from the given offset.
     *
     * @param pos offset to begin reading from
     * @param len number of bytes to read
     *
     * @return temporary buffer containing the requested data.
     * @throw exception in case of I/O error
     *
     * This function doesn't require any alignment for both "pos" and "len"
     *
     * @note size of the returned buffer may be smaller than "len" if EOF is
     *       reached of in case of I/O error.
     */
    template <typename CharType>
    future<temporary_buffer<CharType>> dma_read(uint64_t pos, size_t len) {
        return dma_read_bulk<CharType>(pos, len).then(
                [len] (temporary_buffer<CharType> buf) {
            if (len < buf.size()) {
                buf.trim(len);
            }

            return std::move(buf);
        });
    }

    /// Error thrown when attempting to read past end-of-file
    /// with \ref dma_read_exactly().
    class eof_error : public std::exception {};

    /**
     * Read the exact amount of bytes.
     *
     * @param pos offset in a file to begin reading from
     * @param len number of bytes to read
     *
     * @return temporary buffer containing the read data
     * @throw end_of_file_error if EOF is reached, file_io_error or
     *        std::system_error in case of I/O error.
     */
    template <typename CharType>
    future<temporary_buffer<CharType>>
    dma_read_exactly(uint64_t pos, size_t len) {
        return dma_read<CharType>(pos, len).then(
                [pos, len] (auto buf) {
            if (buf.size() < len) {
                throw eof_error();
            }

            return std::move(buf);
        });
    }

    /// Performs a DMA read into the specified iovec.
    ///
    /// \param pos offset to read from.  Must be aligned to \ref dma_alignment.
    /// \param iov vector of address/size pairs to read into.  Addresses must be
    ///            aligned.
    /// \return a future representing the number of bytes actually read.  A short
    ///         read may happen due to end-of-file or an I/O error.
    future<size_t> dma_read(uint64_t pos, std::vector<iovec> iov) {
        return _file_impl->read_dma(pos, std::move(iov));
    }

    /// Performs a DMA write from the specified buffer.
    ///
    /// \param pos offset to write into.  Must be aligned to \ref dma_alignment.
    /// \param buffer aligned address of buffer to read from.  Buffer must exists
    ///               until the future is made ready.
    /// \param len number of bytes to write.  Must be aligned.
    /// \return a future representing the number of bytes actually written.  A short
    ///         write may happen due to an I/O error.
    template <typename CharType>
    future<size_t> dma_write(uint64_t pos, const CharType* buffer, size_t len) {
        return _file_impl->write_dma(pos, buffer, len);
    }

    /// Performs a DMA write to the specified iovec.
    ///
    /// \param pos offset to write into.  Must be aligned to \ref dma_alignment.
    /// \param iov vector of address/size pairs to write from.  Addresses must be
    ///            aligned.
    /// \return a future representing the number of bytes actually written.  A short
    ///         write may happen due to an I/O error.
    future<size_t> dma_write(uint64_t pos, std::vector<iovec> iov) {
        return _file_impl->write_dma(pos, std::move(iov));
    }

    /// Causes any previously written data to be made stable on persistent storage.
    ///
    /// Prior to a flush, written data may or may not survive a power failure.  After
    /// a flush, data is guaranteed to be on disk.
    future<> flush() {
        return _file_impl->flush();
    }

    /// Returns \c stat information about the file.
    future<struct stat> stat() {
        return _file_impl->stat();
    }

    /// Truncates the file to a specified length.
    future<> truncate(uint64_t length) {
        return _file_impl->truncate(length);
    }

    /// Preallocate disk blocks for a specified byte range.
    ///
    /// Requests the file system to allocate disk blocks to
    /// back the specified range (\c length bytes starting at
    /// \c position).  The range may be outside the current file
    /// size; the blocks can then be used when appending to the
    /// file.
    ///
    /// \param position beginning of the range at which to allocate
    ///                 blocks.
    /// \parm length length of range to allocate.
    /// \return future that becomes ready when the operation completes.
    future<> allocate(uint64_t position, uint64_t length) {
        return _file_impl->allocate(position, length);
    }

    /// Discard unneeded data from the file.
    ///
    /// The discard operation tells the file system that a range of offsets
    /// (which be aligned) is no longer needed and can be reused.
    future<> discard(uint64_t offset, uint64_t length) {
        return _file_impl->discard(offset, length);
    }

    /// Gets the file size.
    future<size_t> size() {
        return _file_impl->size();
    }

    /// Closes the file.
    ///
    /// Flushes any pending operations and release any resources associated with
    /// the file (except for stable storage).
    ///
    /// \note
    /// to ensure file data reaches stable storage, you must call \ref flush()
    /// before calling \c close().
    future<> close() {
        return _file_impl->close();
    }

    /// Returns a directory listing, given that this file object is a directory.
    subscription<directory_entry> list_directory(std::function<future<> (directory_entry de)> next) {
        return _file_impl->list_directory(std::move(next));
    }

    /**
     * Read a data bulk containing the provided addresses range that starts at
     * the given offset and ends at either the address aligned to
     * dma_alignment (4KB) or at the file end.
     *
     * @param offset starting address of the range the read bulk should contain
     * @param range_size size of the addresses range
     *
     * @return temporary buffer containing the read data bulk.
     * @throw system_error exception in case of I/O error or eof_error when
     *        "offset" is beyond EOF.
     */
    template <typename CharType>
    future<temporary_buffer<CharType>>
    dma_read_bulk(uint64_t offset, size_t range_size);

private:
    template <typename CharType>
    struct read_state;

    /**
     * Try to read from the given position where the previous short read has
     * stopped. Check the EOF condition.
     *
     * The below code assumes the following: short reads due to I/O errors
     * always end at address aligned to HW block boundary. Therefore if we issue
     * a new read operation from the next position we are promised to get an
     * error (different from EINVAL). If we've got a short read because we have
     * reached EOF then the above read would either return a zero-length success
     * (if the file size is aligned to HW block size) or an EINVAL error (if
     * file length is not aligned to HW block size).
     *
     * @param pos offset to read from
     * @param len number of bytes to read
     *
     * @return temporary buffer with read data or zero-sized temporary buffer if
     *         pos is at or beyond EOF.
     * @throw appropriate exception in case of I/O error.
     */
    template <typename CharType>
    future<temporary_buffer<CharType>>
    read_maybe_eof(uint64_t pos, size_t len);

    friend class reactor;
};

/// \cond internal

template <typename CharType>
struct file::read_state {
    typedef temporary_buffer<CharType> tmp_buf_type;

    read_state(uint64_t offset, uint64_t front, size_t to_read)
    : buf(tmp_buf_type::aligned(file::dma_alignment,
                                align_up(to_read, file::dma_alignment)))
    , _offset(offset)
    , _to_read(to_read)
    , _front(front) {}

    bool done() const {
        return eof || pos >= _to_read;
    }

    /**
     * Trim the buffer to the actual number of read bytes and cut the
     * bytes from offset 0 till "_front".
     *
     * @note this function has to be called only if we read bytes beyond
     *       "_front".
     */
    void trim_buf_before_ret() {
        assert(have_good_bytes());

        buf.trim(pos);
        buf.trim_front(_front);
    }

    uint64_t cur_offset() const {
        return _offset + pos;
    }

    size_t left_space() const {
        return buf.size() - pos;
    }

    size_t left_to_read() const {
        // positive as long as (done() == false)
        return _to_read - pos;
    }

    void append_new_data(tmp_buf_type& new_data) {
        auto to_copy = std::min(left_space(), new_data.size());

        std::memcpy(buf.get_write() + pos, new_data.get(), to_copy);
        pos += to_copy;
    }

    bool have_good_bytes() const {
        return pos > _front;
    }

public:
    bool         eof      = false;
    tmp_buf_type buf;
    size_t       pos      = 0;
private:
    uint64_t     _offset;
    size_t       _to_read;
    uint64_t     _front;
};

template <typename CharType>
future<temporary_buffer<CharType>>
file::dma_read_bulk(uint64_t offset, size_t range_size) {
    using tmp_buf_type = typename read_state<CharType>::tmp_buf_type;

    auto front = offset & (dma_alignment - 1);
    offset -= front;
    range_size += front;

    auto rstate = make_lw_shared<read_state<CharType>>(offset, front,
                                                       range_size);

    //
    // First, try to read directly into the buffer. Most of the reads will
    // end here.
    //
    auto read = dma_read(offset, rstate->buf.get_write(),
                         rstate->buf.size());

    return read.then([rstate, this] (size_t size) mutable {
        rstate->pos = size;

        //
        // If we haven't read all required data at once -
        // start read-copy sequence. We can't continue with direct reads
        // into the previously allocated buffer here since we have to ensure
        // the aligned read length and thus the aligned destination buffer
        // size.
        //
        // The copying will actually take place only if there was a HW glitch.
        // In EOF case or in case of a persistent I/O error the only overhead is
        // an extra allocation.
        //
        return do_until(
            [rstate] { return rstate->done(); },
            [rstate, this] () mutable {
            return read_maybe_eof<CharType>(
                rstate->cur_offset(), rstate->left_to_read()).then(
                    [rstate] (auto buf1) mutable {
                if (buf1.size()) {
                    rstate->append_new_data(buf1);
                } else if (rstate->have_good_bytes()){
                    rstate->eof = true;
                } else {
                    throw eof_error();
                }

                return make_ready_future<>();
            });
        }).then([rstate] () mutable {
            //
            // If we are here we are promised to have read some bytes beyond
            // "front" so we may trim straight away.
            //
            rstate->trim_buf_before_ret();
            return make_ready_future<tmp_buf_type>(std::move(rstate->buf));
        });
    });
}

template <typename CharType>
future<temporary_buffer<CharType>>
file::read_maybe_eof(uint64_t pos, size_t len) {
    //
    // We have to allocate a new aligned buffer to make sure we don't get
    // an EINVAL error due to unaligned destination buffer.
    //
    temporary_buffer<CharType> buf = temporary_buffer<CharType>::aligned(
               dma_alignment, align_up(len, dma_alignment));

    // try to read a single bulk from the given position
    return dma_read(pos, buf.get_write(), buf.size()).then_wrapped(
            [buf = std::move(buf)](future<size_t> f) mutable {
        try {
            size_t size = std::get<0>(f.get());

            buf.trim(size);

            return std::move(buf);
        } catch (std::system_error& e) {
            //
            // TODO: implement a non-trowing file_impl::dma_read() interface to
            //       avoid the exceptions throwing in a good flow completely.
            //       Otherwise for users that don't want to care about the
            //       underlying file size and preventing the attempts to read
            //       bytes beyond EOF there will always be at least one
            //       exception throwing at the file end for files with unaligned
            //       length.
            //
            if (e.code().value() == EINVAL) {
                buf.trim(0);
                return std::move(buf);
            } else {
                throw;
            }
        }
    });
}

/// \endcond

/// @}

#endif /* FILE_HH_ */
