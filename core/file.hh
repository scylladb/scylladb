/*
 * Copyright 2014 Cloudius Systems
 */

#ifndef FILE_HH_
#define FILE_HH_

#include <sys/stat.h>
#include <sys/ioctl.h>
#include <linux/fs.h>

class file_impl {
public:
    virtual ~file_impl() {}

    virtual future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len) = 0;
    virtual future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov) = 0;
    virtual future<size_t> read_dma(uint64_t pos, void* buffer, size_t len) = 0;
    virtual future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov) = 0;
    virtual future<> flush(void) = 0;
    virtual future<struct stat> stat(void) = 0;
    virtual future<> discard(uint64_t offset, uint64_t length) = 0;
    virtual future<size_t> size(void) = 0;

    friend class reactor;
};

class posix_file_impl : public file_impl {
public:
    int _fd;
    posix_file_impl(int fd) : _fd(fd) {}
    ~posix_file_impl() {
        if (_fd != -1) {
            ::close(_fd);
        }
    }

    future<size_t> write_dma(uint64_t pos, const void* buffer, size_t len);
    future<size_t> write_dma(uint64_t pos, std::vector<iovec> iov);
    future<size_t> read_dma(uint64_t pos, void* buffer, size_t len);
    future<size_t> read_dma(uint64_t pos, std::vector<iovec> iov);
    future<> flush(void);
    future<struct stat> stat(void);
    future<> discard(uint64_t offset, uint64_t length);
    future<size_t> size(void);
};

class blockdev_file_impl : public posix_file_impl {
public:
    blockdev_file_impl(int fd) : posix_file_impl(fd) {}
    future<> discard(uint64_t offset, uint64_t length) override;
    future<size_t> size(void) override;
};

inline
std::unique_ptr<file_impl>
make_file_impl(int fd) {
    struct stat st;
    ::fstat(fd, &st);
    if (S_ISBLK(st.st_mode)) {
        return std::unique_ptr<file_impl>(new blockdev_file_impl(fd));
    } else {
        return std::unique_ptr<file_impl>(new posix_file_impl(fd));
    }
}

class file {
    std::unique_ptr<file_impl> _file_impl;
private:
    explicit file(int fd) : _file_impl(make_file_impl(fd)) {}
public:
    file(file&& x) : _file_impl(std::move(x._file_impl)) {}
    template <typename CharType>
    future<size_t> dma_read(uint64_t pos, CharType* buffer, size_t len) {
        return _file_impl->read_dma(pos, buffer, len);
    }

    future<size_t> dma_read(uint64_t pos, std::vector<iovec> iov) {
        return _file_impl->read_dma(pos, std::move(iov));
    }

    template <typename CharType>
    future<size_t> dma_write(uint64_t pos, const CharType* buffer, size_t len) {
        return _file_impl->write_dma(pos, buffer, len);
    }

    future<size_t> dma_write(uint64_t pos, std::vector<iovec> iov) {
        return _file_impl->write_dma(pos, std::move(iov));
    }

    future<> flush() {
        return _file_impl->flush();
    }

    future<struct stat> stat() {
        return _file_impl->stat();
    }

    future<> discard(uint64_t offset, uint64_t length) {
        return _file_impl->discard(offset, length);
    }

    future<size_t> size() {
        return _file_impl->size();
    }

    friend class reactor;
};

#endif /* FILE_HH_ */
