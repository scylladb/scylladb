#include "core/reactor.hh"
#include "xenstore.hh"
#include <stdlib.h>
#include <string>

using xenstore_transaction = xenstore::xenstore_transaction;

xenstore_transaction xenstore::_xs_null = xenstore::xenstore_transaction();
xenstore *xenstore::_instance = nullptr;

xenstore *xenstore::instance() {

    if (!_instance) {
        _instance = new xenstore;
    }
    return _instance;
}

xenstore::xenstore()
    : _h(xs_daemon_open())
{
    if (!_h) {
        throw std::runtime_error("Failed to initialize xenstore");
    }
}

xenstore::~xenstore()
{
    xs_close(_h);
}

xs_transaction_t xenstore::start_transaction()
{
    auto t = xs_transaction_start(_h);
    if (!t) {
        throw std::runtime_error("Failed to initialize xenstore transaction");
    }
    return t;
}

void xenstore::end_transaction(xs_transaction_t t)
{
    xs_transaction_end(_h, t, false);
}

void xenstore::write(std::string path, std::string value, xenstore_transaction &t)
{
    xs_write(_h, t.t(), path.c_str(), value.c_str(), value.size());
}

void xenstore::remove(std::string path, xenstore_transaction &t)
{
    xs_rm(_h, t.t(), path.c_str());
}

std::string xenstore::read(std::string path, xenstore_transaction &t)
{
    unsigned int len;
    void *ret = xs_read(_h, t.t(), path.c_str(), &len);
    std::string str(ret ? (const char *)ret : "");
    free(ret);
    return str;
}

std::list<std::string> xenstore::ls(std::string path, xenstore_transaction &t)
{
    unsigned int num;
    char **dir = xs_directory(_h, t.t(), path.c_str(), &num);

    std::list<std::string> names;
    for (unsigned int i = 0; i < num; ++i) {
        names.push_back(dir[i]);
    }
    free(dir);

    return names;
}
