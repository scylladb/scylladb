/*
 * Copyright (C) 2015 Cloudius Systems, Ltd.
 */

// Demonstration of file_input_stream.  Don't expect stellar performance
// since no read-ahead or caching is done yet.

#include "core/fstream.hh"
#include "core/app-template.hh"
#include "core/shared_ptr.hh"
#include <algorithm>

struct reader {
    reader(file f) : is(std::move(f), 4096) {}
    file_input_stream is;
    size_t count = 0;

    // for input_stream::consume():
    template <typename Done>
    void operator()(temporary_buffer<char> data, Done&& done) {
        if (data.empty()) {
            done(std::move(data));
        } else {
            count += std::count(data.begin(), data.end(), '\n');
            // FIXME: last line without \n?
        }
    }
};

int main(int ac, char** av) {
    app_template app;
    namespace bpo = boost::program_options;
    app.add_positional_options({
        { "file", bpo::value<std::string>(), "File to process", 1 },
    });
    app.run(ac, av, [&app] {
        auto fname = app.configuration()["file"].as<std::string>();
        engine().open_file_dma(fname).then([] (file f) {
            auto r = make_shared<reader>(std::move(f));
            r->is.consume(*r).then([r] {
               print("%d lines\n", r->count);
               engine().exit(0);
            });
        });
    });
}

