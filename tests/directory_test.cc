/*
 * Copyright (C) 2014 Cloudius Systems, Ltd.
 */


#include "core/reactor.hh"
#include "core/app-template.hh"
#include "core/print.hh"
#include "core/shared_ptr.hh"

int main(int ac, char** av) {
    class lister {
        file _f;
        subscription<directory_entry> _listing;
    public:
        lister(file f)
                : _f(std::move(f))
                , _listing(_f.list_directory([this] (directory_entry de) { return report(de); })) {
        }
        future<> done() { return _listing.done(); }
    private:
        future<> report(directory_entry de) {
            print("%s\n", de.name);
            return make_ready_future<>();
        }
    };
    return app_template().run(ac, av, [] {
        return engine.open_directory(".").then([] (file f) {
            auto l = make_lw_shared<lister>(std::move(f));
            return l->done().then([l] {
                // ugly thing to keep *l alive
                engine.exit(0);
            });
        });
    });
}
