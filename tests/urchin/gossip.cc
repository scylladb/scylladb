#include "core/reactor.hh"
#include "core/app-template.hh"
#include "message/messaging_service.hh"
#include "gms/failure_detector.hh"
#include "gms/gossiper.hh"

namespace bpo = boost::program_options;

int main(int ac, char ** av) {
    app_template app;
    return app.run(ac, av, [&] {
        net::get_messaging_service().start().then([] {
            gms::get_failure_detector().start_single().then([] {
                gms::get_gossiper().start_single().then([] {
                    std::cout << "Start gossiper service ...\n";
                    gms::get_local_gossiper().start(1);
                });
            });
        });
    });
}
