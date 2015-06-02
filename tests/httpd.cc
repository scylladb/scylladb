/*
 * Copyright 2015 Cloudius Systems
 */

#include "http/httpd.hh"
#include "http/handlers.hh"
#include "http/matcher.hh"
#include "http/matchrules.hh"
#include "json/formatter.hh"
#include "http/routes.hh"
#include "http/exception.hh"
#include "http/transformers.hh"
#include "core/future-util.hh"
#include "tests/test-utils.hh"

using namespace httpd;

class handl : public httpd::handler_base {
public:
    virtual future<std::unique_ptr<reply> > handle(const sstring& path,
            std::unique_ptr<request> req, std::unique_ptr<reply> rep) {
        rep->done("html");
        return make_ready_future<std::unique_ptr<reply>>(std::move(rep));
    }
};

SEASTAR_TEST_CASE(test_reply)
{
    reply r;
    r.set_content_type("txt");
    BOOST_REQUIRE_EQUAL(r._headers["Content-Type"], sstring("text/plain"));
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_str_matcher)
{

    str_matcher m("/hello");
    parameters param;
    BOOST_REQUIRE_EQUAL(m.match("/abc/hello", 4, param), 10);
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_param_matcher)
{

    param_matcher m("param");
    parameters param;
    BOOST_REQUIRE_EQUAL(m.match("/abc/hello", 4, param), 10);
    BOOST_REQUIRE_EQUAL(param.path("param"), "/hello");
    BOOST_REQUIRE_EQUAL(param["param"], "hello");
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_match_rule)
{

    parameters param;
    handl* h = new handl();
    match_rule mr(h);
    mr.add_str("/hello").add_param("param");
    httpd::handler_base* res = mr.get("/hello/val1", param);
    BOOST_REQUIRE_EQUAL(res, h);
    BOOST_REQUIRE_EQUAL(param["param"], "val1");
    res = mr.get("/hell/val1", param);
    httpd::handler_base* nl = nullptr;
    BOOST_REQUIRE_EQUAL(res, nl);
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_formatter)
{
    BOOST_REQUIRE_EQUAL(json::formatter::to_json(true), "true");
    BOOST_REQUIRE_EQUAL(json::formatter::to_json(false), "false");
    BOOST_REQUIRE_EQUAL(json::formatter::to_json(1), "1");
    const char* txt = "efg";
    BOOST_REQUIRE_EQUAL(json::formatter::to_json(txt), "\"efg\"");
    sstring str = "abc";
    BOOST_REQUIRE_EQUAL(json::formatter::to_json(str), "\"abc\"");
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_decode_url) {
    request req;
    req._url = "/a?q=%23%24%23";
    sstring url = http_server::connection::set_query_param(req);
    BOOST_REQUIRE_EQUAL(url, "/a");
    BOOST_REQUIRE_EQUAL(req.get_query_param("q"), "#$#");
    req._url = "/a?a=%23%24%23&b=%22%26%22";
    http_server::connection::set_query_param(req);
    BOOST_REQUIRE_EQUAL(req.get_query_param("a"), "#$#");
    BOOST_REQUIRE_EQUAL(req.get_query_param("b"), "\"&\"");
    return make_ready_future<>();
}

SEASTAR_TEST_CASE(test_routes) {
    handl* h1 = new handl();
    handl* h2 = new handl();
    routes route;
    route.add(operation_type::GET, url("/api").remainder("path"), h1);
    route.add(operation_type::GET, url("/"), h2);
    std::unique_ptr<request> req = std::make_unique<request>();
    std::unique_ptr<reply> rep = std::make_unique<reply>();

    auto f1 =
            route.handle("/api", std::move(req), std::move(rep)).then(
                    [] (std::unique_ptr<reply> rep) {
                        BOOST_REQUIRE_EQUAL((int )rep->_status, (int )reply::status_type::ok);
                    });
    req.reset(new request);
    rep.reset(new reply);

    auto f2 =
            route.handle("/", std::move(req), std::move(rep)).then(
                    [] (std::unique_ptr<reply> rep) {
                        BOOST_REQUIRE_EQUAL((int )rep->_status, (int )reply::status_type::ok);
                    });
    req.reset(new request);
    rep.reset(new reply);
    auto f3 =
            route.handle("/api/abc", std::move(req), std::move(rep)).then(
                    [] (std::unique_ptr<reply> rep) {
                    });
    req.reset(new request);
    rep.reset(new reply);
    auto f4 =
            route.handle("/ap", std::move(req), std::move(rep)).then(
                    [] (std::unique_ptr<reply> rep) {
                        BOOST_REQUIRE_EQUAL((int )rep->_status,
                                            (int )reply::status_type::not_found);
                    });
    return when_all(std::move(f1), std::move(f2), std::move(f3), std::move(f4))
            .then([] (std::tuple<future<>, future<>, future<>, future<>> fs) {
        std::get<0>(fs).get();
        std::get<1>(fs).get();
        std::get<2>(fs).get();
        std::get<3>(fs).get();
    });
}

SEASTAR_TEST_CASE(test_transformer) {
    request req;
    content_replace cr("json");
    sstring content = "hello-{{Protocol}}-xyz-{{Host}}";
    cr.transform(content, req, "html");
    BOOST_REQUIRE_EQUAL(content, "hello-{{Protocol}}-xyz-{{Host}}");
    req._headers["Host"] = "localhost";
    cr.transform(content, req, "json");
    BOOST_REQUIRE_EQUAL(content, "hello-http-xyz-localhost");
    return make_ready_future<>();
}
