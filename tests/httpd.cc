/*
 * Copyright 2015 Cloudius Systems
 */

#define BOOST_TEST_DYN_LINK
#define BOOST_TEST_MODULE core

#include <boost/test/included/unit_test.hpp>

#include "http/handlers.hh"
#include "http/matcher.hh"
#include "http/matchrules.hh"
#include "json/formatter.hh"
#include "http/routes.hh"
#include "http/exception.hh"

using namespace httpd;

class handl : public httpd::handler_base {
public:
    virtual void handle(const sstring& path, parameters* params,
            httpd::const_req& req, httpd::reply& rep)
    {

    }
};

BOOST_AUTO_TEST_CASE(test_reply)
{
    reply r;
    r.set_content_type("txt");
    BOOST_REQUIRE_EQUAL(r._headers["Content-Type"], sstring("text/plain"));
}

BOOST_AUTO_TEST_CASE(test_str_matcher)
{

    str_matcher m("/hello");
    parameters param;
    BOOST_REQUIRE_EQUAL(m.match("/abc/hello", 4, param), 10);
}

BOOST_AUTO_TEST_CASE(test_param_matcher)
{

    param_matcher m("param");
    parameters param;
    BOOST_REQUIRE_EQUAL(m.match("/abc/hello", 4, param), 10);
    BOOST_REQUIRE_EQUAL(param["param"], "/hello");

}

BOOST_AUTO_TEST_CASE(test_match_rule)
{

    parameters param;
    handl* h = new handl();
    match_rule mr(h);
    mr.add_str("/hello").add_param("param");
    httpd::handler_base* res = mr.get("/hello/val1", param);
    BOOST_REQUIRE_EQUAL(res, h);
    BOOST_REQUIRE_EQUAL(param["param"], "/val1");
    res = mr.get("/hell/val1", param);
    httpd::handler_base* nl = nullptr;
    BOOST_REQUIRE_EQUAL(res, nl);
}

BOOST_AUTO_TEST_CASE(test_formatter)
{
    BOOST_REQUIRE_EQUAL(json::formatter::to_json(true), "true");
    BOOST_REQUIRE_EQUAL(json::formatter::to_json(false), "false");
    BOOST_REQUIRE_EQUAL(json::formatter::to_json(1), "1");
    const char* txt = "efg";
    BOOST_REQUIRE_EQUAL(json::formatter::to_json(txt), "\"efg\"");
    sstring str = "abc";
    BOOST_REQUIRE_EQUAL(json::formatter::to_json(str), "\"abc\"");

}


BOOST_AUTO_TEST_CASE(test_routes)
{
    handl* h1 = new handl();
    handl* h2 = new handl();
    routes route;
    route.add(operation_type::GET, url("/api").remainder("path"), h1);
    route.add(operation_type::GET, url("/"), h2);
    request req;
    reply rep;
    BOOST_CHECK_NO_THROW(route.handle("/api", req, rep));
    BOOST_REQUIRE_EQUAL((int)rep._status, (int)reply::status_type::ok);
    BOOST_REQUIRE_EQUAL(req.param["path"], "");
    BOOST_CHECK_NO_THROW(route.handle("/", req, rep));
    BOOST_REQUIRE_EQUAL((int)rep._status, (int)reply::status_type::ok);
    BOOST_CHECK_NO_THROW(route.handle("/api/abc", req, rep));
    BOOST_REQUIRE_EQUAL(req.param["path"], "/abc");
    BOOST_CHECK_NO_THROW(route.handle("/ap", req, rep));
    BOOST_REQUIRE_EQUAL((int)rep._status, (int)reply::status_type::not_found);

}
