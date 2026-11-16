/*
 * Copyright (C) 2026-present ScyllaDB
 */

/*
 * SPDX-License-Identifier: LicenseRef-ScyllaDB-Source-Available-1.1
 */

#include <boost/test/unit_test.hpp>
#include <seastar/testing/thread_test_case.hh>

#include <fstream>

#include <fmt/format.h>

#include "test/lib/tmpdir.hh"
#include "utils/http.hh"

// The certificates of test/resource/certs/cacert.pem and scylla.pem.
static const std::string first_certificate = R"(-----BEGIN CERTIFICATE-----
MIID4jCCAsqgAwIBAgIEXbmO/zANBgkqhkiG9w0BAQsFADBXMQswCQYDVQQGEwJV
UzEVMBMGA1UEChMMSHlUcnVzdCBJbmMuMTEwLwYDVQQDEyhIeVRydXN0IEtleUNv
bnRyb2wgQ2VydGlmaWNhdGUgQXV0aG9yaXR5MB4XDTExMDYwMTAwMDAwMFoXDTQ5
MTIzMTIzNTk1OVowVzELMAkGA1UEBhMCVVMxFTATBgNVBAoTDEh5VHJ1c3QgSW5j
LjExMC8GA1UEAxMoSHlUcnVzdCBLZXlDb250cm9sIENlcnRpZmljYXRlIEF1dGhv
cml0eTCCASIwDQYJKoZIhvcNAQEBBQADggEPADCCAQoCggEBAM6XW6EwsSYAMYqq
vaIMGRskgwUgemOsbKsPgCRHKwOUhiWkEFH7tuheM9vikZjzKrF474Ol8uF5NkQG
OwHNGkfBdIkr0zyN8fvCkIj+GNmBeyrkZ2GHFyM4KVpm69ABptrDpX/10Z71BNwc
SmIu4ltfIlZh/rpmwq2pUUOcKOSP+gUS+g2lNeMvmeikmAn558jgbKm96VmwgwcJ
EBBbqrVyO0DmOPTj+JpVjl6uXD7DCDQTnBn8ZQesP5iuoNL4HUy/y5On5NY3hAoM
Oh+G8jUMLmawm0OOvOS5sL8zZ8KX30dsZcxVOHCpOSdg4XQUNOmmoLYR3mGUmmuD
8YSNJ58CAwEAAaOBtTCBsjAdBgNVHQ4EFgQUZq3eRVBdVGgfsFYAZfvR8pdX724w
gYIGA1UdIwR7MHmAFGat3kVQXVRoH7BWAGX70fKXV+9uoVukWTBXMQswCQYDVQQG
EwJVUzEVMBMGA1UEChMMSHlUcnVzdCBJbmMuMTEwLwYDVQQDEyhIeVRydXN0IEtl
eUNvbnRyb2wgQ2VydGlmaWNhdGUgQXV0aG9yaXR5ggRduY7/MAwGA1UdEwQFMAMB
Af8wDQYJKoZIhvcNAQELBQADggEBAKupGQcH8Ll04ahJ273BIfw4OHndLz5ZvpZ5
HRjUXh8xR/u91JbXvod+DeSefHo2yZpf5WM4M2jPsZLQuIEabyMn2HFQQWPOXyBp
ckzNB6s1WP7a1SYeRPSX4/9sgNsxFxNSbPtoNHERr7aEO7Fc02cl4VoxpmiD7MQ+
6PYIYNAqJp7+BwhXbprdbrqiEKsu/c1Soy/gWW0zOQXt/e2ssOeYXvJRABLfTIoM
4hHfQ2XQ86GFWW3Uu6CX98dAY7Mkz+xenkIbzOI2Q9GDeRFIOz3bwyoDT81TLQeN
DihKqVjgJ8NH9qsAzfwx7Zm5Vy4tWnlfSBQ5iw7aHKDWTtSBg0k=
-----END CERTIFICATE-----)";

static const std::string second_certificate = R"(-----BEGIN CERTIFICATE-----
MIIDlTCCAn2gAwIBAgIFAPm5jwcwDQYJKoZIhvcNAQELBQAwVzELMAkGA1UEBhMC
VVMxFTATBgNVBAoTDEh5VHJ1c3QgSW5jLjExMC8GA1UEAxMoSHlUcnVzdCBLZXlD
b250cm9sIENlcnRpZmljYXRlIEF1dGhvcml0eTAgFw0yMTAxMDQxMjMwMTlaGA8y
MDk5MDEwMTEyMzAxOVowNTELMAkGA1UEBhMCVVMxFTATBgNVBAoTDEh5VHJ1c3Qg
SW5jLjEPMA0GA1UEAxMGc2N5bGxhMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIB
CgKCAQEAosGginu/B4eQKyB26ar6rqg8FTMoCTf56iBwQnrOt17YN++IitlEhFyB
X0QdA0z97jLIBckTCbieTg+CdtWDeCnM8IAjoN55C0Z2zBKH6cuPTTnDu0WaZY/8
IafGwxcWllYpgQ3AiJFNK66QRIiiX+ejrS3+Co0PYWPzmSczWoxBgFhnXnTPE4ki
MLvZ+zY1iXt83NbQIw8yMUcL+RYK4RlACf3bPztOss98LmyntIkNkZL8GblLoZbc
AZc6udnDe1GuP+NlMO+1jPmyND/xz/kK2hkU4+yotBWVxM1lwpANnElsAHaRvthP
kGsjoVvZgEgg6MQX+iaDngjmiLtUlwIDAQABo4GHMIGEMAkGA1UdEwQCMAAwLAYJ
YIZIAYb4QgENBB8WHU9wZW5TU0wgR2VuZXJhdGVkIENlcnRpZmljYXRlMB0GA1Ud
DgQWBBTAgszBvUAggtuLWf2XucTzjLQeqDAfBgNVHSMEGDAWgBRmrd5FUF1UaB+w
VgBl+9Hyl1fvbjAJBgNVHREEAjAAMA0GCSqGSIb3DQEBCwUAA4IBAQBJA9q7q5XL
H7O/EiJYagWaqrO+AGVrg3DtkP0NTaumM/zYoOq9klZNwvsGZ88+L0hBQ8fj8O/l
rTobdM7eT5p4JiwfN+MB8zXuZ+XjL+kIpqFqPJdVDtBLPGINHP7itrUo9Uk/9XcW
JqPAwufEMN5X7iwN80aUVj6/iUiQ1yqXKQmERdoIPqiyHOEBwobwIAQ1bQKtEiBT
ZE0hdDdI+ZCrtJYOES4kpR4WI3997doVhEusNGBETCWMm2HoJ8xsk7fgVgnpYlat
95tGZ/ZR1Zaa+fXSm9adxJDCviG77pTZBa7nbzPoyG3wm76cTGSeHso8rvGVHsTh
AraNJqMQkZT5
-----END CERTIFICATE-----)";

static std::string write_bundle(const tmpdir& dir, const std::string& name, const std::string& content) {
    auto path = (dir.path() / name).native();
    std::ofstream(path, std::ios::binary) << content;
    return path;
}

static std::vector<sstring> read_bundle(const std::string& path) {
    return utils::http::internal::read_ca_bundle(path.c_str()).get();
}

SEASTAR_THREAD_TEST_CASE(test_read_ca_bundle_accepts_a_distribution_bundle) {
    tmpdir dir;
    // Comment lines between and after the certificates, as in the RHEL bundle,
    // and CRLF line ends.
    auto crlf = [] (std::string s) {
        std::string out;
        for (char c : s) {
            if (c == '\n') {
                out += '\r';
            }
            out += c;
        }
        return out;
    };
    auto bundle = "# first CA\n" + first_certificate + "\n\n# second CA\r\n" + crlf(second_certificate) + "\r\n# the end\n\n";
    BOOST_REQUIRE_EQUAL(read_bundle(write_bundle(dir, "bundle.crt", bundle)).size(), 2);
    // What openssl x509 -text prints before a certificate, as a CA added by
    // hand can carry it.
    auto described = first_certificate + "\nCertificate:\n    Data:\n        Version: 3 (0x2)\n" + second_certificate + "\n";
    BOOST_REQUIRE_EQUAL(read_bundle(write_bundle(dir, "described.crt", described)).size(), 2);
}

SEASTAR_THREAD_TEST_CASE(test_read_ca_bundle_reads_across_buffers) {
    tmpdir dir;
    // Larger than the 32 KiB the file is read in, so certificates and their
    // markers are split between reads, and what is left of one read carries
    // over into the next.
    std::string bundle;
    for (int i = 0; i < 60; ++i) {
        bundle += fmt::format("# CA {}\n{}\n", i, i % 2 ? first_certificate : second_certificate);
    }
    BOOST_REQUIRE_GT(bundle.size(), 2 * 32 * 1024);
    BOOST_REQUIRE_EQUAL(read_bundle(write_bundle(dir, "large.crt", bundle)).size(), 60);

    // A long comment stays within the bound on a certificate's length.
    auto comment = "# " + std::string(60 * 1024, 'c') + "\n";
    BOOST_REQUIRE_EQUAL(read_bundle(write_bundle(dir, "long-comment.crt", comment + first_certificate + "\n")).size(), 1);
}

SEASTAR_THREAD_TEST_CASE(test_read_ca_bundle_rejects_a_bundle_that_is_not_whole) {
    tmpdir dir;
    auto rejects = [&] (const std::string& name, const std::string& content, std::string_view reason) {
        BOOST_TEST_MESSAGE(name);
        BOOST_REQUIRE_EXCEPTION(read_bundle(write_bundle(dir, name, content)), std::exception, [&] (const std::exception& e) {
            return std::string_view(e.what()).find(reason) != std::string_view::npos;
        });
    };
    rejects("empty", "", "no certificates");
    rejects("only-comments", "# no certificates\n\n", "no certificates");
    rejects("cut-inside-a-certificate", first_certificate + "\n" + second_certificate.substr(0, second_certificate.size() / 2),
            "the file ends inside a certificate");
    rejects("cut-inside-the-begin-line", first_certificate + "\n-----BEGIN CERTIFI", "the file ends inside a certificate");
    rejects("cut-at-the-first-dashes", first_certificate + "\n---", "the file ends inside a certificate");
    rejects("cut-before-another-certificate", "-----BEGIN CERTIFI\n" + first_certificate + "\n",
            "a broken PEM marker before a certificate");
    rejects("end-without-a-beginning", first_certificate.substr(first_certificate.find('\n') + 1), "certificate end without a beginning");
    auto corrupt = second_certificate;
    corrupt[corrupt.find('\n') + 10] = '!';
    // Rejected by gnutls, whose wording is its own; what matters is that the
    // error is its, not one reading the file.
    BOOST_REQUIRE_EXCEPTION(read_bundle(write_bundle(dir, "corrupt", first_certificate + "\n" + corrupt + "\n")), std::system_error,
            [] (const std::system_error& e) {
        return e.code().category() != std::system_category() && e.code().category() != std::generic_category();
    });
    rejects("no-certificate-end", std::string(100 * 1024, 'x'), "no certificate end within");
}

SEASTAR_THREAD_TEST_CASE(test_read_ca_bundle_reports_a_missing_file) {
    // The caller tells an absent bundle, which is expected, from one it cannot
    // read by this error.
    try {
        read_bundle("/nonexistent/ca-bundle.crt");
        BOOST_FAIL("a missing bundle was read");
    } catch (const std::system_error& e) {
        BOOST_REQUIRE(e.code() == std::errc::no_such_file_or_directory);
    }
}
