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

#include "formatter.hh"
#include "json_elements.hh"

using namespace std;

namespace json {

sstring formatter::to_json(const sstring& str) {
    return to_json(str.c_str());
}

sstring formatter::to_json(const char* str) {
    sstring res = "\"";
    res += str;
    res += "\"";
    return res;
}

sstring formatter::to_json(int n) {
    return to_string(n);
}

sstring formatter::to_json(long n) {
    return to_string(n);
}

sstring formatter::to_json(float f) {
    if (std::isinf(f)) {
        throw out_of_range("Infinite float value is not supported");
    } else if (std::isnan(f)) {
        throw invalid_argument("Invalid float value");
    }
    return to_sstring(f);
}

sstring formatter::to_json(double d) {
    if (std::isinf(d)) {
        throw out_of_range("Infinite double value is not supported");
    } else if (std::isnan(d)) {
        throw invalid_argument("Invalid double value");
    }
    return to_sstring(d);
}

sstring formatter::to_json(bool b) {
    return (b) ? "true" : "false";
}

sstring formatter::to_json(const date_time& d) {
    char buff[50];
    sstring res = "\"";
    strftime(buff, 50, TIME_FORMAT, &d);
    res += buff;
    return res + "\"";
}

sstring formatter::to_json(const jsonable& obj) {
    return obj.to_json();
}

sstring formatter::to_json(unsigned long l) {
    return to_string(l);
}

}
