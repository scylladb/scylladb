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

#include "json_elements.hh"
#include <string.h>
#include <string>
#include <vector>
#include <sstream>

using namespace std;

namespace json {

/**
 * The json builder is a helper class
 * To help create a json object
 *
 */
class json_builder {
public:
    json_builder()
            : first(true) {
        result << OPEN;
    }

    /**
     * add a name value to an object
     * @param name the name of the element
     * @param str the value already formated
     */
    void add(const string& name, const string& str) {
        if (first) {
            first = false;
        } else {
            result << ", ";
        }
        result << '"' << name << "\": " << str;
    }

    /**
     * add a json element to the an object
     * @param element
     */
    void add(json_base_element* element) {
        if (element == nullptr || element->_set == false) {
            return;
        }
        add(element->_name, element->to_string());
    }

    /**
     * Get the string representation of the object
     * @return a string of accumulative object
     */
    string as_json() {
        result << CLOSE;
        return result.str();
    }

private:
    static const string OPEN;
    static const string CLOSE;

    stringstream result;
    bool first;

};

const string json_builder::OPEN("{");
const string json_builder::CLOSE("}");

void json_base::add(json_base_element* element, string name, bool mandatory) {
    element->_mandatory = mandatory;
    element->_name = name;
    _elements.push_back(element);
}

string json_base::to_json() const {
    json_builder res;
    for (auto i : _elements) {
        res.add(i);
    }
    return res.as_json();
}

bool json_base::is_verify() const {
    for (auto i : _elements) {
        if (!i->is_verify()) {
            return false;
        }
    }
    return true;
}

}
