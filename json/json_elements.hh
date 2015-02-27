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

#ifndef JSON_ELEMENTS_HH_
#define JSON_ELEMENTS_HH_

#include <string>
#include <vector>
#include <time.h>
#include <sstream>
#include "formatter.hh"
#include "core/sstring.hh"

namespace json {

/**
 * The base class for all json element.
 * Every json element has a name
 * An indication if it was set or not
 * And is this element is mandatory.
 * When a mandatory element is not set
 * this is not a valid object
 */
class json_base_element {
public:
    /**
     * The constructors
     */
    json_base_element()
            : _mandatory(false), _set(false) {
    }

    virtual ~json_base_element() = default;

    /**
     * Check if it's a mandatory parameter
     * and if it's set.
     * @return true if this is not a mandatory parameter
     * or if it is and it's value is set
     */
    virtual bool is_verify() {
        return !(_mandatory && !_set);
    }

    /**
     * returns the internal value in a json format
     * Each inherit class must implement this method
     * @return formated internal value
     */
    virtual std::string to_string() = 0;

    std::string _name;
    bool _mandatory;
    bool _set;
};

/**
 * Basic json element instantiate
 * the json_element template.
 * it adds a value to the base definition
 * and the to_string implementation using the formatter
 */
template<class T>
class json_element : public json_base_element {
public:

    /**
     * the assignment operator also set
     * the set value to true.
     * @param new_value the new value
     * @return the value itself
     */
    json_element &operator=(const T& new_value) {
        _value = new_value;
        _set = true;
        return *this;
    }
    /**
     * the assignment operator also set
     * the set value to true.
     * @param new_value the new value
     * @return the value itself
     */
    template<class C>
    json_element &operator=(const C& new_value) {
        _value = new_value;
        _set = true;
        return *this;
    }
    /**
     * The brackets operator
     * @return the value
     */
    const T& operator()() const {
        return _value;
    }

    /**
     * The to_string return the value
     * formated as a json value
     * @return the value foramted for json
     */
    virtual std::string to_string() override
    {
        return formatter::to_json(_value);
    }

private:
    T _value;
};

/**
 * json_list is based on std vector implementation.
 *
 * When values are added with push it is set the "set" flag to true
 * hence will be included in the parsed object
 */
template<class T>
class json_list : public json_base_element {
public:

    /**
     * Add an element to the list.
     * @param element a new element that will be added to the list
     */
    void push(const T& element) {
        _set = true;
        _elements.push_back(element);
    }

    virtual std::string to_string() override
    {
        return formatter::to_json(_elements);
    }

    std::vector<T> _elements;
};

class jsonable {
public:
    virtual ~jsonable() = default;
    /**
     * create a foramted string of the object.
     * @return the object formated.
     */
    virtual std::string to_json() const = 0;
};

/**
 * The base class for all json objects
 * It holds a list of all the element in it,
 * allowing it implement the to_json method.
 *
 * It also allows iterating over the element
 * in the object, even if not all the member
 * are known in advance and in practice mimic
 * reflection
 */
struct json_base : public jsonable {

    virtual ~json_base() = default;

    json_base() = default;

    json_base(const json_base&) = delete;

    json_base operator=(const json_base&) = delete;

    /**
     * create a foramted string of the object.
     * @return the object formated.
     */
    virtual std::string to_json() const;

    /**
     * Check that all mandatory elements are set
     * @return true if all mandatory parameters are set
     */
    virtual bool is_verify() const;

    /**
     * Register an element in an object
     * @param element the element to be added
     * @param name the element name
     * @param mandatory is this element mandatory.
     */
    virtual void add(json_base_element* element, std::string name,
            bool mandatory = false);

    std::vector<json_base_element*> _elements;
};

/**
 * The json return type, is a helper class to return a json
 * formatted string.
 * It uses autoboxing in its constructor so when a function return
 * type is json_return_type, it could return a type that would be converted
 * ie.
 * json_return_type foo() {
 *     return "hello";
 * }
 *
 * would return a json formatted string: "hello" (rather then hello)
 */
struct json_return_type {
    sstring _res;
    template<class T>
    json_return_type(const T& res) {
        _res = formatter::to_json(res);
    }
};

}

#endif /* JSON_ELEMENTS_HH_ */
