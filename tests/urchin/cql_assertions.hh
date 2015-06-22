#pragma once

#include "transport/messages/result_message_base.hh"
#include "bytes.hh"
#include "core/shared_ptr.hh"
#include "core/future.hh"

class rows_assertions {
    shared_ptr<transport::messages::result_message::rows> _rows;
public:
    rows_assertions(shared_ptr<transport::messages::result_message::rows> rows);
    rows_assertions with_size(size_t size);
    rows_assertions is_empty();
    rows_assertions with_row(std::initializer_list<bytes_opt> values);

    // Verifies that the result has the following rows and only that rows, in that order.
    rows_assertions with_rows(std::initializer_list<std::initializer_list<bytes_opt>> rows);
};

class result_msg_assertions {
    shared_ptr<transport::messages::result_message> _msg;
public:
    result_msg_assertions(shared_ptr<transport::messages::result_message> msg);
    rows_assertions is_rows();
};

result_msg_assertions assert_that(shared_ptr<transport::messages::result_message> msg);

template<typename T>
void assert_that_failed(future<T>& f)
{
    assert(f.failed());
    try {
        f.get();
    }
    catch (...) {
    }
}
