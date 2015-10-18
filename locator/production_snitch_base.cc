#include "locator/production_snitch_base.hh"

namespace locator {
future<> production_snitch_base::load_property_file() {
    return engine().open_file_dma(_prop_file_name, open_flags::ro)
    .then([this] (file f) {
        return do_with(std::move(f), [this] (file& f) {
            return f.size().then([this, &f] (size_t s) {
                _prop_file_size = s;

                return f.dma_read_exactly<char>(0, s);
            });
        }).then([this] (temporary_buffer<char> tb) {
            _prop_file_contents = std::move(std::string(tb.get(), _prop_file_size));
            parse_property_file();

            return make_ready_future<>();
        });
    });
}

void production_snitch_base::parse_property_file() {
    using namespace boost::algorithm;

    std::string line;
    std::istringstream istrm(_prop_file_contents);
    std::vector<std::string> split_line;
    _prop_values.clear();

    while (std::getline(istrm, line)) {
        trim(line);

        // Skip comments or empty lines
        if (!line.size() || line.at(0) == '#') {
            continue;
        }

        split_line.clear();
        split(split_line, line, is_any_of("="));

        if (split_line.size() != 2) {
            throw_bad_format(line);
        }

        auto key = split_line[0]; trim(key);
        auto val = split_line[1]; trim(val);

        if (val.empty() || !allowed_property_keys.count(key)) {
            throw_bad_format(line);
        }

        if (_prop_values.count(key)) {
            throw_double_declaration(key);
        }

        _prop_values[key] = val;
    }
}
} // namespace locator
