#include "locator/production_snitch_base.hh"
#include "db/system_keyspace.hh"
#include "gms/gossiper.hh"
#include "utils/fb_utilities.hh"
#include "db/config.hh"

namespace locator {

production_snitch_base::production_snitch_base(const sstring& prop_file_name)
        : allowed_property_keys({ dc_property_key,
                          rack_property_key,
                          prefer_local_property_key,
                          dc_suffix_property_key }) {
    if (!prop_file_name.empty()) {
        _prop_file_name = prop_file_name;
    } else {
        using namespace boost::filesystem;

        path def_prop_file(db::config::get_conf_dir());
        def_prop_file /= path(snitch_properties_filename);

        _prop_file_name = def_prop_file.string();
    }
}


sstring production_snitch_base::get_rack(inet_address endpoint) {
    if (endpoint == utils::fb_utilities::get_broadcast_address()) {
        return _my_rack;
    }

    return get_endpoint_info(endpoint,
                             gms::application_state::RACK,
                             default_rack);
}

sstring production_snitch_base::get_datacenter(inet_address endpoint) {
    if (endpoint == utils::fb_utilities::get_broadcast_address()) {
        return _my_dc;
    }

    return get_endpoint_info(endpoint,
                             gms::application_state::DC,
                             default_dc);
}

void production_snitch_base::set_my_distributed(distributed<snitch_ptr>* d) {
    _my_distributed = d;
}

void production_snitch_base::reset_io_state() {
    //
    // Reset the promise to allow repeating
    // start()+stop()/pause_io()+resume_io() call sequences.
    //
    _io_is_stopped = promise<>();
}

sstring production_snitch_base::get_endpoint_info(inet_address endpoint, gms::application_state key,
                                                  const sstring& default_val) {
    gms::gossiper& local_gossiper = gms::get_local_gossiper();
    auto* ep_state = local_gossiper.get_application_state_ptr(endpoint, key);
    if (ep_state) {
        return ep_state->value;
    }

    // ...if not found - look in the SystemTable...
    if (!_saved_endpoints) {
        _saved_endpoints = db::system_keyspace::load_dc_rack_info();
    }

    auto it = _saved_endpoints->find(endpoint);

    if (it != _saved_endpoints->end()) {
        if (key == gms::application_state::RACK) {
            return it->second.rack;
        } else { // gms::application_state::DC
            return it->second.dc;
        }
    }

    // ...if still not found - return a default value
    return default_val;
}

void production_snitch_base::set_my_dc(const sstring& new_dc) {
    _my_dc = new_dc;
}

void production_snitch_base::set_my_rack(const sstring& new_rack) {
    _my_rack = new_rack;
}

void production_snitch_base::set_prefer_local(bool prefer_local) {
    _prefer_local = prefer_local;
}

future<> production_snitch_base::load_property_file() {
    return open_file_dma(_prop_file_name, open_flags::ro)
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

[[noreturn]]
void production_snitch_base::throw_double_declaration(const sstring& key) const {
    logger().error("double \"{}\" declaration in {}", key, _prop_file_name);
    throw bad_property_file_error();
}

[[noreturn]]
void production_snitch_base::throw_bad_format(const sstring& line) const {
    logger().error("Bad format in properties file {}: {}", _prop_file_name, line);
    throw bad_property_file_error();
}

[[noreturn]]
void production_snitch_base::throw_incomplete_file() const {
    logger().error("Property file {} is incomplete. Some obligatory fields are missing.", _prop_file_name);
    throw bad_property_file_error();
}



} // namespace locator
