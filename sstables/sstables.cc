/*
 * Copyright 2015 Cloudius Systems
 */

#include "log.hh"
#include <vector>
#include <typeinfo>
#include "core/future.hh"
#include "core/sstring.hh"

#include "sstables.hh"

namespace sstables {

thread_local logging::logger sstlog("sstable");

unordered_map<sstable::version_types, sstring> sstable::_version_string = {
    { sstable::version_types::la , "la" }
};

unordered_map<sstable::format_types, sstring> sstable::_format_string = {
    { sstable::format_types::big , "big" }
};

unordered_map<sstable::component_type, sstring> sstable::_component_map = {
    { component_type::Index, "Index.db"},
    { component_type::CompressionInfo, "CompressionInfo.db" },
    { component_type::Data, "Data.db" },
    { component_type::TOC, "TOC.txt" },
    { component_type::Summary, "Summary.db" },
    { component_type::Digest, "Digest.sha1" },
    { component_type::CRC, "CRC.db" },
    { component_type::Filter, "Filter.db" },
    { component_type::Statistics, "Statistics.db" },
};

const bool sstable::has_component(component_type f) {
    return _components.count(f);
}

const sstring sstable::filename(component_type f) {

    auto& version = _version_string.at(_version);
    auto& format = _format_string.at(_format);
    auto& component = _component_map.at(f);
    auto epoch =  to_sstring(_epoch);

    return _dir + "/" + version + "-" + epoch + "-" + format + "-" + component;
}
}
