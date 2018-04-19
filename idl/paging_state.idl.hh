namespace db {
enum class read_repair_decision : uint8_t {
  NONE,
  GLOBAL,
  DC_LOCAL,
};
}

namespace service {
namespace pager {
class paging_state {
    partition_key get_partition_key();
    std::experimental::optional<clustering_key> get_clustering_key();
    uint32_t get_remaining();
    utils::UUID get_query_uuid() [[version 2.2]] = utils::UUID();
    std::unordered_map<dht::token_range, std::vector<utils::UUID>> get_last_replicas() [[version 2.2]] = std::unordered_map<dht::token_range, std::vector<utils::UUID>>();
    std::experimental::optional<db::read_repair_decision> get_query_read_repair_decision() [[version 2.3]] = std::experimental::nullopt;
};
}
}
