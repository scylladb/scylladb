add_scylla_test(ec2_snitch_test
  KIND SEASTAR)
add_scylla_test(enormous_table_scan_test
  KIND SEASTAR)
add_scylla_test(gce_snitch_test
  KIND SEASTAR)
add_scylla_test(gossip
  KIND SEASTAR)
add_scylla_test(manual_hint_test
  SOURCES hint_test.cc
  KIND SEASTAR)
add_scylla_test(message_test
  SOURCES message.cc
  KIND SEASTAR)
add_scylla_test(partition_data_test
  KIND SEASTAR)
add_scylla_test(row_locker_test
  KIND SEASTAR)
add_scylla_test(sstable_scan_footprint_test
  KIND SEASTAR)
add_scylla_test(streaming_histogram_test
  KIND SEASTAR)
