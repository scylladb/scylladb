add_scylla_test(vector_store_client_test
  LIBRARIES vector_search)

add_scylla_test(load_balancer_test
  LIBRARIES vector_search)

add_scylla_test(client_test
  LIBRARIES vector_search)

add_scylla_test(filter_test
  LIBRARIES vector_search)
add_scylla_test(rescoring_test
  LIBRARIES vector_search)
