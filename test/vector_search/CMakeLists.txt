add_scylla_test(vector_store_client_test
  SOURCES vector_store_client_test.cc vs_mock_server.cc
  LIBRARIES vector_search)

add_scylla_test(load_balancer_test
  LIBRARIES vector_search)
