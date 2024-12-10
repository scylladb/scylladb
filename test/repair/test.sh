#!/bin/bash

# Function to check URL health
check_health() {
    response=$(curl -s "http://$1:8000")
    if [[ $response == *"healthy: $1:8000"* ]]; then
        echo "URL $1 is healthy"
        return 0
    else
        echo "URL $1 is not healthy yet"
        return 1
    fi
}

# URLs to check 
url1="10.6.0.5"
url2="10.6.0.6"

docker-compose up -d

# Wait for both URLs to return "healthy"
while true; do
    if check_health $url1 && check_health $url2; then
        echo "Both URLs are healthy. Continuing with the script."
        break
    fi
    sleep 5  # Wait for 5 seconds before checking again
done

echo "start create table"
docker exec -ti repair-node1-1 bash -c "cqlsh $url1 -f /create_table && sh /compact.sh"

echo "restart scylla"
docker exec -ti repair-node1-1 bash -c "supervisorctl restart scylla"

while true; do
    if check_health $url1 && check_health $url2; then
        echo "Both URLs are healthy. Continuing with the script."
        break
    fi
    sleep 5  # Wait for 5 seconds before checking again
done

echo "repair base table and flush GSI"
docker exec -ti repair-node1-1 bash -c "nodetool repair repair_test repair_test && nodetool flush repair_test repair_test_mv"

directory="/var/lib/scylla/data/repair_test/repair_test_mv-*"
docker exec -ti repair-node1-1 bash -c "find $directory -type f -name "*Data.db" -exec sstabledump {} \;"

echo "consitency level one"
docker exec -ti repair-node1-1 bash -c "cqlsh $url1 -e 'SELECT * FROM repair_test.repair_test_mv;'"

echo "consitency level all"
docker exec -ti repair-node1-1 bash -c "cqlsh $url1 -e 'CONSISTENCY ALL;SELECT * FROM repair_test.repair_test_mv;'"