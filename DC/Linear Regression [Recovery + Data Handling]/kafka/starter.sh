#!/bin/bash -e
(exec "$KAFKA_HOME/bin/kafka-server-start.sh" "/usr/bin/server.properties") &
(exec "$KAFKA_HOME/bin/kafka-server-start.sh" "/usr/bin/server1.properties") &
(exec "$KAFKA_HOME/bin/kafka-server-start.sh" "/usr/bin/server2.properties") &
exec "$KAFKA_HOME/bin/kafka-server-start.sh" "/usr/bin/server3.properties"
#what the heck
