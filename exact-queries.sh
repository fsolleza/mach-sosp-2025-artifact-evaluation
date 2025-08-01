#! /usr/bin/env bash

LOOKBACK=120
for number in 1 2 3 4 5; do
	echo RUN $number;

	# This relies on the fact that replay takes a bit of time to load so
	# the fishstore app will be up by the time the replay tries to connect
	# to the TCP port.
	make rocksdb-fishstore-p2 REPLAY_DURATION=800 > /dev/null &
	REPLAY_PID=$!
	make e2e-fishstore-app QUERY=exact-microbenchmark-$LOOKBACK
	kill $REPLAY_PID
	sleep 1
done

