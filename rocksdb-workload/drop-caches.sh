#! /usr/bin/env zsh

while :
do
	echo "starting drop caches"
	echo 3 | sudo tee /proc/sys/vm/drop_caches
	sleep 40s
done
