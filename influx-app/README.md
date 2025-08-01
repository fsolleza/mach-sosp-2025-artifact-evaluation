
Run influx db

```
rm -rf /nvme/data/tmp/can_delete/influxdb2
taskset -c 0-40 influxd run -config influx.conf
```

