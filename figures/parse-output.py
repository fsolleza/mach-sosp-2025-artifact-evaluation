import re
import statistics

def parse_duration_string(dur):
	dur = dur.strip(" ").strip("\n")
	num = re.findall(r"[\d.]+", dur)[0]
	if dur.endswith("ms"):
		return float(num)/1000.
	if dur.endswith("s"):
		return float(num)

	print("Can't parse duration:", dur)
	exit()

def read_e2e_query_latency(path):
	f = open(path, "r")
	lines = [x for x in f.readlines() if x.startswith("Query Latency")]

	measurements = {}
	for line in lines:
		x = line.split(":")
		name = x[0].strip(" ")
		value = x[1].strip("\n").strip(" ")
		secs = parse_duration_string(value)
		if not name in measurements:
			measurements[name] = []
		measurements[name].append(secs)
	return measurements

def e2e_get_dropped(path):
	f = open(path, "r")
	lines = [x for x in f.readlines()
		if x.startswith("Records received") or x.startswith("Received")]
	last_line = lines[-1]

	regex_text = r"total received [0-9]+ dropped [0-9]+"
	totals = re.findall(regex_text, last_line)[0]

	total_rx_text = re.findall(r"total received [0-9]+", totals)[0]
	total_rx = int(re.findall(r"[0-9]+", total_rx_text)[0])

	dropped_text = re.findall(r"dropped [0-9]+", totals)[0]
	dropped = int(re.findall(r"[0-9]+", dropped_text)[0])

	return dropped/total_rx

####################
# E2E Mach RocksDB #
####################

def e2e_mach_app_rocksdb_p1():

	path = "../evaluation-logs/e2e-mach-app-rocksdb-p1"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (Application Max Latency)'
	median = statistics.median(measurements[name])
	row1 = ["P1", "Mach", "Max", median]

	name = 'Query Latency (Application Tail Latency)'
	median = statistics.median(measurements[name])
	row2 = ["P1", "Mach", "Percentile", median]

	dropped = e2e_get_dropped(path)
	row3 = ["P1", "Mach", "Dropped (%)", dropped]

	return [row1, row2, row3]

def e2e_mach_app_rocksdb_p2():

	path = "../evaluation-logs/e2e-mach-app-rocksdb-p2"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (pread64 Max Latency)'
	median = statistics.median(measurements[name])
	row1 = ["P2", "Mach", "Max", median]

	name = 'Query Latency (pread64 Tail Latency)'
	median = statistics.median(measurements[name])
	row2 = ["P2", "Mach", "Percentile", median]

	dropped = e2e_get_dropped(path)
	row3 = ["P2", "Mach", "Dropped (%)", dropped]

	return [row1, row2, row3]

def e2e_mach_app_rocksdb_p3():

	path = "../evaluation-logs/e2e-mach-app-rocksdb-p3"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (Page Cache Event Count)'
	median = statistics.median(measurements[name])
	row1 = ["P3", "Mach", "Count", median]

	dropped = e2e_get_dropped(path)
	row2 = ["P3", "Mach", "Dropped (%)", dropped]

	return [row1, row2]

#########################
# E2E Fishstore RocksDB #
#########################

def e2e_fishstore_app_rocksdb_p1():

	path = "../evaluation-logs/e2e-fishstore-app-rocksdb-p1"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (Application Max Latency and Tail Latency)'
	median = statistics.median(measurements[name])
	row1 = ["P1", "FishStore-I", "Max", median]
	row2 = ["P1", "FishStore-I", "Percentile", median]

	dropped = e2e_get_dropped(path)
	row3 = ["P1", "Fishstore-I", "Dropped (%)", dropped]

	return [row1, row2, row3]

def e2e_fishstore_app_rocksdb_p2():

	path = "../evaluation-logs/e2e-fishstore-app-rocksdb-p2"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (pread64 Max Latency and Tail Latency)'
	median = statistics.median(measurements[name])
	row1 = ["P2", "FishStore-I", "Max", median]
	row2 = ["P2", "FishStore-I", "Percentile", median]

	dropped = e2e_get_dropped(path)
	row3 = ["P2", "FishStore-I", "Dropped (%)", dropped]

	return [row1, row2, row3]

def e2e_fishstore_app_rocksdb_p3():

	path = "../evaluation-logs/e2e-fishstore-app-rocksdb-p3"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (Page Cache Event Count)'
	median = statistics.median(measurements[name])
	row1 = ["P3", "FishStore-I", "Count", median]

	dropped = e2e_get_dropped(path)
	row2 = ["P3", "FishStore-I", "Dropped (%)", dropped]

	return [row1, row2]

######################
# E2E Influx RocksDB #
######################

def e2e_influx_app_rocksdb_p1():

	path = "../evaluation-logs/e2e-influx-app-rocksdb-p1"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (Application Max Latency)'
	median = statistics.median(measurements[name])
	row1 = ["P1", "Influx-complete", "Max", median]

	name = 'Query Latency (Application Tail Latency)'
	median = statistics.median(measurements[name])
	row2 = ["P1", "Influx-complete", "Percentile", median]

	return [row1, row2]

def e2e_influx_app_rocksdb_p2():

	path = "../evaluation-logs/e2e-influx-app-rocksdb-p2"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (pread64 Max Latency)'
	median = statistics.median(measurements[name])
	row1 = ["P2", "Influx-complete", "Max", median]

	name = 'Query Latency (pread64 Tail Latency)'
	median = statistics.median(measurements[name])
	row2 = ["P2", "Influx-complete", "Percentile", median]

	return [row1, row2]

def e2e_influx_app_rocksdb_p3():

	path = "../evaluation-logs/e2e-influx-app-rocksdb-p3"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (Page Cache Event Count)'
	median = statistics.median(measurements[name])
	row1 = ["P3", "Influx-complete", "Count", median]

	return [row1]

def e2e_influx_app_rocksdb_dropped():
	path = "../evaluation-logs/e2e-influx-app-dropped-rocksdb-p1"
	dropped = e2e_get_dropped(path)
	row1 = ["P1", "Influx", "Dropped (%)", dropped]

	path = "../evaluation-logs/e2e-influx-app-dropped-rocksdb-p2"
	dropped = e2e_get_dropped(path)
	row2 = ["P2", "Influx", "Dropped (%)", dropped]

	path = "../evaluation-logs/e2e-influx-app-dropped-rocksdb-p3"
	dropped = e2e_get_dropped(path)
	row3 = ["P3", "Influx", "Dropped (%)", dropped]

	return [row1, row2, row3]

###################
# E2E Mach Valkey #
###################

def e2e_mach_app_valkey_p1():

	path = "../evaluation-logs/e2e-mach-app-valkey-p1"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (Slow Requests)'
	median = statistics.median(measurements[name])
	row1 = ["P1", "Mach", "Slow Requests", median]

	dropped = e2e_get_dropped(path)
	row2 = ["P1", "Mach", "Dropped (%)", dropped]

	return [row1, row2]

def e2e_mach_app_valkey_p2():

	path = "../evaluation-logs/e2e-mach-app-valkey-p2"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency Slow Requests'
	median = statistics.median(measurements[name])
	row1 = ["P2", "Mach", "Slow Requests", median]

	name = 'Query Latency Slow sendto Execution'
	median = statistics.median(measurements[name])
	row2 = ["P2", "Mach", "Slow sendto", median]

	dropped = e2e_get_dropped(path)
	row3 = ["P2", "Mach", "Dropped (%)", dropped]

	return [row1, row2, row3]

def e2e_mach_app_valkey_p3():

	path = "../evaluation-logs/e2e-mach-app-valkey-p3"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (Maximum Latency Request)'
	median = statistics.median(measurements[name])
	row1 = ["P3", "Mach", "Max Request", median]

	name = 'Query Latency (TCP Packet scan)'
	median = statistics.median(measurements[name])
	row2 = ["P3", "Mach", "Packet Scan", median]

	dropped = e2e_get_dropped(path)
	row3 = ["P3", "Mach", "Dropped (%)", dropped]

	return [row1, row2, row3]

########################
# E2E Fishstore Valkey #
########################

def e2e_fishstore_app_valkey_p1():

	path = "../evaluation-logs/e2e-fishstore-app-valkey-p1"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (Slow Requests)'
	median = statistics.median(measurements[name])
	row1 = ["P1", "FishStore-I", "Slow Requests", median]

	dropped = e2e_get_dropped(path)
	row2 = ["P1", "Fishstore-I", "Dropped (%)", dropped]

	return [row1, row2]

def e2e_fishstore_app_valkey_p2():

	path = "../evaluation-logs/e2e-fishstore-app-valkey-p2"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (Slow Requests)'
	median = statistics.median(measurements[name])
	row1 = ["P2", "FishStore-I", "Slow Requests", median]

	name = 'Query Latency (Slow sendto Execution)'
	median = statistics.median(measurements[name])
	row2 = ["P2", "FishStore-I", "Slow sendto", median]

	dropped = e2e_get_dropped(path)
	row3 = ["P2", "FishStore-I", "Dropped (%)", dropped]

	return [row1, row2, row3]

def e2e_fishstore_app_valkey_p3():

	path = "../evaluation-logs/e2e-fishstore-app-valkey-p3"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (Maximum Latency Request)'
	median = statistics.median(measurements[name])
	row1 = ["P3", "FishStore-I", "Max Request", median]

	name = 'Query Latency (TCP Packet Dump)'
	median = statistics.median(measurements[name])
	row2 = ["P3", "FishStore-I", "Packet Scan", median]

	dropped = e2e_get_dropped(path)
	row3 = ["P3", "FishStore-I", "Dropped (%)", dropped]

	return [row1, row2, row3]

#####################
# E2E Influx Valkey #
#####################

def e2e_influx_app_valkey_p1():

	path = "../evaluation-logs/e2e-influx-app-valkey-p1"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (Slow Requests)'
	median = statistics.median(measurements[name])
	row1 = ["P1", "Influx-complete", "Slow Requests", median]

	return [row1]

def e2e_influx_app_valkey_p2():

	path = "../evaluation-logs/e2e-influx-app-valkey-p2"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (Slow Requests)'
	median = statistics.median(measurements[name])
	row1 = ["P2", "Influx-complete", "Slow Requests", median]

	name = 'Query Latency (Slow sendto Execution)'
	median = statistics.median(measurements[name])
	row2 = ["P2", "Influx-complete", "Slow sendto", median]

	return [row1, row2]

def e2e_influx_app_valkey_p3():

	path = "../evaluation-logs/e2e-influx-app-valkey-p3"
	measurements = read_e2e_query_latency(path)

	name = 'Query Latency (Maximum Latency Request)'
	median = statistics.median(measurements[name])
	row1 = ["P3", "Influx-complete", "Max Request", median]

	name = 'Query Latency (TCP Packet scan)'
	median = statistics.median(measurements[name])
	row2 = ["P3", "Influx-complete", "Packet Scan", median]

	return [row1, row2]

def e2e_influx_app_valkey_dropped():
	path = "../evaluation-logs/e2e-influx-app-dropped-valkey-p1"
	dropped = e2e_get_dropped(path)
	row1 = ["P1", "Influx", "Dropped (%)", dropped]

	path = "../evaluation-logs/e2e-influx-app-dropped-valkey-p2"
	dropped = e2e_get_dropped(path)
	row2 = ["P2", "Influx", "Dropped (%)", dropped]

	path = "../evaluation-logs/e2e-influx-app-dropped-valkey-p3"
	dropped = e2e_get_dropped(path)
	row3 = ["P3", "Influx", "Dropped (%)", dropped]

	return [row1, row2, row3]


######################################
# Combine end 2 end workload results #
######################################

def e2e_rocksdb():
	rows = []
	rows = rows + e2e_mach_app_rocksdb_p1()
	rows = rows + e2e_mach_app_rocksdb_p2()
	rows = rows + e2e_mach_app_rocksdb_p3()
	rows = rows + e2e_fishstore_app_rocksdb_p1()
	rows = rows + e2e_fishstore_app_rocksdb_p2()
	rows = rows + e2e_fishstore_app_rocksdb_p3()
	rows = rows + e2e_influx_app_rocksdb_p1()
	rows = rows + e2e_influx_app_rocksdb_p2()
	rows = rows + e2e_influx_app_rocksdb_p3()
	rows = rows + e2e_influx_app_rocksdb_dropped()

	csv = "phase,database,desc,value\n"
	for row in rows:
		row = [str(x) for x in row]
		csv = csv + ",".join(row) + "\n"
	with open("data/e2e_rocksdb.csv", "w") as f:
		f.write(csv)

def e2e_valkey():
	rows = []
	rows = rows + e2e_mach_app_valkey_p1()
	rows = rows + e2e_mach_app_valkey_p2()
	rows = rows + e2e_mach_app_valkey_p3()
	rows = rows + e2e_fishstore_app_valkey_p1()
	rows = rows + e2e_fishstore_app_valkey_p2()
	rows = rows + e2e_fishstore_app_valkey_p3()
	rows = rows + e2e_influx_app_valkey_p1()
	rows = rows + e2e_influx_app_valkey_p2()
	rows = rows + e2e_influx_app_valkey_p3()
	rows = rows + e2e_influx_app_valkey_dropped()

	csv = "phase,database,desc,value\n"
	for row in rows:
		row = [str(x) for x in row]
		csv = csv + ",".join(row) + "\n"
	with open("data/e2e_valkey.csv", "w") as f:
		f.write(csv)


##############################
# Parse Probe Effects Output #
##############################

def read_pe_throughput(path):
	f = open(path, "r")
	line = [x for x in f.readlines() if x.startswith("Average")][0]
	regex_text = r"[0-9.]+"
	throughput = float(re.findall(regex_text, line)[0])
	return throughput

def probe_effects():
	rows = []
	noop = read_pe_throughput("../evaluation-logs/pe-noop")
	rows.append(["noop", str(noop)])

	mach = read_pe_throughput("../evaluation-logs/pe-mach")
	rows.append(["SysX", str(mach)])

	file_writer = read_pe_throughput("../evaluation-logs/pe-file-writer")
	rows.append(["Raw File", str(file_writer)])

	influx = read_pe_throughput("../evaluation-logs/pe-influx")
	rows.append(["InfluxDB", str(influx)])

	fishstore_i = read_pe_throughput("../evaluation-logs/pe-fishstore")
	rows.append(["FishStore-I", str(fishstore_i)])

	fishstore_n =read_pe_throughput("../evaluation-logs/pe-fishstore-no-index")
	rows.append(["FishStore-N", str(fishstore_n)])

	csv = "sys,throughput\n"
	for row in rows:
		row = [str(x) for x in row]
		csv = csv + ",".join(row) + "\n"
	with open("data/probe_effects.csv", "w") as f:
		f.write(csv)


###############################
# Parse Ingest Scaling Output #
###############################

def parse_is_line(line):
	nums = re.findall(r"[0-9.]+", line)
	return [float(x) for x in nums]

def get_is_median_throughput(path):
	f = open(path, "r")
	lines = [parse_is_line(x) for x in f.readlines()
		 if x.startswith("count tp") or x.startswith("Result rps")]
	counts = []
	bytes = []
	for line in lines:
		counts.append(line[0])
		bytes.append(line[1])
	return [statistics.median(counts), statistics.median(bytes)]

def ingest_scaling():

	rows = "storage,size,mrps,mbps\n"

	def make_line(sys,sz,medians):
		line = [sys, str(sz), str(medians[0]/1000000),
			str(medians[1]/1000000)]
		line = ",".join(line) + "\n"
		return line

	def read_all_sys(sys, name):
		# sys is filename to parse
		# name is the name to write into the csv
		rows = ""
		for sz in [8, 64, 256, 1024]:
			path = "../evaluation-logs/is-{}-{}".format(sys, sz)
			medians = get_is_median_throughput(path)
			rows = rows + make_line(name,sz,medians)
		return rows

	systems = [
		("mach", "mach"),
		("fishstore1", "fishstore-1"),
		("fishstore8", "fishstore-8"),
		("rocksdb1", "rocksdb-1"),
		("rocksdb8", "rocksdb-8"),
		("lmdb", "lmdb"),
	]
	for sys, name in systems:
		rows = rows + read_all_sys(sys, name)

	with open("data/ingest-scaling.csv", "w") as f:
		f.write(rows)

#########################
# Parse Ablation Output #
#########################

def ablation_dur(abl, lb):
	path = "../evaluation-logs/ab-mach-ablation-{}-{}".format(abl,lb)
	f = open(path, "r")
	lines = [x for x in f.readlines() if x.startswith("Duraton:")][0]
	dur_str = lines.split(":")[1].strip(" ");
	dur = parse_duration_string(dur_str)
	return dur

def fishstore_exact_dur(lb):
	path = "../evaluation-logs/e2e-fishstore-app-exact-microbenchmark-{}"
	f = open(path.format(lb), "r")
	lines = [x for x in f.readlines() if x.startswith("Scan done")][0]
	dur_str = re.findall("[0-9.]+", lines)[0]
	return float(dur_str)
	
def ablation():
	ablations = [
		("onlytime", "timeonly"),
		("onlyrange", "rangeonly"),
		("noindex", "noindex"),
		("timerange", "timerange"),
	]

	rows = "ablation,lookback,duration\n"
	for abl in ablations:
		for lb in [20, 60, 120, 300, 600]:
			dur = ablation_dur(abl[0], lb)
			row = [abl[1], str(lb), str(dur)]
			rows = rows + ",".join(row) + "\n"

	# and now read Fishstore exact
	#for lb in [60, 120, 300, 600]:
	for lb in [60, 120, 300, 600]:
		dur = fishstore_exact_dur(lb)
		row = ["fishstore", str(lb), str(dur)]
		rows = rows + ",".join(row) + "\n"

	with open("data/ablation.csv", "w") as f:
		f.write(rows)


if __name__=="__main__":
	e2e_rocksdb()
	e2e_valkey()
	probe_effects()
	ingest_scaling()
	ablation()


