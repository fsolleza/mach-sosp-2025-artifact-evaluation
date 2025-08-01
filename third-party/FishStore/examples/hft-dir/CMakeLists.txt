set(BENCHMARK_HEADERS
)

set(BENCHMARK_SOURCES
)

ADD_FISHSTORE_BENCHMARK(hft)
ADD_FISHSTORE_BENCHMARK(rocksdb-p1)
ADD_FISHSTORE_BENCHMARK(rocksdb-p2)

add_library(psf_lib SHARED psf_lib.cc)
target_link_libraries(psf_lib)
