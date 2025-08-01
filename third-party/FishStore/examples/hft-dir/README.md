
## !!! Difference between NullableInt and NullableInt !!!

NullableInt from fishstore core is DIFFERENT from NullableInt in the
parser_api. (WTF.).
Good thing we did it correctly the first time but here's the difference:
NullableInt in fishstore's core/psf.h is defined as { is_null, value } while the one
in the parser/parser_api.h is defined as { has_value, value }. See the teeny tiny
fucking difference??

I'm pretty sure they mention this elsewhere in the docs but it's in one the tiny paragraphs and such.

## Compiling


```
# add upstream to remotes
git remote add upstream https://github.com/psu-db/FishStore.git

# sync with upstream
git pull upstream <branch>
git push origin <branch>
```

For debug build
```
mkdir -p build/Debug
cd build/Debug
cmake -DCMAKE_BUILD_TYPE=Debug ../..
```

For release build
```
mkdir -p build/Release
cd build/Release
cmake -DCMAKE_BUILD_TYPE=Release ../..
```

Then actually build
```
cmake --build . --target hft psf_lib
```

where QUERY is rocksdb-p1, rocksdb-p2 ... etc.
