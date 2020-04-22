TEMPLATE = app
CONFIG += console c++11
CONFIG -= app_bundle
CONFIG -= qt
INCLUDEPATH += ../
INCLUDEPATH += ../../LevelDBBuffer/include
INCLUDEPATH += ../../rocksdb/include

SOURCES += \
    ../core/core_workload.cc \
    ../db/db_factory.cc \
    ../db/hashtable_db.cc \
    ../db/redis_db.cc \
    ../db/rocksdb_db.cc \
    ../redis/sample.cc \
    ../redis/hiredis/async.c \
    ../redis/hiredis/dict.c \
    ../redis/hiredis/hiredis.c \
    ../redis/hiredis/net.c \
    ../redis/hiredis/read.c \
    #../db/pmemkv_db.cc \
    ../ycsbc.cc \
    ../db/leveldb_db.cc

HEADERS += \
    ../core/client.h \
    ../core/const_generator.h \
    ../core/core_workload.h \
    ../core/counter_generator.h \
    ../core/db.h \
    ../core/discrete_generator.h \
    ../core/generator.h \
    ../core/properties.h \
    ../core/scrambled_zipfian_generator.h \
    ../core/skewed_latest_generator.h \
    ../core/timer.h \
    ../core/uniform_generator.h \
    ../core/utils.h \
    ../core/zipfian_generator.h \
    ../db/basic_db.h \
    ../db/db_factory.h \
    ../db/hashtable_db.h \
    ../db/lock_stl_db.h \
    ../db/redis_db.h \
    ../db/rocksdb_db.h \
    ../db/tbb_rand_db.h \
    ../db/tbb_scan_db.h \
    ../redis/redis_client.h \
    ../redis/hiredis/async.h \
    ../redis/hiredis/dict.h \
    ../redis/hiredis/fmacros.h \
    ../redis/hiredis/hiredis.h \
    ../redis/hiredis/net.h \
    ../redis/hiredis/read.h \
    ../redis/hiredis/adapters/ae.h \
    ../redis/hiredis/adapters/glib.h \
    ../redis/hiredis/adapters/ivykis.h \
    ../redis/hiredis/adapters/libev.h \
    ../redis/hiredis/adapters/libevent.h \
    ../redis/hiredis/adapters/libuv.h \
    ../redis/hiredis/adapters/macosx.h \
    ../redis/hiredis/adapters/qt.h \
    #../db/pmemkv_db.h
    ../db/leveldb_db.h

DISTFILES += \
    ../Makefile


