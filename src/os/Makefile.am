libos_types_la_SOURCES = \
	os/Transaction.cc
libos_types_la_CXXFLAGS = ${AM_CXXFLAGS}
noinst_LTLIBRARIES += libos_types.la

libos_la_SOURCES = \
	os/chain_xattr.cc \
	os/DBObjectMap.cc \
	os/GenericObjectMap.cc \
	os/FileJournal.cc \
	os/FileStore.cc \
	os/FlatIndex.cc \
	os/GenericFileStoreBackend.cc \
	os/HashIndex.cc \
	os/IndexManager.cc \
	os/JournalingObjectStore.cc \
	os/LevelDBStore.cc \
	os/LFNIndex.cc \
	os/NVMStore.cc \
	os/NVMJournal.cc \
	os/ObjectAttrStore.cc \
	os/ObjectDataStore.cc \
	os/MemStore.cc \
	os/KeyValueDB.cc \
	os/KeyValueStore.cc \
	os/ObjectStore.cc \
	os/WBThrottle.cc \
        os/KeyValueDB.cc \
	common/TrackedOp.cc

if LINUX
libos_la_SOURCES += os/BtrfsFileStoreBackend.cc
endif

if WITH_LIBXFS
libos_la_SOURCES += os/XfsFileStoreBackend.cc
endif

if WITH_LIBZFS
libos_la_SOURCES += os/ZFSFileStoreBackend.cc
endif

libos_la_CXXFLAGS = ${AM_CXXFLAGS}
libos_la_LIBADD = $(LIBOS_TYPES)
if WITH_LTTNG
libos_la_LIBADD += $(LIBOS_TP)
endif

noinst_LTLIBRARIES += libos.la

noinst_HEADERS += \
	os/btrfs_ioctl.h \
	os/chain_xattr.h \
	os/BtrfsFileStoreBackend.h \
	os/CollectionIndex.h \
	os/DBObjectMap.h \
	os/GenericObjectMap.h \
	os/FileJournal.h \
	os/FileStore.h \
	os/FlatIndex.h \
	os/FDCache.h \
	os/GenericFileStoreBackend.h \
	os/HashIndex.h \
	os/IndexManager.h \
	os/Journal.h \
	os/JournalingObjectStore.h \
	os/KeyValueDB.h \
	os/LevelDBStore.h \
	os/LFNIndex.h \
	os/NVMStore.h \
	os/NVMJournal.h \
	os/ObjectAttrStore.h \
	os/ObjectDataStore.h \
	os/MemStore.h \
	os/KeyValueStore.h \
	os/ObjectMap.h \
	os/ObjectStore.h \
	os/SequencerPosition.h \
	os/WBThrottle.h \
	os/XfsFileStoreBackend.h \
	os/ZFSFileStoreBackend.h

if WITH_SLIBROCKSDB
libos_rocksdb_la_SOURCES = os/RocksDBStore.cc
libos_rocksdb_la_CXXFLAGS = ${AM_CXXFLAGS} ${LIBROCKSDB_CFLAGS} -std=gnu++11 -I rocksdb/include
libos_rocksdb_la_LIBADD = rocksdb/librocksdb.la
noinst_LTLIBRARIES += libos_rocksdb.la
noinst_HEADERS += os/RocksDBStore.h
endif
if WITH_DLIBROCKSDB
libos_rocksdb_la_SOURCES = os/RocksDBStore.cc
libos_rocksdb_la_CXXFLAGS = ${AM_CXXFLAGS} ${LIBROCKSDB_CFLAGS} -std=gnu++11
libos_rocksdb_la_LIBADD = -lrocksdb
noinst_LTLIBRARIES += libos_rocksdb.la
noinst_HEADERS += os/RocksDBStore.h
endif
if WITH_LIBZFS
libos_zfs_a_SOURCES = os/ZFS.cc
libos_zfs_a_CXXFLAGS = ${AM_CXXFLAGS} ${LIBZFS_CFLAGS}
noinst_LIBRARIES += libos_zfs.a
noinst_HEADERS += os/ZFS.h
endif

if WITH_KINETIC
libos_la_SOURCES += os/KineticStore.cc
libos_la_CXXFLAGS += -std=gnu++11
libos_la_LIBADD += -lkinetic_client -lprotobuf -lglog -lgflags libcrypto.a
noinst_HEADERS += os/KineticStore.h
endif
