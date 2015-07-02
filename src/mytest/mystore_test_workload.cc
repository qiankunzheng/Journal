#include <stdio.h>
#include <string.h>
#include <iostream>
#include <time.h>
#include <sys/mount.h>
#include "os/ObjectStore.h"
#include "os/FileStore.h"
#include "os/KeyValueStore.h"
#include "include/Context.h"
#include "common/ceph_argparse.h"
#include "global/global_init.h"
#include "common/Mutex.h"
#include "common/Cond.h"
#include "common/errno.h"
#include "common/Clock.h"
#include <boost/scoped_ptr.hpp>
#include "gtest/gtest.h"
#include "tp.h"

class StoreTest: public ::testing::Test {
public:
    boost::scoped_ptr<ObjectStore> store;
    StoreTest(): 
	store(0),
	lock("OBJ_LOCK", false, true, false, g_ceph_context),
	inflight(0), seq(0) { }
    virtual void SetUp() {
	int r = ::mkdir("/mnt/store_test", 0777);
	if (r < 0 && errno != EEXIST ) {
	    r = -errno;
	    cerr << __func__ << ": unable to create _store_test" << " : " << cpp_strerror(r) << std::endl;
	    return ;
	}
	ObjectStore *s = ObjectStore::create(g_ceph_context, "memstore", "/mnt/store_test", "/dev/ram0");
	store.reset(s);
	EXPECT_EQ(store->mkfs(), 0);
	EXPECT_EQ(store->mount(), 0);
    }
    virtual void TearDown() {
	store->umount();
    }
    class OnReadable: public Context {
	StoreTest *test;
	ghobject_t obj;
	int seq;
	ObjectStore::Transaction *t;
    public:
	OnReadable(StoreTest *st, const ghobject_t &o, int s, ObjectStore::Transaction *_t) : 
	    test(st), obj(o), seq(s), t(_t) { }

	void finish(int r) {
	    Mutex::Locker l(test->lock);
	    test->inflight--;
	    test->cond.Signal();
	    delete t;
#ifdef WITH_LTTNG
//	    ostringstream oss;
//	    oss << obj;
	    tracepoint(mytest, finish_op, seq, "");
#endif
	}
    };

    ghobject_t create_object() {
	char buf[128];
	snprintf(buf, sizeof(buf), "%u", seq);
	seq ++;
	string name(buf);
	return ghobject_t(hobject_t(name, string(), CEPH_NOSNAP, rand() & 0xFF, 0, ""));
    }

    Mutex lock;
    Cond cond;
    uint32_t inflight;
    std::map<coll_t, set<ghobject_t> > objects;
private:
    uint32_t seq;
};

void computer_cost(const ghobject_t& obj)
{
    int loops = 5;
    while(loops--) {
	ostringstream oss;
	oss << obj << loops << std::endl;
    }
}
TEST_F(StoreTest, WriteTest) {
    int ncolls = 32;
    coll_t colls[ncolls];
    ObjectStore::Sequencer *osr[ncolls];

    uint64_t total = 1024*1024*1024;
    uint64_t iosize = 128*1024;
    uint32_t max_inflight = 64;
    utime_t now, duration;
    int r = 0;
    {
	for (int i = 0; i < ncolls; i++) {
	    ostringstream oss;
	    oss << "collection_" << i ;
	    coll_t cid(oss.str());
	    colls[i] = cid;
	    osr[i] = new ObjectStore::Sequencer(oss.str());
	    ObjectStore::Transaction t;
	    t.create_collection(cid);
	    r = store->apply_transaction(t);
	    ASSERT_EQ(r, 0);
	}
    }

    {
	bufferptr bp(buffer::create_page_aligned(iosize));
	bufferlist bl;
	bl.append(bp);
	uint64_t have_written = 0;
	uint32_t index = 0;
	now = ceph_clock_now(NULL);
	while(have_written < total) {
	    ObjectStore::Transaction *t = new ObjectStore::Transaction();
	    ghobject_t oid = create_object();
	    {
		Mutex::Locker l(lock);
		while(inflight > max_inflight) 
		    cond.Wait(lock);
		coll_t cid = colls[index % ncolls];
		ObjectStore::Sequencer *o = osr[index % ncolls];
		t->write(cid, oid, 0, bl.length(), bl);
		OnReadable *completion = new OnReadable(this, oid, index, t);
#ifdef WITH_LTTNG
		//ostringstream oss;
		//oss << oid ;
		tracepoint(mytest, launch_op, index, "");
#endif
		computer_cost(oid); // simulate the cost of computing of object
		r = store->queue_transaction(o, t,  completion);
		objects[cid].insert(oid);
		inflight ++;
	    }
	    index += 1;
	    have_written += iosize;
	}
	{
	    Mutex::Locker l(lock);
	    while(inflight)
		cond.Wait(lock);
	}
	double duration = ceph_clock_now(NULL) - now;
	double thro = total / duration;
	
	std::cout << " objects = " << index << " duration = " << duration << " throuput = " << thro/(1024*1024) << "MB/s" << std::endl;
    }
 
    {
	map<coll_t, set<ghobject_t> >::iterator it = objects.begin();
	while (it != objects.end()) {
	    for(set<ghobject_t>::iterator p = it->second.begin();
		    p != it->second.end();
		    ++ p) {
		ObjectStore::Transaction t;
		t.remove(it->first, *p);
		r = store->apply_transaction(t);
		ASSERT_EQ(r, 0);
	    }
	    ++ it;
	}
    }

    {
	for (int i=0; i < ncolls; i++) {
	    ObjectStore::Transaction t;
	    t.remove_collection(colls[i]);
	    free(osr[i]);
	    r = store->apply_transaction(t);
	    ASSERT_EQ(r, 0);
	}
    }
}
int main(int argc, char **argv) {
    vector<const char*> args;
    argv_to_vec(argc, (const char **)argv, args);
    
    global_init(NULL, args, CEPH_ENTITY_TYPE_CLIENT, CODE_ENVIRONMENT_UTILITY, 0);
    common_init_finish(g_ceph_context);
    g_ceph_context->_conf->set_val("osd_journal_size", "400");
    g_ceph_context->_conf->set_val("filestore_index_retry_probability", "0.5");
    g_ceph_context->_conf->set_val("filestore_op_thread_timeout", "1000");
    g_ceph_context->_conf->set_val("filestore_op_thread_suicide_timeout", "10000");
    g_ceph_context->_conf->apply_changes(NULL);
    ::testing::InitGoogleTest(&argc, argv);
#ifdef WITH_LTTNG
    return RUN_ALL_TESTS();
#else
    return
#endif

}
