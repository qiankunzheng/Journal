#ifndef CEPH_NVMJOURNAL_H
#define CEPH_NVMJOURNAL_H

#include "include/assert.h"
#include "include/unordered_map.h"
#include "common/Finisher.h"
#include "common/Mutex.h"
#include "common/RWLock.h"
#include "ObjectStore.h"

#include <deque>
#include <map>
#include <list>
#include <libaio.h>

using std::deque;
using std::map;
using std::list;

class BackStore {
public:
	BackStore() { };
	virtual ~BackStore() { };
public:
	virtual void create_async_snapshot() { };
	virtual void flush_snapshot() { };
	virtual int _setattrs(coll_t c, const ghobject_t &oid, map<string, bufferptr> &aset) { return -1; }
	virtual int _rmattr(coll_t c, const ghobject_t &o, const char *name) { return -1; }
	virtual int _rmattrs(coll_t c, const ghobject_t &o) { return -1; }
	virtual int _collection_hint_expected_num_objs(coll_t cid, uint32_t pg_num, uint64_t num_objs) const
		{ return 0; }
	virtual int _collection_setattr(coll_t c, const char *name, const void *value, size_t size) { return -1; }
	virtual int _collection_rmattr(coll_t c, const char *name) { return -1; }

	virtual int _omap_clear(coll_t c, const ghobject_t &o) { return -1; };
	virtual int _omap_setkeys(coll_t c, const ghobject_t& o, const map<string, bufferlist> &aset) { return -1; }
	virtual int _omap_rmkeys(coll_t c, const ghobject_t& o, const set<string> &keys) { return -1;}
	virtual int _omap_rmkeyrange(coll_t c, const ghobject_t& o, const string& first, const string& last) { return -1;}
	virtual int _omap_setheader(coll_t c, const ghobject_t& o, const bufferlist &bl) { return -1; }

	virtual bool collection_exists(coll_t c) { return false; }
	virtual bool exists(coll_t c, const ghobject_t& o) { return false; }
	virtual int _open(coll_t c, const ghobject_t& o) { return -1; }
	virtual int _touch(coll_t c, const ghobject_t& o) { return -1; }
	virtual int _zero(coll_t c, const ghobject_t& o, uint64_t off, size_t len) { return -1; }
	virtual int _truncate(coll_t c, const ghobject_t& o, uint64_t size) { return -1; }
	virtual int _remove(coll_t c, const ghobject_t &o) { return -1; }
	virtual int _clone(coll_t c, const ghobject_t& oo, const ghobject_t& no) { return -1; }
	virtual int _clone_range(coll_t c, const ghobject_t& oo, const ghobject_t& no, 
		uint64_t srcoff, uint64_t len, uint64_t dstoff) { return -1; }

	virtual int _create_collection(coll_t c) { return -1; }
	virtual int _destroy_collection(coll_t c) { return -1; }
	virtual int _collection_add(coll_t dst, coll_t src, const ghobject_t &o) { return -1; }
	virtual int _collection_move_rename(coll_t oc, const ghobject_t& oo, coll_t nc, const ghobject_t& no) { return -1;}
	virtual int _collection_rename(const coll_t &c, const coll_t &nc) { return -1;}
	virtual int _split_collection(coll_t c, uint32_t bits, uint32_t rem, coll_t dest) { return -1; }

        virtual int _read(coll_t c, const ghobject_t& o, uint64_t offset, size_t len, bufferlist& bl) { return -1; }
        virtual int _write(coll_t c, const ghobject_t& o, uint64_t offset, size_t len, 
		const bufferlist& bl, bool replica = false) { return -1; }
} ;

class RWJournal {
    protected:
	BackStore *store;
	Finisher *finisher;
    public:
	RWJournal(BackStore *s, Finisher *fin) : 
		store(s),
		finisher(fin),
		evict_threshold(0.7) {	}
	virtual ~RWJournal() { }

	virtual int create() = 0;
	virtual void stop() = 0;
	virtual int mkjournal() = 0;

	typedef ObjectStore::Transaction Transaction;
	typedef ObjectStore::Sequencer Sequencer;
	typedef ObjectStore::Sequencer_impl Sequencer_impl;

	virtual void submit_entry(Sequencer *posr, list<Transaction*> &tls, ThreadPool::TPHandle *handle) = 0;

	virtual int read_object(coll_t cid, const ghobject_t &oid, uint64_t off, size_t len, bufferlist &bl) = 0;

	void set_evict_threshold(double ev) {
		evict_threshold = ev;
	}

    protected:
	double evict_threshold;
	// double cur_fraction;

};

class NVMJournal : public RWJournal {
	uint32_t create_crc32(uint32_t *data, int len) {
		uint32_t crc = 0;
		while(len--) 
			crc ^= data[len];
		return crc;
	}

	int check_crc32(uint32_t *data, int len) {
		uint32_t res = 0;
		while(len--)
			res ^= data[len];
		return res == 0 ? 0 : -1;
	}

	struct header_t {
		uint64_t start_pos;
		uint64_t meta_sync_pos;
		uint64_t data_sync_pos;
		uint32_t crc;
	};

	friend void encode(const header_t&, bufferlist&, uint64_t);
	friend void decode(header_t&, bufferlist::iterator&);
	void build_header(bufferlist& bl);

	static const uint32_t magic = 0x12345678;
	static const uint32_t _max_transactions_per_entry = 16;
	struct entry_header_t {
		uint32_t magic;
		uint32_t wrap;
		uint64_t seq;	// sequence of the first transaction
		uint32_t ops;	// number of the transactions in the journal entry
		uint32_t pre_pad;
		uint32_t post_pad;
		uint32_t length;
		uint32_t data_len;
		uint32_t crc;
	};

	header_t header;
	uint64_t cur_seq;
	enum {
	    WRITE_THROUGH_MODE = 1,
	    WRITE_BACK_MODE,
	    DEBUG_MODE,
	    AI_MODE
	}mode;

	uint64_t write_pos;
	uint64_t meta_sync_pos;
	uint64_t data_sync_pos;
	uint64_t data_sync_pos_recorded;
	uint64_t start_pos;
	static const uint64_t sync_threshold = 256 << 20; // 256MB

	atomic_t total_wrap;
	atomic_t total_write;
	atomic_t total_flush;
	atomic_t total_evict;
	atomic_t total_cached;
	void dump_journal_stat();

	uint64_t max_length;

	string path;
	string conf;
	int fd;

	int _open(bool io_direct = true);
	/* replay journal */
	bool replay; 
	bool not_update_meta;
	void check_replay_point(uint64_t pos);
	int _journal_replay();

	class ApplyManager {
	    bool blocked;
	    uint32_t open_ops;
	    Mutex lock;
	    Cond cond;
	public:
	    ApplyManager()
	    	: blocked(false),
		open_ops(0), 
	    	lock("NVMJournal::ApplyManager::lock", false, true, false, g_ceph_context)
	    	{ }
	    void sync_start();
	    void sync_finish();
	    void op_apply_start(uint32_t ops = 1);
	    void op_apply_finish(uint32_t ops = 1);
	};
	ApplyManager apply_manager;

	Mutex sync_lock;
	void _sync(); // update meta.conf on disk
	
    public:
	int create();
	void stop();
	int mkjournal();

    private:
	// Writer : Append entry to the tail of Journal
	struct OpSequencer ;
	struct write_item {
	    uint64_t seq;
	    OpSequencer *posr;
	    list<Transaction*> tls;
	    write_item(uint64_t s, OpSequencer *o, list<Transaction *> &l)
		: seq(s), posr(o), tls(l) { }
	};

	deque<write_item> writeq;
	Mutex writeq_lock;
	Cond writeq_cond;

	Mutex op_throttle_lock;
	Cond op_throttle_cond;
	uint64_t op_queue_len;
	static const uint64_t op_queue_max = 128; 
	void op_queue_reserve_throttle(ThreadPool::TPHandle *handle);
	void op_queue_release_throttle();
    public:
	void submit_entry(Sequencer *posr, list<Transaction*> &tls, ThreadPool::TPHandle *handle);


    private:
	struct aio_info {
	    struct iocb iocb;
	    uint64_t seq;
	    bufferlist bl; // KEEP REFERENCE TO THE IOV BUFFER
	    uint32_t len;
	    struct iovec *iov;
	    bool done;

	    aio_info(bufferlist &other, uint64_t s) :
		seq(s), len(other.length()), iov(NULL), done(false) 
	    { 
		bl.claim(other);
		memset((void*)&iocb, 0, sizeof(iocb));
	    }

	    ~aio_info() {
		delete[] iov;
	    }
	};

	Mutex aioq_lock;
	Cond aioq_cond;	
	uint32_t aio_num;
	deque<aio_info> aio_queue; 

	io_context_t aio_ctx;
	bool aio_ctx_ready;

	void do_aio_write(bufferlist &bl, uint64_t seq);

	/* latency statics */
	Mutex latency_lock;
	utime_t journal_aio_latency;
	utime_t osr_queue_latency;
	utime_t do_op_latency;
	int ops;
	void update_latency(utime_t l1, utime_t l2, utime_t l3) {
	    Mutex::Locker l(latency_lock);
	    journal_aio_latency += l1;
	    osr_queue_latency += l2;
	    do_op_latency += l3;
	    ++ops;
	}

	struct Op {
	    utime_t journal_latency;
	    utime_t osr_queue_latency;
	    utime_t do_op_latency;
	    uint64_t seq;
	    uint64_t entry_pos;
	    uint32_t data_offset;
	    bool replay;
	    OpSequencer *posr;
	    list<Transaction*> tls;
	};

	char *zero_buf;
	bool wrap;
	
	uint64_t prepare_single_write(bufferlist &meta, 
		bufferlist &data, 
		Op *op);

	int prepare_multi_write(bufferlist& bl, 
		list<Op*>& ops);

	void do_wrap();

	int read_entry(uint64_t &pos, 
		uint64_t &next_seq, 
		Op *op);

	/* Journal writer */

	void writer_entry();

	class Writer : public Thread {
	    NVMJournal *Journal;
	public:
	    Writer(NVMJournal *j) : Journal(j) { }
	    void *entry() {
		Journal->writer_entry();
		return 0;
	    }
	} writer;

	bool writer_stop;
	void stop_writer() {
	    {
		Mutex::Locker l(writeq_lock);
		writer_stop = true;
		writeq_cond.Signal();	
	    }
	    writer.join();	
	}

	/* reaper */

	bool reaper_stop;
	deque<Op*> op_queue;
	Mutex op_queue_lock;

	void check_aio_completion();
	void notify_on_committed(Op *op);
	void notify_on_applied(Op *op);
	void reaper_entry();
	
	class Reaper : public Thread {
	    NVMJournal *Journal;
	public:
	    Reaper(NVMJournal *J) : Journal(J) { }
	    void *entry() {
		Journal->reaper_entry();
		return 0;
	    }
	} reaper;

	void stop_reaper() {
	    {
		Mutex::Locker locker(aioq_lock);
		reaper_stop = true;
		aioq_cond.Signal();
	    }
	    reaper.join();
	}

	/* thread pool: do op */
	/* op sequencer */
	class OpSequencer : public Sequencer_impl {
	    Mutex lock;
	    Cond cond;
	    list<Op *> tq;
	    list<uint64_t> op_q;
	    list< pair<uint64_t, Context *> > flush_waiters;
	  public:
	    OpSequencer() : Sequencer_impl(),
		lock("NVMJournal::OpSequencer::lock", false, true, false, g_ceph_context),
		apply_lock("NVMJournal::OpSequencer::apply_lock", false, true, false, g_ceph_context) {}
	    ~OpSequencer() {
		flush();
	    }
	    void register_op(uint64_t seq) {
		Mutex::Locker l(lock);
		op_q.push_back(seq);
	    }
            void unregister_op() {
                Mutex::Locker l(lock);
                op_q.pop_front();
            }
	    void wakeup_flush_waiters(list<Context *> &to_queue)
	    {
		Mutex::Locker l(lock);
		uint64_t seq = 0;
		if (op_q.empty())
		    seq = -1;
		else
		    seq = op_q.front();
	
		while (!flush_waiters.empty()) {
		    if (flush_waiters.front().first >= seq)
			break;
		    to_queue.push_back(flush_waiters.front().second);
		    flush_waiters.pop_front();
		}
		cond.Signal();
	    }
	    Op* peek_queue() {
		assert(apply_lock.is_locked());
		return tq.front();
	    }
            void queue(Op *op) {
                Mutex::Locker l(lock);  
                tq.push_back(op);
            }
	    void dequeue() {
		Mutex::Locker l(lock);
		tq.pop_front();
	    }
	    void flush() {
		Mutex::Locker l(lock);
		if (op_q.empty())
		    return;
		uint64_t seq = op_q.back();
		while (!op_q.empty() && op_q.front() <= seq)
		    cond.Wait(lock);
	    }
	    bool flush_commit(Context *c) {
	    	Mutex::Locker l(lock);
		uint64_t seq = 0;
		if (op_q.empty()) {
		    delete c;
		    return true;
		}
		seq = op_q.back();
		flush_waiters.push_back( make_pair(seq, c));
		return false;
	    }

	  public:
	    Mutex apply_lock;
	};

	OpSequencer default_osr;
	deque<OpSequencer*> os_queue;

	ThreadPool op_tp;
	bool op_tp_started;
	struct OpWQ: public ThreadPool::WorkQueue<OpSequencer> {
	    NVMJournal *Journal;
	public:
	    OpWQ(time_t timeout, time_t suicide_timeout, ThreadPool *tp, NVMJournal *J) 
		: ThreadPool::WorkQueue<OpSequencer>("NVMJournal::OpWQ", timeout, suicide_timeout, tp), Journal(J) { }
	    bool _enqueue(OpSequencer *posr) {
		Journal->os_queue.push_back(posr);
		return true;
	    }
	    void _dequeue(OpSequencer *posr) {
		assert(0);
	    }
	    bool _empty() {
		return Journal->os_queue.empty();
	    }
	    OpSequencer *_dequeue() {
		if (Journal->os_queue.empty())
		    return NULL;
		OpSequencer *osr = Journal->os_queue.front();
		Journal->os_queue.pop_front();
		return osr;
	    }
	    void _process(OpSequencer *osr, ThreadPool::TPHandle &handle) {
		Journal->_do_op(osr, &handle);
	    }
	    void _process_finish(OpSequencer *osr) {
	    }
	    void _clear() {
	    	// assert (Journal->op_queue.empty());
	    }
	}op_wq;

	void queue_op(OpSequencer *osr, Op *op) {
	    osr->queue(op);
	    op_wq.queue(osr);
	}
	void _do_op(OpSequencer *posr, ThreadPool::TPHandle *handle);

	void do_op(Op *op, ThreadPool::TPHandle *handle = NULL);
	void do_transaction(Transaction *t, uint64_t seq, uint64_t entry_pos, uint32_t &off);
	int _touch(coll_t cid, const ghobject_t &oid);
	int _write(coll_t cid, const ghobject_t &oid, uint32_t off, uint32_t len, 
		const bufferlist& bl, uint64_t entry_pos, uint32_t boff);
	int _zero(coll_t cid, const ghobject_t &oid, uint32_t off, uint32_t len);
	int _truncate(coll_t cid, const ghobject_t &oid, uint32_t off);
	int _remove(coll_t cid, const ghobject_t &oid);
	int _clone(coll_t cid, const ghobject_t &src, const ghobject_t &dst);
	int _clone_range(coll_t cid, ghobject_t &src, ghobject_t &dst,
		uint64_t off, uint64_t len, uint64_t dst_off);
	int _create_collection(coll_t cid);
	int _destroy_collection(coll_t cid);
	int _collection_add(coll_t dst, coll_t src, const ghobject_t &oid);
	int _collection_move_rename(coll_t oldcid, const ghobject_t &oldoid, coll_t newcid, const ghobject_t &newoid);
	int _collection_rename(coll_t cid, coll_t ncid);
	int _split_collection(coll_t src, uint32_t bits, uint32_t match, coll_t dst);
	int do_other_op(int op, Transaction::iterator& p);

	/* memory data structure */

	struct Object;
	typedef Object* ObjectRef;

	struct BufferHead {
	    ObjectRef owner;
	    struct extent{
		uint32_t start;
		uint32_t end;
	    } ext;
            enum {ZERO = ~(uint32_t)3, TRUNC};
	    uint32_t bentry;
	    uint32_t boff;
	    bool dirty;
	};

	void _flush_bh(ObjectRef obj, BufferHead *pbh);
	class ThreadLocalPipe {
	    pthread_key_t thread_pipe_key;
	    void *init_resource() {
		int *fds = new int[2];
		assert(fds);
		assert(::pipe(fds) == 0);
		::fcntl(fds[1], F_SETPIPE_SZ, 4*1024*1024);
		::fcntl(fds[0], F_SETFL, O_NONBLOCK);
		::fcntl(fds[1], F_SETFL, O_NONBLOCK);
		return (void *)fds;
	    }
	    static void release_resource(void *rc) {
	    	if (!rc)
    			return;
		int *fds = (int *)rc;
	    	close (fds[0]);
    		close (fds[1]);
	    }
	public:
	    ThreadLocalPipe() {
		pthread_key_create(&thread_pipe_key, release_resource);
	    }
	    void *get_resource() {
		void *rc = pthread_getspecific(thread_pipe_key);
		if (!rc) {
		    while (!rc)
			rc = init_resource();
		    pthread_setspecific (thread_pipe_key, rc);
		}
		return rc;
	    }

	} tls_pipe;

	deque<BufferHead*> Journal_queue;
	Mutex Journal_queue_lock;
	Cond Journal_queue_cond;

	/* writer will wait on this lock if not enougth space remained in journal */	
	struct EvOp {
	    uint64_t seq;
	    uint32_t synced;
	    ObjectRef obj;
	    bool done;
	    deque<BufferHead *> queue;
	    
	    EvOp(ObjectRef o, deque<BufferHead *> &q)
		: seq(0), synced(0), obj(0), done(false) {
		obj = o;
		queue.swap(q);
		}
	};

	uint64_t ev_seq;
	map< uint64_t, deque<BufferHead *> > evict_in_flight;
	deque<EvOp *> running_ev;
	deque<EvOp *> ev_queue;

	ThreadPool ev_tp;
	bool ev_tp_started;
	class EvWQ: public ThreadPool::WorkQueue<EvOp> {
	    NVMJournal *Journal;
	public:
	    EvWQ(time_t timeout, time_t suicide_timeout, ThreadPool *tp, NVMJournal *J) 
		: ThreadPool::WorkQueue<EvOp>("NVMJournal::EvWQ", timeout, suicide_timeout, tp), Journal(J) { }
	    bool _enqueue(EvOp *op) {
		Journal->ev_queue.push_back(op);
		return true;
	    }
	    void _dequeue(EvOp *op) {
		assert(0);
	    }
	    bool _empty() {
		return Journal->ev_queue.empty();
	    }
	    EvOp *_dequeue() {
		if (Journal->ev_queue.empty())
		    return NULL;
		EvOp *op = Journal->ev_queue.front();
		Journal->ev_queue.pop_front();
		return op;
	    }
	    void _process(EvOp *op, ThreadPool::TPHandle &handle) {
		Journal->do_ev(op, &handle);
	    }
	    void _process_finish(EvOp *op) {
	    }
	    void _clear() {
	    	// assert (Journal->op_queue.empty());
	    }
	}ev_wq;
	void queue_ev(EvOp *op) {
		ev_wq.queue(op);
	}
	void do_ev(EvOp *ev, ThreadPool::TPHandle *handle);

	Mutex evict_lock;
	Cond evict_cond;
	Mutex waiter_lock;
	Cond waiter_cond;

	bool should_evict();
	void wait_for_more_space(uint64_t min);
	void evict_entry();
	void check_ev_completion();

	
	class JournalEvictor : public Thread {
	    NVMJournal *Journal;
	public:
	    JournalEvictor(NVMJournal *j) : Journal(j) {	}
	    void *entry() {
		Journal->evict_entry();
		return 0;
	    }
	} evictor;

	bool ev_stop;
	bool force_evict;
	uint32_t ev_pause;
	bool ev_paused;
	void evict_trigger();
	void stop_evictor();
	Cond evict_pause_cond;
	void pause_ev_work();
	void unpause_ev_work();


	deque<BufferHead*> reclaim_queue;
	Mutex reclaim_queue_lock;
	bool should_reclaim();
	void do_reclaim();

	struct Object 
	{
	    set< pair<coll_t, ghobject_t> > alias;
	    ObjectRef parent;
	    atomic_t ref;
	    bool cachable;

	    /* statics */
	    uint32_t s_write;
	    uint32_t s_overlap;
	    uint32_t s_ops;

	    uint32_t size;
	    map<uint32_t, BufferHead*> data;
	    RWLock lock;
	    
	    Object(const coll_t &c, const ghobject_t &o) :
		parent(NULL),
		ref(0),
		cachable(false),
		s_write(0),
		s_overlap(0),
		size(0),
		lock("NVMJournal::lock") {
		    alias.insert( make_pair(c, o) );
	    }
	    
	    uint32_t get() { return ref.inc(); }
	    uint32_t put() { return ref.dec(); }
	};

	struct Collection {
		coll_t cid;
		ceph::unordered_map<ghobject_t, ObjectRef> Object_hash;
		map<ghobject_t, ObjectRef> Object_map;
		OpSequencer *osr;
		Mutex lock; 
		/* ReplicatedPG is already sequencing the reads and writes, 
		 * the lock is to protect object_map/hash from concurrently access 
		 * from backend ev/reclaim work and normal operation
		 */

		Collection(coll_t c) 
		    : cid(c), lock("NVMJournal::Collection::lock",false, true, false, g_ceph_context) { }
	};

	typedef ceph::shared_ptr<Collection> CollectionRef;
	
	struct CollectionMap {
	    ceph::unordered_map<coll_t, CollectionRef> collections;
	    Mutex lock;
	    CollectionMap() :
		    lock("NVMJournal::CacheShard::lock",false, true, false, g_ceph_context) {}
	} coll_map;


	CollectionRef get_collection(coll_t cid, bool create = false) ;

	ObjectRef get_object(coll_t cid, const ghobject_t &oid, bool create = false) ;
	ObjectRef get_object(CollectionRef coll, const ghobject_t &oid, bool create = false);

	inline void erase_object_with_lock_hold(CollectionRef coll, const ghobject_t &obj) ;

	void put_object(ObjectRef obj, bool locked = false) ;

	// we should never try to obtain a lock of an object
	// when we have got a lock of a collection
	void get_read_lock(ObjectRef obj) {
	    assert(obj);
	    obj->lock.get_read();
	}
	void put_read_lock(ObjectRef obj) {
	    assert(obj);
	    obj->lock.put_read();
	}
	void get_write_lock(ObjectRef obj) {
	    assert(obj);
	    obj->lock.get_write();
	}
	void put_write_lock(ObjectRef obj) {
	    assert(obj);
	    obj->lock.put_write();
	}

	atomic_t merge_ops;
	atomic_t merge_size;
	atomic_t merge_and_overlap_ops;
	atomic_t merge_and_overlap_size;
	void dump_merge_static();
	void merge_new_bh(ObjectRef obj, BufferHead *bh);
	void delete_bh(ObjectRef obj, uint32_t off, uint32_t end, uint32_t bentry);

	/* Read operation */
	struct ReadOp 
	{
	    coll_t cid;
	    ghobject_t oid;
	    ObjectRef obj;
	    uint32_t off;
	    uint32_t length;
	    bufferlist buf; 
	    list<ObjectRef> parents;

	    map<uint32_t, uint32_t> hits; // off->len 
	    map<uint32_t, uint64_t> trans; // off->ssd_off

	    map<uint32_t, uint32_t> missing; // 
	};
	void dump(const ReadOp &op);
	void map_read(ObjectRef obj, uint32_t off, uint32_t end,
                        map<uint32_t, uint32_t> &hits,
                        map<uint32_t, uint64_t> &trans,
                        map<uint32_t, uint32_t> &missing,
			bool trunc_as_zero = false);
	void build_read(coll_t &cid, const ghobject_t &oid, uint64_t off, size_t len, ReadOp &op);
	void build_read_from_parent(ObjectRef parent, ObjectRef obj, ReadOp& op);
	int do_read(ReadOp &op);

    public:
	int read_object(coll_t cid, const ghobject_t &oid, uint64_t off, size_t len, bufferlist &bl);

	NVMJournal(string dev, string conf, BackStore *s, Finisher *fin);

	virtual ~NVMJournal();

};
WRITE_RAW_ENCODER(NVMJournal::header_t);
#endif
