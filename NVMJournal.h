#ifndef CEPH_NVMJOURNAL_H
#define CEPH_NVMJOURNAL_H

#include "include/assert.h"
#include "include/unordered_map.h"
#include "common/Finisher.h"
#include "common/Mutex.h"
#include "common/RWLock.h"
#include "ObjectStore.h"

#include <deque>
#include <libaio.h>

using std::deque;

class MemStore;

class RWJournal {
    protected:
	MemStore *store;
	Finisher *finisher;
    public:
	RWJournal(MemStore *s, Finisher *fin) : 
		store(s),
		finisher(fin) {	}
	virtual ~RWJournal() { }

	virtual int create() = 0;
	virtual int mkjournal() = 0;

	typedef ObjectStore::Transaction Transaction;
	typedef ObjectStore::Sequencer Sequencer;
	typedef ObjectStore::Sequencer_impl Sequencer_impl;

	virtual void submit_entry(Sequencer *posr, list<Transaction*> &tls)= 0;

	virtual void read_object(coll_t &cid, ghobject_t &oid, uint64_t off, size_t len,
			bufferlist &bl) = 0;

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

	uint64_t write_pos;
	uint64_t meta_sync_pos;
	uint64_t data_sync_pos;
	uint64_t data_sync_pos_recorded;
	uint64_t start_pos;
	static const uint64_t sync_threshold = 256 << 20; // 256MB

	uint64_t max_length;

	string path;
	string conf;
	int fd;

	int _open(bool io_direct = true);
	int _journal_replay(); // replay

	class ApplyManager {
	    bool blocked;
	    uint32_t open_ops;
	    Mutex lock;
	    Cond cond;
	public:
	    ApplyManager()
	    	:open_ops(0), 
	    	lock("NVMJournal::ApplyManager::lock", false, true, false, g_ceph_context)
	    	{ }
	    void set_block();
	    void reset_block();
	    void op_apply_start(uint32_t ops = 1);
	    void op_apply_finish(uint32_t ops = 1);
	};
	ApplyManager apply_manager;

	Mutex sync_lock;
	void _sync(); // update meta.conf on disk

        deque< pair<uint64_t, header> > sync_in_fligths;
        void _sync_top_half() { }          // create snapshot async
        void _sync_bottom_half() { }       // waiting for completion of snapshot
	
    public:
	int create();
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

	Cond op_throttle_cond;
	Mutex op_throttle_lock;
	uint64_t op_queue_len;
	static const uint64_t op_queue_max = 64; 
	void op_queue_reserve_throttle(ThreadPool::TPHandle *handle);
	void op_queue_release_throttle();
    public:
	void submit_entry(Sequencer *posr, list<Transaction*> &tls, ThreadPool::TPHandle *handle);


    private:
	struct aio_info {
	    struct iocb iocb;
	    uint64_t seq;
	    bufferlist bl; // KEEP REFERENCE TO THE IOV BUFFER
	    int len;
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

	struct Op {
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
	struct Ev; // flushOp
	struct task {
	    enum { OP, EV } tag;
	    void *pt;

	    task(Op *op) :tag(OP), pt(op) { }
	    task(Ev *op) :tag(EV), pt(op) { }
	};

	class OpSequencer : public Sequencer_impl {
	    Mutex lock;
	    Cond cond;
	    list<task> tq;
	    list<uint64_t> op_q;
	    list< pair<uint64_t, Context *> > flush_waiters;
	  public:
	    OpSequencer() : Sequencer_impl(),
		lock("NVMJournal::OpSequencer::lock", false, true, false, g_ceph_context),
		apply_lock("NVMJournal::OpSequencer::apply_lock", false, true, false, g_ceph_context) {}
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
		    to_queue.push_back(flush_waiter.front().second);
		    flush_waiters.pop_front();
		}
		cond.Signal();
	    }
	    task &peek_queue() {
		assert(apply_lock.is_locked());
		return tq.front();
	    }
            void queue(const task &t) {
                Mutex::Locker l(lock);  
                tq.push_back(t);
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
		while (!op_q.empty() && op_q.font() <= seq)
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

	    Mutex apply_lock;
	};

	OpSequencer default_osr;
	deque<OpSequencer*> os_queue;

	ThreadPool op_tp;
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
	    void _process(OpSequencer *osr, ThreadPool::TPHandle *handle) {
		Journal->_do_op(osr, handle);
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
	void queue_ev(OpSequencer *osr, Ev *ev) {
	    osr->queue(task(ev));
	    op_wq.queue(osr);
	}
	void _do_op(OpSequencer *posr, ThreadPool::TPHandle *handle);

	void do_op(Op *op, ThreadPool::TPHandle *handle);
	void do_ev(Ev *evi, ThreadPool::TPHandle *handle);
	void do_transaction(Transaction *t, uint64_t seq, uint64_t entry_pos, uint32_t &off);
	int _touch(coll_t cid, const ghobject_t& oid);
	int _write(coll_t cid, const ghobject_t& oid, uint32_t off, uint32_t len, uint64_t entry_pos, uint32_t boff);
	int _zero(coll_t cid, const ghobject_t& oid, uint32_t off, uint32_t len);
	int _truncate(coll_t cid, const ghobject_t& oid, uint32_t off);
	int _remove(coll_t cid, const ghobect_t& oid);
	int _clone(coll_t cid, const ghobject_t& oid, const ghobject_t& noid, bool flush);

	/* memory data structure */

	struct Object;
	//typedef ceph::shared_ptr<Object> ObjectRef; 
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
	};

	deque<BufferHead*> Journal_queue;
	Mutex Journal_queue_lock;
	Cond Journal_queue_cond;

	deque<BufferHead*> reclaim_queue;
	Mutex reclaim_queue_lock;

	Mutex waiter_lock; // writer wait for more space... 
	Cond waiter_cond;

	/* writer will wait on this lock if not enougth space remained in journal */
	struct Ev {
	    uint64_t seq;
	    uint32_t synced;
	    ObjectRef obj;	
            bool done;    
	    deque<BufferHead *> queue;

	    Ev(ObjectRef o, deque<BufferHead *> q> 
		: seq(0), synced(0), obj(o), done(false), queue(q) { }
	};

	deque<Ev*> running_ev;
	uint64_t ev_seq;
	map<uint64_t, deque<BufferHead*> > evicting_bh;

	class TLS {
	    pthread_key_t thread_pipe_key;
	    static void put_value(void *value);
	public:
	    TLS();
	    void* get_value();
	}ThreadLocalPipe;

	Mutex evict_lock;
	Cond evict_cond;

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
	void stop_evictor() {
	    {
	    	Mutex::Locker locker(evict_lock);
	    	ev_stop = true;
	    	evict_cond.Signal();
	    }
	    evictor.join();
	}
	
	bool should_relcaim();
	void do_reclaim();

	struct Object 
	{
		coll_t coll;
		ghobject_t oid;
		atomic_t ref;

		enum { CLEAN, DIRTY, REMOVED } stat;

		ObjectRef parent;
		map<uint32_t, BufferHead*> data;
		RWLock lock;

		Object(coll_t c, ghobject_t o) :
		    coll(c), oid(o), 
		    ref(0), 
		    stat(DIRTY),
		    lock("NVMJournal::lock"){ }

		void get() { 
		    ref.inc(); 
		}
		uint32_t put() { 
		    return ref.dec();
		}
	};

	struct Collection {
		ceph::unordered_map<ghobject_t, ObjectRef> Object_hash;
		map<ghobject_t, ObjectRef> Object_map;
		OpSequencer *osr;
		Mutex lock;

		Collection(OpSequencer *o) 
		    : osr(o), 
		    lock("NVMJournal::Collection::lock",false, true, false, g_ceph_context) { }
	};

	typedef ceph::shared_ptr<Collection> CollectionRef;
	
	struct CacheShard {
		map<coll_t, CollectionRef> collections;
		Mutex lock;
		CacheShard() :
		    lock("NVMJournal::CacheShard::lock",false, true, false, g_ceph_context) {}
	};

	static const uint32_t NR_SLOT = 32;
	CacheShard Cache[NR_SLOT];


	CollectionRef get_collection(coll_t &cid, bool create = false) ;

	ObjectRef get_object(coll_t &cid, const ghobject_t &oid, bool create = false) ;
	ObjectRef get_object(CollectionRef coll, const ghobject_t &oid, bool create = false);

	inline void erase_object_without_lock(CollectionRef coll, ObjectRef obj) ;
	void erase_object_with_lock(CollectionRef coll, ObjectRef obj) {
		if (!coll)
			return;
		Mutex::Locker l(coll->lock);
		erase_object_without_lock(coll, obj);
	}

	void put_object(ObjectRef obj, bool locked = false) ;

	// we should never try to obtain a lock of an object
	// when we have got a lock of a collection
	void get_coll_lock(CollectionRef coll) {
		assert (coll);
		coll->lock.Lock();
	}
	void put_coll_lock(CollectionRef coll) {
		assert (coll && coll->lock.is_locked());
		coll->lock.Unlock();

	}
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
	    obj->lock.get_wrtie();
	}
	void put_write_lock(ObjectRef obj) {
	    assert(obj);
	    obj->lock.put_write();
	}

	inline void NVMJournal::get_objects_lock(ObjectRef a, ObjectRef b);
	inline void NVMJournal::put_objects_lock(ObjectRef a, ObjectRef b);

	void merge_new_bh(ObjectRef obj, BufferHead *bh);
	void delete_bh(ObjectRef obj, uint32_t off, uint32_t end, uint32_t bentry);

	void clone(coll_t &ocid, coll_t &ncid, ghobject_t &oid);

	struct ReadOp 
	{
	    ObjectRef obj;
	    uint32_t off;
	    uint32_t length;
	    bufferlist buf;
	    // 
	    list<ObjectRef> parents;

	    map<uint32_t, uint32_t> hits; // off->len 
	    map<uint32_t, uint64_t> trans; // off->ssd_off

	    map<uint32_t, uint32_t> missing; // 
	};
	void build_read(coll_t &cid, const ghobject_t &oid, uint64_t off, size_t len, ReadOp &op);
	void build_read_from_parent(ObjectRef parent, ReadOp& op);
	void do_read(ReadOp &op);

    public:
	void read_object(coll_t &cid, ghobject_t &oid, uint64_t off, size_t& len, bufferlist &buf);

	NVMJournal(string dev, string conf, MemStore *s, Finisher *fin);

	virtual ~NVMJournal();

};
WRITE_RAW_ENCODER(NVMJournal::header_t);
#endif
