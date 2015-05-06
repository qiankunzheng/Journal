#include <iostream>
#include <map>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <errno.h>
#include <pthread.h>

#include "include/types.h"
#include "include/unordered_map.h"
#include "include/buffer.h"
#include "include/compat.h"
#include "common/safe_io.h"
#include "common/errno.h"
#include "common/blkdev.h"
#include "NVMJournal.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << "NVMJournal "

int NVMJournal::_open(bool io_direct)
{
    int flags = O_RDWR | O_DSYNC;;
    int ret;

    if (io_direct)
        flags |=  O_DIRECT;

    if (fd >= 0) {
	if (TEMP_FAILURE_RETRY(::close(fd))) {
	    derr << "NVMJournal::Open: error closing old fd: "
		<< cpp_strerror(errno) << dendl;
	}
    }
    fd = TEMP_FAILURE_RETRY(::open(path.c_str(), flags, 0644));
    if (fd < 0) {
	derr << "NVMJournal::Open: unable to open journal " 
	    << path << " : " << cpp_strerror(errno) << dendl;
	return -errno;
    }

    struct stat st;
    ret = ::fstat(fd, &st);
    if (ret) {
	derr << "NVMJournal::Open: unable to fstat journal:" << cpp_strerror(errno) << dendl;
	goto out_fd;
    }
    // 
    if (!S_ISBLK(st.st_mode)) {
	derr << "NVMJournal::Open: journal not be a block device" << dendl;
	goto out_fd;
    }
    int64_t bdev_size;
    ret = get_block_device_size (fd, &bdev_size);
    if (ret) {
	derr << "NVMJournal::Open: failed to read block device size." << dendl;
	goto out_fd;
    }

    if (bdev_size < (1024*1024*1024)) {
	derr << "NVMJournal::Open: Your Journal device must be at least ONE GB" << dendl;
	goto out_fd;
    }
    max_length = bdev_size - bdev_size % CEPH_PAGE_SIZE;

    if (io_direct) {
        if (aio_ctx_ready) {
            io_destroy(aio_ctx);
        }
        aio_ctx_ready = false;        

        aio_ctx = 0;
        ret = io_setup(128, &aio_ctx);
        if (ret < 0) {
            derr << "NVMJournal::Open: unable to setup io_context " << cpp_strerror(errno) << dendl;
            goto out_fd;
        }
        aio_ctx_ready = true;
    }
   
    return 0;

out_fd:
    VOID_TEMP_FAILURE_RETRY(::close(fd));
    return ret;
}
int NVMJournal::mkjournal()
{
    _open();
    string fn = conf + "journal_header.dec";
    bufferlist bl;
    header_t hdr;
    memset(&hdr, 0, sizeof(hdr));
    ::encode(hdr, bl);
    bl.write_file(fn.c_str());
    return true;
}
// FIXME: need to rollback to snapshot of backend storage
int NVMJournal::_journal_replay()
{
    int ret;
    uint64_t seq, pos = data_sync_pos;
    Op op;
    replay = true;
    do {
	ret = read_entry(pos, seq, &op);
	if (!ret && pos) {
	    do_op(&op);
	    write_pos = pos;
	    cur_seq = seq;
	}
    } while(!ret);
    replay = false;
    return 0;
}
int NVMJournal::create()
{
    bufferlist bl;
    string fn = conf + "journal_header.dec";
    string err;
    bool direct = false;

    int r = bl.read_file(fn.c_str(), &err);
    if (r < 0) 
        return r;

    try {
        bufferlist::iterator p = bl.begin();
        ::decode(header, p);
    } 
    catch (buffer::error& e) {
        derr << "read journal header error!!" << dendl;
        memset(&header, 0, sizeof(header));
    }
    
    r = check_crc32((uint32_t*)&header, sizeof(header)/sizeof(uint32_t));
    if (r) {
        derr << "invalid header of Journal" << dendl;
        return r;
    }

    // open the Journal without O_DIRECT flag
    r =  _open(direct);
    if (r)
       return r;
   
   start_pos = header.start_pos;
   meta_sync_pos = header.meta_sync_pos;
   data_sync_pos = header.data_sync_pos;
   write_pos = 0;
   cur_seq = 0;

   r = _journal_replay();
   if (r)
       return r;

   // reopen the Journal with flag |= O_DIRECT
   direct = true;
   r = _open(true);
   if (r)
        return r; 

   if (zero_buf)
        delete[] zero_buf;
   zero_buf = new char[CEPH_PAGE_SIZE];
   memset(zero_buf, 0, CEPH_PAGE_SIZE);

   // create working thread...
   op_tp.start();
   writer.create();
   reaper.create();
   evictor.create();
   return 0;
}
void NVMJournal::ApplyManager::sync_start()
{
        Mutex::Locker locker(lock);
        blocked = true;
        while (open_ops > 0)
            cond.Wait(lock);
}
void NVMJournal::ApplyManager::sync_finish()
{
        blocked = false;
        cond.Signal();
}
void NVMJournal::ApplyManager::op_apply_start(uint32_t ops)
{
        Mutex::Locker locker(lock);
        while (blocked)
            cond.Wait(lock);
        open_ops += ops;
}
void NVMJournal::ApplyManager::op_apply_finish(uint32_t ops)
{
        Mutex::Locker locker(lock);
        open_ops -= ops;
        cond.Signal();
}
void NVMJournal::build_header(bufferlist& bl)
{
        header_t hdr;
        memset(&hdr, 0, sizeof(hdr));
        hdr.start_pos = start_pos;
        hdr.meta_sync_pos = meta_sync_pos;
        hdr.data_sync_pos = data_sync_pos;
        hdr.crc = create_crc32((uint32_t*)&hdr, sizeof(hdr)/sizeof(uint32_t));
        ::encode(hdr, bl);
}
// FIXME
void NVMJournal::_sync()
{
    assert(sync_lock.is_locked());

    op_tp.pause();
    apply_manager.sync_start();
    data_sync_pos_recorded = data_sync_pos;

    bufferlist hdr;
    build_header(hdr); 

    // create async snapshot  using btrfs's interface 
    // store->sync();
    
    string fn = conf + "journal_header.dec";
    int r = hdr.write_file(fn.c_str());
    if (r < 0) {
        assert(0 == "error update journal header");
    }
    apply_manager.sync_finish();
    op_tp.unpause();
}
NVMJournal::NVMJournal(string dev, string c, BackStore *s, Finisher *fin)
    :RWJournal (s, fin), 
    write_pos(0),
    meta_sync_pos(0),
    data_sync_pos(0),
    data_sync_pos_recorded(0),
    start_pos(0),
    max_length(0),
    path (dev), conf (c), fd(-1),
    replay(false),
    sync_lock ("NVMJournal::sync_lock", false, true, false, g_ceph_context),
    writeq_lock ("NVMJournal::writeq_lock", false, true, false, g_ceph_context),
    op_throttle_lock ("NVMJournal::op_throttle_lock", false, true, false, g_ceph_context),
    op_queue_len(0),
    aioq_lock ("NVMJournal::aioq_lock",false, true, false, g_ceph_context), 
    aio_num(0),
    aio_ctx(0), 
    aio_ctx_ready(false),
    zero_buf (NULL), 
    wrap (false),
    writer(this),
    writer_stop (false), 
    reaper_stop (false),
    op_queue_lock ("NVMJournal::op_queue_lock", false, true, false, g_ceph_context),
    reaper(this),
    op_tp (g_ceph_context, "NVMJournal::op_tp", g_conf->filestore_op_threads, "filestore_op_threads"),
    op_wq (0, 0, &op_tp, this),
    Journal_queue_lock ("NVMJournal::Journal_queue_lock", false, true, false, g_ceph_context),
    ev_seq(0),
    ev_tp (g_ceph_context, "NVMJournal::ev_tp", g_conf->filestore_op_threads, "filestore_op_threads"),
    ev_wq (0, 0, &ev_tp, this),
    evict_lock ("NVMJournal::evict_lock", false, true, false, g_ceph_context),
    waiter_lock ("NVMJournal::Waiter_lock", false, true, false, g_ceph_context),
    evictor(this),
    ev_stop(false),
    ev_pause(0),
    ev_paused(false),
    reclaim_queue_lock ("NVMJournal::reclaim_queue_lock", false, true, false, g_ceph_context)
{
}
NVMJournal::~NVMJournal() 
{
    stop_writer();
    stop_evictor();
    stop_reaper();
    op_tp.stop();
    
    if (zero_buf) {
        delete[] zero_buf;
        zero_buf = 0;
    }
}
void NVMJournal::op_queue_reserve_throttle(ThreadPool::TPHandle *handle)
{
    Mutex::Locker l(op_throttle_lock);
    while (op_queue_len + 1 > op_queue_max) {
	if (handle)
	    handle->suspend_tp_timeout();
	op_throttle_cond.Wait(op_throttle_lock);
	if (handle)
	    handle->reset_tp_timeout();
    }
    ++ op_queue_len;
    
}
void NVMJournal::op_queue_release_throttle()
{
    Mutex::Locker l(op_throttle_lock);
    op_queue_len -= 1;
    op_throttle_cond.Signal();
}
void NVMJournal::submit_entry(Sequencer *posr, list<Transaction*> &tls, ThreadPool::TPHandle *handle)
{
    op_queue_reserve_throttle(handle);
    {
	Mutex::Locker locker(writeq_lock);
	OpSequencer *osr;
	if (!posr) {
	    osr = &default_osr;
	} else if (posr->p) {
	    osr = static_cast<OpSequencer *> (posr->p);
	} else {
	    osr = new OpSequencer;
	    posr->p = osr;
	}

	osr->register_op(cur_seq);
	writeq.push_back(write_item(cur_seq++, osr, tls));
    }
    writeq_cond.Signal();
}

uint64_t NVMJournal::prepare_single_write(bufferlist &meta, bufferlist &data, Op *op)
{
    write_item *pitem;       
    uint64_t seq = 0; 
    bufferlist tmeta, tdata;
    uint32_t meta_len, data_len, pre_pad = 0, size;

    {
    	Mutex::Locker locker(writeq_lock); // check tailer and header of journal entry
    	if (writeq.empty())
    	    return seq;
    	pitem = &writeq.front();
    }

    for (list<Transaction*>::iterator t = pitem->tls.begin();
	    t != pitem->tls.end();
	    t ++) {
	(*t)->_encode(tmeta, tdata);
    }
    
    meta_len = meta.length() + tmeta.length();
    data_len = data.length() + tdata.length();
    
    if (data_len) {
    	pre_pad = (-(uint32_t)sizeof(entry_header_t) - (uint32_t)meta_len) & ~CEPH_PAGE_MASK;
    }

    size = sizeof(entry_header_t)*2 + meta_len + data_len + pre_pad;
    size = ROUND_UP_TO(size, CEPH_PAGE_SIZE);

    if (write_pos + size + CEPH_PAGE_SIZE > max_length) {
    	wrap = true;
    }
    // wrap of Journal
    size += CEPH_PAGE_SIZE;

    wait_for_more_space(size);

    if (wrap) 
    	return 0;

    assert(op);
    op->seq = pitem->seq;
    op->entry_pos = 0;
    op->data_offset = data.length();
    op->posr = pitem->posr;
    op->tls.swap(pitem->tls);

    ::encode(tmeta, meta);
    data.append(tdata);
    
    {
    	Mutex::Locker locker(writeq_lock);
        writeq.pop_front();
    }
    return op->seq;
}

int NVMJournal::prepare_multi_write(bufferlist& bl, list<Op*>& ops)
{
    bufferlist part1, part2;
    uint64_t first = 0;
    uint32_t count = 0;
    const int max_transactions_per_entry = 16;

    ops.clear();

    {
	int trans = max_transactions_per_entry - 1; 
        uint64_t seq;
	do {
	    Op *op = new Op();
	    seq = prepare_single_write(part1, part2, op);
	    if(seq) {
		ops.push_back(op);
		count ++;
	    } else {
	    	delete op;
	    }
	    if(!first) 
                first = seq;
	} while(seq && trans);
    }

    // no op && no need to wrap journal
    if(!count && !wrap) 
	return -1;

    if(!count) // wrap the journal
    	return 0;

    entry_header_t header;
    memset(&header, 0, sizeof(header));
    header.magic = magic;
    header.seq = first;
    header.ops = count;

    uint32_t pre_pad, post_pad;
    pre_pad = post_pad = 0;
    if(part2.length()) {
	pre_pad = (-(uint32_t)sizeof(header) - (uint32_t)part1.length()) & ~CEPH_PAGE_MASK;
    }
    uint32_t size = sizeof(header)*2 + pre_pad + part1.length() + part2.length();
    post_pad = ROUND_UP_TO(size, CEPH_PAGE_SIZE) - size;

    header.pre_pad = pre_pad;
    header.post_pad = post_pad;
    header.length = part1.length() + part2.length();
    header.data_len = part2.length();
    header.crc = create_crc32((uint32_t*)&header, sizeof(header)/sizeof(uint32_t));

    //
    bl.append((const char*)&header, sizeof(header));
    if (pre_pad) {
	bufferptr bp = buffer::create_static(pre_pad, zero_buf);
	bl.push_back(bp);
    }
    bl.append(part1);
    bl.append(part2);
    if (post_pad) {
	bufferptr bp = buffer::create_static(post_pad, zero_buf);
	bl.push_back(bp);
    }
    bl.append((const char*)&header, sizeof(header));

    // update 
    for (list<Op*>::iterator p = ops.begin();
	    p != ops.end();
	    p ++) {
    	Op *op = *p;
	op->entry_pos = write_pos;
	op->data_offset += sizeof(header) + header.pre_pad + part1.length();
	op->replay = false;
    }

    return 0;
}
/* read_entry
 * this function read the journal with fd 
 * which opened without flag DIRECT, SO there
 * is no need PAGE-ALIGNED buffer... 
 */
int NVMJournal::read_entry(uint64_t &pos, uint64_t &next_seq, Op *op)
{
    entry_header_t h;
    bufferlist bl;
    uint64_t entry_pos = pos;
    uint32_t off;

    ssize_t cnt = safe_pread(fd, &h, sizeof(h), pos);
    if (cnt != sizeof(h) 
	    || check_crc32 ((uint32_t*)&h, sizeof(h)/sizeof(uint32_t)) 
	    || (!h.wrap && h.seq < next_seq))
	return -1;

    pos += sizeof(h) + h.pre_pad;
    if (h.wrap) {
    	assert(h.seq == 0);
    	entry_header_t h2;
    	ssize_t r = safe_pread(fd, &h2, sizeof(h2), pos);
        assert(r == sizeof(h2));
    	if (memcmp(&h,&h2,sizeof(h)))
    		return -1;
    	pos = 0;
    	return 0;
    }
    uint32_t size = h.length - h.data_len;
    off = sizeof(h) + h.pre_pad + size; // offset of data part
    bufferptr bp = buffer::create(size);
 
    cnt = safe_pread(fd, bp.c_str(), size, pos);
    assert (cnt == size);
    bl.push_back(bp);
   
    entry_header_t h2;
    pos += h.length + h.post_pad;
    cnt = safe_pread(fd, &h2, sizeof(h2), pos);
    assert (cnt == sizeof(h2));
    if (memcmp(&h, &h2, sizeof(h)))
	return -1;
    pos += sizeof(h);

    bufferlist::iterator p = bl.begin();

    assert(op);
    op->seq = h.seq;
    // use default sequencer
    op->posr = &default_osr;

    op->entry_pos = entry_pos;
    op->data_offset = off;
    op->replay = true; // we should delete transaction by ourselves

    // next_seq >= h.seq+1
    next_seq = h.seq + 1;

    for (uint32_t n = 0; n < h.ops; n++) {
        bufferlist ts;
        ::decode(ts, p);
        bufferlist::iterator t = ts.begin();
        Transaction *trans = NULL;
        bool have_more = true;
        do {
	    trans = new ObjectStore::Transaction();
	    try {
	    	trans->_decode (t);
	    }
	    catch (buffer::error& e) {
	    	delete trans;
	    	trans = NULL;
	    	have_more = false;
	    }
	    if (trans)
	    	op->tls.push_back(trans);
        }while(have_more);
    }
    return 0;
}

void NVMJournal::do_aio_write(bufferlist &entry, uint64_t seq)
{
    if (entry.length() == 0)
	return;

    Mutex::Locker locker(aioq_lock);
    off64_t pos = write_pos;

    entry.rebuild_page_aligned();
    assert((entry.length()& ~CEPH_PAGE_MASK) == 0);
    assert((pos & ~CEPH_PAGE_MASK) == 0);

    while (entry.length() > 0) {
	int max = MIN(entry.buffers().size(), IOV_MAX - 1);
	iovec *iov = new iovec[max];
	int n = 0;
	unsigned len = 0;
	for (std::list<buffer::ptr>::const_iterator p = entry.buffers().begin();
		n < max;
		++p, ++n) {
	    iov[n].iov_base = (void*)p->c_str();
	    iov[n].iov_len = p->length();
	    len += iov[n].iov_len;
	}

	bufferlist tbl;
	tbl.splice(0, len, &tbl);
	aio_queue.push_back(aio_info(tbl, tbl.length() ? 0 : seq));
	aio_info& aio = aio_queue.back();
	aio.iov = iov;

	io_prep_pwritev(&aio.iocb, fd, aio.iov, n, pos);
	
	aio_num ++;
	iocb *piocb = &aio.iocb;
	int attempts = 10;
	do {
	    int r = io_submit(aio_ctx, 1, &piocb);
	    if (r < 0) {
		if(r == -EAGAIN && attempts--) {
		    usleep(500);
		    continue;
		}
		assert( 0 == "io_submit got unexpected error");
	    }
	}while(false);
	pos += aio.len;
    }
    write_pos = pos;
    aioq_cond.Signal();
}

void NVMJournal::do_wrap()
{
     entry_header_t h;
     bufferlist bl;

     memset(&h, 0, sizeof(h));
     h.magic = magic;
     h.wrap = 1;
     uint64_t size = sizeof(h) * 2;
     h.pre_pad = ROUND_UP_TO(size, CEPH_PAGE_SIZE) - size;
     h.crc = create_crc32((uint32_t*)&h, sizeof(h)/sizeof(uint32_t));

     bl.append((const char*)&h, sizeof(h));
     if (h.pre_pad) {
	bufferptr bp = buffer::create_static(h.pre_pad, zero_buf);
	bl.push_back(bp);
     }
     bl.append((const char*)&h, sizeof(h));
     do_aio_write(bl, 0);
     write_pos = 0;
}

void NVMJournal::writer_entry()
{
   while(true) 
   {
       {
	   Mutex::Locker locker(writeq_lock);
	   if (writer_stop)
		return;
	   if (writeq.empty()) {
	       writeq_cond.Wait(writeq_lock);
	       continue;
	   }
       }

       bufferlist Jentry;
       list<Op*> ops;
       int r = prepare_multi_write(Jentry, ops);
       if (r != 0)
	   continue;

       uint64_t seq = 0;
       if (Jentry.length()) {
	   Mutex::Locker locker(op_queue_lock);
	   seq = ops.back()->seq;
	   //op_queue.splice(op_queue.end(), ops);
           op_queue.insert(op_queue.end(), ops.begin(), ops.end());
       }

       //
       do_aio_write(Jentry, seq);
       if (wrap) {
       	   do_wrap();
       	   wrap = false;
       }
   }
}

/* reaper */
void NVMJournal::reaper_entry() 
{
    while (true) 
    {
	{
	    Mutex::Locker locker(aioq_lock);
	    if (aio_queue.empty()) {
		if (reaper_stop) 
		    break;
		aioq_cond.Wait(aioq_lock);
                continue;
	    }
	}
	io_event event[32];
	int r = io_getevents(aio_ctx, 1, 16, event, NULL);
	if (r < 0) {
	    if (r == -EINTR) 
		continue;
	    assert(0 == "got unexpected error from io_getevents");
	}
	
	for (int i=0; i<r; i++) {
	    aio_info *aio = (aio_info*)event[i].obj;
	    if (event[i].res != aio->len) {
		assert(0 == "unexpected aio error");
	    }
	    aio->done = true;
	}

        check_aio_completion();
    }
}

void NVMJournal::check_aio_completion()
{
    uint64_t new_completed_seq = 0;
    {
	Mutex::Locker locker(aioq_lock);
	deque<aio_info>::iterator it = aio_queue.begin();
	while (it != aio_queue.end() && it->done) {
	    if (it->seq) {
		new_completed_seq = it->seq;
	    }
	    aio_num --;
	    aio_queue.erase(it++);
	}
    }

    if (new_completed_seq) {
	deque<Op*> completed;
	{
	    Mutex::Locker locker(op_queue_lock);
	    while(op_queue.front()->seq <= new_completed_seq) {
		completed.push_back(op_queue.front());
		op_queue.pop_front();
	    }
	}
        
        uint32_t ops = completed.size();
        // reserve ops
        apply_manager.op_apply_start(ops);

	while(!completed.empty()) {
            Op *op = completed.front();
	    notify_on_committed(op);
	    queue_op(op->posr, op);
	    op_wq.queue(op->posr);
	    completed.pop_front();
	}
    }
}
void NVMJournal::notify_on_committed(Op *op) 
{
    assert(op);
    Context *ondisk = NULL;
    Transaction::collect_contexts(
	    op->tls, NULL, &ondisk, NULL);
    if (ondisk)
	finisher->queue(ondisk);
}
void NVMJournal::notify_on_applied(Op *op)
{
    assert(op);
    Context *onreadable, *onreadable_sync;
    ObjectStore::Transaction::collect_contexts(
	    op->tls, &onreadable, NULL, &onreadable_sync);
    if (onreadable_sync)
	onreadable_sync->complete(0);
    if (onreadable)
	finisher->queue(onreadable);
}
/* object */
NVMJournal::CollectionRef NVMJournal::get_collection(coll_t cid, bool create) 
{
    Mutex::Locker locker(coll_map.lock);
    ceph::unordered_map<coll_t, CollectionRef>::iterator itr = coll_map.collections.find(cid);
    if(itr == coll_map.collections.end()) {
	if (!create)
	    return CollectionRef();
	if (!replay && store->collection_exists(cid))
	    return CollectionRef();
	coll_map.collections[cid].reset(new Collection(cid));
    }
    return coll_map.collections[cid];
}
NVMJournal::ObjectRef NVMJournal::get_object(CollectionRef coll, const ghobject_t &oid, bool create) 
{
        ObjectRef obj = NULL;
        assert(coll);
        Mutex::Locker l(coll->lock);
        ceph::unordered_map<ghobject_t, ObjectRef>::iterator p = coll->Object_hash.find(oid);
        if (p == coll->Object_hash.end()) {
	    if (!create)
		return NULL;
            obj = new Object(coll->cid, oid);
            coll->Object_hash[oid] = obj;
            coll->Object_map[oid] = obj;
	    if (!replay)
		store->_touch(coll->cid, oid);
        }
              
        obj = coll->Object_hash[oid];
        obj->get();
        return obj;
}

NVMJournal::ObjectRef NVMJournal::get_object(coll_t cid, const ghobject_t &oid, bool create) 
{
    CollectionRef coll;
    coll = get_collection(cid, create);
    if(!coll)
	return NULL;
    return get_object(coll, oid, create);
}

void NVMJournal::erase_object_with_lock_hold (CollectionRef coll, const ghobject_t &oid)
{
        assert (coll);
        coll->Object_hash.erase(oid);
        coll->Object_map.erase(oid);
}

void NVMJournal::put_object(ObjectRef obj, bool locked) 
{
    if (0 == obj)
	return;

    if (obj->alias.empty()) {
	if ( 0 == obj->put())
	    delete obj;
    }
    else {
	assert (obj->lock.is_locked() == false);
	map< CollectionRef, shared_ptr<Mutex::Locker> > lockers;
	{
	    RWLock::RLocker l (obj->lock);
	    set< pair<coll_t, ghobject_t> >::iterator p = obj->alias.begin();
	    while (p != obj->alias.end()) {
		CollectionRef c = get_collection(p->first);
		if (lockers.count(c) == 0)
		    lockers[c] = shared_ptr<Mutex::Locker>(new Mutex::Locker(c->lock));
		++ p;
	    }
	}
	if (0 == obj->put()) {
	    put_object(obj->parent);
	    for (set< pair<coll_t, ghobject_t> >::iterator p = obj->alias.begin(); 
                        p != obj->alias.end(); 
                        ++p ) {
		CollectionRef coll = get_collection(p->first);
		erase_object_with_lock_hold (coll, p->second);
	    }
	   obj->alias.clear();
	}

    }
}

/* op_wq */
void NVMJournal::_do_op(OpSequencer *osr, ThreadPool::TPHandle *handle) 
{
    Mutex::Locker l(osr->apply_lock);
    Op *op = osr->peek_queue();
    do_op(op, handle);
    notify_on_applied(op);
    delete op;

    handle->reset_tp_timeout();
    
    list<Context *> to_queue;
    osr->unregister_op();
    osr->wakeup_flush_waiters(to_queue);
    finisher->queue(to_queue);
    op_queue_release_throttle();

    osr->dequeue();
    // op_queue_release_throttle();

};
void NVMJournal::do_op(Op *op, ThreadPool::TPHandle *handle)
{
    list<Transaction*>::iterator p = op->tls.begin();
    uint64_t seq = op->seq;
    uint64_t entry_pos = op->entry_pos;
    uint32_t offset = op->data_offset;
    while (p != op->tls.end()) {
	Transaction *t = *p++;
	do_transaction(t, seq, entry_pos, offset);
        if(handle)
            handle->reset_tp_timeout();
	if (op->replay)
	    delete t;
    }
    apply_manager.op_apply_finish();
}
// check if need to update the backend store
bool NVMJournal::need_to_update_store(uint64_t entry_pos)
{
    if (start_pos < entry_pos && entry_pos < meta_sync_pos)
    	return false;
    if (!(meta_sync_pos < entry_pos && entry_pos < start_pos))
    	return false;
    replay = false;
    return true;
}
// FIXME
void NVMJournal::do_transaction(Transaction* t, uint64_t seq, uint64_t entry_pos, uint32_t &offset) 
{ 
    bool update_store = need_to_update_store(entry_pos);
    Transaction::iterator i = t->begin();
    while (i.have_op()) {
	int op = i.decode_op();
	int r = 0;
	/**
	 * deal with opcode :
	 * OP_WRITE, OP_ZERO, OP_TRUNCATE
	 * OP_REMOVE, OP_CLONE, OP_CLONE_RANGE
	 * DESTROY_COLLECTION, COLLECTION_ADD, SPLIT_COLLECTION
	 * these operations will change the content of objects
	 * or move objects between collections, and should be reflected
         * to MEMORY index ...
	 */
	switch (op) {
            case Transaction::OP_NOP:
                break;
            case Transaction::OP_TOUCH:
                {
                        coll_t cid = i.decode_cid();
                        ghobject_t oid = i.decode_oid();
                        /* do nothing */
                        _touch(cid, oid);
                }
                break;
	    case Transaction::OP_WRITE:
		{
		    coll_t cid = i.decode_cid();
		    ghobject_t oid = i.decode_oid();
		    uint64_t off = i.decode_length();
		    uint64_t len = i.decode_length();
		    // update Index of "/cid/oid"
		    _write(cid, oid, off, len, entry_pos, offset);
		    offset += len;
		}
		break;
            case Transaction::OP_ZERO:
                {
                        coll_t cid = i.decode_cid();
                        ghobject_t oid = i.decode_oid();
                        uint64_t off = i.decode_length();
                        uint64_t len = i.decode_length();
                        // we should create a special bufferhead which stand for zero, 
                        // and commit to backend storage later 
                        r = _zero (cid, oid, off, len);
                }
                break;
            case Transaction::OP_TRIMCACHE:
                {
                        i.decode_cid();
                        i.decode_oid();
                        i.decode_length();
                        i.decode_length();
                        r = -EOPNOTSUPP;
                }
                break;
            case Transaction::OP_TRUNCATE:
                {
                        coll_t cid = i.decode_cid();
                        ghobject_t oid = i.decode_oid();
                        uint64_t off = i.decode_length();
                        // create a special bufferhead which stand for trun,
                        // and commit to backend storage later
                        r = _truncate(cid, oid, off, update_store);
                }
                break;
            case Transaction::OP_REMOVE:
                {
                        coll_t cid = i.decode_cid();
                        ghobject_t oid = i.decode_oid();
                        // SET object.stat to REMOVED
                        // and call store->_remove(cid, oid)
                        r = _remove (cid, oid, update_store);
                }
                break;
	   
	    /* deal with clone operation */
	    case Transaction::OP_CLONE:
		{   /*clone data, xattr and omap ...*/
		    coll_t cid = i.decode_cid();
		    ghobject_t src = i.decode_oid();
		    ghobject_t dst = i.decode_oid();
		    r = _clone(cid, src, dst, update_store);
		}
		break;
	    case Transaction::OP_CLONERANGE:
	    case Transaction::OP_CLONERANGE2:
		{
		    /*clone data only ...*/
		    coll_t cid = i.decode_cid();
		    ghobject_t src = i.decode_oid();
		    ghobject_t dst = i.decode_oid();
		    uint64_t off = i.decode_length();
		    uint64_t len = i.decode_length();
		    uint64_t dstoff = off;
		    if (op == Transaction::OP_CLONERANGE2)
			dstoff = i.decode_length();
		    r = _clone_range(cid, src, dst, off, len, dstoff, update_store);
		}
                break;
	    case Transaction::OP_MKCOLL:
		{
		    coll_t cid = i.decode_cid();
		    r = _create_collection(cid, update_store);
		}
		break;
	    case Transaction::OP_RMCOLL:
		{
		    coll_t cid = i.decode_cid();
		    r = _destroy_collection(cid, update_store);
		}
		break;
	    case Transaction::OP_COLL_ADD:
		{
		    coll_t ncid = i.decode_cid();
		    coll_t ocid = i.decode_cid();
		    ghobject_t oid = i.decode_oid();
		    r = _collection_add(ncid, ocid, oid, update_store);
		}
		break;
	    case Transaction::OP_COLL_REMOVE:
		{
		    coll_t cid = i.decode_cid();
		    ghobject_t oid = i.decode_oid();
		    r = _remove(cid, oid, update_store);
		}
	    case Transaction::OP_COLL_MOVE:
		{
		    assert(0 == "deprecated");
		    break;
		}
	    case Transaction::OP_COLL_MOVE_RENAME:
		{
		    coll_t oldcid = i.decode_cid();
		    ghobject_t oldoid = i.decode_oid();
		    coll_t newcid = i.decode_cid();
		    ghobject_t newoid = i.decode_oid();
		    r = _collection_move_rename(oldcid, oldoid, newcid, newoid, update_store);
		}
		break;
	    case Transaction::OP_COLL_RENAME:
		{
		    coll_t cid = i.decode_cid();
		    coll_t ncid = i.decode_cid();
		    r = _collection_rename(cid, ncid, update_store);
		}
		break;
	    case Transaction::OP_SPLIT_COLLECTION:
		assert(0 == "deprecated");
		break;
	    case Transaction::OP_SPLIT_COLLECTION2:
		{
		    coll_t cid = i.decode_cid();
		    uint32_t bits = i.decode_u32();
		    uint32_t rem = i.decode_u32();
		    coll_t dest = i.decode_cid();
		    r = _split_collection(cid, bits, rem, dest, update_store);
		}
		break;
         default:
                if (update_store)
		    r = do_other_op(op, i);
        }
        assert(r == 0);
    }
}
int NVMJournal::do_other_op(int op, Transaction::iterator& i)
{
    int r = 0 ;
    switch(op)   /* deal with the attributes of objects */
    {
	case Transaction::OP_SETATTR: 
	    {
		coll_t cid = i.decode_cid();
		ghobject_t oid = i.decode_oid();
		string name = i.decode_attrname();
		bufferlist bl;
		i.decode_bl(bl);
		map<string, bufferptr> to_set;
		to_set[name] = bufferptr(bl.c_str(), bl.length());
		r = store->_setattrs(cid, oid, to_set);
            }
	    break;
	case Transaction::OP_SETATTRS:
	    {
		coll_t cid = i.decode_cid();
		ghobject_t oid = i.decode_oid();
		map<string, bufferptr> aset;
		i.decode_attrset(aset);
		r = store->_setattrs(cid, oid, aset);
	    }
	    break;
        case Transaction::OP_RMATTR:
            {
		coll_t cid = i.decode_cid();
		ghobject_t oid = i.decode_oid();
		string name = i.decode_attrname();
		r = store->_rmattr(cid, oid, name.c_str());
	    }
	    break;
	case Transaction::OP_RMATTRS:
	    {
		coll_t cid = i.decode_cid();
		ghobject_t oid = i.decode_oid();
		r = store->_rmattrs(cid, oid);
	    }
	    break;
        case Transaction::OP_COLL_HINT:
            {
		coll_t cid = i.decode_cid();
                uint32_t type = i.decode_u32();
                bufferlist hint;
                i.decode_bl(hint);
                bufferlist::iterator pitr = hint.begin();
		if (type == Transaction::COLL_HINT_EXPECTED_NUM_OBJECTS) {
		    uint32_t pg_num;
                    uint64_t num_objs;
                    ::decode(pg_num, pitr);
                    ::decode(num_objs, pitr);
                    r = store->_collection_hint_expected_num_objs(cid, pg_num, num_objs);
		} 
            }
            break;
	case Transaction::OP_COLL_SETATTR:
	    {
		coll_t cid = i.decode_cid();
		string name = i.decode_attrname();
		bufferlist bl;
		i.decode_bl(bl);
		r = store->_collection_setattr(cid, name.c_str(), bl.c_str(), bl.length());
	    }
	    break;
	case Transaction::OP_COLL_RMATTR:
	    {
		coll_t cid = i.decode_cid();
		string name = i.decode_attrname();
		r = store->_collection_rmattr(cid, name.c_str());
	    }
	    break;
	case Transaction::OP_OMAP_CLEAR:
	    {
		coll_t cid = i.decode_cid();
		ghobject_t oid = i.decode_oid();
		r = store->_omap_clear(cid, oid);
	    }
	    break;
	case Transaction::OP_OMAP_SETKEYS:
	    {
		coll_t cid = i.decode_cid();
		ghobject_t oid = i.decode_oid();
		map<string, bufferlist> aset;
		i.decode_attrset(aset);
		r = store->_omap_setkeys(cid, oid, aset);
	    }
	    break;
	case Transaction::OP_OMAP_RMKEYS:
	    {
		coll_t cid = i.decode_cid();
		ghobject_t oid = i.decode_oid();
		set<string> keys;
		i.decode_keyset(keys);
		r = store->_omap_rmkeys(cid, oid, keys);
	    }
	    break;
	case Transaction::OP_OMAP_RMKEYRANGE:
	    {
		coll_t cid = i.decode_cid();
		ghobject_t oid = i.decode_oid();
		string first = i.decode_key();
		string last = i.decode_key();
		r = store->_omap_rmkeyrange(cid, oid, first, last);
	    }
	    break;
	case Transaction::OP_OMAP_SETHEADER:
	    {
		coll_t cid = i.decode_cid();
		ghobject_t oid = i.decode_oid();
		bufferlist bl;
		i.decode_bl(bl);
		r = store->_omap_setheader(cid, oid, bl);
	    }
	    break;
	case Transaction::OP_SETALLOCHINT:
	    {
		coll_t cid = i.decode_cid();
		ghobject_t oid = i.decode_oid();
		i.decode_length();
		i.decode_length();
	    }
	    break;
	default:
	    derr << "bad op " << op << dendl;
	    assert(0);
    }
    return r;
}
int NVMJournal::_touch(coll_t cid, const ghobject_t &oid)
{
    // make sure the existance of object in the backend storage
    return store->_touch(cid, oid);
}
/* do write */
int NVMJournal::_write(coll_t cid, const ghobject_t& oid, uint32_t off, uint32_t len, uint64_t entry_pos, uint32_t boff)
{
    assert((entry_pos & CEPH_PAGE_MASK) == 0);

    // keep reference to the object, and touch the object if needed..
    ObjectRef obj = get_object(cid, oid, true);
    if (!obj) {
	assert(0 == "got unexpected error from _write ");
    }

    BufferHead *bh = new BufferHead;
    bh->owner = obj;
    bh->ext.start = off;
    bh->ext.end = off+len;
    bh->bentry = entry_pos >> CEPH_PAGE_SHIFT;
    bh->boff = boff;

    {
	RWLock::WLocker locker(obj->lock);
	merge_new_bh(obj, bh);
    }

    {
        Mutex::Locker locker(Journal_queue_lock);
        Journal_queue.push_back(bh);
    }
    return 0;
}
/* zero */
int NVMJournal::_zero(coll_t cid, const ghobject_t &oid, uint32_t off, uint32_t len)
{
    ObjectRef obj = get_object(cid, oid, true);
    if (!obj)
        assert(0 == "get unexpected error from _zero"); 

    BufferHead *bh = new BufferHead;
    bh->owner = obj;
    bh->ext.start = off;
    bh->ext.end = off + len;
    bh->bentry = BufferHead::ZERO;
    bh->boff = 0;

    {
        RWLock::WLocker l(obj->lock);
        merge_new_bh (obj, bh);
    }

    {
        // deallocate the object space when evicted
        Mutex::Locker l(Journal_queue_lock);
        Journal_queue.push_back (bh);
    }
    return 0;
}
#define _ONE_GB	(1024*1024*1024)
/* do truncate */
int NVMJournal::_truncate(coll_t cid, const ghobject_t &oid, uint32_t off, bool update_store)
{
        ObjectRef obj = get_object(cid, oid, true);
        assert (obj);

        BufferHead *bh = new BufferHead;
        bh->owner = obj;
        bh->ext.start = off;
        bh->ext.end = _ONE_GB;
        bh->bentry = BufferHead::TRUNC;
        bh->boff = 0;

        {
	    RWLock::WLocker l(obj->lock);
	    merge_new_bh (obj, bh);
        }

	if (update_store)
	    store->_truncate(cid, oid, (uint64_t)off);
	
	{
	    Mutex::Locker l(Journal_queue_lock);
	    Journal_queue.push_back (bh);
	}
        return 0;
}
/* _remove */
int NVMJournal::_remove(coll_t cid, const ghobject_t& oid, bool update_store)
{
    CollectionRef coll = get_collection(cid);
    if (coll != NULL) {
	ObjectRef obj = get_object(coll, oid);
	if (obj != NULL) {
	    RWLock::WLocker l(obj->lock); 
	    obj->alias.erase(make_pair(cid, oid));
	    {
		/**
		 * protect coll->object_hash/map from concurrent access from
		 * normal operation and ev work(put_object)
		 */
		Mutex::Locker l(coll->lock);
		erase_object_with_lock_hold(coll, oid);
	    }
	    put_object(obj);
	}
    }
    if (update_store)
	store->_remove(cid, oid);
    return 0;
}


int NVMJournal::_clone(coll_t cid, const ghobject_t& src, const ghobject_t &dst, bool update_store)
{
    CollectionRef coll = get_collection(cid);
    ObjectRef srco = get_object(coll, src);
    ObjectRef dsto = NULL;
    int ret = 0;
    if (srco)
	dsto = get_object(coll, dst, true);

    // flush object to store, then do the store->clone 
    if (update_store) {
	if (srco) {
	    RWLock::RLocker l(srco->lock);
	    map<uint32_t, BufferHead *>::iterator p = srco->data.begin();
	    while (p != srco->data.end()) {
		BufferHead *pbh = p->second;
		_flush_bh(srco, pbh);
		p ++;
	    } 
	}
	ret = store->_clone(cid, src, dst);	
    }
    
    if (ret != 0) {
        put_object(srco);
        put_object(dsto);
        return ret;
    }
    // create a new_src_object whose parent is src_object
    // src_object is a readonly object now, the dst object and new src object
    // share iis data1

    // we should never waitting for lock of an object when we hold lock of a collection
   
    RWLock::WLocker l1 (MIN (srco, dsto)->lock);
    RWLock::WLocker l2 (MAX (srco, dsto)->lock);
    
    map< CollectionRef, shared_ptr<Mutex::Locker> >lockers;
    set< pair<coll_t, ghobject_t> >::iterator p = srco->alias.begin();
    while (p != srco->alias.end()) {
	CollectionRef c = get_collection(p->first);
	if (lockers.find(c) == lockers.end())
	    lockers[c] = shared_ptr<Mutex::Locker>(new Mutex::Locker(c->lock));
	++ p;
    }

    if (srco && !srco->data.empty() && dsto) 
    {
	erase_object_with_lock_hold (coll, src);
        ObjectRef new_srco = get_object(coll, src, true); 
        new_srco->parent = srco;
        srco->get(); 
        dsto->parent = srco;
        srco->get();
	
	new_srco->alias.swap(srco->alias);
	srco->alias.clear();
	for (p = new_srco->alias.begin(); 
		p != new_srco->alias.end(); 
		++p) {
	    CollectionRef coll = get_collection(p->first);
	    coll->Object_hash[p->second] = new_srco;
	    coll->Object_map[p->second] = new_srco;
	}

	// using bufferhead to keep reference of new_src_obj and dst_obj 
        BufferHead *src_bh, *dst_bh;
	src_bh = new BufferHead;
        assert (src_bh);
	src_bh->owner = new_srco;
        src_bh->ext.start = 0;
	src_bh->ext.end = 0;
	src_bh->bentry = BufferHead::ZERO;
	dst_bh = new BufferHead;
	assert (dst_bh);
	*dst_bh = *src_bh;
	dst_bh->owner = dsto;

	dsto->get(); // for new bufferhead
	
	{
	    Mutex::Locker l(Journal_queue_lock);
	    Journal_queue.push_back(src_bh);
	    Journal_queue.push_back(dst_bh);
	}
	
    }

    put_object(srco);
    put_object(dsto);
    return ret;
}
int NVMJournal::_clone_range(coll_t cid, ghobject_t &src, ghobject_t &dst, 
	uint64_t off, uint64_t len, uint64_t dst_off, bool update_store)
{
    ObjectRef obj = get_object(cid, src);
    int ret = 0;
    if (update_store) {
	if (obj) {
	    RWLock::RLocker l(obj->lock);
	    map<uint32_t, BufferHead *>::iterator p = obj->data.find(off);
	    if (p != obj->data.begin()) 
		-- p;
	    uint32_t end = off + len;
	    while (p != obj->data.end()) {
		if (p->first >= end)
		    break;
		BufferHead *pbh = p->second;
		_flush_bh (obj, pbh);
	    }
	}
    }
    /**
     * we must lock the dsto before revokd store->clone...
     */
    ObjectRef dsto = get_object(cid, dst);
    if (dsto) {
	get_write_lock(dsto);
    }

    ret = store->_clone_range(cid, src, dst, off, len, dst_off);
    put_object(obj);

    if (dsto) {
	BufferHead *bh = new BufferHead;
	bh->owner = dsto;
	bh->ext.start = dst_off;
	bh->ext.end = dst_off + len;
	bh->bentry = BufferHead::ZERO;
	bh->boff = 0;
	{
	    RWLock::WLocker l(dsto->lock);
	    merge_new_bh (obj, bh);
	    delete_bh (obj, bh->ext.start, bh->ext.end, BufferHead::ZERO);
	}
	delete bh;
	put_write_lock(dsto);
    }
    put_object(dsto);
    return ret;
}
int NVMJournal::_create_collection(coll_t cid, bool update_store)
{
    int r = 0;
    if (update_store)
	r = store->_create_collection(cid);
    return r;
}
int NVMJournal::_destroy_collection(coll_t cid, bool update_store)
{
    int ret = 1;
    {
	Mutex::Locker locker(coll_map.lock);
	ceph::unordered_map<coll_t, CollectionRef>::iterator itr = coll_map.collections.find(cid);
	if (itr != coll_map.collections.end()) {
	    CollectionRef c = itr->second;
	    Mutex::Locker l(c->lock); 
	    if (c->Object_hash.empty()) {
		coll_map.collections.erase(itr);
		ret = 0;
	    }
	}
	else
	    ret = 0;
    }

    if (ret && update_store)
	ret = store->_destroy_collection(cid);
    return ret;

}
int NVMJournal::_collection_add(coll_t dst, coll_t src, const ghobject_t &oid, bool update_store)
{
    int ret = 0;
    ObjectRef obj = get_object(src, oid);
    if (obj) {
	CollectionRef dstc = get_collection(dst, true);
	assert(dstc);
	RWLock::WLocker l1 (obj->lock);
	{
	    Mutex::Locker l2 (dstc->lock);
	    dstc->Object_hash[oid] = obj;
	    dstc->Object_map[oid] = obj;
	}
	obj->alias.insert( make_pair(dst, oid));
    }
    if (update_store)
	ret = store->_collection_add(dst, src, oid);
    return ret;
}

int NVMJournal::_collection_move_rename(coll_t oldcid, const ghobject_t &oldoid,
	coll_t newcid, const ghobject_t &newoid, bool update_store)
{
    int ret = 0;
    CollectionRef srcc = get_collection(oldcid);
    if (srcc) {
	CollectionRef dstc = get_collection(newcid, true);
	assert(dstc);
	ObjectRef obj = get_object(srcc, oldoid);
	RWLock::WLocker locker (obj->lock);
	if (obj) {
	    if (srcc != dstc) {
		Mutex::Locker l1 (MIN(&(*srcc), &(*dstc))->lock);
		Mutex::Locker l2 (MAX(&(*srcc), &(*dstc))->lock);
		erase_object_with_lock_hold(srcc, oldoid);
		dstc->Object_hash[newoid] = obj;
		dstc->Object_map[newoid] = obj;
	    }
	    else {
		Mutex::Locker l (srcc->lock);
		erase_object_with_lock_hold(srcc, oldoid);
		srcc->Object_hash[newoid] = obj;
		srcc->Object_map[newoid] = obj;
	    }
	    obj->alias.erase( make_pair(oldcid, oldoid));
	    obj->alias.insert( make_pair(newcid, newoid));
	}
    }
    if (update_store)
	ret = store->_collection_move_rename(oldcid, oldoid, newcid, newoid);
    return ret;
}
int NVMJournal::_collection_rename(coll_t cid, coll_t ncid, bool update_store)
{
    int ret = 0;
    CollectionRef coll = get_collection(cid);
    // we should pause evict here...
    pause_ev_work();
    if (coll) {
	{
	    Mutex::Locker l(coll_map.lock);
	    coll_map.collections[ncid] = coll;
	    coll_map.collections.erase(cid);
	}
	// update the alias of each object 
	for (map<ghobject_t, ObjectRef>::iterator itr = coll->Object_map.begin();
		itr != coll->Object_map.end();
		++itr) {
	    ObjectRef o = itr->second;
	    o->alias.erase( make_pair(cid, itr->first));
	    o->alias.insert( make_pair(ncid, itr->first));
	}
    }
    if (update_store)
	ret = store->_collection_rename(cid, ncid);
    unpause_ev_work();
    return ret;
}
int NVMJournal::_split_collection(coll_t src, uint32_t bits, uint32_t match, coll_t dst, bool update_store)
{
    int ret = 0;
    pause_ev_work();
    CollectionRef srcc = get_collection(src);
    if (srcc) {
	CollectionRef dstc = get_collection(dst, true);
	map<ghobject_t, ObjectRef>::iterator p = srcc->Object_map.begin();
	while (p != srcc->Object_map.end())
	{
	    if (p->first.match(bits, match)) {
		ObjectRef obj = p->second;
		srcc->Object_hash.erase(p->first);
		dstc->Object_hash[p->first] = obj;
		dstc->Object_map[p->first] = obj;
		// update the alias of object
		obj->alias.insert( make_pair(dst, p->first));
		obj->alias.erase( make_pair(src, p->first));
		srcc->Object_map.erase(p++);
	    }
	    else 
		++ p;
	}
    }
    if (update_store)
	ret = store->_split_collection(src, bits, match, dst);
    unpause_ev_work();
    return ret;
}
void NVMJournal::merge_new_bh(ObjectRef obj, BufferHead* new_bh)
{
    assert(obj->lock.is_wlocked());
    map<uint32_t, BufferHead*>::iterator p;
    p = obj->data.lower_bound(new_bh->ext.start); 
    if (p != obj->data.begin())
	p--;

    uint32_t new_start = new_bh->ext.start;
    uint32_t new_end = new_bh->ext.end;
    if (new_start == new_end)
	return;
    while (p != obj->data.end()) {
	BufferHead *bh = p->second;
	uint32_t start, end;
	start = bh->ext.start;
	end = bh->ext.end;

	if (new_start <= start){
	    /* new_start, start, end, new_end */
	    if (end <= new_end) {
		bh->ext.start = bh->ext.end; // 
		obj->data.erase(p++);
		if (!bh->owner) 
		    delete bh;
		continue;
	    }
	    /* new_start, start, new_end, end */
	    else if (start < new_end){
		bh->boff += new_end - start;
		bh->ext.start = new_end;
		obj->data.erase(p);
		obj->data[bh->ext.start] = bh;
	    }
	    /* new_start, new_end, start, end */
	    else { /* do nothing */}
	    break;
	}
	else {
	    /* start, end, new_start, new_end */
	    if (end <= new_start) {
		p++; /* pass */
	    }
	    /* start, new_start, end, new_end */
	    else if (end <= new_end) {
		bh->ext.end = new_start;
		p ++;
	    }
	    /*start, new_start, new_end, end */
	    else {
	    	obj->data.erase(p);
	    	/* create two new BufferHead */
		BufferHead *left, *right;
		left = new BufferHead();
		left->owner = NULL; 
		left->ext.start = start;
		left->ext.end = new_start;
                left->bentry = bh->bentry;
		left->boff = bh->boff;
                if (left->bentry == BufferHead::TRUNC)
                    left->bentry = BufferHead::ZERO;
		obj->data[start] = left;
		// obj->get();

		right = new BufferHead();
		right->owner = NULL;
		right->ext.start = new_end;
		right->ext.end = end;
		right->bentry = bh->bentry;
		right->boff = bh->boff + (new_end-start);
		obj->data[new_end] = right;
		// obj->get();  

		if (!bh->owner) 
		    delete bh;

		break;	
	    }
	}
    }

    obj->data[new_bh->ext.start] = new_bh;
}

void NVMJournal::delete_bh(ObjectRef obj, uint32_t off, uint32_t end, uint32_t bentry)
{
    assert(obj->lock.is_wlocked());
    map<uint32_t, BufferHead*>::iterator p = obj->data.lower_bound(off);
    if (p!=obj->data.begin())
	p--;
    while (p != obj->data.end()) {
	BufferHead *bh = p->second;
	if (bh->ext.start >= end)
	    break;
	if (bh->ext.end <= off) {
	    p ++;
	    continue;
	}
	if (bh->bentry == bentry){
	    obj->data.erase(p++);
	    if (!bh->owner) 
		delete bh;
	}
    }
}

/* READ */
#define SSD_OFF(pbh) (((uint64_t)(pbh->bentry) << CEPH_PAGE_SHIFT) + pbh->boff)

void NVMJournal::map_read(ObjectRef obj, uint32_t off, uint32_t end,
                        map<uint32_t, uint32_t> &hits,
                        map<uint32_t, uint64_t> &trans,
                        map<uint32_t, uint32_t> &missing)
{
        assert (obj && obj->lock.is_locked());
        map<uint32_t,BufferHead*>::iterator p = obj->data.lower_bound(off);
        if (p != obj->data.begin())
                -- p;
        while (p != obj->data.end()) 
        {
                BufferHead *pbh = p->second;
                if (pbh->ext.start <= off) {
                    // _bh_off_, _bh_end_, off, len
                    if (pbh->ext.end <= off ) {
                        p++;
                        continue;
                    }
                    // _bh_off_, off, _bh_end_, end
                    else if (pbh->ext.end < end) 
                    {
                        assert(pbh->bentry != BufferHead::TRUNC);
                        if (pbh->bentry != BufferHead::ZERO )
                              trans[off] = SSD_OFF(pbh) + (off - pbh->ext.start);
                        else 
                              trans[off] = pbh->bentry << CEPH_PAGE_SHIFT;

                        hits[off] = pbh->ext.end - off;

                        off = pbh->ext.end;
                        p++;
                        continue;
                    }
                    // _bh_off_, off, end, _bh_end_     
                    else { 
                        if (pbh->bentry != BufferHead::ZERO && pbh->bentry != BufferHead::TRUNC)
                              trans[off] = SSD_OFF(pbh) + (off - pbh->ext.start);
                        else 
                              trans[off] = pbh->bentry << CEPH_PAGE_SHIFT;

                        hits[off] = end - off;
                        return;
                    }
                } 
                else {
                    if (end <= pbh->ext.start) {
                        missing[off] = end - off;
                        return;
                    }
                    // off, _bh_off_, _bh_end_, end OR off, _bh_off_, end, _bh_end_
                    missing[off] = pbh->ext.start - off;
                    off = pbh->ext.start;
                    p ++;
                }
        }

    if (off < end)
        missing[off] = end-off;
}

void NVMJournal::build_read(coll_t &cid, const ghobject_t &oid, uint64_t off, size_t len, ReadOp &op) 
{
    // keep reference of object
    uint32_t attempts = 4;
    ObjectRef obj = NULL;
    do {
	obj = get_object(cid, oid);
	if (!obj) 
	    break;
	get_read_lock(obj);
	if (!obj->alias.empty())
	    break;
	put_object(obj);
    } while(attempts--);

    op.cid = cid;
    op.oid = oid;
    op.obj = obj;
    op.off = off;
    op.length = len;

    if (!op.obj || obj->alias.empty()) {
	op.missing[off] = len;
	return;
    }

    map_read(obj, off, len, op.hits, op.trans, op.missing);

    if (obj->parent && !op.missing.empty())
        build_read_from_parent(obj->parent, obj, op);
}
void NVMJournal::build_read_from_parent(ObjectRef parent, ObjectRef obj, ReadOp& op)
{
        assert (parent);
        if (parent->data.empty())
                return;

        get_read_lock(parent);
        op.parents.push_back(parent);                
        map<uint32_t, uint32_t> missing;
        map<uint32_t, uint32_t>::iterator p = op.missing.begin();
	while (p != op.missing.end()) {
	    map_read(parent, p->first, p->first+p->second, 
		    op.hits, op.trans, missing);
	    p ++;
	}
	missing.swap(op.missing);
	if (parent->parent && !missing.empty())
	    build_read_from_parent(parent->parent, parent, op);
}

int NVMJournal::do_read(ReadOp &op)
{
    if (!store->exists(op.cid, op.oid))
        return -ENOENT;

    map<uint32_t, bufferptr> data;
    for (map<uint32_t, uint32_t>::iterator p = op.hits.begin();
	    p != op.hits.end(); 
	    ++p) {
        uint64_t off = op.trans[p->first];
        uint32_t len = p->second;

        uint32_t bentry = off >> CEPH_PAGE_SHIFT;
        if (bentry != BufferHead::ZERO && bentry != BufferHead::TRUNC) {
            uint64_t start = off & CEPH_PAGE_MASK;
            uint64_t end = ROUND_UP_TO(off+len, CEPH_PAGE_SIZE);

            bufferptr ptr(buffer::create_page_aligned(end - start));
	    ssize_t r = safe_pread(fd, ptr.c_str(), ptr.length(), start);
            if(r != ptr.length()) {
                assert(0 == "NVMJournal::do_read::safe_pread error!");
            }
            off -= start;
            data[p->first] = bufferptr(ptr, off, len);
        } 
        else if (bentry == BufferHead::ZERO) {
            bufferptr bp(len);
            bp.zero();
            data[p->first] = bp;
        }
        else {
	    map<uint32_t, uint32_t>::iterator next = p;
	    ++ next;
	    assert(next == op.hits.end());
            assert(bentry == BufferHead::TRUNC);
            /* do nothing */
        } 
    }

    while(!op.parents.empty()) {
       put_read_lock(op.parents.front());
       op.parents.pop_front();
    }
    
    if (op.obj) {
        put_read_lock(op.obj);
        put_object(op.obj);
    }
    
    map<uint32_t, uint32_t>::iterator p = op.missing.begin();
    while(p != op.missing.end()) {
	bufferptr ptr(p->second);
        ssize_t r = store->_read(op.cid, op.oid, p->first, p->second, ptr);
        if (r < p->second) {
            data[p->first] = ptr;
            ++ p;
            while (p != op.missing.end() 
                && p->second < op.obj->size) {
                uint32_t len = op.obj->size - p->first;
                if (len > p->second)
                        len = p->second;
                bufferptr ptr(len);
                memset(ptr.c_str(), 0, len);
                data[p->first] = ptr;
            }     
            break;
        }
        data[p->first] = ptr;
        ++p;
    }

    map<uint32_t, bufferptr>::iterator q = data.begin();
    while (q != data.end()) {
	op.buf.append(p->second);
	++ q;
    }
    return 0;
}

int NVMJournal::read_object(coll_t cid, const ghobject_t &oid, uint64_t off, size_t len, bufferlist &bl)
{
    ReadOp op;
    build_read(cid, oid, off, len, op);
    int r = do_read(op);
    bl.swap(op.buf);
    len = bl.length();
    return r;
}


/* backgroud evict thread */
void NVMJournal::evict_entry()
{
    static uint32_t synced = 0, cur = 0;
    uint64_t seq;
    utime_t interval;

    interval.set_from_double(1.0);
    while (true) 
    {
        check_ev_completion();
	{
	    Mutex::Locker l(evict_lock);
	    if (ev_stop)
		return;
	    if (!should_evict() || ev_pause) {
                if (ev_pause && running_ev.empty())
                        ev_paused = true;
		evict_cond.WaitInterval(g_ceph_context, evict_lock, interval);
		continue;
	    }
	}
        ev_paused = false;

	deque<BufferHead*> to_evict, to_reclaim;
	{
	    Mutex::Locker l(Journal_queue_lock);
	    to_evict.assign(Journal_queue.begin(), Journal_queue.begin() + 32);
	}

	deque<BufferHead*>::iterator p = to_evict.begin();
	map<ObjectRef, deque<BufferHead*> > obj2bh;

	while (p != to_evict.end())
	{
	    BufferHead *pbh = *p++;
	    ObjectRef obj = pbh->owner;
	    // 
	    assert(obj);

	    if (pbh->bentry != cur) {
		if(cur)
		    synced = cur; // the log entry which all the bh has been evicted
		cur = pbh->bentry;
	    }

	    if (pbh->ext.start == pbh->ext.end) {
		put_object(obj);
		delete pbh;
		continue;
	    }

	    to_reclaim.push_back(pbh);

            if (obj2bh.find(obj) == obj2bh.end())
                obj->get();
            obj2bh[obj].push_back(pbh);
	}
	
	seq = ev_seq ++;
        uint32_t ops = obj2bh.size();
        // reserve the evict op
        apply_manager.op_apply_start(ops);

	map<ObjectRef, deque<BufferHead *> >::iterator itr = obj2bh.begin();

	while (itr != obj2bh.end()) 
	{
                ObjectRef obj = itr->first;
                EvOp *ev = new EvOp(obj, itr->second);
                assert(ev);
                if (ops == 1) {
                        ev->synced = synced;
                        ev->seq = seq;
                }
                queue_ev(ev);
                running_ev.push_back(ev);
                ++ itr;
                -- ops;               
	}
        obj2bh.clear();

        hang_ev[seq].swap(to_reclaim);
    }
}
void NVMJournal::stop_evictor()
{
        {
                Mutex::Locker l(evict_lock);
                ev_stop = true;
                evict_cond.Signal();
        }
        evictor.join();
}
void NVMJournal::pause_ev_work()
{
        {
                Mutex::Locker l(evict_lock);
                ev_pause ++;
                while (!ev_paused)
                        evict_cond.Wait(evict_lock);
        }
}
void NVMJournal::unpause_ev_work()
{
        Mutex::Locker l(evict_lock);
        ev_pause --;
        evict_cond.Signal();
}
void NVMJournal::check_ev_completion()
{
    uint64_t new_completed_ev = 0;
    uint32_t new_synced = 0;
    deque<EvOp*>::iterator it = running_ev.begin();
    while (it != running_ev.end() && (*it)->done) {
        EvOp *ev = *it;
        if (ev->seq) {
            new_completed_ev = ev->seq;
            new_synced = ev->synced;
        }
        running_ev.erase(it++);
        delete ev;
    }

    if (new_completed_ev) {
        Mutex::Locker l(reclaim_queue_lock);
        map<uint64_t, deque<BufferHead*> >::iterator p = hang_ev.begin();
        while (p->first <= new_completed_ev) {
                reclaim_queue.insert(reclaim_queue.end(), p->second.begin(), p->second.end());
                hang_ev.erase(p++);
        }
        data_sync_pos = new_synced << CEPH_PAGE_SHIFT;
        waiter_cond.Signal();
    }
    {
	Mutex::Locker l(sync_lock);
	uint64_t sync_not_recorded = 0;
	if (data_sync_pos > data_sync_pos_recorded)
	    sync_not_recorded = data_sync_pos - data_sync_pos_recorded;
	else
	    sync_not_recorded = (max_length - data_sync_pos_recorded) + data_sync_pos;
	
	if (sync_not_recorded >= sync_threshold)
	    _sync();
    }
}

void NVMJournal::_flush_bh(ObjectRef obj, BufferHead *pbh)
{
    assert (obj->lock.is_locked());
    static const int flags = SPLICE_F_NONBLOCK;
    int *fds = (int *)tls_pipe.get_resource();
    assert(fds);

    uint64_t pos = pbh->bentry;
    uint32_t off = pbh->ext.start;
    uint32_t len = pbh->ext.end - off;
    if (!len)
	return;
    if (pos == BufferHead::TRUNC) {
	/* do nothing */
    }
    else if (pos == BufferHead::ZERO) {

    }
    else {
	bool need_to_flush = false;
	pos = pos << CEPH_PAGE_SHIFT;
	if (write_pos >= pos && pos >= data_sync_pos)
	    need_to_flush = true;
	else if (!(write_pos < pos && pos < data_sync_pos))
	    need_to_flush = true;
	if (need_to_flush) {
	    loff_t ssd_off = SSD_OFF(pbh);
	    loff_t obj_off = pbh->ext.start;
            uint32_t len = pbh->ext.end - pbh->ext.start;

	    /**
             * don't need to worry about the sector align problem
             * even though ssd_fd was opened with O_DIRECT
             **/
	    ssize_t r = safe_splice(fd, &ssd_off, fds[1], NULL, len, flags);
            assert(r == len);

	    /**
             * when using disk as backend storage:
             *    out_fd = store->open(obj->coll, obj->oid);
	     *    r = safe_splice(fds[0], NULL, out_fd, obj_off, len, flags);
             *    ...
             **/
             {
                // just for debug ...
                bufferptr bp(len);
                r = safe_read(fds[0], bp.c_str(), len);
                assert (r == len);
                bufferlist bl;
                bl.append(bp);
                const coll_t &cid = obj->alias.begin()->first;
                const ghobject_t &oid = obj->alias.begin()->second;
                store->_write(cid, oid, obj_off, len, bl, 0);
             }
	}
    }
}
void NVMJournal::do_ev(EvOp *ev, ThreadPool::TPHandle *handle) 
{
    ObjectRef obj = ev->obj;
    deque<BufferHead *>::iterator p = ev->queue.begin();

    RWLock::WLocker l(obj->lock);

    if (obj->alias.empty())
        goto done;

    while (p != ev->queue.end()) {
	BufferHead *bh = *p;
        // check the object 

        bool valid = false; 
        map<uint32_t, BufferHead *>::iterator t = obj->data.lower_bound(bh->ext.start);
        if (t != obj->data.begin())
            -- t;

	// nothing special to do with removed object,
	// because of the empty data map
        while (t != obj->data.end()) {
            uint32_t off = t->first;
            if (off > bh->ext.end) 
                break;

            BufferHead *bh2 = t->second;
            assert (off == bh2->ext.start);
            if (bh2->ext.start < bh->ext.end 
                && bh2->ext.end > bh->ext.start) 
            {
                // if bh2 is the child of bh, or bh2 == bh,  then ...
                if (bh2->bentry == bh->bentry)
                    _flush_bh (obj, bh2);
            }// if 
            ++ t;
        }// while

        // mark the invalid bufferhead ...
        if (!valid)
            bh->ext.end = bh->ext.start;
	++ p;
        if (handle)
            handle->reset_tp_timeout();
    } // while

 done:
    ev->done = true;
    {
        Mutex::Locker locker(evict_lock);
        evict_cond.Signal();
    }
    apply_manager.op_apply_finish();
}

bool NVMJournal::should_evict() 
{
    uint64_t used;
    uint64_t threshold = evict_threshold * max_length;
    static bool evicting = false;
    const uint64_t batch = 64 << 20; // 64MB;

    if (write_pos > data_sync_pos)
	used = write_pos - data_sync_pos;
    else
	used = max_length - (data_sync_pos - write_pos);
   
    if (used > threshold) {
	evicting = true;
    }
    else if (evicting){
	if (threshold - used >= batch)
	    evicting = false; 
    }

    return evicting;
}

bool NVMJournal::should_relcaim()
{
    double threshold = 0.75, used = 0;
    if (write_pos > start_pos)
	used = write_pos - start_pos;
    else
	used = max_length - (start_pos - write_pos);
    used = used / max_length;
    return used > threshold;
}

void NVMJournal::do_reclaim()
{
    deque<BufferHead *> to_relaim;
    {
	Mutex::Locker locker(reclaim_queue_lock);
	to_relaim.assign (reclaim_queue.begin(), reclaim_queue.begin() + 128);
    }

    static uint32_t pre = 0, cur = 0;

    while (!to_relaim.empty()) 
    {
	BufferHead *bh = to_relaim.front();
	ObjectRef obj = bh->owner;
        // skip invalid bufferhead ...
        if (bh->ext.start != bh->ext.end) {
            RWLock::WLocker locker(obj->lock);
	    delete_bh(obj, bh->ext.start, bh->ext.end, bh->bentry);
        }


	// dec reference of object
	put_object(obj);
	if (bh->bentry != cur) {
	    pre = cur;
	    cur = bh->bentry;
    	}
        to_relaim.pop_front();
        delete bh; // free BufferHead 
    }

    if (pre != 0)
	start_pos = pre << CEPH_PAGE_SHIFT;
    
    {
	Mutex::Locker l(sync_lock);
	if (start_pos >= data_sync_pos_recorded) {
	    _sync();
	}
    }
}

void NVMJournal::wait_for_more_space(uint64_t min)
{
    uint64_t free;
    do {
	if (should_evict())
	    evict_cond.Signal();
	if (should_relcaim())
	    do_reclaim();

	if (write_pos > start_pos) 
	    free = write_pos - start_pos;
	else
	    free = max_length - (start_pos - write_pos);
	
	if(free < min) {
	    utime_t interval;
	    interval.set_from_double(1.0);
	    Mutex::Locker locker(waiter_lock);
	    waiter_cond.Wait(waiter_lock);
	    continue;
	}
    }while(false);
}

