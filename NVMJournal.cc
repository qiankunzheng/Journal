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
            io_destory(aio_ctx);
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
    uint64_t seq, pos = sync_data_pos;
    Op op;
    do {
	ret = read_entry(pos, seq, &op);
	if (!ret && pos) {
	    do_op(&op);
	    write_pos = pos;
	    cur_seq = seq;
	}
    } while(!ret);
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
        retuurn r; 

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
// FIXME
NVMJournal::NVMJournal(string dev, string c, MemStore *s, Finisher *fin)
    :RWJournal (s, fin), 
    path (dev), conf (c), fd(-1),
    writeq_lock ("NVMJournal::writeq_lock", false, true, false, g_ceph_context),
    op_throttle_lock("NVMJournal::op_throttle_lock", false, true, false, g_ceph_context),
    op_qeueu_len(0),
    aioq_lock ("NVMJournal::aioq_lock",false, true, false, g_ceph_context), 
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
    reclaim_queue_lock ("NVMJournal::reclaim_queue_lock", false, true, false, g_ceph_context),
    waiter_lock ("NVMJournal::waiter_lock",false, true, false, g_ceph_context),
    ev_seq(0),
    evictor(this),
    ev_stop(false),
    evict_lock("NVMJournal::evict_lock", false, true, false, g_ceph_context)
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

	osr->register_op(cur);
	writeq.push_back(write_item(cur_seq++, osr, tls));
    }
    writeq_cond.Signal();
}

uint64_t NVMJournal::prepare_single_write(bufferlist &meta, bufferlist &data, Op *op)
{
    write_item *pitem;       
    uint64_t seq = 0; 
    bufferlist tmeta, tdata;
    uint32_t meta_len, data_len, pre_pad, size;

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
    	safe_pread(fd, &h2, sizeof(h2), pos);
    	if (memcmp(&h,&h2,sizeof(h)))
    		return -1;
    	pos = 0;
    	return 0;
    }
    uint32_t size = h.length - h.data_len;
    off = sizeof(h) + h.pre_pad + size; // offset of data part
    bufferptr bp = buffer::create(size);
 
    safe_pread(fd, bp.c_str(), size, pos);
    bl.push_back(bp);
   
    entry_header_t h2;
    pos += h.length + h.post_pad;
    safe_pread(fd, &h2, sizeof(h2), pos);
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

    for (int n = 0; n < h.ops; n++) {
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
NVMJournal::CollectionRef NVMJournal::get_collection(coll_t &cid, bool create) 
{
    const string &cstr = cid.to_str();
    unsigned index = ceph_str_hash_linux(cstr.c_str(), cstr.length()) % NR_SLOT;
    {
        Mutex::Locker locker(Cache[index].lock);
        map<coll_t, CollectionRef> &colls = Cache[index].collections;
        map<coll_t, CollectionRef>::iterator itr = colls.find(cid);
        if(itr == colls.end()) {
            if (!create)
                return CollectionRef();
            colls[cid].reset(new Collection(osr));
        }
        return colls[cid];
    }
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
                obj = new Object(cid, oid);
                coll->Object_hash[oid] = obj;
                coll->Object_map[oid] = obj;
                store->_touch(coid, oid);
        }
              
        ObjectRef obj = coll->Object_hash[oid];
        obj->get();
        return obj;
}

NVMJournal::ObjectRef NVMJournal::get_object(coll_t &cid, const ghobject_t &oid, bool create) 
{
    CollectionRef coll;
    coll = get_collection(cid, create);
    if(!coll)
	return NULL;
    return get_object(coll, oid, create);
}

void NVMJournal::erase_object_without_lock(CollectionRef coll, ObjectRef obj)
{
        assert (coll);
        coll->Object_hash.erase(obj->oid);
        coll->Object_map.erase(obj->oid);
}

void NVMJournal::put_object(ObjectRef obj) 
{
    assert (obj->lock.is_locked() == false);
    CollectionRef coll = get_collection(obj->coll);
    if (!obj->put()) {
	    erase_object_with_lock(coll, obj);
            delete obj;
    }
}

/* op_wq */
void NVMJournal::_do_op(OpSequencer *osr, ThreadPool::TPHandle *handle) 
{
    Mutex::Locker l(osr->apply_lock);
    task &t = osr->peek_queue();
    switch(t.tag) {
	case task::OP:
        {
	    Op *op = (Op*)(t.pt);
	    do_op(op, handle);
	    notify_on_applied(op);
	    delete op;
        }
        break;
        // FIXME: no need to sequentialize ev* 
	case task::EV:
        {
	    Ev *ev = (Ev*)(t.pt);
	    do_ev(ev, handle);
	    delete ev;
        }
        break;
	default:
	    assert(0 == "unexpectted error happened in _do_op()");
    }
    handle->reset_tp_timeout();
    if (t.tag == task::OP){
	list<Context *> to_queue;
	osr->unregister_op();
	osr->wakeup_flush_waiters(to_queue);
	finisher->queue(to_queue);
	op_queue_release_throttle();
    }
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
    return true;
}
// FIXME
void NVMJournal::do_transaction(Transaction* t, uint64_t seq, uint64_t entry_pos, uint32_t &offset) 
{
    Transaction::iterator i = t->begin();
    int r = 0;
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
                        if(need_to_update_store(entry_pos))
                        	r = _touch(cid, oid);
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
                        r = _truncate(cid, oid, off);
                }
                break;
            case Transaction::OP_REMOVE:
                {
                        coll_t cid = i.decode_cid();
                        ghobject_t oid = i.decode_oid();
                        // SET object.stat to REMOVED
                        // and call store->_remove(cid, oid)
                        r = _remove (cid, oid);
                        if (need_to_update_store(entry_pos))
                            r = store->_remove(cid, oid);
                }
                break;
	   
	    /* deal with clone operation */
	    case Transaction::OP_CLONE:
		{   /*clone data, xattr and omap ...*/
		    coll_t cid = i.decode_cid();
		    ghobject_t oid = i.decode_oid();
		    ghobject_t noid = i.decode_oid();
		    // FIXME: how to deal with clone op
		    // 1) flush journal data to backend store, if update_store == true
		    // 2) invoke store->_clone(...)
		    // 3) mark this object clean && remove it from collection
		    // 4) create two objects as the children of original object
		    bool update_store = need_to_update_store(entry_pos);
		    r = _clone(cid, oid, noid, update_store);
		}
		break;
	    case Transcation::OP_CLONERANGE:
		{
		    /*clone data only ...*/
		    // FIXME:
		    // 1) flush converd range of data to backend storage
		}
                break;

         default:
                if (need_to_update_store(entry_pos))
                	r = do_other_op(op, i);
        }
        ++pos;
    }
}
int NVMJournal::do_other_op(int op, Transaction::iterator& p)
{
    int r;
    switch(op)   /* deal with the attributes of objects */
    {
	case Transation::OP_SETATTR: 
	    {
		coll_t cid = i.decode_cid();
		ghobject_t oid = i.decode_oid();
		string name = i.decode_attrname();
		bufferlist bl;
		i.decode_bl(bl);
		map<string, bufferptr> to_set;
		to_set[name] = bufferptr(bl.c_str(), bl.length());
		r = store->setattrs(cid, oid, to_set);
            }
	    break;
	case Transaction::OP_SETATTRS:
	    {
		coll_t cid = i.decode_cid();
		ghobject_t oid = i.decode_oid();
		map<string, bufferptr> aset;
		i.decode_attrset(aset);
		r = store->_setattrs();
	    }
	    break;
        case Transaction::OP_RMATTR:
            {
		coll_t cid = i.decode_cid();
		ghobject_t oid = i.decode_oid();
		string name = i.decode_attrname();
		r = store->_rmattr(cid, oid, name);
	    }
	    break;
	case Transaction::OP_RMATTRS:
	    {
		coll_t cid = i.decode_cid();
		ghobject_t oid = i.decode_oid();
		r = store->_rmattrs(cid, oid);
	    }
	    break;
	deafult:
	    derr << "bad op " << op << endl;
	    assert(0);
    }
    return r;
}
int NVMJournal::_touch(coll_t cid, ghobject_t oid)
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
    
    if (obj->stat == Object::REMOVED)
        return 0;

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
define _ONE_GB  \
        (1024*1024*1024)
/* do truncate */
int NVMJournal::_truncate(coll_t cid, const ghobject_t &oid, uint32_t off)
{
        ObjectRef obj = get_object(cid, oid, true);
        if (!obj || obj->stat == Object::REMOVED)
                return -ENOENT;

        BufferHead *bh = new BufferHead;
        bh->owner = obj;
        bh->ext.start = off;
        bh->ext.end = _ONE_GB;
        bh->bentry = BufferHead::TRUNC;
        bh->boff = 0;

        {
                RWLock::Locker l(obj->lock);
                merge_new_bh (obj, bh);
                store->_truncate(cid, oid, (uint64_t)off);
        }

        return 0;
}
/* _remove */
int NVMJournal::_remove(coll_t cid, const ghobject_t& oid)
{
        ObjectRef obj = get_object(cid, oid);
        // invalid the object in ssd
        if (obj) {
                BufferHead *bh = new BufferHead;
                bh->owner = obj;
                bh->ext.start = 0;
                bh->ext.end = _ONE_GB;
                bh->ext.bentry = BufferHead::ZERO;

                {
                        RWLock::Locker l(obj->lock);
                        merge_new_bh(obj, bh);
                        delete_bh(obj, 0, _ONE_GB, BufferHead::ZERO);
                        obj->stat = Object::REMOVED;
			//store->_remove(cid, oid);
                }

                delete bh;
                put_object(obj);
        }
	return 0;
}

void NVMJournal::get_objects_lock(ObjectRef a, ObjectRef b)
{
    assert (a != b);
    if (a > b) {
	get_write_lock(a);
	get_write_lock(b);
    }
    else {
	get_write_lock(b); 
	get_write_lock(a);
    }
}
void NVMJournal::put_objects_lock(ObjectRef a, ObjectRef b)
{
    assert (a != b);
    put_write_lock(a);
    put_write_lock(b);
}
// FIXME
int NVMJournal::_clone(coll_t cid, const ghobject_t& src, const ghobject_t *dst, bool update_store)
{
    CollectionRef coll = get_collection(cid);
    ObjectRef src_obj = get_object(cid, src);
    ObjectRef dst_obj = NULL;
    int ret = 0;
    if(src_obj) {
	dst_obj = get_object(cid, dst, true);
	get_objects_lock(src_obj, dst_obj);
    }

    if (update_store) 
    {
	if (src_obj && src_obj->stat == Object::DIRTY) 
	{
	    map<uint32_t, BufferHead *>::iterator p = src_obj->data.begin();
	    while (p != src_obj->data.end()) 
	    {
		BufferHead *pbh = *p->second;
		uint64_t pos = pbh->bentry;
		uint64_t off = pbh->ext.start;
		size_t len = pbh->ext.end - off;

		if (pos == BufferHead::TRUNC) {
		    /* do nothing */
		}
		else if (pos == BufferHead::ZERO) {
		    store->_zero(cid, oid, off, len);
		}
		else {
		    bool need_to_write = false;
		    pos = pos << CEPH_PAGE_SHIFT;
		    if (write_pos >= pos && pos >= data_sync_pos) 
			need_to_write = true;
		    else if (!(write_pos < pos && pos < data_sync_pos))
			need_to_write = true;
		    if (need_to_write) {
			/* bufferhead -> store */
		    }
		}
		++ p;
	    } // while
	    obj->stat = Object::CLEAN;
	}
	ret = store->_clone(cid, oid, noid);	
    }
    
    if (ret != 0) {
        put_objects_lock(src_obj, dst_obj);
        goto out;
    }

    // create a new_src_object whose parent is src_object
    // src_object is a readonly object now, the dst object and new src object
    // share iis data1

    // we should never waitting for lock of an object when we hold lock of a collection
    Mutex::Locker l(coll->lock);     

    if (src_obj && !src_obj->data.empty() && dst_obj) 
    {
	erase_object_without_lock(coll, src_obj);
        ObjectRef new_src_obj = get_object(coll, src); // hold ref because of its parent
        new_src_obj->parent = src_obj;
        src_obj->get(); // for src_obj's added child
        dst_obj->parent = src_obj;
        // the ref count equal to the number of child object + number of bufferhead + parent
        src_obj->get(); // for src_obj's added child
        dst_obj->get(); // for dst_obj's parent
    }

    put_objects_lock(src_obj, dst_obj);

out:
    put_object(src_obj);
    put_object(dst_obj);
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
		if (!bh->owner) {
		    // dec the reference of obj
		    // put_object(obj);
		    delete bh;
		}
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

		if (!bh->owner) {
		    // put_object(obj);
		    delete bh;
		}
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

void NVMJournal::build_read(coll_t &cid, ghobject_t &oid, uint64_t off, size_t len, ReadOp &op) 
{
    // keep reference of object
    op.obj = get_object(cid, oid);
    op.off = off;
    op.length = len;

    if (!op.obj) {
	bufferptr ptr(len);
	op.miss[off] = len;
	return;
    }

    ObjectRef obj = op.obj;
    get_read_lock(obj);
    map<uint32_t,BufferHead*>::iterator p = obj->data.lower_bound(off);
    if (p != obj->data.begin())
	p--;

    uint32_t end = off + len;

    while (p != obj->data.end()) {
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
		      op.trans[off] = SSD_OFF(pbh) + (off - pbh->ext.start);
                else 
                      op.trans[off] = pbh->bentry << CEPH_PAGE_SHIFT;

		op.hit[off] = pbh->ext.end - off;

		off = pbh->ext.end;
		p++;
		continue;
	    }
            // _bh_off_, off, end, _bh_end_     
	    else { 
                if (pbh->bentry != BufferHead::ZERO && pbh->bentry != BufferHead::TRUNC)
                      op.trans[off] = SSD_OFF(pbh) + (off - pbh->ext.start);
                else 
                      op.trans[off] = pbh->entry << CEPH_PAGE_SHIFT;

		op.hit[off] = end - off;
		return;
	    }
	} 
        else {
	    if (end <= pbh->ext.start) {
		op.miss[off] = end - off;
		return;
	    }
	    // off, _bh_off_, _bh_end_, end OR off, _bh_off_, end, _bh_end_
	    op.miss[off] = pbh->ext.start - off;
	    off = pbh->ext.start;
	    p ++;
	}
    }

    if (off < end)
	op.miss[off] = end-off;

    if (obj->parent && !op.miss.empty()) {
        ObjectRef parent = obj->parent;
        build_read_from_parent(parent, obj, op);
    }
}
void NVMJournal::build_read_from_parent(ObjectRef parent, ObjectRef obj, ReadOp& op)
{
        if (0 = parent)
                return ;
        if (parent->data.empty()) {
                obj.parent = NULL;
                put_object(parent);
                return ;
        }

        {
                Mutex::Locker l(parent->lock);
                map<uint32_t, uint32_t>::iterator p = op->miss.begin();
                while (p != op->miss.end()) {

                }

                if (parent->parent && op.miss.empty()) {
                        build_read_from_parent(parent->parent, parent, op);
                }
        }
}

void NVMJournal::do_read(ReadOp &op)
{
    map<uint32_t, bufferptr> data;

    for (map<uint32_t, uint32_t>::iterator p = op.hit.begin();
	    p != op.hit.end(); 
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
            pb.zero();
            data[p->front] = bp;
        }
        else {
	    map<uint32_t, uint32_t>::iterator next = p;
	    ++ next;
	    assert(next == op.hit.end());
            assert(bentry == BufferHead::TRUNC);
            /* do nothing */
        } 
    }

    put_read_lock(op.obj);
    put_object(op.obj);
    
    map<uint32_t, uint32_t>::iterator p = op.miss.begin();
    while(p != op.miss.end()) {
	// FIXME: should get missing blocks from memstore ...
	data[p->first] = bufferptr(p->second);
    }

    map<uint32_t, bufferptr>::iterator q = data.begin();
    while (q != data.end()) {
	op.buf.append(p->second);
	++ q;
    }
}

void NVMJournal::read_object(coll_t &cid, ghobject_t &oid, uint64_t off, size_t& len, bufferlist &buf)
{
    ReadOp op;
    build_read(cid, oid, off, len, op);
    do_read(op);
    buf.swap(op.buf);
    len = buf.length();
}


/* backgroud evict thread */
/* TLS : every WORK thread has its own PIPE */
NVMJournal::TLS::TLS() 
{
    pthread_key_create(&thread_pipe_key, put_value);
}

void* NVMJournal::TLS::get_value()
{
    int *fds = (int *)pthread_getspecific (thread_pipe_key);
    if (!fds) {
        fds = new int[2];
        assert(fds);
        assert(::pipe(fds) == 0);

        ::fcntl(fds[1], F_SETPIPE_SZ, 4*1024*1024);
        ::fcntl(fds[0], F_SETFL, O_NONBLOCK);
        ::fcntl(fds[1], F_SETFL, O_NONBLOCK);
        pthread_setspecific (thread_pipe_key, fds);
    }
    return fds;
}

void NVMJournal::TLS::put_value(void *value) 
{
    int *fds = (int *)value;
    if (fds) {
        close (fds[0]);
        close (fds[1]);
    }
    delete[] fds;
}

void NVMJournal::evict_entry()
{
    static uint32_t synced = 0, cur = 0;
    uint64_t seq;
    utime_t interval;

    interval.set_from_double(1.0);
    while (true) 
    {
        // check if any eviction have completed
	check_ev_completion();
	{
	    Mutex::Locker l(evict_lock);
	    if (ev_stop)
		return;
	    if (!should_evict()) {
		evict_cond.WaitInterval(g_ceph_context, evict_lock, interval);
		continue;
	    }
	}

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
		continue;
	    }

            to_reclaim.push_back(pbh);

            if (obj2bh.find(obj) == obj2bh.end()) {
                obj->get();
                obj2bh[obj];
            }
            obj2bh[obj].push_back(pbh);
	}
	
	seq = ev_seq ++;
	evicting_bh[seq].swap(to_reclaim);

        uint32_t ops = obj2bh.size();
        // reserve the evict op
        apply_manager.op_apply_start(ops);

	map<ObjectRef, deque<BufferHead *> >::iterator it = obj2bh.begin();

	while (it != obj2bh.end()) 
	{
	    ObjectRef obj = it->first;
	    if (!it->second.empty()) 
	    {
		Ev *ev = new Ev(obj, it->second);
                // mark the last evict task
                if (obj2bh.size() == 1) {
                    ev->synced = synced;
                    ev->seq = seq;
                }
                // using default_osr, actually we need no osr here
                // but the threadpool need it 
		queue_ev(&default_osr, ev); 
		running_ev.push_back(ev);
	    }
	    put_object(obj); // dec reference of object
	}
    }
}

void NVMJournal::check_ev_completion(void)
{
    uint64_t new_completed_ev = 0;
    uint32_t new_synced = 0;
    deque<Ev*>::iterator it = running_ev.begin();
    while (it != running_ev.end() && (*it)->done) {
	Ev *ev = *it;
	if (ev->seq) {
	    new_completed_ev = ev->seq;
	    new_synced = ev->synced;
	}
	running_ev.erase(it++);
	delete ev;
    }

    if (new_completed_ev) {
	map<uint64_t, deque<BufferHead*> >::iterator p = evicting_bh.begin();
	Mutex::Locker l(waiter_lock);
	while (p->first <= new_completed_ev) {
	    reclaim_queue.insert(reclaim_queue.end(), p->second.begin(), p->second.end());
	    evicting_bh.erase(++p);
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
void NVMJournal::do_ev(Ev *ev, ThreadPool::TPHandle *handle) 
{
    ObjectRef obj = ev->obj;
    deque<BufferHead *>::iterator p = ev->queue.begin();
    static const int flags = SPLICE_F_NONBLOCK;
    int *fds = (int *)ThreadLocalPipe.get_value();
    assert(fds);

    RWLock::WLocker l(obj->lock);

    if (obj->stat == Object::CLEAN)
        goto OUT;

    while (p != ev->queue.end()) {
	BufferHead *bh = *p;
        // check the object 

        bool valid = false; 
        map<uint32_t, BufferHead *>::iterator t = obj->data.lower_bound(start);
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
                if (bh2->bentry == bh->bentry) {
                    if (bh->bentry != BufferHead::ZERO && bh->bentry == BufferHead::TRUNC)
                    {
                        loff_t ssd_off = bh2->bentry << CEPH_PAG E_SHIFT + bh2->boff;
                        loff_t obj_off = bh2->ext.start;
                        uint32_t len = bh2->ext.end - bh2->ext.start;
                        // don't need to worry about the page align problem
                        // even though ssd_fd was opened with O_DIRECT
                        ssize_t r = safe_splice(fd, &ssd_off, fds[1], NULL, len, flags);
                        assert(r == len);

                        // FIXME .../
                        // out_fd = store->open(obj->coll, obj->oid);
                        // r = safe_splice(fds[0], NULL, out_fd, obj_off, flags);
                        valid = true;
                    }
                    else if (bh->bentry == BufferHead::ZERO) 
                    {
                        // FIXME
                        // store->zero(obj->coll, obj->oid);
                    }
                    else if (bh->bentry == BufferHead::TRUNC)
                    {
                        /* do nothing */
                    }
                }
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
    // wake up evitor ...
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
	Mutex::Locker l(sync_mutex);
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

