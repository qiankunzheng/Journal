#include <string>
#include "include/types.h"
#include "include/stringify.h"
#include "common/errno.h"
#include "NVMStore.h"

#define dout_subsys ceph_subsys_filestore
#undef dout_prefix
#define dout_prefix *_dout << __FILE__ << ": " << __func__ << ": " << __LINE__ << ": "

int NVMStore::BackStore_Imp::_setattrs(coll_t c, const ghobject_t &oid, map<string, bufferptr> &aset)
{
    return store->attr.setattrs(c, oid, aset);
}

int NVMStore::BackStore_Imp::_rmattr(coll_t c, const ghobject_t &oid, const char *name)
{
    return store->attr.rmattr(c, oid, name);
}

int NVMStore::BackStore_Imp::_rmattrs(coll_t c, const ghobject_t &oid)
{
    return store->attr.rmattrs(c, oid);
}

int NVMStore::BackStore_Imp::_collection_setattr(coll_t c, const char *name, const void *value, size_t size)
{
    return store->data.collection_setattr(c, name, value, size);
}

int NVMStore::BackStore_Imp::_collection_rmattr(coll_t c, const char *name)
{
    return store->data.collection_rmattr(c, name);
}

int NVMStore::BackStore_Imp::_omap_clear(coll_t c, const ghobject_t &oid)
{
    return store->attr.omap_clear(c, oid);
}

int NVMStore::BackStore_Imp::_omap_setheader(coll_t c, const ghobject_t &oid, const bufferlist &bl)
{
    return store->attr.omap_setheader(c, oid, bl);
}
int NVMStore::BackStore_Imp::_omap_setkeys(coll_t c, const ghobject_t &oid, const map<string, bufferlist> &aset)
{
    return store->attr.omap_setkeys(c, oid, aset);
}

int NVMStore::BackStore_Imp::_omap_rmkeys(coll_t c, const ghobject_t &oid, const set<string> &keys)
{
    return store->attr.omap_rmkeys(c, oid, keys);
}

int NVMStore::BackStore_Imp::_omap_rmkeyrange(coll_t c, const ghobject_t &oid, const string &first, const string &last)
{
    return store->attr.omap_rmkeyrange(c, oid, first, last);
}

bool NVMStore::BackStore_Imp::collection_exists(coll_t c)
{
    return store->data.collection_exists(c);
}

bool NVMStore::BackStore_Imp::exists(coll_t c, const ghobject_t &oid)
{
    return store->data.exists(c, oid);
}

int NVMStore::BackStore_Imp::_open(coll_t c, const ghobject_t &oid)
{
    return store->data.open(c, oid);
}

int NVMStore::BackStore_Imp::_touch(coll_t c, const ghobject_t &oid)
{
    int r = store->attr.touch(c, oid);
    if (!r)
	r = store->data.touch(c, oid);
    return r;
}

int NVMStore::BackStore_Imp::_zero(coll_t c, const ghobject_t &oid, uint64_t off, size_t len)
{
    return store->data.zero(c, oid, off, len);
}

int NVMStore::BackStore_Imp::_truncate(coll_t c, const ghobject_t &oid, uint64_t size)
{
    return store->data.truncate(c, oid, size);
}

int NVMStore::BackStore_Imp::_remove(coll_t c, const ghobject_t &oid)
{
    store->attr.remove(c, oid);
    store->data.remove(c, oid);
    return 0;
}

int NVMStore::BackStore_Imp::_clone(coll_t c, const ghobject_t &oo, const ghobject_t &no)
{
    store->attr.clone(c, oo, no);
    store->data.clone(c, oo, no);
    return 0;
}

int NVMStore::BackStore_Imp::_clone_range(coll_t c, const ghobject_t &oo, const ghobject_t &no, 
	uint64_t srcoff, uint64_t len, uint64_t dstoff)
{
    return store->data.clone_range(c, oo, no, srcoff, len, dstoff);
}

int NVMStore::BackStore_Imp::_create_collection(coll_t c)
{
    store->attr.create_collection(c);
    return store->data.create_collection(c);
}

int NVMStore::BackStore_Imp::_destroy_collection(coll_t c)
{
    int r = store->attr.destroy_collection(c);
    if (!r)
	r = store->data.destroy_collection(c);
    return r;
}

int NVMStore::BackStore_Imp::_collection_add(coll_t dst, coll_t src, const ghobject_t &oid)
{
    int r = store->attr.collection_add(dst, src, oid);
    if (!r)
	r = store->data.collection_add(dst, src, oid);
    return r;
}

int NVMStore::BackStore_Imp::_collection_move_rename(coll_t oc, const ghobject_t &oo, coll_t nc, const ghobject_t &no)
{
    int r = store->attr.collection_move_rename(oc, oo, nc, no);
    if (!r)
	r = store->data.collection_move_rename(oc, oo, nc, no);
    return r;
}

int NVMStore::BackStore_Imp::_collection_rename(const coll_t &c, const coll_t &nc)
{
    int r = store->attr.collection_rename(c, nc);
    if (!r)
	r = store->data.collection_rename(c, nc);
    return r;
}

int NVMStore::BackStore_Imp::_split_collection(coll_t c, uint32_t bits, uint32_t rem, coll_t dest)
{
    assert(0 == "no supported yet!");
    return 0;
}

int NVMStore::BackStore_Imp::_read(coll_t c, const ghobject_t &oid, uint64_t offset, size_t len, bufferlist &bl)
{
    return store->data.read(c, oid, offset, len, bl);
}

int NVMStore::BackStore_Imp::_write(coll_t c, const ghobject_t &oid, uint64_t offset, size_t len, 
	const bufferlist &bl, bool replica)
{
    store->attr.touch(c, oid);
    return store->data.write(c, oid, offset, len, bl);
}

int NVMStore::statfs(struct statfs *st)
{
    return data.statfs(st);
}

bool NVMStore::exists(coll_t cid, const ghobject_t &oid)
{
    return data.exists(cid, oid);
}

int NVMStore::stat(coll_t cid, const ghobject_t &oid, struct stat *st, bool allow_eio)
{
    return data.stat(cid, oid, st);
}

int NVMStore::read(coll_t cid, const ghobject_t &oid, uint64_t offset, size_t len, bufferlist &bl, bool allow_eio)
{
    size_t max = 16 << 20; // The size of object must be smaller the 1MB
    if (!len)
	len = max;
    int r = Journal->read_object(cid, oid, offset, len, bl);
    if (r < 0)
	return r;
    return bl.length();
}

int NVMStore::fiemap(coll_t cid, const ghobject_t &oid, 
	uint64_t offset, size_t len, bufferlist &bl) 
{
    assert(0 == "no supportted yet");
    return 0;
}

int NVMStore::getattr(coll_t cid, const ghobject_t &oid, const char *name, bufferptr &value)
{
    return attr.getattr(cid, oid, name, value);
}

int NVMStore::getattrs(coll_t cid, const ghobject_t &oid, map<string, bufferptr> &aset)
{
    return attr.getattrs(cid, oid, aset);
}

int NVMStore::list_collections(vector<coll_t> &ls)
{
    return data.list_collections(ls);
}

bool NVMStore::collection_exists(coll_t c)
{
    return data.collection_exists(c);
}

int NVMStore::collection_getattr(coll_t cid, const char *name,
	void *value, size_t size)
{
    return data.collection_getattr(cid, name, value, size);
}

int NVMStore::collection_getattr(coll_t cid, const char *name, bufferlist &bl)
{
    return data.collection_getattr(cid, name, bl);
}

int NVMStore::collection_getattrs(coll_t cid, map<string, bufferptr> &aset)
{
    return data.collection_getattrs(cid, aset);
}

bool NVMStore::collection_empty(coll_t cid)
{
    return data.collection_empty(cid);
}

int NVMStore::collection_list(coll_t cid, vector<ghobject_t> &o)
{
    return data.collection_list(cid, o);
}

int NVMStore::collection_list_partial(coll_t cid, ghobject_t start, 
	int min, int max, snapid_t snap,
	vector<ghobject_t> *ls, ghobject_t *next)
{
    vector<ghobject_t> lst;
    data.collection_list(cid, lst);
    set<ghobject_t> oset(lst.begin(), lst.end());
    int count = 0;
    set<ghobject_t>::iterator it = oset.lower_bound(start);
    while (it != oset.end() && count < max) {
	ls->push_back(*it);
	++ count;
	++ it;
    }
    if (it == oset.end())
	*next = ghobject_t::get_max();
    else
	*next = *it;
    return 0;
}

int NVMStore::collection_list_range(coll_t cid, ghobject_t start, ghobject_t end, 
	snapid_t seq, vector<ghobject_t> *ls)
{
    vector<ghobject_t> lst;
    data.collection_list(cid, lst);
    set<ghobject_t> oset(lst.begin(), lst.end());
    set<ghobject_t>::iterator it = oset.lower_bound(start);
    while (it != oset.end() && *it < end) {
	ls->push_back(*it);
	++ it;
    }
    return 0;
}

int NVMStore::omap_get(coll_t cid, const ghobject_t &oid, bufferlist *header, map<string, bufferlist> *out)
{
    return attr.omap_get(cid, oid, header, out);
}

int NVMStore::omap_get_header(coll_t cid, const ghobject_t &oid, bufferlist *header, bool allow_eio)
{
    return attr.omap_get_header(cid, oid, header);
}

int NVMStore::omap_get_keys(coll_t cid, const ghobject_t &oid, set<string> *keys)
{
    return attr.omap_get_keys(cid, oid, keys);
}

int NVMStore::omap_get_values(coll_t cid, const ghobject_t &oid, const set<string> &keys, map<string, bufferlist> *out)
{
    return attr.omap_get_values(cid, oid, keys, out);
}

int NVMStore::omap_check_keys(coll_t cid, const ghobject_t &oid, const set<string> &keys, set<string> *out)
{
    return attr.omap_check_keys(cid, oid, keys, out);
}

ObjectMap::ObjectMapIterator NVMStore::get_omap_iterator(coll_t cid, const ghobject_t &oid)
{
    return attr.get_omap_iterator(cid, oid);
}

int NVMStore::mount()
{
    finisher.start();
    int r = data.mount();
    if (r < 0) {
	data.umount();
	return r;
    }
    r = Journal->create();
    if (r < 0)
	data.umount();
    return r;
}

int NVMStore::umount()
{
    Journal->stop();
    data.umount();
    finisher.stop();
    return 0;
}

int NVMStore::mkfs()
{
    string fsid;
    int r = read_meta("fs_fsid", &fsid);
    if (r == -ENOENT) {
	uuid_d id;
	id.generate_random();
	fsid = stringify(id);
	r = write_meta("fs_fsid", fsid);
	if (r < 0)
	    return r;
	dout(1) << "new fsid " << fsid << dendl;
    }
    else
	dout(1) << "had fsid" << fsid << dendl;

    r = Journal->mkjournal();
    if (r < 0) {
	dout(0) << "failed to mkjournal" << dendl;
	return r;
    }
    return data.mkfs();
}

int NVMStore::mkjournal()
{
    return 0;
}

void NVMStore::set_fsid(uuid_d u)
{
    int r =  write_meta("fs_fsid", stringify(u));
    assert(r>=0);
}

uuid_d NVMStore::get_fsid()
{
    string fsid;
    int r = read_meta("fs_fsid", &fsid);
    assert(r >= 0);
    uuid_d uuid;
    bool b = uuid.parse(fsid.c_str());
    assert(b);
    return uuid;
}

int NVMStore::queue_transactions(Sequencer *osr, list<Transaction*>& tls,
	TrackedOpRef op, ThreadPool::TPHandle *handle)
{
    Journal->submit_entry(osr, tls, handle);
    return 0;
}
