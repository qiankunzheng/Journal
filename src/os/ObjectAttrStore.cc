#include "include/types.h"
#include "include/stringify.h"
#include "include/unordered_map.h"
#include "ObjectAttrStore.h"

int ObjectAttrStore::create_collection(const coll_t &c)
{
    RWLock::WLocker l(coll_lock);
    ceph::unordered_map<coll_t, CollectionRef>::iterator it = coll_map.find(c);
    if (it!=coll_map.end())
	return -EEXIST;
    coll_map[c].reset(new Collection);
}

int ObjectAttrStore::destroy_collection(const coll_t &c)
{
    RWLock::WLocker l(coll_lock);
    ceph::unordered_map<coll_t, CollectionRef>::iterator it = coll_map.find(c);
    if (it==coll_map.end())
	return -ENOENT;
    {
	RWLock::RLocker l2(it->second->lock);
	if (!it->second->object_map.empty())
	    return -ENOTEMPTY;
    }
    coll_map.erase(it);
    return 0;
}

int ObjectAttrStore::collection_add(const coll_t &dst, const coll_t &src, const ghobject_t &oid)
{
    CollectionRef c = get_collection(src);
    if (!c)
	return -ENOENT;
    CollectionRef nc = get_collection(dst);
    if (!nc)
	return -ENOENT;
    RWLock::WLocker l1(MIN(&(*c), &(*nc))->lock);
    RWLock::WLocker l2(MAX(&(*c), &(*nc))->lock);
    if (c->object_hash.count(oid) == 0)
	return -ENOENT;
    if (nc->object_hash.count(oid))
	return -EEXIST;
    ObjectRef obj = c->object_hash[oid];
    nc->object_hash[oid] = obj;
    nc->object_map[oid] = obj;
    return 0;
}

int ObjectAttrStore::collection_move_rename(const coll_t &oc, const ghobject_t &oo, const coll_t &nc, const ghobject_t &no)
{
    CollectionRef ocoll = get_collection(oc);
    if (!ocoll)
	return -ENOENT;
    CollectionRef ncoll = get_collection(nc);
    if (!ncoll)
	return -ENOENT;
    ceph::shared_ptr<RWLock::WLocker> l1,l2;
    if (&(*ocoll) == &(*ncoll))
	l1.reset(new RWLock::WLocker(ocoll->lock));
    else {
	l1.reset(new RWLock::WLocker(ocoll->lock));
	l2.reset(new RWLock::WLocker(ncoll->lock));
    }
    if (ocoll->object_hash.count(oo) == 0)
	return -ENOENT;
    if (ncoll->object_hash.count(no))
	return -EEXIST;
    ObjectRef obj = ocoll->object_hash[oo];
    ncoll->object_hash[no] = obj;
    ncoll->object_map[no] = obj;
    ocoll->object_hash.erase(oo);
    ocoll->object_map.erase(oo);
    return 0;
}

int ObjectAttrStore::collection_rename(const coll_t &c, const coll_t &nc)
{
    assert(c != nc);
    RWLock::WLocker l(coll_lock);
    if (coll_map.count(c) == 0)
	return -ENOENT;
    if (coll_map.count(nc))
	return -EEXIST;
    coll_map[nc] = coll_map[c];
    coll_map.erase(c);
    return 0;
}

int ObjectAttrStore::touch(const coll_t &cid, const ghobject_t &oid)
{
    CollectionRef coll = get_collection(cid);
    if (!coll)
	return -ENOENT;
    RWLock::WLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj) {
	obj.reset(new Object);
	coll->object_hash[oid] = obj;
	coll->object_map[oid] = obj;
    }
    return 0;
}

int ObjectAttrStore::remove(const coll_t &cid, const ghobject_t &oid)
{
    CollectionRef coll = get_collection(cid);
    if (!coll)
	return -ENOENT;
    RWLock::WLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    coll->object_hash.erase(oid);
    coll->object_map.erase(oid);
    return 0;
}

int ObjectAttrStore::clone(const coll_t &cid, const ghobject_t &oo, const ghobject_t &no)
{
    CollectionRef coll = get_collection(cid);
    if (!coll)
	return -ENOENT;
    RWLock::WLocker l(coll->lock);
    ObjectRef src = coll->get_object(oo);
    if (!src)
	return -ENOENT;
    ObjectRef dst = coll->get_object(no);
    if (!dst) {
	dst.reset(new Object);
	coll->object_hash[no] = dst;
	coll->object_map[no] = dst;
    }
    dst->omap_header = src->omap_header;
    dst->omap = src->omap;
    dst->xattr = src->xattr;
    return 0;
}

int ObjectAttrStore::setattrs(const coll_t &c, const ghobject_t &oid,const map<string, bufferptr> &aset)
{
    CollectionRef coll = get_collection(c);
    if (!coll)
	return -ENOENT;
    RWLock::WLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    for (map<string, bufferptr>::const_iterator it = aset.begin();
	    it != aset.end();
	    ++it) {
	obj->xattr[it->first] = it->second;
    }
    return 0;
}

int ObjectAttrStore::rmattr(const coll_t &c, const ghobject_t &oid, const char *name)
{
    CollectionRef coll = get_collection(c);
    if (!coll)
	return -ENOENT;
    RWLock::WLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    if (!obj->xattr.count(name))
	return -ENODATA;
    obj->xattr.erase(name);
    return 0;
}
int ObjectAttrStore::rmattrs(const coll_t &c, const ghobject_t &oid)
{
    CollectionRef coll = get_collection(c);
    if (!coll)
	return -ENOENT;
    RWLock::WLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    obj->xattr.clear();
    return 0;
}

int ObjectAttrStore::getattr(const coll_t &c, const ghobject_t &oid, const char *name, bufferptr &value)
{
    CollectionRef coll = get_collection(c);
    if (!coll)
	return -ENOENT;
    RWLock::RLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    string k(name);
    if (!obj->xattr.count(k))
	return -ENODATA;
    value = obj->xattr[k];
    return 0;
}

int ObjectAttrStore::getattrs(const coll_t &c, const ghobject_t &oid, map<string, bufferptr> &aset)
{
    CollectionRef coll = get_collection(c);
    if (!coll)
	return -ENOENT;
    RWLock::RLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    aset = obj->xattr;
    return 0;
}

int ObjectAttrStore::omap_clear(const coll_t &c, const ghobject_t &oid)
{
    CollectionRef coll = get_collection(c);
    if (!coll)
	return -ENOENT;
    RWLock::WLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    obj->omap.clear();
    return 0;
}

int ObjectAttrStore::omap_setkeys(const coll_t &c, const ghobject_t &oid, const map<string, bufferlist> &aset)
{
    CollectionRef coll = get_collection(c);
    if (!coll)
	return -ENOENT;
    RWLock::WLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    for(map<string, bufferlist>::const_iterator it = aset.begin();
	    it != aset.end();
	    ++ it) {
	obj->omap[it->first] = it->second;
    }
    return 0;
}

int ObjectAttrStore::omap_rmkeys(const coll_t &c, const ghobject_t &oid, const set<string> &keys)
{
    CollectionRef coll = get_collection(c);
    if (!coll)
	return -ENOENT;
    RWLock::WLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    for(set<string>::const_iterator it = keys.begin();
	    it != keys.end();
	    ++ it) {
	obj->omap.erase(*it);
    }
    return 0;
}

int ObjectAttrStore::omap_rmkeyrange(const coll_t &c, const ghobject_t &oid, const string &first, const string &last)
{
    CollectionRef coll = get_collection(c);
    if (!coll)
	return -ENOENT;
    RWLock::WLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    map<string, bufferlist>::iterator p = obj->omap.upper_bound(first);
    map<string, bufferlist>::iterator end = obj->omap.lower_bound(last);
    while(p != end)
	obj->omap.erase(p++);
    return 0;
}

int ObjectAttrStore::omap_setheader(const coll_t &c, const ghobject_t &oid, const bufferlist &bl)
{
    CollectionRef coll = get_collection(c);
    if (!coll)
	return -ENOENT;
    RWLock::WLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    obj->omap_header = bl;
    return 0;
}

int ObjectAttrStore::omap_get_header(const coll_t &c, const ghobject_t &oid, bufferlist *header)
{
    CollectionRef coll = get_collection(c);
    if (!coll)
	return -ENOENT;
    RWLock::RLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    *header = obj->omap_header;
    return 0;
}

int ObjectAttrStore::omap_get(const coll_t &c, const ghobject_t &oid, bufferlist *header, map<string, bufferlist> *out)
{
    CollectionRef coll = get_collection(c);
    if (!coll)
	return -ENOENT;
    RWLock::RLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    *header = obj->omap_header;
    *out = obj->omap;
    return 0;
}
int ObjectAttrStore::omap_get_keys(const coll_t &c, const ghobject_t &oid, set<string> *keys)
{
    CollectionRef coll = get_collection(c);
    if (!coll)
	return -ENOENT;
    RWLock::RLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    for(map<string, bufferlist>::const_iterator it = obj->omap.begin();
	    it != obj->omap.end();
	    ++ it)
	keys->insert(it->first);
    return 0;
}

int ObjectAttrStore::omap_get_values(const coll_t &c, const ghobject_t &oid, const set<string> &keys, map<string, bufferlist> *out)
{
    CollectionRef coll = get_collection(c);
    if (!coll)
	return -ENOENT;
    RWLock::WLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    for(set<string>::const_iterator it = keys.begin();
	    it != keys.end();
	    ++it) {
	map<string, bufferlist>::iterator p = obj->omap.find(*it);
	if (p!=obj->omap.end())
	    out->insert(*p);
    }
    return 0;
}
int ObjectAttrStore::omap_check_keys(const coll_t &c, const ghobject_t &oid, const set<string> &keys, set<string> *out)
{
    CollectionRef coll = get_collection(c);
    if (!coll)
	return -ENOENT;
    RWLock::WLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return -ENOENT;
    for(set<string>::const_iterator it = keys.begin();
	    it != keys.end();
	    ++it) {
	map<string, bufferlist>::iterator p = obj->omap.find(*it);
	if (p!=obj->omap.end())
	    out->insert(p->first);
    }
    return 0;
}

ObjectMap::ObjectMapIterator ObjectAttrStore::get_omap_iterator(const coll_t &cid, const ghobject_t &oid)
{
    CollectionRef coll = get_collection(cid);
    if (!coll)
	return ObjectMap::ObjectMapIterator();
    RWLock::WLocker l(coll->lock);
    ObjectRef obj = coll->get_object(oid);
    if (!obj)
	return ObjectMap::ObjectMapIterator();
    return ObjectMap::ObjectMapIterator(new OmapIteratorImpl(coll, obj));
}
