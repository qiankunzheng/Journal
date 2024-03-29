# Journal
ceph osd中的objectstore封装了所有底层的IO操作，向上层模块提供事务更新接口。<br>
当前主要有3类主要的实现，即filestore，memstore和keyvaluestore，其中filestore使用本地文件系统存储数据，而memstore直接使用内存实现数据存储，keyvaluestore是对底层存储引擎的抽象，因而可以支持leveldb，Rocksdb和kinect API等。<br>

这里为osd实现了另一种存储引擎nvmstore。filestore使用journal的方式实现事务接口，每次更新数据前都会先写journal，这就造成了双重写入。利用ssd没有寻到延迟的特征，通过增加内存索引，将journal改造成一个具有写缓存功能的journal，当journal可用空间达到一个阈值时再将更新写回到文件系统，从而将对相同对象的多次写操作合并成一次写。<br>

# 代码组织结构
  NVMStore继承了ObjectStore，它实现了ObjectStore要求的对象读写接口和事务接口。<br>
  NVMStore内部包含了ObjectDataStore，ObjectAttrStore和NVMJournal模块，Object*Store主要实现对象的数据和属性的存储，NVMJournal则实现Journal功能。<br>
  NVMJournal模块中包含5个主要的线程(池)，分别是Journal的写线程(包括aio的writer和reaper线程)，事务的执行线程池，以及负责回收journal空间的线程和执行线程池。NVMJournal向NVMStore提供事务提交接口，当NVMStore将事务提交给NVMJournal之后，NVMJournal将事务放入到一个队列，writer线程将队列中的多个事务合并成一个大的aio写操作写入到Journal，reaper线程则在aio完成的时候将事务交给事务的执行线程进行执行。如果数据写入操作且当前的模式允许，则直接在内存建立指向journal的索引即可，而不用去更新文件系统上的数据。journal的回收线程定时检查是否需要回收空间，并向执行线程池发送回收任务，由执行线程将journal中有效数据写回到NVMStore的ObjectDataStore。<br>

# 使用
采用版本为0.87的ceph源码，将src/os/下的代码拷贝到ceph的相应目录中，然后按照现有的原有的方式编译安装<br>
修改ceph集群配置文件ceph.conf中的osd journal：<br>
```cpp
  osd objectstore = nvmstore
  osd journal = path_of_the_journal
```
表示使用nvmstore存储引擎<br>

# src/mytest/
该目录下使用gtest实现了一个objectstore的自动性能测试工具，通过插入lttng探针来检查每个事务的执行时间。<br>
