# kv_storage
Distributed remote dictionary server in memory but data can be persistent in disk.

1、服务端：基于runtime.netpoll、IO多路复用和runtime.scheduler构建goroutine-per-connection 风格的简洁高性能网络模型。
2、通信协议：基于TCP连接，按照Redis的通信协议RESP的规范实现了协议解析器，支持Redis的官方客户端工具redis-cli连接。
3、内存数据存储：所有键值数据默认在内存中用并发安全的哈希表存储。
4、数据持久化：为防止服务挂掉数据丢失，可开启数据持久化功能把内存数据同步到磁盘中，该功能会异步向指定磁盘文件中写入命令执行日志，当服务挂掉重启后会重新执行已经记录的命令，在内存中构建好初始数据状态后在对外提供服务。
5、数据结构和命令：目前键值数据中的值类型支持字符串、列表、有序集合。列表使用双向链表实现，有序集合使用跳表实现。并实现了Redis中操作string、list、sortedSet、key的大部分命令。
6、集群模式：通过把单进程服务扩展为多进程并行服务并相互协调对外提供服务的方式来提高系统容量。集群是去中心化的，没有主从节点，集群中所有节点的职责是相同的。而且对客户端是透明的，只要连接上集群中任意一个节点就可以访问集群中所有数据。

1、支持的数据结构

	string
	list 
	sortedSet

2、已实现的命令  

	string:  
		set  
		get
		mset
		mget
		msetnx
	key:
		keys
		del
		exists
		expire
		ttl
		persist
		expireat
	list:
		lpush
		rpush
		llen
		lrange
		lindex
		linsert
		lrem
		ltrim
		lset
		lpop
		rpop
	sortedSet:
		zadd
		zrange
		zrevrange
		zrem
		zcard
		zcount
		zrangebyscore
		zrevrangebyscore
		zrevrank
	
3、集群模式  

	集群是去中心化模式，没有主从节点，所有节点的职责是相同的。
	而且对客户端是透明的，只要连接上集群中任意一个节点就可以访问集群中所有数据。