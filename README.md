# kv_storage
Distributed remote dictionary server in memory but data can be persistent in disk.

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

	集群是去中心化模式，没有主从节点，所有节点的职责是相同的。而且对客户端是透明的，只要连接上集群中任意一个节点就可以访问集群中所有数据。