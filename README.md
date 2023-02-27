# kv_storage
Distributed remote dictionary server in memory but data can be persistent in disk.

1、已实现的命令  

	string:  
		set  
		get
		mset
		mget
		msetnx
		del
	key:
		keys
		exists
		expire
		ttlss
		persist:
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
      
2、支持的数据结构

	string
	list 
	sortedSet
	
3、实现分布式集群