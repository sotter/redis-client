/*****************************************************************************
 Name        : redis_pool.h
 Author      : tianshan
 Date        : 2015年10月23日
 Description : 使用时用imredis.h接口

 由小到大分为四级:

 ->RedisConn      （单个redis连接）
 ->RedisConnPool  （同一个redis的连接池，其中包含主redis连接池和从redis连接池）
 ->RedisCluster   （多个redis组成集群，存储同一种业务， 根据hash分发)
 ->RedisCache     （区分不同存储业务的多个redis集群）

只在最上一级 RedisCache的checkout和checkin加锁；
1. 根据cachename获取到哪一个redis的连接池；
2. 从连接池根据hash散列取出一个连接簇；之前的场景相当于现在的一个特例，即Redispool只有一个元素；
3. 从连接簇找出一个可用的RedisConn返回给用户使用；

网络异常：目前不通过单独的线程重连，重连依赖业务线程；单独的线程的监测和重连，放到后续实现吧；
1. 断开： 如果在用户在外面监测是网络异常，RedisConn中设置为断开状态
2. 重连： RedisCluster获取没有时重连一次； 可能存在问题：当连接不上时，每一条消息过来都先连接一次redis，可以加入时间间隔；
 ******************************************************************************/

#ifndef SRC_BASE_REDIS_POOL_H_
#define SRC_BASE_REDIS_POOL_H_

#include <string>
#include <list>
#include <vector>
#include <map>
#include <stdint.h>
#include <sstream>
#include <iostream>
#include "hiredis/hiredis.h"

using namespace std;


#define REDIS_CACHE     "redis-cache"

class RedisConn;
class RedisConnPool;
class RedisCluster;
class RedisCache;

struct ConnConfig
{
	ConnConfig():_port(0), _slave_port(0) {};

	string _ip;
	unsigned short _port;

	string _slave_ip;
	unsigned short _slave_port;

	string to_string()
	{
		char buffer[64] = {0};
		snprintf(buffer, sizeof(buffer) - 1, "master:%s:%d,slave:%s:%d",
				_ip.c_str(), _port, _slave_ip.c_str(), _slave_port);
		return buffer;
	}
};

typedef ConnConfig  config_conn_t;
typedef vector<ConnConfig> config_list_t;
typedef vector<ConnConfig>::iterator  config_list_iter;

//单个redis连接，暂不支持主从；
class RedisConn
{
public:

//重连的时间间隔为5s
#define MIN_CONN_INTERVAL   5

	RedisConn(const char *ip, unsigned short port) :
		_redis_type(0),
		_redis_ip(ip),
		_redis_port(port),
		_is_connect(false),
		_last_connect_time(0),
		_redis_conn(NULL),
		_owner(NULL){
	}

	~RedisConn();

	//force true: 是否强制重连；false：等待一定的时间间隔后才重连，否则不重连；
	void reconnect(bool force = false);

	bool isconn();

	void set_disconn();

	bool incr(const std::string &key, int64_t &ret);

	bool lpush(const std::string &key, const std::string &value, int64_t &ret);

	bool ltrim(const std::string &key, int begin, int end);

	bool lrange(const std::string &key, int64_t begin, int64_t limit, std::vector<std::string> &ret);

	bool expire(const std::string &key, int timeout);

	//!!!注意：对于一个不是从Redis分配出来的RedisConn，如果调用此接口会把自己删除掉;
	void checkin();

	void set_owner(RedisConnPool *cluster)
	{
		_owner = cluster;
	}

	int get_redis_type() {
		return _redis_type;
	}

	void set_redis_type(int type) {
		_redis_type = type;
	}

	string get_info() {
		std::ostringstream oss;
		oss << _redis_ip << ":" << _redis_port;
		return oss.str();
	}
private:

	bool get_int(int64_t &ret, const char *format, ...);

	bool get_string(string &ret, const char *format, ...);

	bool get_array(vector<string> &ret, const char *format, ...);

	string get_ip(){
		return _redis_ip;
	}

	unsigned short get_port(){
		return _redis_port;
	}
private:
	int             _redis_type;      //0:master 1:slave;
	string          _redis_ip;
	unsigned short  _redis_port;
	bool            _is_connect;
	time_t          _last_connect_time;
	redisContext*   _redis_conn;
	RedisConnPool*  _owner;         //自己的所有者，方便回收；
};

//一台Redis实体，每个实体可以有多个连接；
class RedisConnPool
{
public:

//每个redis实例的最大连接数， 按需分配;
#define REDIS_CLUSTER_MAX_CONN 32
#define MASTER 0
#define SLAVE  1

	RedisConnPool(config_conn_t config): _redis_config(config), _size(0) {}

	virtual ~RedisConnPool();

	//checkout 出来
	RedisConn *checkout();

	void checkin(RedisConn *conn);

private:

	//从master或slave的连接池中获取一个
	RedisConn *checkout(bool master);

private:
	//为什么采用list，方便checkout和checkin;
	//0号list：主redis的连接池 1号：从redis的连接池
	list<RedisConn *> _redis_conns[2];
	config_conn_t     _redis_config;
	int               _size;
};

class RedisCluster
{
public:

	virtual ~RedisCluster();

	bool init(config_list_t &config_list);

	RedisConn *checkout(const char *key);

	//此接口实际上可以是没用的
	void checkin(RedisConn *conn);

private:

	vector<RedisConnPool *> _redis_pool;
};

class RedisCache
{
public:
	RedisCache() : _init(false) {}

	virtual ~RedisCache();

	bool init();

	bool init(map<string, string> &configs);


	RedisConn *checkout(const char *cachename, const char *key);

	void checkin(RedisConn *conn);

private:

	config_list_t parse_config(const char *config);

private:

	bool       					_init;
	map<string, RedisCluster*> 	_redis_cache;
};

#endif /* SRC_BASE_REDIS_POOL_H_ */
