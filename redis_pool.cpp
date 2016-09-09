/*****************************************************************************
 Name        : redis_pool.cpp
 Author      : tianshan
 Date        : 2015年10月23日
 Description : 
 ******************************************************************************/

#include <string.h>
#include <stdlib.h>
#include "redis_pool.h"
//#include "imconf.h"

//#define LOG_MOD_ID "redis"
//#include "comlog.h"

#define LOG_FSYS(fmt, args...)          printf(fmt"\n", ##args)
#define LOG_ESYS(fmt, args...)          printf(fmt"\n", ##args)
#define LOG_WSYS(fmt, args...)          printf(fmt"\n", ##args)
#define LOG_ISYS(fmt, args...)          printf(fmt"\n", ##args)
#define LOG_DSYS(fmt, args...)          printf(fmt"\n", ##args)

#define FREE_REDIS(p)  if ( p != NULL ) { redisFree( p) ; p = NULL ; }

static uint32_t hash_seed = 5381;
static uint32_t str_hash(const void *key, int len)
{
	/* 'm' and 'r' are mixing constants generated offline.
	 They're not really 'magic', they just happen to work well.  */
	uint32_t seed = hash_seed;
	const uint32_t m = 0x5bd1e995;
	const int r = 24;

	/* Initialize the hash to a 'random' value */
	uint32_t h = seed ^ len;

	/* Mix 4 bytes at a time into the hash */
	const unsigned char *data = (const unsigned char *) key;

	while (len >= 4) {
		uint32_t k = *(uint32_t*) data;

		k *= m;
		k ^= k >> r;
		k *= m;

		h *= m;
		h ^= k;

		data += 4;
		len -= 4;
	}

	/* Handle the last few bytes of the input array  */
	switch (len) {
	case 3:
		h ^= data[2] << 16;
	case 2:
		h ^= data[1] << 8;
	case 1:
		h ^= data[0];
		h *= m;
	};

	/* Do a few final mixes of the hash to ensure the last few
	 * bytes are well-incorporated. */
	h ^= h >> 13;
	h *= m;
	h ^= h >> 15;

	return h;
}

static void strsplit(const std::string &str, const std::string &separators, std::vector<std::string> & str_list,
		bool skip /* = false*/)
{
	str_list.clear();

	size_t head = 0;
	size_t tail = str.find_first_of(separators, head);

	for (; tail != str.npos; head = tail + 1, tail = str.find_first_of(separators, head)) {
		if (tail == head && skip) {
			continue;
		}
		str_list.push_back(str.substr(head, tail - head));
	}

	if ((!skip && head > 0) || str.substr(head).size() > 0) {
		str_list.push_back(str.substr(head));
	}

	return;
}

RedisConn::~RedisConn()
{
	FREE_REDIS(_redis_conn);

}

void RedisConn::reconnect(bool force)
{
	time_t now = time(0);

	//如果不是强制连接，每隔MIN_CONN_INTERVAL 连接一次
	if (!force) {
//		printf("test reconnect %x!!!!!\n", this);
		if (now - _last_connect_time < MIN_CONN_INTERVAL) {
			return ;
		} else {
			_last_connect_time = now;
		}
	}

	if (_is_connect) {
		//todo : 如果发现在线，重新ping一次，如果还是在线，直接返回；
		LOG_DSYS("Redis already connected");
		return;
	}

	FREE_REDIS(_redis_conn);

	struct timeval tv = { 10, 1000 };
	struct timeval timeout = { 0, 500000 }; // 0.5 seconds

	redisContext *c = redisConnectWithTimeout((char*) _redis_ip.c_str(), _redis_port, timeout);

	if (c == NULL) {
		LOG_WSYS("redisConnectWithTimeout %s %d fail", _redis_ip.c_str(), _redis_port);
		return;
	}

	if (c->err) {
		LOG_WSYS("Redis Connect %s:%d error:%s", _redis_ip.c_str(), _redis_port, c->errstr);
		FREE_REDIS(c);
		return;
	}

	//Redis超时时间10s
	if (redisSetTimeout(c, tv) != REDIS_OK) {
		LOG_WSYS("redisSetTimeout %s:%d error:%s", _redis_ip.c_str(), _redis_port, c->errstr);
		FREE_REDIS(c);
		return;
	}

	_redis_conn = c;
	_is_connect = true;
	return;
}

bool RedisConn::isconn()
{
	return _is_connect;
}

void RedisConn::set_disconn()
{
	_is_connect = false;
	FREE_REDIS(_redis_conn);
}

bool RedisConn::incr(const std::string &key, int64_t &ret)
{
	return get_int(ret, "INCR %s", key.c_str());
}

bool RedisConn::lpush(const std::string &key, const std::string &value, int64_t &ret)
{
	return get_int(ret, "LPUSH %s %s", key.c_str(), value.c_str());
}

bool RedisConn::ltrim(const std::string &key, int begin, int end)
{
	string ret;
	get_string(ret, "LTRIM %s %d %d", key.c_str(), begin, end);

	return ret == "OK";
}

bool RedisConn::lrange(const std::string &name, int64_t begin, int64_t limit, std::vector<std::string> &ret)
{
	return get_array(ret, "LRANGE %s %d %d", name.c_str(), begin, limit);
}

bool RedisConn::expire(const std::string &key, int timeout)
{
	int64_t ret;
	get_int(ret, "EXPIRE %s %d", key.c_str(), timeout);
	return ret > 0;
}

// 取得字符串型的值
bool RedisConn::get_string(std::string &val, const char *format, ...)
{
	if (!_is_connect)
		return false;

	va_list ap;
	va_start(ap, format);
	redisReply *reply = (redisReply *) redisvCommand(_redis_conn, format, ap);
	va_end(ap);

	if (reply == NULL) {
		LOG_WSYS("Redis error %d:%s", _redis_conn->err, _redis_conn->errstr);
		//如果是因为网络问题断开的直接退出的
		//在测试中发现，如果直接把server关闭掉，返回的错误码为REDIS_ERR_EOF
		if (_redis_conn->err == REDIS_ERR_IO || _redis_conn->err == REDIS_ERR_EOF) {
			//设置关闭，等待重连
			set_disconn();
		}
		return false;
	}

	if (reply->type != REDIS_REPLY_STRING && reply->type != REDIS_REPLY_STATUS) {
		LOG_WSYS("Redis get tring type error, type:%d", reply->type);
		freeReplyObject(reply);
		return false;
	}

	val.assign(reply->str, reply->len);
	freeReplyObject(reply);

	return true;
}

// 取得数据字型
bool RedisConn::get_int(int64_t &ret, const char *format, ...)
{
	if (!_is_connect)
		return false;

	va_list ap;
	va_start(ap, format);
	redisReply *reply = (redisReply *) redisvCommand(_redis_conn, format, ap);
	va_end(ap);

	if (reply == NULL) {
		LOG_WSYS("Redis error %d:%s", _redis_conn->err, _redis_conn->errstr);
		//如果是因为网络问题断开的直接退出的
		if (_redis_conn->err == REDIS_ERR_IO || _redis_conn->err == REDIS_ERR_EOF) {
			set_disconn();
		}
		return false;
	}

	if (reply->type != REDIS_REPLY_INTEGER) {
		LOG_WSYS("Redis get integer type error, type:%d", reply->type);
		freeReplyObject(reply);
		return false;
	}

	ret = reply->integer;

	freeReplyObject(reply);

	return true;
}

bool RedisConn::get_array(vector<string> &ret, const char *format, ...)
{
	if (!_is_connect)
		return false;

	va_list ap;
	va_start(ap, format);
	redisReply *reply = (redisReply *) redisvCommand(_redis_conn, format, ap);
	va_end(ap);

	if (reply == NULL) {
		LOG_WSYS("Redis error %d:%s", _redis_conn->err, _redis_conn->errstr);
		//如果是因为网络问题断开的直接退出的
		if (_redis_conn->err == REDIS_ERR_IO || _redis_conn->err == REDIS_ERR_EOF) {
			set_disconn();
		}
		return false;
	}

	if (reply->type != REDIS_REPLY_ARRAY) {
		LOG_WSYS("Redis get tring type error, type:%d", reply->type);
		freeReplyObject(reply);
		return false;
	}

	for (size_t i = 0; i < reply->elements; i++) {
		redisReply* value_reply = reply->element[i];
		string value(value_reply->str, value_reply->len);
		//todo: 确认下value_reply是否需要释放；
		ret.push_back(value);
	}

	freeReplyObject(reply);

	return true;
}

void RedisConn::checkin()
{
	if (_owner == NULL) {
		delete this;
	}
	_owner->checkin(this);
}

RedisConnPool::~RedisConnPool()
{
	for (int i = 0; i < 2; i++) {
		list<RedisConn *>::iterator iter = _redis_conns[i].begin();

		for (; iter != _redis_conns[i].end(); ++iter) {
			delete *iter;
		}
	}
}

//checkout 出来
RedisConn *RedisConnPool::checkout()
{
	//从master连接池中获取一个连接
	RedisConn *conn = checkout(true);
	if(conn == NULL) {
		//如果master获取不到， 从slave连接池中获取一个连接;
		conn  = checkout(false);
	}

	return conn;
}

RedisConn *RedisConnPool::checkout(bool master)
{
	RedisConn *conn = NULL;

	int index = master ? MASTER : SLAVE;
	string ip = master ? _redis_config._ip : _redis_config._slave_ip;
	unsigned short port = master ? _redis_config._port : _redis_config._slave_port;

	list<RedisConn*> &redis_conns = _redis_conns[index];

	if (redis_conns.empty()) {
		conn = new RedisConn(ip.c_str(), port);
		if (conn == NULL){
			LOG_WSYS("Create RedisConn %s:%d fail", ip.c_str(), port);
			return NULL;
		}

		conn->set_owner(this);
		master ? conn->set_redis_type(0) : conn->set_redis_type(1);

		conn->reconnect();
		if (conn->isconn()) {
			return conn;
		} else {
			LOG_WSYS("Redis checkout fail %s:%d", ip.c_str(), port);
			//第一次分配出来，即使连不上也要放到_redis_cluster上，方便以后重连；
			redis_conns.push_back(conn);
			_size++;
			return NULL;
		}
	}

	conn = redis_conns.front();
	if (!conn->isconn()) {
		//发起一次重连，如果发现重连失败或是没有到重连时间间隔，直接返回失败，那么这条消息就存储失败了；
		conn->reconnect();
		if (!conn->isconn()) {
			return NULL;
		}
	}

	redis_conns.pop_front();
	_size--;

	return conn;
}

void RedisConnPool::checkin(RedisConn *conn)
{
	//不管是master还是slave，超过这个数，立马关闭掉
	if (_size >= REDIS_CLUSTER_MAX_CONN) {
		LOG_WSYS("redis %s overmax, delete conn", _redis_config.to_string().c_str());
		delete conn;
	} else {
		int index = conn->get_redis_type() == 0 ? 0 : 1;
		//不管是否处于连接状态都放回去；
		_redis_conns[index].push_back(conn);
		_size++;
	}
}

RedisCluster::~RedisCluster()
{
	for(size_t i = 0; i < _redis_pool.size(); i++) {
		delete _redis_pool[i];
	}
}

bool RedisCluster::init(config_list_t &config_list)
{
	config_list_iter iter = config_list.begin();
	for (; iter != config_list.end(); ++iter) {
		LOG_ISYS("create RedisConn %s", iter->to_string().c_str());
		RedisConnPool *cluster = new RedisConnPool(*iter);
		_redis_pool.push_back(cluster);
	}

	return !_redis_pool.empty();
}

RedisConn *RedisCluster::checkout(const char *key)
{
	int index = str_hash(key, strlen(key)) % _redis_pool.size();

	return _redis_pool[index]->checkout();
}

void RedisCluster::checkin(RedisConn *conn)
{
	if (conn != NULL) {
		conn->checkin();
	}
}

RedisCache::~RedisCache()
{
	map<string, RedisCluster*>::iterator iter = _redis_cache.begin();
	for (; iter != _redis_cache.end(); ++iter) {
		delete iter->second;
	}

	_redis_cache.clear();
}

config_list_t RedisCache::parse_config(const char *config)
{
	config_list_t ret;
	if (config == NULL) {
		return ret;
	}

	vector<string> vec_temp_0, vec_temp_1, vec_temp_2;
	ConnConfig conn_config;

	//Push=127.0.0.1:6379|127.0.0.1:6380,192.168.0.1:6379|192.168.0.1:6380

	strsplit(config, ",", vec_temp_0, true);
	for (size_t i = 0; i < vec_temp_0.size(); i++) {
		vec_temp_1.clear();

		strsplit(vec_temp_0[i], "|", vec_temp_1, true);
		for (size_t i = 0; i < vec_temp_1.size(); ++i) {
		    vec_temp_2.clear();
			strsplit(vec_temp_1[i], ":", vec_temp_2, true);
			if (vec_temp_2.size() != 2) {
				continue;
			}

			if(i == 0) {
				conn_config._ip = vec_temp_2[0];
				conn_config._port = (unsigned short) atoi(vec_temp_2[1].c_str());
			} else if(i == 1) {
				conn_config._slave_ip = vec_temp_2[0];
				conn_config._slave_port = (unsigned short) atoi(vec_temp_2[1].c_str());
			} //其他的不理会
		}

		if(!conn_config._ip.empty() || !conn_config._slave_ip.empty()){
			ret.push_back(conn_config);
		}
	}

	return ret;
}

bool RedisCache::init()
{
	map<string, string> configs;
//	string redis_cache = IMConfig::GetInstance()->GetString(REDIS_CACHE);

	vector<string> vec_cache;
//	strsplit(redis_cache, ",", vec_cache, true);

	for(size_t i = 0; i < 1; ++i) {
//		string value = IMConfig::GetInstance()->GetString(vec_cache[i].c_str());
		string value = "127.0.0.1:6379|127.0.0.1:6380";
		if(!value.empty()) {
			configs.insert(make_pair("pushmsg", value));
		}
	}

	return init(configs);

}

bool RedisCache::init(map<string, string> &configs)
{
	if(configs.empty()) {
		LOG_WSYS("Get RedisCache config fail");
		return false;
	}

	map<string, string>::iterator iter = configs.begin();
	for (; iter != configs.end(); ++iter) {
		config_list_t list_conn_confs = parse_config(iter->second.c_str());
		if (!iter->first.empty() && !list_conn_confs.empty()) {
			RedisCluster *cluster = new RedisCluster;
			cluster->init(list_conn_confs);
			_redis_cache.insert(make_pair(iter->first, cluster));
		}
	}

	if(_redis_cache.empty())
		return false;

	_init = true;
	return true;
}

RedisConn *RedisCache::checkout(const char *cachename, const char *key)
{
	if(!_init) {
		LOG_WSYS("RedisCache checkout fail : it is not init");
		return NULL;
	}

//	im::Guard g(_mutex);

	map<string, RedisCluster*>::iterator iter = _redis_cache.find(cachename);
	if (iter == _redis_cache.end()) {
		return NULL;
	}

	RedisConn * conn =  iter->second->checkout(key);
	if(conn == NULL) {
		LOG_WSYS("Redis checkout connection fail cachename:%s, key:%s", cachename, key);
	}

	return conn;
}

void RedisCache::checkin(RedisConn *conn)
{
//	im::Guard g(_mutex);

	if (conn != NULL) {
		conn->checkin();
	}
}
