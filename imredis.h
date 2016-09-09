/*****************************************************************************
Name        : imredis.h
Author      : tianshan
Date        : 2015年10月27日
Description : 
******************************************************************************/

#ifndef REDIS_CLIENT_IMREDIS_H_
#define REDIS_CLIENT_IMREDIS_H_

#include "redis_pool.h"

RedisCache g_redis_pool;

struct IMRedisConn
{
public:

	IMRedisConn(const string &cache_name, const string &key)
	{
		_redis_conn = g_redis_pool.checkout(cache_name.c_str(), key.c_str());
	}

	~IMRedisConn()
	{
		if(_redis_conn != NULL) {
			g_redis_pool.checkin(_redis_conn);
			_redis_conn = NULL;
		}
	}

	bool incr(const std::string &key, int64_t incrby, int64_t &ret)
	{
		if(_redis_conn != NULL && _redis_conn->isconn()) {
			return _redis_conn->incr(key, ret);
		}

		return false;
	}

	bool lpush(const std::string &key, const std::string &value, int64_t &ret)
	{
		if(_redis_conn != NULL && _redis_conn->isconn()) {
			return _redis_conn->lpush(key, value, ret);
		}

		return false;
	}

	bool ltrim(const std::string &key, int begin, int end)
	{
		if(_redis_conn != NULL && _redis_conn->isconn()) {
			return _redis_conn->ltrim(key, begin, end);
		}
		return false;
	}

	bool lrange(const std::string &key, int64_t begin, int64_t limit, std::vector<std::string> &ret)
	{
		if(_redis_conn != NULL && _redis_conn->isconn()) {
			return _redis_conn->lrange(key, begin, limit, ret);
		}
		return false;
	}

	bool expire(const std::string &key, int timeout)
	{
		if(_redis_conn != NULL && _redis_conn->isconn()) {
			return _redis_conn->expire(key, timeout);
		}
		return false;
	}

	string get_info()
	{
		return (_redis_conn != NULL) ? _redis_conn->get_info() : "";
	}

private:
	RedisConn *_redis_conn;
};




#endif /* REDIS_CLIENT_IMREDIS_H_ */
