/*****************************************************************************
Name        : main.cpp
Author      : tianshan
Date        : 2015年10月26日
Description : 
******************************************************************************/


#include "imredis.h"
#include <string.h>


#define MAX_SAVE 45
#define MIN_SAVE 35

char value[1024] = {0};

/*

 1024个字节；

 [sotter@ redis-client]$ ./test
cnt:100000 usetime:2730610 average 2730us
 *
 */
void save_message(const char *key, const char *value)
{
	IMRedisConn redis("pushmsg", key);

	int64_t ret = 0;
	if(! redis.lpush(key, value, ret)) {
		printf("lpush fail \n");
	}

	if(ret == 1) {
		redis.expire(key, 100);
	} else if (ret >= MAX_SAVE) {
		if(!redis.ltrim(key, 0, MIN_SAVE - 1)) {
			printf("key = %s, ret = %d, rem_ret != ret - MIN_SAVE\n", key, ret);
		}
	}
	return;
}

void test_fun(int cnt)
{
	int num = cnt;

	struct timeval begin, end;
	gettimeofday(&begin, NULL);

	for(int i = 0; i < 100; i++) {
		for (int j = 0; j < num; j ++) {
			char key[32] = {0};
			snprintf(key, sizeof(key) - 1, "sotter-%d", j);
			save_message(key, value);
		}
	}

	gettimeofday(&end, NULL);

	long time = (end.tv_sec - begin.tv_sec) * 1000 * 1000 + (end.tv_usec - begin.tv_usec);
	printf("cnt:%d usetime:%ld average %ldus\n", cnt * 100, time, (time / cnt));

	return;
}


int  main()
{
	map<string, string> configs;
	configs["pushmsg"] = "127.0.0.1:6379|127.0.0.1:6380";

	memset(value, 0x31, sizeof(value) - 1);
	value[sizeof(value) - 1] = 0;

	if(!g_redis_pool.init(configs)) {
		printf("redis_cache init fail\n");
		return -1;
	} else {
		printf("redis_cache init success\n");
	}

	int i = 0;
	int num = 10;
	while(num--) {
		char key[32] = {0};
		snprintf(key, sizeof(key) - 1, "sotter-%d", i++);
		IMRedisConn redis("pushmsg", key);

		int64_t ret = 0;
		if(! redis.lpush(key, value, ret)) {
			printf("lpush fail \n");
		} else {
			printf("lpush redis:%s key:%s success!\n", redis.get_info().c_str(), key);
		}

		vector<string> test;
		string get_key = key;
		redis.lrange(get_key, 0, 100, test);
		printf("test size:%u\n", test.size());

		sleep(1);
	}
//	test_fun(1000);

	return 0;
}



