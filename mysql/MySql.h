#pragma once

#include <cassert>
#include <cstdint>
#include <cstring>
#include <mysql/mysql.h>

class MySql
{
public:

	MySql(
		const char* const hostname,
		const char* const username,
		const char* const password,
		const char* const database,
		const uint16_t port = 3306,
		const uint32_t connect_timeout = 0);

	MySql(
		const char* hostname,
		const char* username,
		const char* password,
		const uint16_t port = 3306);

	~MySql();

	MySql(const MySql& rhs) = delete;
	MySql(MySql&& rhs) = delete;
	MySql& operator=(const MySql& rhs) = delete;
	MySql& operator=(MySql&& rhs) = delete;


private:
	void Connect(const char* const hostname,
		const char* const username,
		const char* const password,
		const char* const database,
		const uint16_t port = 3306,
		const uint32_t connect_timeout = 0);

private:
	MYSQL* connection_;
};

