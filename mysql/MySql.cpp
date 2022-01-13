#include "MySql.h"
#include "MySqlException.h"

MySql::MySql(
	const char* const hostname,
	const char* const username,
	const char* const password,
	const char* const database,
	const uint16_t port /*= 3306*/,
	const uint32_t connect_timeout/*=0*/)
	: connection_(mysql_init(nullptr))
{
	Connect(hostname, username, password, database, port, connect_timeout);
}

MySql::MySql(const char* hostname, const char* username, const char* password, const uint16_t port /*= 3306*/)
	: MySql(hostname, username, password, nullptr, port)
{

}

MySql::~MySql()
{
	mysql_close(connection_);
}

void MySql::Connect(
	const char* const hostname,
	const char* const username,
	const char* const password,
	const char* const database,
	const uint16_t port /*= 3306*/,
	const uint32_t connect_timeout/*=0*/)
{
	if (nullptr == connection_)
	{
		throw MySqlException("Unable to connect to MySQL");
	}

	//mysql_options 说明  https://blog.csdn.net/qq_38570571/article/details/79870946 

	//以秒为单位的连接超时
	if (connect_timeout > 0 && mysql_options(connection_, MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout) != 0)
	{
		throw MySqlException(connection_);
	}

	// MYSQL_OPT_WRITE_TIMEOUT 写入服务器的超时
	// MYSQL_OPT_READ_TIMEOUT  从服务器读取信息的超时

	//如果发现连接丢失，启动或禁止与服务器的自动再连接 
	//如果 MYSQL_OPT_RECONNECT 没有被设置为 1(开启)，那么mysql_ping()不会完成自动重连，只是简单返回一个error
	char value = 1;
	if (0 != mysql_options(connection_, MYSQL_OPT_RECONNECT, &value))
	{
		throw MySqlException(connection_);
	}

	//用作默认字符集的字符集的名称
	if (0 != mysql_options(connection_, MYSQL_SET_CHARSET_NAME, "utf8"))
	{
		throw MySqlException(connection_);
	}


	const MYSQL* const success = mysql_real_connect(
		connection_,
		hostname,
		username,
		password,
		database,
		port,
		nullptr,
		0);

	if (nullptr == success)
	{
		MySqlException mse(connection_);
		mysql_close(connection_);
		throw mse;
	}



}
