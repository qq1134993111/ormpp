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

	//mysql_options ˵��  https://blog.csdn.net/qq_38570571/article/details/79870946 

	//����Ϊ��λ�����ӳ�ʱ
	if (connect_timeout > 0 && mysql_options(connection_, MYSQL_OPT_CONNECT_TIMEOUT, &connect_timeout) != 0)
	{
		throw MySqlException(connection_);
	}

	// MYSQL_OPT_WRITE_TIMEOUT д��������ĳ�ʱ
	// MYSQL_OPT_READ_TIMEOUT  �ӷ�������ȡ��Ϣ�ĳ�ʱ

	//����������Ӷ�ʧ���������ֹ����������Զ������� 
	//��� MYSQL_OPT_RECONNECT û�б�����Ϊ 1(����)����ômysql_ping()��������Զ�������ֻ�Ǽ򵥷���һ��error
	char value = 1;
	if (0 != mysql_options(connection_, MYSQL_OPT_RECONNECT, &value))
	{
		throw MySqlException(connection_);
	}

	//����Ĭ���ַ������ַ���������
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
