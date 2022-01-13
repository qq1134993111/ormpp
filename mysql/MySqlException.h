#pragma once

#include "mysql/mysql.h"
#include "../SqlException.h"

class MySqlException :public SqlException
{
public:
	using SqlException::SqlException;

	explicit MySqlException(const MYSQL* const connection) :SqlException(GetErrorCode(connection), GetErrorMessage(connection)) {}
	explicit MySqlException(const MYSQL_STMT* const statement) :SqlException(GetErrorCode(statement), GetErrorMessage(statement)) {}
public:
	static const char* GetErrorMessage(const MYSQL* const connection)
	{
		const char* const message = mysql_error(const_cast<MYSQL*>(connection));
		if ('\0' != message[0])
		{  // Error message isn't empty
			return message;
		}
		return "Unknown error";
	}
	static const char* GetErrorMessage(const MYSQL_STMT* const statement)
	{
		const char* const message = mysql_stmt_error(const_cast<MYSQL_STMT*>(statement));
		if ('\0' != message[0])
		{  // Error message isn't empty
			return message;
		}
		return "Unknown error";
	}
	static int GetErrorCode(const MYSQL* const connection)
	{
		return mysql_errno(const_cast<MYSQL*>(connection));
	}
	static int GetErrorCode(const MYSQL_STMT* const statement)
	{
		return mysql_stmt_errno(const_cast<MYSQL_STMT*>(statement));
	}
};

