#pragma once

#include "mysql/mysql.h"
#include "sql_exception.h"

namespace ormpp
{
	class mysql_exception :public sql_exception
	{
	public:
		using sql_exception::sql_exception;

		explicit mysql_exception(const MYSQL* const connection) :sql_exception(get_error_code(connection), get_error_message(connection)) {}
		explicit mysql_exception(const MYSQL_STMT* const statement) :sql_exception(get_error_code(statement), get_error_message(statement)) {}
	public:
		static const char* get_error_message(const MYSQL* const connection)
		{
			const char* const message = mysql_error(const_cast<MYSQL*>(connection));
			if ('\0' != message[0])
			{  // Error message isn't empty
				return message;
			}
			return "Unknown error";
		}
		static const char* get_error_message(const MYSQL_STMT* const statement)
		{
			const char* const message = mysql_stmt_error(const_cast<MYSQL_STMT*>(statement));
			if ('\0' != message[0])
			{  // Error message isn't empty
				return message;
			}
			return "Unknown error";
		}
		static int get_error_code(const MYSQL* const connection)
		{
			return mysql_errno(const_cast<MYSQL*>(connection));
		}
		static int get_error_code(const MYSQL_STMT* const statement)
		{
			return mysql_stmt_errno(const_cast<MYSQL_STMT*>(statement));
		}
	};

}
