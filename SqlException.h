#pragma once
#include <exception>
#include <string>
#include <sstream>

class SqlException : public std::exception
{
public:
	explicit SqlException(const char* message)
	{
		ss << message;
	}
	SqlException(int err_no, const char* message)
	{
		ss << "ERROR " << err_no << " :" << message;
	}
	const char* what() const noexcept
	{
		return ss.str().c_str();
	}
protected:
	std::stringstream  ss;
};

