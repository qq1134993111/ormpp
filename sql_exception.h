#pragma once
#include <exception>
#include <string>
#include <sstream>

namespace ormpp
{

	class sql_exception : public std::exception
	{
	public:
		explicit sql_exception(std::stringstream& ss)
		{
			message_ = std::move(ss.str());
			ss.clear();
		}
		explicit sql_exception(std::string message)
		{
			message_ = std::move(message);
		}

		explicit sql_exception(const char* message)
		{
			message_ = message;
		}
		sql_exception(int err_no, const char* message)
		{
			message_ += "ERROR " + std::to_string(err_no) + " :" + message;
		}
		const char* what() const noexcept
		{
			return message_.c_str();
		}
	private:
		std::string message_;
	};
}

