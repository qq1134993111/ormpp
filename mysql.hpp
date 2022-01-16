//
// Created by qiyu on 10/20/17.
//

#ifndef ORM_MYSQL_HPP
#define ORM_MYSQL_HPP
#include <string_view>
#include <utility>
#include <climits>
#include <map>
#include <list>
#include "entity.hpp"
#include "type_mapping.hpp"
#include "utility.hpp"
#include "mysql_exception.h"

namespace ormpp {

	class mysql_result_set
	{
	public:
		mysql_result_set() = delete;
		mysql_result_set(const mysql_result_set&) = delete;
		mysql_result_set& operator=(const mysql_result_set&) = delete;
		mysql_result_set(mysql_result_set&&) = default;
		mysql_result_set& operator=(mysql_result_set&&) = default;
	private:
		friend class mysql_prepared_statement;
		mysql_result_set(std::shared_ptr<MYSQL_STMT> stmt,unsigned long field_count):stmt_(stmt), field_count_(field_count)
		{
			//field_count_ = mysql_stmt_field_count(stmt_.get());
			//out_parameters_.resize(field_count_, {});
			//out_lengths_.resize(field_count_, 0);
			//out_null_flags_.resize(field_count_);
		}
	public:
		unsigned long get_field_count()
		{
			return field_count_;
		}

		template<typename...ARGS >
		void bind_result_by_tuple(std::tuple<ARGS...>& tp)
		{
			if (sizeof...(ARGS) != get_field_count())
			{
				std::string err_msg("Incorrect number of output parameters; query required ");
				err_msg += std::to_string(get_field_count());
				err_msg += " but ";
				err_msg += std::to_string(sizeof...(ARGS));
				err_msg += " parameters were provided";
				throw mysql_exception(err_msg);
			}
			
			if (out_parameters_.size() != get_field_count())
			{
				out_parameters_.resize(field_count_, {});
				out_lengths_.resize(field_count_, 0);
				out_null_flags_.resize(field_count_);
			}
			
		
			iguana::for_each(tp,
				[this](auto& item, auto I)
				{
					using U = std::remove_reference_t<decltype(item)>;
					if constexpr (std::is_arithmetic_v<U>)
					{
						out_parameters_[I].buffer_type = (enum_field_types)ormpp_mysql::type_to_id(identity<U>{});
						out_parameters_[I].buffer = &item;
						out_parameters_[I].buffer_length = sizeof(item);
						out_parameters_[I].length = &out_lengths_[I];
						out_parameters_[I].is_null = reinterpret_cast<bool*>(&out_null_flags_[I]);

					}
					else if constexpr (std::is_same_v<std::string, U>)
					{
						static char s_placeholder;
						out_parameters_[I].buffer_type = MYSQL_TYPE_VAR_STRING;
						out_parameters_[I].buffer = &s_placeholder;
						out_parameters_[I].buffer_length = sizeof(s_placeholder);
						out_parameters_[I].length = &out_lengths_[I];
						out_parameters_[I].is_null = reinterpret_cast<bool*>(&out_null_flags_[I]);
					}
					else if constexpr (is_std_char_array_v<U>)
					{
						out_parameters_[I].buffer_type = MYSQL_TYPE_STRING;
						out_parameters_[I].buffer = &item[0];
						out_parameters_[I].buffer_length = sizeof(item);
						out_parameters_[I].length = &out_lengths_[I];
						out_parameters_[I].is_null = reinterpret_cast<bool*>(&out_null_flags_[I]);
					}
					else
					{
						std::cout << typeid(U).name() << std::endl;
						static_assert(false, "Unknown value type");
					}

				},
				std::make_index_sequence<std::tuple_size_v<std::remove_reference_t<decltype(tp)>> >{}
				);

			if (mysql_stmt_bind_result(stmt_.get(), &out_parameters_[0]))
			{
				throw mysql_exception(stmt_.get());
			}

		}

		template<typename...ARGS >
		bool fetch_row_data_by_tuple(std::tuple<ARGS...>& tp)
		{
			auto fetch_status = mysql_stmt_fetch(stmt_.get());
			if (fetch_status == 0 || fetch_status == MYSQL_DATA_TRUNCATED)
			{

				if (fetch_status == MYSQL_DATA_TRUNCATED)
				{
					iguana::for_each(tp,
						[this](auto& item, auto I)
						{
							using U = std::remove_reference_t<decltype(item)>;
							if constexpr (std::is_same_v<std::string, U>)
							{
								if (!out_null_flags_[I])
								{
									const size_t untruncated_length = out_lengths_[I];

									item.resize(untruncated_length);
									MYSQL_BIND bind = {};
									bind.buffer = item.data();
									bind.buffer_length = item.size();

									const int status = mysql_stmt_fetch_column(
										stmt_.get(),
										&bind,
										I,
										0);

									if (0 != status)
									{
										throw mysql_exception(stmt_.get());
									}

								}

							}
						},
						std::make_index_sequence<std::tuple_size_v<std::remove_reference_t<decltype(tp)>>>{}
						);
				}

				return true;
			}

			switch (fetch_status)
			{
			case MYSQL_NO_DATA:
				// No problem! All rows fetched.
				break;
			case 1:
			{  // Error occurred {
				throw mysql_exception(stmt_.get());
			}
			default:
			{
				//assert(false && "Unknown error code from mysql_stmt_fetch");
				throw mysql_exception(stmt_.get());
			}
			}

			return false;
		}


	private:
		std::shared_ptr<MYSQL_STMT> stmt_ = nullptr;
		unsigned long field_count_ = 0;

		std::vector<MYSQL_BIND> out_parameters_;
		std::vector<unsigned long> out_lengths_;
		std::vector<uint8_t> out_null_flags_;
		
	};

	class  mysql_prepared_statement
	{
	public:
		mysql_prepared_statement() = delete;
		mysql_prepared_statement(const mysql_prepared_statement&) = delete;
		mysql_prepared_statement& operator=(const mysql_prepared_statement&) = delete;
		mysql_prepared_statement(mysql_prepared_statement&&) = default;
		mysql_prepared_statement& operator=(mysql_prepared_statement&&) = default;
	private:
		friend class mysql;
		mysql_prepared_statement(MYSQL* con, const std::string& sql)
		{
			auto* stmt = mysql_stmt_init(con);
			if (stmt == nullptr)
			{
				throw mysql_exception("mysql_prepared_statement mysql_stmt_init failed");
			}

			stmt_ = std::shared_ptr<MYSQL_STMT>(stmt,
				[](MYSQL_STMT* p)
				{
					auto status_ = mysql_stmt_close(p);

					if (status_)
						fprintf(stderr, "mysql_prepared_statement ,close statment error code %d\n", status_);
				});

			if (0 != mysql_stmt_prepare(stmt, sql.data(), static_cast<unsigned long>(sql.size())))
			{
				std::stringstream ss;
				ss << "ERROR " << mysql_exception::get_error_code(stmt) << " :" << mysql_exception::get_error_message(stmt);
				if (0 != mysql_stmt_free_result(stmt))
				{
					ss << "; There was an error freeing this statement";
				}
				if (0 != mysql_stmt_close(stmt))
				{
					ss << "; There was an error closing this statement";
				}

				ss << ";arg sql is : " << sql;
				throw mysql_exception(ss);
			}

			parameter_count_ = mysql_stmt_param_count(stmt);
			field_count_ = mysql_stmt_field_count(stmt);


		}
	public:
		unsigned long get_param_count()
		{
			return parameter_count_;
		}
		unsigned long get_field_count()
		{
			return field_count_;
		}


		template<typename Type>
		void set_index_param_bind(unsigned short param_index, Type&& value)
		{
			if (param_binds_.empty() && get_param_count() != 0)
			{
				param_binds_.resize(get_param_count(), {});
			}

			if (param_index >= param_binds_.size())
			{
				std::stringstream ss;
				ss << "mysql_prepared_statement::set_index_param_bind param_binds_ size is " << param_binds_.size();
				ss << ", but set param_index is " << param_index;
				throw mysql_exception(ss);
			}

			auto& param = param_binds_[param_index];
			using U = std::remove_const_t<std::remove_reference_t<Type>>;
			if constexpr (std::is_arithmetic_v<U>)
			{
				param.buffer_type = (enum_field_types)ormpp_mysql::type_to_id(identity<U>{});
				param.buffer = const_cast<void*>(static_cast<const void*>(&value));
				param.is_unsigned = std::is_signed_v<U>;
			}
			else if constexpr (std::is_same_v<std::string, U>)
			{
				param.buffer_type = MYSQL_TYPE_STRING;
				param.buffer = (void*)(value.c_str());
				param.buffer_length = (unsigned long)value.size();
			}
			else if constexpr (std::is_same_v<const char*, U>)
			{
				param.buffer_type = MYSQL_TYPE_STRING;
				param.buffer = (void*)(value);
				param.buffer_length = (unsigned long)strlen(value);
			}
			else if constexpr (is_std_char_array_v<U>)
			{
				param.buffer_type = MYSQL_TYPE_STRING;
				param.buffer = (void*)(value);
				param.buffer_length = std::min<unsigned long>(strlen(value), notstd::is_array<U>::array_size);
			}
			else
			{
				static_assert(false, "Unknown value type");
				std::stringstream ss;
				ss << "Unknown value type." << "index:" << param_index << ",type_name:" << typeid(decltype(value)).name();
				throw mysql_exception(ss);
			}
		}

		template<typename... ARGS>
		void set_param_bind(ARGS&&... args)
		{
			auto tp = std::forward_as_tuple(std::forward<ARGS&&>(args)...);
			iguana::for_each(
				tp,
				[this](auto&& item, auto I)
				{
					set_index_param_bind(I, std::forward<decltype(item)&&>(item));
				},
				std::make_index_sequence<sizeof...(args)>{}
				);

		}

		void execute()
		{
			bind_param();
			if (mysql_stmt_execute(stmt_.get()))
			{
				throw mysql_exception(stmt_.get());
			}
		}

		mysql_result_set execute_query()
		{
			bind_param();
			if (mysql_stmt_execute(stmt_.get()))
			{
				throw mysql_exception(stmt_.get());
			}

			if (mysql_stmt_store_result(stmt_.get()))
			{
				throw mysql_exception(stmt_.get());
			}

			mysql_result_set result(stmt_,get_field_count());

			return result;
		}

		uint64_t affected_rows()
		{
			return mysql_stmt_affected_rows(stmt_.get());
		}
	private:
		void bind_param()
		{
			if (param_binds_.size() != get_param_count())
			{
				std::stringstream ss;
				ss << "mysql_prepared_statement::bind_param get_param_count is " << get_param_count();
				ss << "but param_binds_ size is " << param_binds_.size();
				throw mysql_exception(ss);
			}

			if (param_binds_.empty())
				return;

			for (std::size_t i = 0; i < param_binds_.size(); i++)
			{
				if (param_binds_[i].buffer == nullptr)
				{
					std::stringstream ss;
					ss << "param_binds_ index " << i << " no param set";
					throw mysql_exception(ss);
				}
			}

			if (mysql_stmt_bind_param(stmt_.get(), &param_binds_[0]))
			{
				throw mysql_exception(stmt_.get());
			}

		}
	private:
		std::shared_ptr<MYSQL_STMT> stmt_ = nullptr;
		unsigned long parameter_count_ = 0;
		unsigned long field_count_ = 0;
		std::vector<MYSQL_BIND> param_binds_;
	};

	class mysql {
	public:
		~mysql() {
			disconnect();
		}

		//host,user,passwd,db,port,timeout
		template<typename... Args>
		void connect(Args&&... args) 
		{
			if (con_ != nullptr)
			{
				disconnect();
			}

			con_ = mysql_init(nullptr);
			if (con_ == nullptr)
			{
				throw mysql_exception("mysql_init init failed");
			}

			int timeout = -1;
			auto tp = std::tuple_cat(get_tp(timeout, std::forward<Args>(args)...), std::make_tuple(0, nullptr, 0));

			if (timeout > 0) 
			{
				if (mysql_options(con_, MYSQL_OPT_CONNECT_TIMEOUT, &timeout) != 0)
				{
					throw  mysql_exception(con_);
				}
			}

			char value = 1;
			if (mysql_options(con_, MYSQL_OPT_RECONNECT, &value) != 0)
			{
				throw  mysql_exception(con_);
			}
			if (mysql_options(con_, MYSQL_SET_CHARSET_NAME, "utf8") != 0)
			{
				throw  mysql_exception(con_);
			}

			if (std::apply(&mysql_real_connect, tp) == nullptr) 
			{
				throw  mysql_exception(con_);
			}
		}


		bool ping() 
		{
			return mysql_ping(con_) == 0;
		}

		template<typename... Args>
		bool disconnect(Args&&... args) 
		{
			if (con_ != nullptr) {
				mysql_close(con_);
				con_ = nullptr;
			}

			return true;
		}

		uint64_t get_last_affect_rows()
		{
			return mysql_affected_rows(con_);
		}

		uint64_t exec_commmand(const std::string& sql)
		{
			if (mysql_real_query(con_, sql.data(), sql.size()) != 0)
			{
				throw mysql_exception(con_);
			}

			return get_last_affect_rows();
		}

		template <typename... Args>
		uint64_t exec_commmand(const std::string& sql, Args&&... args)
		{
			mysql_prepared_statement statement(con_,sql);
			
			if (0 != statement.get_field_count())
			{
				throw mysql_exception("Tried to run execute_query with exec_commmand");
			}

			if (sizeof...(args) != statement.get_param_count())
			{
				std::string err_msg = sql + " ";
				err_msg += "Incorrect number of parameters; command required ";
				err_msg += std::to_string(statement.get_param_count());
				err_msg += " but ";
				err_msg += std::to_string(sizeof...(args));
				err_msg += " parameters were provided.";

				throw mysql_exception(err_msg);
			}

			statement.set_param_bind(std::forward<Args&&>(args)...);
			statement.execute();
			return statement.affected_rows();
		}

		/*
		template <typename... Args>
		uint64_t exec_commmand(const std::string& sql, Args&&... args)
		{
			auto stmt = mysql_stmt_init(con_);
			if (stmt == nullptr)
			{
				throw mysql_exception("execute mysql_stmt_init failed");
			}
			auto guard = guard_statment(stmt);

			if (0 != mysql_stmt_prepare(stmt, sql.data(), sql.size()))
			{
				std::stringstream ss;
				ss << "ERROR " << mysql_exception::get_error_code(stmt) << " :" << mysql_exception::get_error_message(stmt);
				if (0 != mysql_stmt_free_result(stmt))
				{
					ss << "; There was an error freeing this statement";
				}
				if (0 != mysql_stmt_close(stmt))
				{
					ss << "; There was an error closing this statement";
				}
				throw mysql_exception(ss);
			}

			auto parameter_count = mysql_stmt_param_count(stmt);
			auto field_count = mysql_stmt_field_count(stmt);

			if (0 != field_count)
			{
				throw mysql_exception("Tried to run execute_query with execute");
			}

			if (sizeof...(args) != parameter_count)
			{
				std::string err_msg = sql + " ";
				err_msg += "Incorrect number of parameters; command required ";
				err_msg += std::to_string(parameter_count);
				err_msg += " but ";
				err_msg += std::to_string(sizeof...(args));
				err_msg += " parameters were provided.";

				throw mysql_exception(err_msg);
			}


			std::vector<MYSQL_BIND> param_binds;
			set_param_bind(param_binds, args...);

			if (mysql_stmt_bind_param(stmt, &param_binds[0]))
			{
				throw mysql_exception(stmt);
			}


			if (mysql_stmt_execute(stmt))
			{
				throw mysql_exception(stmt);
			}

			return mysql_stmt_affected_rows(stmt);

		}
		*/

		//template <typename... InputArgs, typename... OutputArgs>
		//uint64_t exec_query(std::vector<std::tuple<OutputArgs...>>& results, const std::string& query, InputArgs&&... args) const
		//{
		//	auto* stmt = mysql_stmt_init(con_);
		//	if (stmt == nullptr)
		//	{
		//		throw mysql_exception("execute_query mysql_stmt_init failed");
		//	}

		//	auto guard = guard_statment(stmt);

		//	if (0 != mysql_stmt_prepare(stmt, query.data(), query.size()))
		//	{
		//		std::stringstream ss;
		//		ss << "ERROR " << mysql_exception::get_error_code(stmt) << " :" << mysql_exception::get_error_message(stmt);
		//		if (0 != mysql_stmt_free_result(stmt))
		//		{
		//			ss << "; There was an error freeing this statement";
		//		}
		//		if (0 != mysql_stmt_close(stmt))
		//		{
		//			ss << "; There was an error closing this statement";
		//		}
		//		throw mysql_exception(ss);
		//	}

		//	auto parameter_count = mysql_stmt_param_count(stmt);
		//	auto field_count = mysql_stmt_field_count(stmt);

		//	if (0 == field_count)
		//	{
		//		throw mysql_exception("Tried to run execute with execute_query");
		//	}

		//	if (sizeof...(args) != parameter_count)
		//	{
		//		std::string err_msg = query + " ";
		//		err_msg += "Incorrect number of parameters; command required ";
		//		err_msg += std::to_string(parameter_count);
		//		err_msg += " but ";
		//		err_msg += std::to_string(sizeof...(args));
		//		err_msg += " parameters were provided.";

		//		throw mysql_exception(err_msg);
		//	}

		//	if constexpr (sizeof...(args) > 0)
		//	{
		//		std::vector<MYSQL_BIND> param_binds;
		//		set_param_bind(param_binds, args...);

		//		if (mysql_stmt_bind_param(stmt, &param_binds[0]))
		//		{
		//			throw mysql_exception(stmt);
		//		}
		//	}


		//	// Check that the sizes match
		//	if (field_count != sizeof...(OutputArgs))
		//	{
		//		std::string err_msg("Incorrect number of output parameters; query required ");
		//		err_msg += std::to_string(field_count);
		//		err_msg += " but ";
		//		err_msg += std::to_string(sizeof...(OutputArgs));
		//		err_msg += " parameters were provided";
		//		throw mysql_exception(err_msg);
		//	}


		//	std::vector<MYSQL_BIND> out_parameters(field_count);
		//	std::vector<unsigned long> out_lengths(field_count, 0);
		//	std::vector<uint8_t> out_null_flags( field_count,0 );
		//	std::tuple<OutputArgs...> tp{};

		//	size_t index = 0;
		//	iguana::for_each(tp,
		//		[&out_parameters, &out_lengths, &out_null_flags](auto& item, auto I)
		//		{
		//			using U = std::remove_reference_t<decltype(item)>;
		//			if constexpr (std::is_arithmetic_v<U>)
		//			{
		//				out_parameters[I].buffer_type = (enum_field_types)ormpp_mysql::type_to_id(identity<U>{});
		//				out_parameters[I].buffer = &item;
		//				out_parameters[I].buffer_length = sizeof(item);
		//				out_parameters[I].length =&out_lengths[I];
		//				out_parameters[I].is_null =reinterpret_cast<bool*>(&out_null_flags[I]);

		//			}
		//			else if constexpr (std::is_same_v<std::string, U>)
		//			{
		//				item.resize(20, 0);
		//				out_parameters[I].buffer_type = MYSQL_TYPE_VAR_STRING;
		//				out_parameters[I].buffer = item.data();
		//				out_parameters[I].buffer_length = item.size();
		//				out_parameters[I].length =&out_lengths[I];
		//				out_parameters[I].is_null = reinterpret_cast<bool*>(&out_null_flags[I]);
		//			}
		//			else if constexpr (is_std_char_array_v<U>)
		//			{
		//				out_parameters[I].buffer_type = MYSQL_TYPE_STRING;
		//				out_parameters[I].buffer = &item[0];
		//				out_parameters[I].buffer_length = sizeof(item);
		//				out_parameters[I].length = &out_lengths[I];
		//				out_parameters[I].is_null = reinterpret_cast<bool*>(&out_null_flags[I]);
		//			}
		//			else
		//			{
		//				std::cout << typeid(U).name() << std::endl;
		//				static_assert(false, "Unknown value type");
		//			}

		//		},
		//		std::make_index_sequence<std::tuple_size_v<decltype(tp)>>{});

		//	if (mysql_stmt_bind_result(stmt, &out_parameters[0]))
		//	{
		//		throw mysql_exception(stmt);
		//	}

		//	if (mysql_stmt_execute(stmt))
		//	{
		//		throw mysql_exception(stmt);
		//	}

		//	if (mysql_stmt_store_result(stmt))
		//	{
		//		throw mysql_exception(stmt);
		//	}

		//	int fetch_status = 0;
		//	for (;;)
		//	{
		//		fetch_status = mysql_stmt_fetch(stmt);
		//		if (fetch_status != 0 && fetch_status != MYSQL_DATA_TRUNCATED)
		//		{
		//			break;
		//		}

		//		//if (fetch_status == MYSQL_DATA_TRUNCATED)
		//		{
		//			iguana::for_each(tp,
		//				[&out_parameters, &out_lengths, &out_null_flags, &stmt](auto& item, auto I)
		//				{
		//					using U = std::remove_reference_t<decltype(item)>;
		//					if constexpr (std::is_same_v<std::string, U>)
		//					{
		//						if (!out_null_flags[I])
		//						{
		//							const size_t untruncated_length = out_lengths[I];
		//							if (untruncated_length > item.size())
		//							{
		//								const size_t already_retrieved = item.size();
		//								item.resize(untruncated_length, 0);
		//								MYSQL_BIND& bind = out_parameters[I];
		//								bind.buffer = &item[already_retrieved];
		//								bind.buffer_length = item.size() - already_retrieved;

		//								const int status = mysql_stmt_fetch_column(
		//									stmt,
		//									&out_parameters[0],
		//									I,
		//									already_retrieved);

		//								if (0 != status)
		//								{
		//									throw mysql_exception(stmt);
		//								}

		//							}
		//							else if (untruncated_length < item.size())
		//							{
		//								item.resize(untruncated_length);
		//							}
		//						}
		//						else
		//						{
		//							item.clear();
		//						}

		//						// Now, for subsequent fetches, we need to reset the buffers

		//						out_parameters[I].buffer = !item.empty()?item.data():nullptr;
		//						out_parameters[I].buffer_length = !item.empty() ? item.size():1;//buffer_length 为0会触发mysql断言,改为最小为1

		//					}

		//				},
		//				std::make_index_sequence<sizeof...(OutputArgs)>{});

		//			// If we've changed the buffers, we need to rebind
		//			if (0 != mysql_stmt_bind_result(stmt, out_parameters.data()))
		//			{
		//				throw mysql_exception(stmt);
		//			}
		//		}

		//		results.push_back(tp);

		//	}

		//	switch (fetch_status) 
		//	{
		//	case MYSQL_NO_DATA:
		//		// No problem! All rows fetched.
		//		break;
		//	case 1: 
		//	{  // Error occurred {
		//		throw mysql_exception(stmt);
		//	}
		//	default: 
		//	{
		//		//assert(false && "Unknown error code from mysql_stmt_fetch");
		//		throw mysql_exception(stmt);
		//	}
		//	}

		//	return mysql_stmt_affected_rows(stmt);

		//}

		template <typename... InputArgs, typename... OutputArgs>
		uint64_t exec_query(std::vector<std::tuple<OutputArgs...>>& results, const std::string& query, InputArgs&&... args) const
		{
			mysql_prepared_statement statement(con_, query);

			if (0 == statement.get_field_count())
			{
				throw mysql_exception("Tried to run execute with execute_query");
			}


			if (sizeof...(args) != statement.get_param_count())
			{
				std::string err_msg = query + " ";
				err_msg += "Incorrect number of parameters; command required ";
				err_msg += std::to_string(statement.get_param_count());
				err_msg += " but ";
				err_msg += std::to_string(sizeof...(args));
				err_msg += " parameters were provided.";

				throw mysql_exception(err_msg);
			}

			statement.set_param_bind(std::forward<InputArgs&&>(args)...);
			auto result_set=statement.execute_query();

			std::tuple<OutputArgs...> tp{};
			result_set.bind_result_by_tuple(tp);
			while (result_set.fetch_row_data_by_tuple(tp))
			{
				results.push_back(tp);
			}

			return statement.affected_rows();
		}

		template <typename... Args>
		uint64_t execute(Args&&... args)
		{
			return exec_commmand(std::forward<Args>(args)...);
		}

		//transaction
		void begin()
		{
			execute("BEGIN");
		}

		void commit()
		{
			execute("COMMIT");
		}

		void rollback()
		{
			execute("ROLLBACK");
		}

	public:
		template<typename T, typename... Args >
		constexpr bool create_datatable(Args&&... args) {
			std::string sql = generate_createtb_sql<T>(std::forward<Args>(args)...);
			sql += " DEFAULT CHARSET=utf8";

			execute(sql);

			return true;
		}

		template<typename T, typename... Args>
		constexpr int insert(const std::vector<T>& t, Args&&... args) {
			auto name = get_name<T>();
			std::string sql = auto_key_map_[name].empty() ? generate_insert_sql<T>(false) : generate_auto_insert_sql<T>(auto_key_map_, false);

			return insert_impl(sql, t, std::forward<Args>(args)...);
		}

		template<typename T, typename... Args>
		constexpr int update(const std::vector<T>& t, Args&&... args) {
			std::string sql = generate_insert_sql<T>(true);

			return insert_impl(sql, t, std::forward<Args>(args)...);
		}

		template<typename T, typename... Args>
		constexpr int insert(const T& t, Args&&... args) {
			//insert into person values(?, ?, ?);
			auto name = get_name<T>();
			std::string sql = auto_key_map_[name].empty() ? generate_insert_sql<T>(false) : generate_auto_insert_sql<T>(auto_key_map_, false);

			return insert_impl(sql, t, std::forward<Args>(args)...);
		}

		template<typename T, typename... Args>
		constexpr int update(const T& t, Args&&... args) {
			std::string sql = generate_insert_sql<T>(true);
			return insert_impl(sql, t, std::forward<Args>(args)...);
		}

		template<typename T, typename... Args>
		constexpr bool delete_records(Args&&... where_conditon) {
			auto sql = generate_delete_sql<T>(std::forward<Args>(where_conditon)...);
			
			execute(sql);

			return true;
		}



		//for tuple and string with args...
		template<typename T, typename Arg, typename... Args>
		constexpr std::enable_if_t<!iguana::is_reflection_v<T>, std::vector<T>> query(const Arg& s, Args&&... args) {
			static_assert(iguana::is_tuple<T>::value);
			constexpr auto SIZE = std::tuple_size_v<T>;

			std::string sql = s;
			constexpr auto Args_Size = sizeof...(Args);
			if (Args_Size != 0) {
				if (Args_Size != std::count(sql.begin(), sql.end(), '?')) {
					has_error_ = true;
					return {};
				}

				sql = get_sql(sql, std::forward<Args>(args)...);
			}

			stmt_ = mysql_stmt_init(con_);
			if (!stmt_) {
				has_error_ = true;
				return {};
			}

			if (mysql_stmt_prepare(stmt_, sql.c_str(), (int)sql.size())) {
				fprintf(stderr, "%s\n", mysql_error(con_));
				has_error_ = true;
				return {};
			}

			auto guard = guard_statment(stmt_);

			std::array<MYSQL_BIND, result_size<T>::value> param_binds = {};
			std::list<std::vector<char>> mp;

			std::vector<T> v;
			T tp{};

			size_t index = 0;
			iguana::for_each(tp, [&param_binds, &mp, &index](auto& item, auto I) {
				using U = std::remove_reference_t<decltype(item)>;
				if constexpr (std::is_arithmetic_v<U>) {
					param_binds[index].buffer_type = (enum_field_types)ormpp_mysql::type_to_id(identity<U>{});
					param_binds[index].buffer = &item;
					index++;
				}
				else if constexpr (std::is_same_v<std::string, U>) {
					std::vector<char> tmp(65536, 0);
					mp.emplace_back(std::move(tmp));
					param_binds[index].buffer_type = MYSQL_TYPE_STRING;
					param_binds[index].buffer = &(mp.back()[0]);
					param_binds[index].buffer_length = 65536;
					index++;
				}
				else if constexpr (iguana::is_reflection_v<U>) {
					iguana::for_each(item, [&param_binds, &mp, &item, &index](auto& ele, auto i) {
						using U = std::remove_reference_t<decltype(std::declval<U>().*ele)>;
						if constexpr (std::is_arithmetic_v<U>) {
							param_binds[index].buffer_type = (enum_field_types)ormpp_mysql::type_to_id(identity<U>{});
							param_binds[index].buffer = &(item.*ele);
						}
						else if constexpr (std::is_same_v<std::string, U>) {
							std::vector<char> tmp(65536, 0);
							mp.emplace_back(std::move(tmp));
							param_binds[index].buffer_type = MYSQL_TYPE_STRING;
							param_binds[index].buffer = &(mp.back()[0]);
							param_binds[index].buffer_length = 65536;
						}
						else if constexpr (is_char_array_v<U>) {
							std::vector<char> tmp(sizeof(U), 0);
							mp.emplace_back(std::move(tmp));
							param_binds[index].buffer_type = MYSQL_TYPE_VAR_STRING;
							param_binds[index].buffer = &(mp.back()[0]);
							param_binds[index].buffer_length = (unsigned long)sizeof(U);
						}
						index++;
						});
				}
				else if constexpr (is_char_array_v<U>) {
					param_binds[index].buffer_type = MYSQL_TYPE_VAR_STRING;
					std::vector<char> tmp(sizeof(U), 0);
					mp.emplace_back(std::move(tmp));
					param_binds[index].buffer = &(mp.back()[0]);
					param_binds[index].buffer_length = (unsigned long)sizeof(U);
					index++;
				}
				else {
					std::cout << typeid(U).name() << std::endl;
				}
				}, std::make_index_sequence<SIZE>{});

			if (mysql_stmt_bind_result(stmt_, &param_binds[0])) {
				//                fprintf(stderr, "%s\n", mysql_error(con_));
				has_error_ = true;
				return {};
			}

			if (mysql_stmt_execute(stmt_)) {
				//                fprintf(stderr, "%s\n", mysql_error(con_));
				has_error_ = true;
				return {};
			}

			while (mysql_stmt_fetch(stmt_) == 0) {
				auto it = mp.begin();
				iguana::for_each(tp, [&mp, &it](auto& item, auto i) {
					using U = std::remove_reference_t<decltype(item)>;
					if constexpr (std::is_same_v<std::string, U>) {
						item = std::string(&(*it)[0], strlen((*it).data()));
						it++;
					}
					else if constexpr (is_char_array_v<U>) {
						memcpy(item, &(*it)[0], sizeof(U));
					}
					else if constexpr (iguana::is_reflection_v<U>) {
						iguana::for_each(item, [&it, &item](auto ele, auto i) {
							using V = std::remove_reference_t<decltype(std::declval<U>().*ele)>;
							if constexpr (std::is_same_v<std::string, V>) {
								item.*ele = std::string(&(*it)[0], strlen((*it).data()));
								it++;
							}
							else if constexpr (is_char_array_v<V>) {
								memcpy(item.*ele, &(*it)[0], sizeof(V));
							}
							});
					}
					}, std::make_index_sequence<SIZE>{});

				if (index > 0)
					v.push_back(std::move(tp));
			}

			return v;
		}

		//if there is a sql error, how to tell the user? throw exception?
		template<typename T, typename... Args>
		constexpr std::enable_if_t<iguana::is_reflection_v<T>, std::vector<T>> query(Args&&... args) {
			std::string sql = generate_query_sql<T>(args...);
			constexpr auto SIZE = iguana::get_value<T>();

			stmt_ = mysql_stmt_init(con_);
			if (!stmt_) {
				has_error_ = true;
				return {};
			}

			if (mysql_stmt_prepare(stmt_, sql.c_str(), (unsigned long)sql.size())) {
				has_error_ = true;
				return {};
			}

			auto guard = guard_statment(stmt_);

			std::array<MYSQL_BIND, SIZE> param_binds = {};
			std::map<size_t, std::vector<char>> mp;

			std::vector<T> v;
			T t{};
			int index = 0;
			iguana::for_each(t, [&](auto item, auto i) {
				constexpr auto Idx = decltype(i)::value;
				using U = std::remove_reference_t<decltype(std::declval<T>().*item)>;
				if constexpr (std::is_arithmetic_v<U>) {
					param_binds[Idx].buffer_type = (enum_field_types)ormpp_mysql::type_to_id(identity<U>{});
					param_binds[Idx].buffer = &(t.*item);
					index++;
				}
				else if constexpr (std::is_same_v<std::string, U>) {
					param_binds[Idx].buffer_type = MYSQL_TYPE_STRING;
					std::vector<char> tmp(65536, 0);
					mp.emplace(decltype(i)::value, tmp);
					param_binds[Idx].buffer = &(mp.rbegin()->second[0]);
					param_binds[Idx].buffer_length = (unsigned long)tmp.size();
					index++;
				}
				else if constexpr (is_char_array_v<U>) {
					param_binds[Idx].buffer_type = MYSQL_TYPE_VAR_STRING;
					std::vector<char> tmp(sizeof(U), 0);
					mp.emplace(decltype(i)::value, tmp);
					param_binds[Idx].buffer = &(mp.rbegin()->second[0]);
					param_binds[Idx].buffer_length = (unsigned long)sizeof(U);
					index++;
				}
				});

			if (index == 0) {
				return {};
			}

			if (mysql_stmt_bind_result(stmt_, &param_binds[0])) {
				//                fprintf(stderr, "%s\n", mysql_error(con_));
				has_error_ = true;
				return {};
			}

			if (mysql_stmt_execute(stmt_)) {
				//                fprintf(stderr, "%s\n", mysql_error(con_));
				has_error_ = true;
				return {};
			}

			while (mysql_stmt_fetch(stmt_) == 0) {
				using TP = decltype(iguana::get(std::declval<T>()));

				iguana::for_each(t, [&mp, &t](auto item, auto i) {
					using U = std::remove_reference_t<decltype(std::declval<T>().*item)>;
					if constexpr (std::is_same_v<std::string, U>) {
						auto& vec = mp[decltype(i)::value];
						t.*item = std::string(&vec[0], strlen(vec.data()));
					}
					else if constexpr (is_char_array_v<U>) {
						auto& vec = mp[decltype(i)::value];
						memcpy(t.*item, vec.data(), vec.size());
					}
					});

				for (auto& p : mp) {
					p.second.assign(p.second.size(), 0);
				}

				v.push_back(std::move(t));
				iguana::for_each(t, [&mp, &t](auto item, auto i) {
					using U = std::remove_reference_t<decltype(std::declval<T>().*item)>;
					if constexpr (std::is_arithmetic_v<U>) {
						memset(&(t.*item), 0, sizeof(U));
					}
					});
			}

			return v;
		}





	private:
		template<typename T, typename... Args >
		std::string generate_createtb_sql(Args&&... args) {
			const auto type_name_arr = get_type_names<T>(DBType::mysql);
			auto name = get_name<T>();
			std::string sql = std::string("CREATE TABLE IF NOT EXISTS ") + name.data() + "(";
			auto arr = iguana::get_array<T>();
			constexpr auto SIZE = sizeof... (Args);
			auto_key_map_[name.data()] = "";

			//auto_increment_key and key can't exist at the same time
			using U = std::tuple<std::decay_t <Args>...>;
			if constexpr (SIZE > 0) {
				//using U = std::tuple<std::decay_t <Args>...>;//the code can't compile in vs
				static_assert(!(iguana::has_type<ormpp_key, U>::value && iguana::has_type<ormpp_auto_key, U>::value), "should only one key");
			}
			auto tp = sort_tuple(std::make_tuple(std::forward<Args>(args)...));
			const size_t arr_size = arr.size();
			for (size_t i = 0; i < arr_size; ++i) {
				auto field_name = arr[i];
				bool has_add_field = false;
				for_each0(tp, [&sql, &i, &has_add_field, field_name, type_name_arr, name, this](auto item) {
					if constexpr (std::is_same_v<decltype(item), ormpp_not_null>) {
						if (item.fields.find(field_name.data()) == item.fields.end())
							return;
					}
					else {
						if (item.fields != field_name.data())
							return;
					}

					if constexpr (std::is_same_v<decltype(item), ormpp_not_null>) {
						if (!has_add_field) {
							append(sql, field_name.data(), " ", type_name_arr[i]);
						}
						append(sql, " NOT NULL");
						has_add_field = true;
					}
					else if constexpr (std::is_same_v<decltype(item), ormpp_key>) {
						if (!has_add_field) {
							append(sql, field_name.data(), " ", type_name_arr[i]);
						}
						append(sql, " PRIMARY KEY");
						has_add_field = true;
					}
					else if constexpr (std::is_same_v<decltype(item), ormpp_auto_key>) {
						if (!has_add_field) {
							append(sql, field_name.data(), " ", type_name_arr[i]);
						}
						append(sql, " AUTO_INCREMENT");
						append(sql, " PRIMARY KEY");
						auto_key_map_[name.data()] = item.fields;
						has_add_field = true;
					}
					else if constexpr (std::is_same_v<decltype(item), ormpp_unique>) {
						if (!has_add_field) {
							append(sql, field_name.data(), " ", type_name_arr[i]);
						}

						append(sql, ", UNIQUE(", item.fields, ")");
						has_add_field = true;
					}
					else {
						append(sql, field_name.data(), " ", type_name_arr[i]);
					}
					}, std::make_index_sequence<SIZE>{});

				if (!has_add_field) {
					append(sql, field_name.data(), " ", type_name_arr[i]);
				}

				if (i < arr_size - 1)
					sql += ", ";
			}

			sql += ")";

			return sql;
		}

		template<typename T>
		constexpr void set_param_bind(std::vector<MYSQL_BIND>& param_binds, T&& value) const
		{
			MYSQL_BIND param = {};

			using U = std::remove_const_t<std::remove_reference_t<T>>;
			if constexpr (std::is_arithmetic_v<U>) {
				param.buffer_type = (enum_field_types)ormpp_mysql::type_to_id(identity<U>{});
				param.buffer = const_cast<void*>(static_cast<const void*>(&value));
				param.is_unsigned = std::is_signed_v<U>;
			}
			else if constexpr (std::is_same_v<std::string, U>) {
				param.buffer_type = MYSQL_TYPE_STRING;
				param.buffer = (void*)(value.c_str());
				param.buffer_length = (unsigned long)value.size();
			}
			else if constexpr (std::is_same_v<const char*, U>) {
				param.buffer_type = MYSQL_TYPE_STRING;
				param.buffer = (void*)(value);
				param.buffer_length = (unsigned long)strlen(value);
			}
			else if constexpr (is_std_char_array_v<U>)
			{
				param.buffer_type = MYSQL_TYPE_STRING;
				param.buffer = (void*)(value);
				param.buffer_length = std::min<unsigned long>(strlen(value), notstd::is_array<U>::array_size);
			}
			else
			{
				static_assert(false, "Unknown value type");
			}
			param_binds.push_back(param);
		}

		template<typename T, typename... ARGS>
		constexpr void set_param_bind(std::vector<MYSQL_BIND>& param_binds, T&& value, ARGS&&... args) const
		{
			set_param_bind(param_binds, std::forward<T&&>(value));
			if (sizeof...(args) > 0)
			{
				set_param_bind(param_binds, std::forward<ARGS&&>(args)...);
			}
		}


		template<typename T>
		uint64_t stmt_execute(const T& t) {
			std::vector<MYSQL_BIND> param_binds;
			auto it = auto_key_map_.find(get_name<T>());
			std::string auto_key = (it == auto_key_map_.end()) ? "" : it->second;

			iguana::for_each(t, [&t, &param_binds, &auto_key, this](const auto& v, auto i) {
				/*if (!auto_key.empty() && auto_key == iguana::get_name<T>(decltype(i)::value).data())
					return;*/

				set_param_bind(param_binds, t.*v);
				});

			if (mysql_stmt_bind_param(stmt_, &param_binds[0])) {
				//                fprintf(stderr, "%s\n", mysql_error(con_));
				return INT_MIN;
			}

			if (mysql_stmt_execute(stmt_)) {
				fprintf(stderr, "%s\n", mysql_error(con_));
				return INT_MIN;
			}

			int count = (int)mysql_stmt_affected_rows(stmt_);
			if (count == 0) {
				return INT_MIN;
			}

			return count;
		}


		struct guard_statment {
			guard_statment(MYSQL_STMT* stmt) :stmt_(stmt) {}
			MYSQL_STMT* stmt_ = nullptr;
			int status_ = 0;
			~guard_statment() {
				if (stmt_ != nullptr)
					status_ = mysql_stmt_close(stmt_);

				if (status_)
					fprintf(stderr, "close statment error code %d\n", status_);
			}
		};

		template<typename T, typename... Args>
		constexpr int insert_impl(const std::string& sql, const T& t, Args&&... args) {
			stmt_ = mysql_stmt_init(con_);
			if (!stmt_)
				return INT_MIN;

			if (mysql_stmt_prepare(stmt_, sql.c_str(), (int)sql.size())) {
				return INT_MIN;
			}

			auto guard = guard_statment(stmt_);

			if (stmt_execute(t) < 0)
				return INT_MIN;

			return 1;
		}

		template<typename T, typename... Args>
		constexpr int insert_impl(const std::string& sql, const std::vector<T>& t, Args&&... args) {
			stmt_ = mysql_stmt_init(con_);
			if (!stmt_)
				return INT_MIN;

			if (mysql_stmt_prepare(stmt_, sql.c_str(), (int)sql.size())) {
				return INT_MIN;
			}

			auto guard = guard_statment(stmt_);

			//transaction
			begin();
			
			for (auto& item : t) {
				int r = stmt_execute(item);
				if (r == INT_MIN) {
					rollback();
					return INT_MIN;
				}
			}

			commit();

			return (int)t.size() ;
		}

		template<typename... Args>
		auto get_tp(int& timeout, Args&&... args) {
			auto tp = std::make_tuple(con_, std::forward<Args>(args)...);
			if constexpr (sizeof...(Args) == 5) {
				auto [c, s1, s2, s3, s4, i] = tp;
				timeout = i;
				return std::make_tuple(c, s1, s2, s3, s4);
			}
			else
				return tp;
		}

	private:
		MYSQL* con_ = nullptr;
		MYSQL_STMT* stmt_ = nullptr;
		bool has_error_ = false;
		std::string last_error_;
		inline static std::map<std::string, std::string> auto_key_map_;
	};
}

#endif //ORM_MYSQL_HPP
