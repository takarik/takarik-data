require "log"
require "./takarik/data/string"
require "./takarik/data/validations"
require "./takarik/data/associations"
require "./takarik/data/query_builder"
require "./takarik/data/base_model"
require "./takarik/data/migration"

module Takarik::Data
  VERSION = "0.1.0"

  # Logger for database queries
  Log = ::Log.for("takarik-data")

  # Helper method to log database queries
  def self.log_query(sql : String, params : Array(DB::Any)? = nil, execution_time : Time::Span? = nil, model_name : String? = nil, operation : String? = nil)
    # Determine operation from SQL if not provided
    if operation.nil?
      operation = case sql.strip.upcase
                  when .starts_with?("SELECT")
                    "Load"
                  when .starts_with?("INSERT")
                    "Create"
                  when .starts_with?("UPDATE")
                    "Update"
                  when .starts_with?("DELETE")
                    "Destroy"
                  when .starts_with?("CREATE")
                    "Schema"
                  when .starts_with?("DROP")
                    "Schema"
                  when .starts_with?("ALTER")
                    "Schema"
                  else
                    "SQL"
                  end
    end

    # Build the message in Rails format
    prefix = if model_name
               "#{model_name} #{operation}"
             else
               operation.to_s
             end

    time_str = if execution_time
                 " (#{execution_time.total_milliseconds.round(1)}ms)"
               else
                 ""
               end

    param_str = if params && params.size > 0
                  # Convert parameters to Rails format [["param1", "value1"], ["param2", "value2"]]
                  param_pairs = params.map_with_index do |p, i|
                    param_name = "param#{i + 1}"
                    param_value = p.nil? ? "NULL" : p.to_s
                    "[\"#{param_name}\", \"#{param_value}\"]"
                  end
                  "  [#{param_pairs.join(", ")}]"
                else
                  ""
                end

    message = "#{prefix}#{time_str}  #{sql}#{param_str}"
    Log.info { message }
  end

  # Helper method to execute and log queries
  def self.exec_with_logging(connection : DB::Database, sql : String, args : Array(DB::Any)? = nil, model_name : String? = nil, operation : String? = nil)
    start_time = Time.monotonic
    result = if args && args.size > 0
               connection.exec(sql, args: args)
             else
               connection.exec(sql)
             end
    end_time = Time.monotonic

    log_query(sql, args, end_time - start_time, model_name, operation)
    result
  end

  # Helper method to execute scalar queries with logging
  def self.scalar_with_logging(connection : DB::Database, sql : String, args : Array(DB::Any)? = nil, model_name : String? = nil, operation : String? = nil)
    start_time = Time.monotonic
    result = if args && args.size > 0
               connection.scalar(sql, args: args)
             else
               connection.scalar(sql)
             end
    end_time = Time.monotonic

    log_query(sql, args, end_time - start_time, model_name, operation)
    result
  end

  # Helper method to execute query with logging and yield result set
  def self.query_with_logging(connection : DB::Database, sql : String, args : Array(DB::Any)? = nil, model_name : String? = nil, operation : String? = nil, &block : DB::ResultSet ->)
    start_time = Time.monotonic
    result = if args && args.size > 0
               connection.query(sql, args: args, &block)
             else
               connection.query(sql, &block)
             end
    end_time = Time.monotonic

    log_query(sql, args, end_time - start_time, model_name, operation)
    result
  end

  # Helper method to execute and log queries within transactions
  def self.exec_with_logging(connection : DB::Connection, sql : String, *args : DB::Any)
    start_time = Time.monotonic
    result = connection.exec(sql, *args)
    end_time = Time.monotonic

    log_query(sql, args.to_a, end_time - start_time)
    result
  end

  # Helper method to execute and log queries within transactions with args array
  def self.exec_with_logging(connection : DB::Connection, sql : String, args : Array(DB::Any), model_name : String? = nil, operation : String? = nil)
    start_time = Time.monotonic
    result = connection.exec(sql, args: args)
    end_time = Time.monotonic

    log_query(sql, args, end_time - start_time, model_name, operation)
    result
  end

  # Helper method to handle both DB::Database and DB::Connection with args array
  def self.exec_with_logging(connection : DB::Database | DB::Connection, sql : String, args : Array(DB::Any), model_name : String? = nil, operation : String? = nil)
    start_time = Time.monotonic
    result = connection.exec(sql, args: args)
    end_time = Time.monotonic

    log_query(sql, args, end_time - start_time, model_name, operation)
    result
  end
end
