require "db"
require "json"
require "base64"
require "./string"
require "./query_builder"
require "./validations"
require "./associations"

module Takarik::Data
  # Module-level connection variable that's shared across all classes
  @@connection : DB::Database?

  def self.connection
    @@connection || raise "Database connection not established. Call Takarik::Data.establish_connection first."
  end

  def self.establish_connection(database_url : String)
    @@connection = DB.open(database_url)
  end

  # Exception raised when a record is not found
  class RecordNotFound < Exception
    def initialize(message : String = "Record not found")
      super(message)
    end
  end

  # Exception raised when attempting to modify a readonly record
  class ReadOnlyRecord < Exception
    def initialize(message : String = "Attempting to modify a readonly record")
      super(message)
    end
  end

  # Exception raised when optimistic locking fails
  class StaleObjectError < Exception
    def initialize(message : String = "Attempted to update a stale object")
      super(message)
    end
  end

  # Exception raised when strict loading is violated (accessing non-preloaded associations)
  class StrictLoadingViolationError < Exception
    def initialize(message : String = "Attempted to access association that was not preloaded")
      super(message)
    end
  end

  # Base class for all ORM models, providing ActiveRecord-like functionality
  # but designed specifically for Crystal language features
  abstract class BaseModel
    include Validations
    include Associations

    # ========================================
    # CLASS VARIABLES
    # ========================================

    # Class variable to store custom table name for each model
    @@table_name : String?

    # Class variable to store primary key name for each model
    @@primary_key_name = "id"

    # Class variable to store column names for each model
    @@column_names = {} of String => Array(String)

    # Class variable to store locking column name for each model
    @@locking_column = {} of String => String

    # Class variable to store optimistic locking setting for each model
    @@lock_optimistically = {} of String => Bool

    # Global optimistic locking setting (default: true)
    @@global_lock_optimistically = true

    # ========================================
    # INSTANCE VARIABLES
    # ========================================

    @attributes = {} of String => DB::Any
    @persisted = false
    @changed_attributes = Set(String).new
    @_last_action : Symbol?
    @association_cache = {} of String => (BaseModel | Nil)
    @loaded_associations = Set(String).new
    @readonly = false
    @strict_loading = false

    # ========================================
    # CLASS METHODS - CONFIGURATION
    # ========================================

    def self.connection
      Takarik::Data.connection
    end

    # Helper method to get the simple class name without namespaces
    def self.model_name
      self.name.split("::").last
    end

    # Helper method to extract model name from any class
    def self.model_name_from(klass)
      klass.name.split("::").last
    end

    def self.table_name
      @@table_name || model_name.tableize
    end

    def self.primary_key
      @@primary_key_name
    end

    def self.column_names
      @@column_names[self.name]? || [] of String
    end

    # ========================================
    # CLASS METHODS - TRANSACTIONS
    # ========================================

    # Executes the given block within a database transaction.
    # If an exception is raised within the block, the transaction is rolled back.
    # Otherwise, the transaction is committed.
    #
    # Example:
    #   User.transaction do
    #     user = User.create(name: "John")
    #     user.posts.create(title: "Hello World")
    #   end
    def self.transaction(&block)
      connection.transaction(&block)
    end

    # Adds a locking clause to queries for pessimistic locking
    def self.lock(lock_type : String = "FOR UPDATE")
      all.lock(lock_type)
    end

    def self.add_column_name(name : String)
      @@column_names[self.name] ||= [] of String
      @@column_names[self.name] << name unless @@column_names[self.name].includes?(name)
    end

    # ========================================
    # CLASS METHODS - LOCKING CONFIGURATION
    # ========================================

    def self.locking_column
      @@locking_column[self.name]? || "lock_version"
    end

    def self.locking_column=(column_name : String)
      @@locking_column[self.name] = column_name
    end

    def self.lock_optimistically
      @@lock_optimistically[self.name]? || @@global_lock_optimistically
    end

    def self.lock_optimistically=(value : Bool)
      @@lock_optimistically[self.name] = value
    end

    # Global optimistic locking configuration
    def self.global_lock_optimistically
      @@global_lock_optimistically
    end

    def self.global_lock_optimistically=(value : Bool)
      @@global_lock_optimistically = value
    end

    # ========================================
    # CLASS METHODS - QUERY BUILDING
    # ========================================

    def self.unscoped
      QueryBuilder(self).new(self)
    end

    # Remove all scoping and execute the given block in an unscoped context
    # This is useful for temporarily bypassing default scopes
    #
    # Examples:
    #   Book.unscoped { Book.out_of_print }  # Executes without default scope
    #   User.unscoped { User.where(active: false) }  # Bypasses default scope
    #
    # The block is executed in the context of the unscoped model class
    def self.unscoped(&block : -> QueryBuilder(self))
      # Set a flag to bypass default scope during block execution
      @@_unscoped_context = true
      begin
        yield
      ensure
        @@_unscoped_context = false
      end
    end

    # Class variable to track unscoped context
    @@_unscoped_context = false

    # Internal query method - users should use all() instead
    protected def self.query
      apply_default_scope_if_exists
    end

    # Default method that does nothing - can be overridden by default_scope macro
    def self.apply_default_scope_if_exists
      unscoped
    end

    def self.all
      query
    end

    def self.where(conditions : Hash(String, DB::Any))
      query.where(conditions)
    end

    def self.where(**conditions)
      query.where(**conditions)
    end

    def self.where(condition : String, *params : DB::Any)
      query.where(condition, *params)
    end

    def self.where(column_with_operator : String, value : DB::Any)
      query.where(column_with_operator, value)
    end

    def self.where(column : String, values : Array(DB::Any))
      query.where(column, values)
    end

    # Named placeholder conditions
    def self.where(condition : String, named_params : Hash(String, DB::Any))
      query.where(condition, named_params)
    end

    def self.where(condition : String, named_params : NamedTuple)
      query.where(condition, named_params)
    end

    def self.where(condition : String, **named_params)
      query.where(condition, **named_params)
    end

    # New clean syntax methods
    def self.not(conditions : Hash(String, DB::Any))
      query.not(conditions)
    end

    def self.not(**conditions)
      query.not(**conditions)
    end

    def self.not(column : String, values : Array(DB::Any))
      query.not(column, values)
    end

    def self.not(condition : String, param : DB::Any)
      query.not(condition, param)
    end

    def self.not(condition : String, *params : DB::Any)
      query.not(condition, *params)
    end

    def self.not(column_with_operator : String, value : DB::Any)
      query.not(column_with_operator, value)
    end

    def self.associated(association_name : String | Symbol)
      query.associated(association_name)
    end

    def self.missing(association_name : String | Symbol)
      query.missing(association_name)
    end

    # Logical operator methods
    def self.or(conditions : Hash(String, DB::Any))
      query.or(conditions)
    end

    def self.or(**conditions)
      query.or(**conditions)
    end

    def self.or(condition : String, param : DB::Any)
      query.or(condition, param)
    end

    def self.or(condition : String, *params : DB::Any)
      query.or(condition, *params)
    end

    def self.or(column_with_operator : String, value : DB::Any)
      query.or(column_with_operator, value)
    end

    def self.or(column : String, values : Array(DB::Any))
      query.or(column, values)
    end

    def self.select(*columns : String)
      query.select(*columns)
    end

    def self.select(*columns : Symbol)
      query.select(*columns)
    end

    def self.select(columns : Array(String))
      query.select(columns)
    end

    def self.select(columns : Array(Symbol))
      query.select(columns)
    end

    def self.distinct(value : Bool = true)
      query.distinct(value)
    end

    def self.order(column : Symbol)
      query.order(column)
    end

    def self.order(column : String, direction : String = "ASC")
      query.order(column, direction)
    end

    def self.order(**columns)
      query.order(**columns)
    end

    def self.order(order_hash : Hash(Symbol | String, Hash(Symbol | String, Symbol | String) | Symbol | String))
      query.order(order_hash)
    end

    def self.order(*columns : String)
      query.order(*columns)
    end

    def self.order(first_column : Symbol, **additional_columns)
      query.order(first_column, **additional_columns)
    end

    def self.order(columns : Array(String))
      query.order(columns)
    end

    def self.limit(count : Int32)
      query.limit(count)
    end

    def self.offset(count : Int32)
      query.offset(count)
    end

    # Rails-compatible joins methods
    def self.joins(*associations : String | Symbol)
      query.joins(*associations)
    end

    def self.joins(associations : Array(String | Symbol))
      query.joins(associations)
    end

    def self.joins(nested_associations : Hash(String | Symbol, Array(String | Symbol) | String | Symbol))
      query.joins(nested_associations)
    end

    def self.joins(custom_sql : String)
      query.joins(custom_sql)
    end

    def self.joins(**named_associations)
      query.joins(**named_associations)
    end

    def self.joins(table : String, on : String)
      query.joins(table, on)
    end

    def self.inner_join(table : String, on : String)
      query.inner_join(table, on)
    end

    def self.inner_join(association_name : String | Symbol)
      query.inner_join(association_name)
    end

    def self.left_join(table : String, on : String)
      query.left_join(table, on)
    end

    def self.left_join(association_name : String | Symbol)
      query.left_join(association_name)
    end

    def self.right_join(table : String, on : String)
      query.right_join(table, on)
    end

    def self.right_join(association_name : String | Symbol)
      query.right_join(association_name)
    end

    def self.includes(*association_names : String | Symbol)
      query.includes(*association_names)
    end

    def self.includes(association_names : Array(String | Symbol))
      query.includes(association_names)
    end

    def self.preload(*association_names : String | Symbol)
      query.preload(*association_names)
    end

    def self.preload(association_names : Array(String | Symbol))
      query.preload(association_names)
    end

    def self.eager_load(*association_names : String | Symbol)
      query.eager_load(*association_names)
    end

    def self.eager_load(association_names : Array(String | Symbol))
      query.eager_load(association_names)
    end

    def self.group(*columns : String)
      query.group(*columns)
    end

    def self.group(*columns : Symbol)
      query.group(*columns)
    end

    def self.group(columns : Array(String))
      query.group(columns)
    end

    def self.group(columns : Array(Symbol))
      query.group(columns)
    end

    # Overriding condition methods
    def self.unscope(*clauses : Symbol)
      query.unscope(*clauses)
    end

    def self.unscope(*, where clause_name : Symbol)
      query.unscope(where: clause_name)
    end

    def self.only(*clauses : Symbol)
      query.only(*clauses)
    end

    def self.reselect(*columns : String)
      query.reselect(*columns)
    end

    def self.reselect(*columns : Symbol)
      query.reselect(*columns)
    end

    def self.reselect(columns : Array(String))
      query.reselect(columns)
    end

    def self.reselect(columns : Array(Symbol))
      query.reselect(columns)
    end

    def self.reorder(*columns : String)
      query.reorder(*columns)
    end

    def self.reorder(*columns : Symbol)
      query.reorder(*columns)
    end

    def self.reorder(column : String, direction : String = "ASC")
      query.reorder(column, direction)
    end

    def self.reorder(**columns)
      query.reorder(**columns)
    end

    def self.rewhere(conditions : Hash(String, DB::Any))
      query.rewhere(conditions)
    end

    def self.rewhere(**conditions)
      query.rewhere(**conditions)
    end

    def self.rewhere(condition : String, *params : DB::Any)
      query.rewhere(condition, *params)
    end

    def self.regroup(*columns : String)
      query.regroup(*columns)
    end

    def self.regroup(*columns : Symbol)
      query.regroup(*columns)
    end

    def self.regroup(columns : Array(String))
      query.regroup(columns)
    end

    def self.regroup(columns : Array(Symbol))
      query.regroup(columns)
    end

    def self.reverse_order
      query.reverse_order
    end

    def self.none
      query.none
    end

    def self.readonly
      query.readonly
    end

    def self.strict_loading
      query.strict_loading
    end

    def self.merge(other_relation : QueryBuilder)
      query.merge(other_relation)
    end

    def self.having(condition : String)
      query.having(condition)
    end

    def self.having(condition : String, *params : DB::Any)
      query.having(condition, *params)
    end

    def self.page(page_number : Int32, per_page : Int32)
      query.page(page_number, per_page)
    end

    # ========================================
    # CLASS METHODS - AGGREGATION
    # ========================================

    def self.sum(column : String)
      query.sum(column)
    end

    def self.average(column : String)
      query.average(column)
    end

    def self.minimum(column : String)
      query.minimum(column)
    end

    def self.maximum(column : String)
      query.maximum(column)
    end

    # ========================================
    # CLASS METHODS - FINDERS
    # ========================================

    # Find record by primary key. Returns the record or nil if not found.
    #
    # Examples:
    #   Customer.find(10)  # => Customer or nil
    #
    # SQL: SELECT * FROM customers WHERE (customers.id = 10) LIMIT 1
    def self.find(id : DB::Any)
      query.where(primary_key, id).first
    end

    # Find multiple records by array of primary keys. Returns array of records.
    # Raises RecordNotFound if any ID is missing.
    #
    # Examples:
    #   Customer.find([1, 10])  # => Array of Customer records
    #
    # SQL: SELECT * FROM customers WHERE (customers.id IN (1,10))
    def self.find(ids : Array)
      return [] of self if ids.empty?

      results = query.where(primary_key, ids).to_a

      # Check if all IDs were found
      if results.size != ids.size
        found_ids = results.map { |r| r.get_attribute(primary_key) }
        missing_ids = ids - found_ids
        raise RecordNotFound.new("Couldn't find all #{model_name} with '#{primary_key}' in #{ids.inspect} (found #{found_ids.size} results, but was looking for #{ids.size}). Missing IDs: #{missing_ids.inspect}")
      end

      results
    end

    # Find multiple records by splat arguments. Returns array of records.
    # Equivalent to find([id1, id2, ...])
    #
    # Examples:
    #   Customer.find(1, 10)  # Same as Customer.find([1, 10])
    def self.find(*ids)
      find(ids.to_a)
    end

    # Find record by primary key. Raises RecordNotFound exception if not found.
    #
    # Examples:
    #   Customer.find!(10)  # => Customer or raises RecordNotFound
    def self.find!(id : DB::Any)
      find(id) || raise RecordNotFound.new("Couldn't find #{model_name} with '#{primary_key}'=#{id}")
    end

    # Find multiple records by array of primary keys. Raises RecordNotFound if not found.
    #
    # Examples:
    #   Customer.find!([1, 10])  # => Array of Customer records or raises RecordNotFound
    def self.find!(ids : Array)
      results = find(ids)
      if results.empty? && !ids.empty?
        raise RecordNotFound.new("Couldn't find #{model_name} with '#{primary_key}' in #{ids.inspect}")
      end
      results
    end

    # Find multiple records by splat arguments. Raises RecordNotFound if not found.
    #
    # Examples:
    #   Customer.find!(1, 10)  # Same as Customer.find!([1, 10])
    def self.find!(*ids)
      find!(ids.to_a)
    end

    # Find the first record ordered by primary key (default).
    # If default scope contains an order method, returns the first record according to that ordering.
    #
    # Examples:
    #   Customer.first  # => Customer or nil
    #
    # SQL: SELECT * FROM customers ORDER BY customers.id ASC LIMIT 1
    # SQL (composite): SELECT * FROM customers ORDER BY customers.store_id ASC, customers.id ASC LIMIT 1
    def self.first
      # Check if there's already ordering from default scope or query chain
      query_builder = query

      # If no existing order, add primary key ordering
      if query_builder.@order_clauses.empty?
        # Handle composite primary keys
        if primary_key.includes?(",")
          primary_keys = primary_key.split(",").map(&.strip)
          primary_keys.each do |key|
            query_builder = query_builder.order(key, "ASC")
          end
        else
          query_builder = query_builder.order(primary_key, "ASC")
        end
      end

      query_builder.limit(1).first
    end

    # Find up to the specified number of records ordered by primary key (default).
    #
    # Examples:
    #   Customer.first(3)  # => Array of up to 3 Customer records
    #
    # SQL: SELECT * FROM customers ORDER BY customers.id ASC LIMIT 3
    def self.first(limit : Int32)
      # Check if there's already ordering from default scope or query chain
      query_builder = query

      # If no existing order, add primary key ordering
      if query_builder.@order_clauses.empty?
        # Handle composite primary keys
        if primary_key.includes?(",")
          primary_keys = primary_key.split(",").map(&.strip)
          primary_keys.each do |key|
            query_builder = query_builder.order(key, "ASC")
          end
        else
          query_builder = query_builder.order(primary_key, "ASC")
        end
      end

      query_builder.limit(limit).to_a
    end

    # Find the first record ordered by primary key. Raises RecordNotFound if no record found.
    #
    # Examples:
    #   Customer.first!  # => Customer or raises RecordNotFound
    def self.first!
      first || raise RecordNotFound.new("Couldn't find #{model_name}")
    end

    # Find up to the specified number of records ordered by primary key.
    # Raises RecordNotFound if no records found.
    #
    # Examples:
    #   Customer.first!(3)  # => Array of Customer records or raises RecordNotFound
    def self.first!(limit : Int32)
      results = first(limit)
      if results.empty?
        raise RecordNotFound.new("Couldn't find #{model_name}")
      end
      results
    end

    # Find the last record ordered by primary key (default).
    # If default scope contains an order method, returns the last record according to that ordering.
    #
    # Examples:
    #   Customer.last  # => Customer or nil
    #
    # SQL: SELECT * FROM customers ORDER BY customers.id DESC LIMIT 1
    # SQL (composite): SELECT * FROM customers ORDER BY customers.store_id DESC, customers.id DESC LIMIT 1
    def self.last
      # Check if there's already ordering from default scope or query chain
      query_builder = query

      if query_builder.@order_clauses.empty?
        # No existing order, add primary key ordering (DESC for last)
        if primary_key.includes?(",")
          primary_keys = primary_key.split(",").map(&.strip)
          primary_keys.each do |key|
            query_builder = query_builder.order(key, "DESC")
          end
        else
          query_builder = query_builder.order(primary_key, "DESC")
        end
      else
        # There's existing order, reverse it for last
        query_builder = query_builder.reverse_order
      end

      query_builder.limit(1).first
    end

    # Find up to the specified number of records ordered by primary key (default) in reverse.
    #
    # Examples:
    #   Customer.last(3)  # => Array of up to 3 Customer records (highest IDs first)
    #
    # SQL: SELECT * FROM customers ORDER BY customers.id DESC LIMIT 3
    def self.last(limit : Int32)
      # Check if there's already ordering from default scope or query chain
      query_builder = query

      if query_builder.@order_clauses.empty?
        # No existing order, add primary key ordering (DESC for last)
        if primary_key.includes?(",")
          primary_keys = primary_key.split(",").map(&.strip)
          primary_keys.each do |key|
            query_builder = query_builder.order(key, "DESC")
          end
        else
          query_builder = query_builder.order(primary_key, "DESC")
        end
      else
        # There's existing order, reverse it for last
        query_builder = query_builder.reverse_order
      end

      query_builder.limit(limit).to_a
    end

    # Find the last record ordered by primary key. Raises RecordNotFound if no record found.
    #
    # Examples:
    #   Customer.last!  # => Customer or raises RecordNotFound
    def self.last!
      last || raise RecordNotFound.new("Couldn't find #{model_name}")
    end

    # Find up to the specified number of records ordered by primary key in reverse.
    # Raises RecordNotFound if no records found.
    #
    # Examples:
    #   Customer.last!(3)  # => Array of Customer records or raises RecordNotFound
    def self.last!(limit : Int32)
      results = last(limit)
      if results.empty?
        raise RecordNotFound.new("Couldn't find #{model_name}")
      end
      results
    end

    # Retrieve a record without any implicit ordering. Returns nil if no record found.
    #
    # Examples:
    #   Customer.take  # => Customer or nil
    #
    # SQL: SELECT * FROM customers LIMIT 1
    def self.take
      query.limit(1).first
    end

    # Retrieve up to the specified number of records without any implicit ordering.
    #
    # Examples:
    #   Customer.take(2)  # => Array of up to 2 Customer records
    #
    # SQL: SELECT * FROM customers LIMIT 2
    def self.take(limit : Int32)
      query.limit(limit).to_a
    end

    # Retrieve a record without any implicit ordering. Raises RecordNotFound if no record found.
    #
    # Examples:
    #   Customer.take!  # => Customer or raises RecordNotFound
    def self.take!
      take || raise RecordNotFound.new("Couldn't find #{model_name}")
    end

    # Retrieve up to the specified number of records without any implicit ordering.
    # Raises RecordNotFound if no records found.
    #
    # Examples:
    #   Customer.take!(2)  # => Array of Customer records or raises RecordNotFound
    def self.take!(limit : Int32)
      results = take(limit)
      if results.empty?
        raise RecordNotFound.new("Couldn't find #{model_name}")
      end
      results
    end

    def self.count : Int64 | Hash(String, Int64)
      query.count
    end

    # Find the first record matching the given conditions without any implicit ordering.
    # Returns nil if no record is found.
    #
    # Examples:
    #   Customer.find_by(first_name: "Lifo")  # => Customer or nil
    #   Customer.find_by(first_name: "Lifo", last_name: "Smith")  # => Customer or nil
    #
    # SQL: SELECT * FROM customers WHERE (customers.first_name = 'Lifo') LIMIT 1
    def self.find_by(conditions : Hash(String, DB::Any))
      where(conditions).take
    end

    def self.find_by(**conditions)
      where(**conditions).take
    end

    # Find the first record matching the given conditions without any implicit ordering.
    # Raises RecordNotFound if no record is found.
    #
    # Examples:
    #   Customer.find_by!(first_name: "Lifo")  # => Customer or raises RecordNotFound
    #   Customer.find_by!(first_name: "NonExistent")  # => raises RecordNotFound
    def self.find_by!(conditions : Hash(String, DB::Any))
      where(conditions).take!
    end

    def self.find_by!(**conditions)
      where(**conditions).take!
    end

    # ========================================
    # CLASS METHODS - BATCH PROCESSING
    # ========================================

    # Retrieve records in batches and yield each one to the block.
    # This is efficient for processing large datasets without loading everything into memory.
    #
    # Examples:
    #   Customer.find_each do |customer|
    #     NewsMailer.weekly(customer).deliver_now
    #   end
    #
    #   Customer.find_each(start: 2000, batch_size: 5000) do |customer|
    #     process_customer(customer)
    #   end
    #
    #   # For composite primary keys
    #   Order.find_each(order: [:asc, :desc]) do |order|
    #     process_order(order)
    #   end
    #
    # Options:
    #   :start - Starting cursor value (inclusive)
    #   :finish - Ending cursor value (inclusive)
    #   :batch_size - Number of records per batch (default: 1000)
    #   :error_on_ignore - Raise error if existing order is present (default: nil)
    #   :cursor - Column(s) to use for batching (default: primary_key)
    #   :order - Cursor order (:asc/:desc or array like [:asc, :desc], default: :asc)
    def self.find_each(start : DB::Any? = nil, finish : DB::Any? = nil, batch_size : Int32 = 1000,
                       error_on_ignore : Bool? = nil, cursor : String | Array(String) = primary_key,
                       order : Symbol | Array(Symbol) = Takarik::Data::QueryBuilder::DEFAULT_ORDER, &block : self ->)
      query.find_each(start: start, finish: finish, batch_size: batch_size,
        error_on_ignore: error_on_ignore, cursor: cursor, order: order, &block)
    end

    # Returns an Enumerator when no block is given
    def self.find_each(start : DB::Any? = nil, finish : DB::Any? = nil, batch_size : Int32 = 1000,
                       error_on_ignore : Bool? = nil, cursor : String | Array(String) = primary_key,
                       order : Symbol | Array(Symbol) = Takarik::Data::QueryBuilder::DEFAULT_ORDER)
      query.find_each(start: start, finish: finish, batch_size: batch_size,
        error_on_ignore: error_on_ignore, cursor: cursor, order: order)
    end

    # Yields each batch of records as an array.
    #
    # Examples:
    #   Customer.find_in_batches do |batch|
    #     batch.each { |customer| NewsMailer.weekly(customer).deliver_now }
    #   end
    #
    #   Customer.find_in_batches(start: 1000, batch_size: 500) do |batch|
    #     batch.each { |customer| process_customer(customer) }
    #   end
    def self.find_in_batches(start : DB::Any? = nil, finish : DB::Any? = nil, batch_size : Int32 = 1000,
                             error_on_ignore : Bool? = nil, cursor : String | Array(String) = primary_key,
                             order : Symbol | Array(Symbol) = Takarik::Data::QueryBuilder::DEFAULT_ORDER, &block : Array(self) ->)
      query.find_in_batches(start: start, finish: finish, batch_size: batch_size,
        error_on_ignore: error_on_ignore, cursor: cursor, order: order, &block)
    end

    # Returns an Enumerator when no block is given
    def self.find_in_batches(start : DB::Any? = nil, finish : DB::Any? = nil, batch_size : Int32 = 1000,
                             error_on_ignore : Bool? = nil, cursor : String | Array(String) = primary_key,
                             order : Symbol | Array(Symbol) = Takarik::Data::QueryBuilder::DEFAULT_ORDER)
      query.find_in_batches(start: start, finish: finish, batch_size: batch_size,
        error_on_ignore: error_on_ignore, cursor: cursor, order: order)
    end

    # Yields QueryBuilder objects to work with a batch of records.
    # This is similar to Rails' in_batches method.
    #
    # Examples:
    #   User.where("age > 21").in_batches do |relation|
    #     relation.delete_all
    #     sleep(1) # Throttle the delete queries
    #   end
    #
    #   User.in_batches.each_with_index do |relation, batch_index|
    #     puts "Processing relation ##{batch_index}"
    #     relation.delete_all
    #   end
    def self.in_batches(of batch_size : Int32 = 1000, start : DB::Any? = nil, finish : DB::Any? = nil,
                        load : Bool = false, error_on_ignore : Bool? = nil,
                        cursor : String | Array(String) = primary_key, order : Symbol | Array(Symbol) = Takarik::Data::QueryBuilder::DEFAULT_ORDER,
                        use_ranges : Bool? = nil, &block : Takarik::Data::QueryBuilder(self) ->)
      query.in_batches(of: batch_size, start: start, finish: finish, load: load,
        error_on_ignore: error_on_ignore, cursor: cursor, order: order, use_ranges: use_ranges, &block)
    end

    # Returns a BatchEnumerator when no block is given
    def self.in_batches(of batch_size : Int32 = 1000, start : DB::Any? = nil, finish : DB::Any? = nil,
                        load : Bool = false, error_on_ignore : Bool? = nil,
                        cursor : String | Array(String) = primary_key, order : Symbol | Array(Symbol) = Takarik::Data::QueryBuilder::DEFAULT_ORDER,
                        use_ranges : Bool? = nil)
      query.in_batches(of: batch_size, start: start, finish: finish, load: load,
        error_on_ignore: error_on_ignore, cursor: cursor, order: order, use_ranges: use_ranges)
    end

    # ========================================
    # INSTANCE METHODS - ID VALUE ACCESS
    # ========================================

    # Returns the value of the :id column specifically (not primary key).
    # This is different from primary key access in composite key models.
    #
    # Examples:
    #   customer = Customer.last
    #   customer.id_value  # => 10 (returns the :id column value)
    #
    # For composite primary key models like [:store_id, :id], this returns
    # only the :id column value, not the full composite key.
    def id_value
      get_attribute("id")
    end

    # ========================================
    # CLASS METHODS - CREATION
    # ========================================

    def self.create(attributes : Hash(String, DB::Any))
      instance = new
      attributes.each do |key, value|
        instance.set_attribute(key, value)
      end
      instance.save
      instance
    end

    def self.create(**attributes)
      # Process association objects first
      processed_attributes = process_association_attributes_for_create(attributes)
      create(processed_attributes)
    end

    # ========================================
    # INITIALIZE
    # ========================================

    def initialize
      @attributes = {} of String => DB::Any
      @persisted = false
      @changed_attributes = Set(String).new
      @_last_action = nil

      # Run after_initialize callbacks automatically for new instances
      run_after_initialize_callbacks
    end

    # ========================================
    # INSTANCE METHODS - STATE
    # ========================================

    def persisted?
      @persisted
    end

    def new_record?
      !@persisted
    end

    def changed?
      @changed_attributes.any?
    end

    def changed_attributes
      @changed_attributes.to_a
    end

    # ========================================
    # INSTANCE METHODS - ATTRIBUTES
    # ========================================

    def set_attribute(name : String, value : DB::Any)
      @attributes[name] = value
      @changed_attributes << name

      # Also set the instance variable if it exists
      set_single_instance_variable(name, value)
    end

    def get_attribute(name : String)
      @attributes[name]?
    end

    def to_h
      @attributes.dup
    end

    # ========================================
    # INSTANCE METHODS - ASSOCIATION CACHE
    # ========================================

    def association_loaded?(association_name : String)
      @association_cache.has_key?(association_name)
    end

    def get_cached_association(association_name : String)
      @association_cache[association_name]?
    end

    def cache_association(association_name : String, value : (BaseModel | Nil))
      @association_cache[association_name] = value
      @loaded_associations.add(association_name)
    end

    def association_loaded?(association_name : String)
      @loaded_associations.includes?(association_name)
    end

    def loaded?(association_name : String | Symbol)
      @loaded_associations.includes?(association_name.to_s)
    end

    def load(association_name : String | Symbol)
      association_name_str = association_name.to_s
      return if loaded?(association_name_str)

      # Find the association definition
      associations = self.class.associations
      association = associations.find { |a| a.name == association_name_str }

      unless association
        raise "Association '#{association_name_str}' not found for #{self.class.name}"
      end

      # Load the association based on its type
      case association.type
      when .belongs_to?
        load_belongs_to_association(association)
      when .has_one?
        load_has_one_association(association)
      when .has_many?
        load_has_many_association(association)
      else
        raise "Cannot explicitly load association type: #{association.type}"
      end
    end

    private def load_belongs_to_association(association)
      return unless association.class_type

      foreign_key_value = get_attribute(association.foreign_key)
      if foreign_key_value
        result = association.class_type.not_nil!.find(foreign_key_value)
        cache_association(association.name, result)
      else
        cache_association(association.name, nil)
      end
    end

    private def load_has_one_association(association)
      return unless association.class_type

      primary_key_value = get_attribute(association.primary_key)
      if primary_key_value
        conditions = Hash(String, DB::Any).new
        conditions[association.foreign_key] = primary_key_value
        result = association.class_type.not_nil!.where(conditions).first
        cache_association(association.name, result)
      else
        cache_association(association.name, nil)
      end
    end

    private def load_has_many_association(association)
      return unless association.class_type

      primary_key_value = get_attribute(association.primary_key)
      if primary_key_value
        conditions = Hash(String, DB::Any).new
        conditions[association.foreign_key] = primary_key_value
        results = association.class_type.not_nil!.where(conditions).to_a
        # For has_many, we just mark as loaded but don't cache the array
        cache_association(association.name, nil)
      else
        cache_association(association.name, nil)
      end
    end

    def mark_as_persisted
      @persisted = true
      @changed_attributes.clear
    end

    # ========================================
    # INSTANCE METHODS - PERSISTENCE
    # ========================================
    #
    # These methods implement real database transactions with proper callback execution:
    #
    # Execution order:
    # 1. before_* callbacks (outside transaction)
    # 2. START TRANSACTION
    # 3. Database operation (INSERT/UPDATE/DELETE)
    # 4. after_create/after_update/after_destroy callbacks (inside transaction)
    # 5. after_save callbacks (inside transaction, for create/update only)
    # 6. COMMIT TRANSACTION (automatic if no exceptions)
    # 7. after_commit callbacks (after successful commit)
    #
    # On failure or exception:
    # - ROLLBACK TRANSACTION (automatic on exception, explicit on failed operation)
    # - after_rollback callbacks (after rollback)
    #
    def save
      # Check if record is readonly
      if @readonly
        raise Takarik::Data::ReadOnlyRecord.new
      end

      # Run before_validation callbacks
      run_before_validation_callbacks

      # Run validation if the model includes validations
      {% if @type.ancestors.any? { |a| a.name.stringify.includes?("Validations") } %}
        valid_result = valid?

        # Run after_validation callbacks
        run_after_validation_callbacks

        return false unless valid_result
      {% else %}
        # Run after_validation callbacks even if no validations module
        run_after_validation_callbacks
      {% end %}

      result = if new_record?
                 insert_record
               else
                 update_record if changed?
               end

      result
    end

    def save!
      # Check if record is readonly
      if @readonly
        raise Takarik::Data::ReadOnlyRecord.new
      end

      # Run before_validation callbacks
      run_before_validation_callbacks

      # Run validation if the model includes validations
      {% if @type.ancestors.any? { |a| a.name.stringify.includes?("Validations") } %}
        valid_result = valid?

        # Run after_validation callbacks
        run_after_validation_callbacks

        unless valid_result
          raise Takarik::Data::Validations::ValidationError.new(@validation_errors)
        end
      {% else %}
        # Run after_validation callbacks even if no validations module
        run_after_validation_callbacks
      {% end %}

      result = if new_record?
                 insert_record
               else
                 update_record if changed?
               end

      result || raise "Failed to save record"
    end

    def update(attributes : Hash(String, DB::Any))
      # Check if record is readonly
      if @readonly
        raise Takarik::Data::ReadOnlyRecord.new
      end

      # Check if any attributes are association objects and process them
      processed_attributes = process_db_any_attributes_for_associations(attributes)
      processed_attributes.each do |key, value|
        set_attribute(key, value)
      end
      save
    end

    def update(**attributes)
      # Check if record is readonly
      if @readonly
        raise Takarik::Data::ReadOnlyRecord.new
      end

      # Process association objects first
      processed_attributes = process_association_attributes_for_update(attributes)
      update(processed_attributes)
    end

    def touch(*attributes)
      return false if new_record?

      # Check if record is readonly
      if @readonly
        raise Takarik::Data::ReadOnlyRecord.new
      end

      current_time = Time.utc

      # If no specific attributes provided, touch updated_at by default
      if attributes.empty?
        # Try to set updated_at if it exists
        begin
          self.updated_at = current_time
        rescue
          # Ignore if updated_at doesn't exist
        end
      else
        # Touch specified attributes
        attributes.each do |attr|
          attr_name = attr.to_s
          set_attribute(attr_name, current_time.as(DB::Any))
        end

        # Always update updated_at if it exists and wasn't already specified
        unless attributes.any? { |attr| attr.to_s == "updated_at" }
          begin
            self.updated_at = current_time
          rescue
            # Ignore if updated_at doesn't exist
          end
        end
      end

      # Save without validation (like ActiveRecord touch)
      result = update_record if changed?

      # Run after_touch callbacks if the update was successful
      if result
        run_after_touch_callbacks
      end

      result
    end

    def destroy
      return false if new_record?

      # Check if record is readonly
      if @readonly
        raise Takarik::Data::ReadOnlyRecord.new
      end

      # Run before_destroy callbacks
      run_before_destroy_callbacks

      query = "DELETE FROM #{self.class.table_name} WHERE #{self.class.primary_key} = ?"
      id_value = get_attribute(self.class.primary_key)

      begin
        self.class.connection.transaction do |tx|
          result = Takarik::Data.exec_with_logging(tx.connection, query, [id_value], self.class.name, "Destroy")

          if result.rows_affected > 0
            @persisted = false

            # Call dependent associations handling after main record is destroyed but within transaction
            {% if @type.ancestors.any? { |a| a.name.stringify.includes?("Associations") } %}
              destroy_dependent_associations(tx.connection)
            {% end %}

            # Set the action for callbacks
            @_last_action = :destroy

            # Run after_destroy callbacks (inside transaction)
            run_after_destroy_callbacks

            # Transaction will commit automatically here
          else
            # Explicitly rollback the transaction
            tx.rollback
            @_last_action = :destroy
            execute_rollback_callbacks(:destroy)
            return false
          end
        end

        # Transaction committed successfully - run after_commit callbacks
        @_last_action = :destroy
        execute_commit_callbacks(:destroy)
        true
      rescue ex
        # Transaction was rolled back due to exception - run after_rollback callbacks
        @_last_action = :destroy
        execute_rollback_callbacks(:destroy)
        raise ex
      end
    end

    # Transaction-aware destroy method for use within existing transactions
    def destroy_with_connection(connection)
      return false if new_record?

      # Run before_destroy callbacks
      run_before_destroy_callbacks

      query = "DELETE FROM #{self.class.table_name} WHERE #{self.class.primary_key} = ?"
      id_value = get_attribute(self.class.primary_key)

      result = Takarik::Data.exec_with_logging(connection, query, [id_value], self.class.name, "Destroy")

      if result.rows_affected > 0
        @persisted = false

        # Call dependent associations handling within the same transaction
        {% if @type.ancestors.any? { |a| a.name.stringify.includes?("Associations") } %}
          destroy_dependent_associations(connection)
        {% end %}

        # Set the action for callbacks
        @_last_action = :destroy

        # Run after_destroy callbacks (inside transaction)
        run_after_destroy_callbacks

        true
      else
        false
      end
    end

    def reload
      return self if new_record?

      id_value = get_attribute(self.class.primary_key)
      fresh_record = self.class.find(id_value)

      if fresh_record
        @attributes = fresh_record.@attributes
        sync_instance_variables_from_attributes
        @changed_attributes.clear
        self
      else
        raise "Record no longer exists"
      end
    end

    # ========================================
    # LOCKING METHODS
    # ========================================

    # Obtains a row lock on this record, reloads the attributes, and yields to the block.
    # The lock is released when the block completes.
    # This is useful for ensuring that only one process can modify a record at a time.
    #
    # Example:
    #   book = Book.first
    #   book.with_lock do
    #     # This block is called within a transaction,
    #     # book is already locked.
    #     book.increment!(:views)
    #   end
    def with_lock(lock_type : String? = nil, &block)
      return yield if new_record?

      self.class.connection.transaction do |tx|
        # Reload the record with a lock
        id_value = get_attribute(self.class.primary_key)
        lock_clause = lock_type || "FOR UPDATE"

        sql = "SELECT * FROM #{self.class.table_name} WHERE #{self.class.primary_key} = ? #{lock_clause}"

        # Use the transaction connection directly
        tx.connection.query(sql, [id_value]) do |rs|
          if rs.move_next
            load_from_result_set(rs)
            @changed_attributes.clear
          else
            raise RecordNotFound.new("Couldn't find #{self.class.model_name} with '#{self.class.primary_key}'=#{id_value}")
          end
        end

        yield
      end
    end

    # ========================================
    # PRIVATE METHODS - PERSISTENCE
    # ========================================

    private def insert_record
      # Run before_save callbacks first
      run_before_save_callbacks

      # Run before_create callbacks
      run_before_create_callbacks

      columns = @attributes.keys
      return false if columns.empty?

      placeholders = (["?"] * columns.size).join(", ")
      query = "INSERT INTO #{self.class.table_name} (#{columns.join(", ")}) VALUES (#{placeholders})"

      # Check if we're already in a transaction by trying to get the current connection
      # If we're in a transaction, use the existing connection, otherwise start a new transaction
      begin
        # Try to execute without starting a new transaction first
        result = Takarik::Data.exec_with_logging(self.class.connection, query, @attributes.values.to_a, self.class.name, "Create")

        if result.rows_affected > 0
          # Get the inserted ID if it's an auto-increment primary key and not already set
          primary_key_name = self.class.primary_key
          unless @attributes.has_key?(primary_key_name)
            id_value = result.last_insert_id
            @attributes[primary_key_name] = id_value.as(DB::Any)
            # Also set the instance variable directly without going through set_attribute
            # to avoid adding it to changed_attributes
            set_single_instance_variable(primary_key_name, id_value.as(DB::Any))
          end
          @persisted = true
          @changed_attributes.clear

          # Set the action for callbacks
          @_last_action = :create

          # Run after_create callbacks
          run_after_create_callbacks

          # Run after_save callbacks
          run_after_save_callbacks

          # Run after_commit callbacks
          @_last_action = :create
          execute_commit_callbacks(:create)
          true
        else
          @_last_action = :create
          execute_rollback_callbacks(:create)
          false
        end
      rescue ex
        # Run after_rollback callbacks
        @_last_action = :create
        execute_rollback_callbacks(:create)
        raise ex
      end
    end

    private def update_record
      return false if @changed_attributes.empty?

      # Run before_save callbacks first
      run_before_save_callbacks

      # Run before_update callbacks
      run_before_update_callbacks

      # Handle optimistic locking
      locking_column = self.class.locking_column
      current_lock_version = nil

      if self.class.lock_optimistically && self.class.column_names.includes?(locking_column)
        current_lock_version = get_attribute(locking_column)
        # Increment the lock version
        new_lock_version = case current_lock_version
                           when Int32
                             current_lock_version + 1
                           when Int64
                             current_lock_version + 1
                           when Nil
                             1
                           else
                             1
                           end
        set_attribute(locking_column, new_lock_version)
        @changed_attributes.add(locking_column)
      end

      set_clause = @changed_attributes.map { |attr| "#{attr} = ?" }.join(", ")

      # Build WHERE clause with optimistic locking check
      where_clause = "#{self.class.primary_key} = ?"
      args = @changed_attributes.map { |attr| @attributes[attr] }.to_a
      id_value = get_attribute(self.class.primary_key)
      args << id_value

      if self.class.lock_optimistically && self.class.column_names.includes?(locking_column) && current_lock_version
        where_clause += " AND #{locking_column} = ?"
        args << current_lock_version
      end

      query = "UPDATE #{self.class.table_name} SET #{set_clause} WHERE #{where_clause}"

      begin
        result = Takarik::Data.exec_with_logging(self.class.connection, query, args, self.class.name, "Update")

        if result.rows_affected > 0
          @changed_attributes.clear

          # Set the action for callbacks
          @_last_action = :update

          # Run after_update callbacks
          run_after_update_callbacks

          # Run after_save callbacks
          run_after_save_callbacks

          # Run after_commit callbacks
          @_last_action = :update
          execute_commit_callbacks(:update)
          true
        else
          # Check if this was an optimistic locking failure
          if self.class.lock_optimistically && self.class.column_names.includes?(locking_column) && current_lock_version
            # Rollback the lock version change
            set_attribute(locking_column, current_lock_version)
            @changed_attributes.delete(locking_column)

            @_last_action = :update
            execute_rollback_callbacks(:update)

            raise StaleObjectError.new("Attempted to update a stale object: #{self.class.name}")
          else
            @_last_action = :update
            execute_rollback_callbacks(:update)
            false
          end
        end
      rescue ex
        # Run after_rollback callbacks
        @_last_action = :update
        execute_rollback_callbacks(:update)
        raise ex
      end
    end

    # Process Hash(String, DB::Any) attributes to handle association objects
    private def process_db_any_attributes_for_associations(attributes : Hash(String, DB::Any))
      processed = Hash(String, DB::Any).new

      # Get all belongs_to associations for this model
      belongs_to_associations = self.class.associations.select(&.type.belongs_to?)

      attributes.each do |key, value|
        # Check if this key corresponds to a belongs_to association
        association = belongs_to_associations.find { |a| a.name == key }

        if association && value.is_a?(BaseModel)
          # Extract the primary key from the model object
          primary_key_value = value.get_attribute(association.primary_key)
          processed[association.foreign_key] = primary_key_value
        else
          # Regular attribute - keep as is
          processed[key] = value
        end
      end

      processed
    end

    # Process attributes to handle association objects for update
    private def process_association_attributes_for_update(attributes)
      processed = Hash(String, DB::Any).new

      # Get all belongs_to associations for this model (including polymorphic)
      belongs_to_associations = self.class.associations.select { |a| a.type.belongs_to? || a.type.belongs_to_polymorphic? }

      attributes.each do |key, value|
        key_str = key.to_s

        # Check if this key corresponds to a belongs_to association
        association = belongs_to_associations.find { |a| a.name == key_str }

        if association && value.is_a?(BaseModel)
          if association.type.belongs_to_polymorphic?
            # Handle polymorphic association
            primary_key_value = value.get_attribute(association.primary_key)
            processed[association.foreign_key] = primary_key_value
            processed[association.polymorphic_type.not_nil!] = self.class.model_name_from(value.class)
          else
            # Handle regular belongs_to association
            primary_key_value = value.get_attribute(association.primary_key)
            processed[association.foreign_key] = primary_key_value
          end
        else
          # Regular attribute - convert to DB::Any
          processed[key_str] = value.as(DB::Any)
        end
      end

      processed
    end

    # ========================================
    # INSTANCE METHODS - COMPARISON
    # ========================================

    def ==(other : BaseModel)
      return false unless other.class == self.class
      return false if new_record? || other.new_record?

      self_id = get_attribute(self.class.primary_key)
      other_id = other.get_attribute(self.class.primary_key)
      self_id == other_id
    end

    # ========================================
    # INSTANCE METHODS - JSON SERIALIZATION
    # ========================================

    def to_json(json : JSON::Builder)
      json.object do
        @attributes.each do |key, value|
          json.field key do
            case value
            when Nil
              json.null
            when Bool
              json.bool(value)
            when String
              json.string(value)
            when Int32, Int64, Float32, Float64
              json.number(value)
            when Time
              json.string(value.to_rfc3339)
            when Bytes
              # Convert binary data to base64 string
              json.string(Base64.strict_encode(value))
            end
          end
        end
      end
    end

    def to_json(io : IO) : Nil
      JSON.build(io) do |json|
        to_json(json)
      end
    end

    def to_json : String
      JSON.build do |json|
        to_json(json)
      end
    end

    # ========================================
    # PROTECTED METHODS
    # ========================================

    protected def load_from_result_set(rs : DB::ResultSet)
      rs.column_names.each_with_index do |column_name, index|
        value = rs.read
        @attributes[column_name] = value
      end

      sync_instance_variables_from_attributes
      @persisted = true
      @changed_attributes.clear

      # Run after_find callbacks since we loaded from database
      run_after_find_callbacks
    end

    protected def load_from_prefixed_result_set(rs : DB::ResultSet)
      table_prefix = "#{self.class.table_name}_"

      rs.column_names.each_with_index do |column_name, index|
        value = rs.read

        # Only process columns that belong to this table (have the correct prefix)
        if column_name.starts_with?(table_prefix)
          # Remove the table prefix to get the actual column name
          actual_column_name = column_name[table_prefix.size..-1]
          @attributes[actual_column_name] = value
        end
      end

      sync_instance_variables_from_attributes
      @persisted = true
      @changed_attributes.clear

      # Run after_find callbacks since we loaded from database
      run_after_find_callbacks
    end

    protected def load_from_result_set_with_includes(rs : DB::ResultSet, includes : Array(String))
      table_prefix = "#{self.class.table_name}_"

      # Read all values at once
      all_values = [] of DB::Any
      rs.column_names.each do |column_name|
        all_values << rs.read
      end

      # Process main model attributes
      rs.column_names.each_with_index do |column_name, index|
        value = all_values[index]

        # Only process columns that belong to this table (have the correct prefix)
        if column_name.starts_with?(table_prefix)
          # Remove the table prefix to get the actual column name
          actual_column_name = column_name[table_prefix.size..-1]
          @attributes[actual_column_name] = value
        end
      end

      # Process associated models
      includes.each do |association_name|
        load_association_from_values(rs.column_names, all_values, association_name)
      end

      sync_instance_variables_from_attributes
      @persisted = true
      @changed_attributes.clear

      # Run after_find callbacks since we loaded from database
      run_after_find_callbacks
    end

    private def load_association_from_values(column_names : Array(String), values : Array(DB::Any), association_name : String)
      associations = self.class.associations
      association = associations.find { |a| a.name == association_name }
      return unless association && association.class_type && !association.polymorphic

      associated_table = association.class_type.not_nil!.table_name
      associated_prefix = "#{associated_table}_"

      # Check if the association's primary key is not null (indicating a real association)
      primary_key_column = "#{associated_prefix}#{association.primary_key}"
      primary_key_index = column_names.index(primary_key_column)

      if primary_key_index && values[primary_key_index] && !values[primary_key_index].nil?
        # Create and populate the associated instance
        associated_instance = association.class_type.not_nil!.new

        column_names.each_with_index do |column_name, index|
          if column_name.starts_with?(associated_prefix)
            # Remove the table prefix to get the actual column name
            actual_column_name = column_name[associated_prefix.size..-1]
            associated_instance.set_attribute(actual_column_name, values[index])
          end
        end

        # Mark as persisted since it came from the database
        associated_instance.mark_as_persisted

        # Cache the association
        cache_association(association_name, associated_instance)
      else
        # No associated record, cache nil
        cache_association(association_name, nil)
      end
    end

    # ========================================
    # PRIVATE METHODS - HELPERS
    # ========================================

    private def set_single_instance_variable(column_name : String, value : DB::Any)
      assign_instance_variables_from_attributes
    end

    private def sync_instance_variables_from_attributes
      @attributes.each do |column_name, value|
        assign_instance_variables_from_attributes
      end
    end

    # Process attributes to handle association objects for create
    private def self.process_association_attributes_for_create(attributes)
      processed = Hash(String, DB::Any).new

      # Get all belongs_to associations for this model (including polymorphic)
      belongs_to_associations = associations.select { |a| a.type.belongs_to? || a.type.belongs_to_polymorphic? }

      attributes.each do |key, value|
        key_str = key.to_s

        # Check if this key corresponds to a belongs_to association
        association = belongs_to_associations.find { |a| a.name == key_str }

        if association && value.is_a?(BaseModel)
          if association.type.belongs_to_polymorphic?
            # Handle polymorphic association
            primary_key_value = value.get_attribute(association.primary_key)
            processed[association.foreign_key] = primary_key_value
            processed[association.polymorphic_type.not_nil!] = model_name_from(value.class)
          else
            # Handle regular belongs_to association
            primary_key_value = value.get_attribute(association.primary_key)
            processed[association.foreign_key] = primary_key_value
          end
        else
          # Regular attribute - convert to DB::Any
          processed[key_str] = value.as(DB::Any)
        end
      end

      processed
    end

    # ========================================
    # PRIVATE METHODS - CALLBACKS
    # ========================================

    private def run_before_save_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("before_save_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_save_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_save_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_before_create_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("before_create_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_create_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_create_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_before_update_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("before_update_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_update_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_update_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_before_destroy_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("before_destroy_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_destroy_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_destroy_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_before_validation_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("before_validation_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_validation_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_validation_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_commit_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_commit_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_rollback_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_rollback_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_initialize_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_initialize_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_find_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_find_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_touch_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_touch_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    # ========================================
    # MACROS - CONFIGURATION
    # ========================================

    macro inherited
      # Call internal macro to set up defaults (can be overridden by explicit primary_key call)
      setup_default_primary_key

      # Register this class for polymorphic lookups
      {% if @type.ancestors.any? { |a| a.name.stringify.includes?("Associations") } %}
        Takarik::Data::Associations.register_polymorphic_class({{@type.name.split("::").last}}, {{@type}})
      {% end %}
    end

    macro setup_default_primary_key
      # Define class variable for this model's primary key
      @@primary_key_name = "id"

      # Automatically define id property if no primary_key macro is called
      define_property_with_accessors(id, Int64)
    end

    macro timestamps
      define_property_with_accessors(created_at, Time)
      define_property_with_accessors(updated_at, Time)

      before_create do
        now = Time.utc
        self.created_at = now
        self.updated_at = now
      end

      before_update do
        self.updated_at = Time.utc
      end
    end

    macro primary_key(name_or_keys, type = Int64)
      {% if name_or_keys.is_a?(ArrayLiteral) %}
        # Handle composite primary key: primary_key [:shop_id, :id]
        {% keys_array = name_or_keys.map(&.id.stringify) %}
        @@primary_key_name = {{keys_array.join(",")}}

        # Define properties for each key
        {% for key in name_or_keys %}
          define_property_with_accessors({{key}}, {{type}})

                    # Generate dynamic finder methods for each composite key
          def self.find_by_{{key.id}}(value)
            conditions = Hash(String, DB::Any).new
            conditions[{{key.id.stringify}}] = value.as(DB::Any)
            find_by(conditions)
          end

          def self.find_by_{{key.id}}!(value)
            conditions = Hash(String, DB::Any).new
            conditions[{{key.id.stringify}}] = value.as(DB::Any)
            find_by!(conditions)
          end
        {% end %}

        # Override find methods for composite key support
        def self.find(composite_key : Array)
          return nil if composite_key.empty?

          # Handle single composite key [store_id, id]
          primary_keys = @@primary_key_name.split(",").map(&.strip)

          if composite_key.size != primary_keys.size
            raise ArgumentError.new("Wrong number of primary key values (given #{composite_key.size}, expected #{primary_keys.size})")
          end

          conditions = Hash(String, DB::Any).new
          primary_keys.each_with_index do |key, index|
            conditions[key] = composite_key[index].as(DB::Any)
          end

          query.where(conditions).first
        end

        def self.find(composite_keys : Array(Array))
          return [] of self if composite_keys.empty?

          # Handle multiple composite keys [[1, 8], [7, 15]]
          primary_keys = @@primary_key_name.split(",").map(&.strip)
          query_builder = unscoped

          composite_keys.each_with_index do |composite_key, index|
            if composite_key.size != primary_keys.size
              raise ArgumentError.new("Wrong number of primary key values in composite key #{index} (given #{composite_key.size}, expected #{primary_keys.size})")
            end

            conditions = Hash(String, DB::Any).new
            primary_keys.each_with_index do |key, key_index|
              conditions[key] = composite_key[key_index].as(DB::Any)
            end

            if index == 0
              query_builder = query_builder.where(conditions)
            else
              query_builder = query_builder.or(conditions)
            end
          end

          results = query_builder.to_a

          # Check if all composite keys were found
          if results.size != composite_keys.size
            raise RecordNotFound.new("Couldn't find all #{model_name} with composite keys #{composite_keys.inspect} (found #{results.size} results, but was looking for #{composite_keys.size})")
          end

          results
        end

        # Find with exception for composite keys
        def self.find!(composite_key : Array)
          find(composite_key) || raise RecordNotFound.new("Couldn't find #{model_name} with composite key #{composite_key.inspect}")
        end

        def self.find!(composite_keys : Array(Array))
          results = find(composite_keys)
          if results.empty? && !composite_keys.empty?
            raise RecordNotFound.new("Couldn't find #{model_name} with composite keys #{composite_keys.inspect}")
          end
          results
        end
      {% else %}
        # Handle single primary key: primary_key :id or primary_key "custom_id"
        @@primary_key_name = {{name_or_keys.id.stringify}}

        # Define the property for the primary key
        define_property_with_accessors({{name_or_keys}}, {{type}})

                # Generate dynamic finder methods for the primary key
        def self.find_by_{{name_or_keys.id}}(value)
          conditions = Hash(String, DB::Any).new
          conditions[{{name_or_keys.id.stringify}}] = value.as(DB::Any)
          find_by(conditions)
        end

        def self.find_by_{{name_or_keys.id}}!(value)
          conditions = Hash(String, DB::Any).new
          conditions[{{name_or_keys.id.stringify}}] = value.as(DB::Any)
          find_by!(conditions)
        end
      {% end %}
    end

    macro column(name, type, **options)
      define_property_with_accessors({{name}}, {{type}})

      # Generate dynamic finder methods for this column
      def self.find_by_{{name.id}}(value)
        conditions = Hash(String, DB::Any).new
        conditions[{{name.id.stringify}}] = value.as(DB::Any)
        find_by(conditions)
      end

      def self.find_by_{{name.id}}!(value)
        conditions = Hash(String, DB::Any).new
        conditions[{{name.id.stringify}}] = value.as(DB::Any)
        find_by!(conditions)
      end
    end

    macro table_name(name)
      # Override the default table name by setting the class variable
      {% if name.is_a?(SymbolLiteral) %}
        @@table_name = {{name.id.stringify}}
      {% else %}
        @@table_name = {{name}}
      {% end %}
    end

    # Enhanced scope macro supporting arguments and conditionals like ActiveRecord
    macro scope(name, &block)
      {% if block.args.size > 0 %}
        # Scope with arguments - define on model class
        def self.{{name.id}}({{block.args.splat}})
          # Execute the scope body and ensure we always return a QueryBuilder
          result = {{block.body}}

          # If result is nil or false (from conditional), return all to ensure chainability
          if result.nil? || result == false
            all
          else
            result
          end
        end
      {% else %}
        # Scope without arguments - define on model class
        def self.{{name.id}}
          # Execute the scope body and ensure we always return a QueryBuilder
          result = {{block.body}}

          # If result is nil or false (from conditional), return all to ensure chainability
          if result.nil? || result == false
            all
          else
            result
          end
        end
      {% end %}
    end

    # Enumerate macro for creating enumerated attributes with automatic scopes and methods
    macro enumerate(name, values)
      {% name_str = name.id.stringify %}

      # Store the enum values mapping for runtime use
      \{% unless @type.class_vars.map(&.name.stringify).includes?("enum_mappings") %}
        @@enum_mappings = {} of String => Hash(String, Int32)
      \{% end %}

      @@enum_mappings[{{name_str}}] = {} of String => Int32

      {% for value, index in values %}
        @@enum_mappings[{{name_str}}][{{value.id.stringify}}] = {{index}}
      {% end %}

      # Define the property with enum constraint
      define_property_with_accessors({{name.id}}, Int32)

      # Create class method to get enum mappings
      def self.{{name.id}}_mappings
        @@enum_mappings[{{name_str}}]
      end

      # Create scopes for each enum value
      {% for value, index in values %}
        # Positive scopes (e.g., Order.shipped)
        scope :{{value.id}} do
          where({{name.id}}: {{index}})
        end

        # Negative scopes (e.g., Order.not_shipped)
        scope :not_{{value.id}} do
          not({{name.id}}: {{index}})
        end
      {% end %}

      # Create instance query methods (e.g., order.shipped?)
      {% for value, index in values %}
        def {{value.id}}?
          {{name.id}} == {{index}}
        end
      {% end %}

      # Create instance setter methods with save (e.g., order.shipped!)
      {% for value, index in values %}
        def {{value.id}}!
          self.{{name.id}} = {{index}}
          save!
          {{value.id}}?
        end
      {% end %}

      # Create a method to get the string representation of the current enum value
      def {{name.id}}_name
        case {{name.id}}
        {% for value, index in values %}
        when {{index}}
          {{value.id.stringify}}
        {% end %}
        else
          nil
        end
      end

      # Create a method to set enum value by string name
      def {{name.id}}_name=(value_name : String)
        case value_name
        {% for value, index in values %}
        when {{value.id.stringify}}
          self.{{name.id}} = {{index}}
        {% end %}
        else
          raise "Invalid {{name.id}} value: #{value_name}. Valid values are: #{self.class.{{name.id}}_mappings.keys.join(", ")}"
        end
      end

      # Create a class method to get all enum values
      def self.{{name.id}}_values
        [{% for value in values %}{{value.id.stringify}}, {% end %}]
      end
    end

    # Default scope macro for setting model-wide default conditions
    macro default_scope(&block)
      # Override the apply_default_scope_if_exists method
      def self.apply_default_scope_if_exists
        # If we're in an unscoped context, always return unscoped
        return unscoped if @@_unscoped_context
        unscoped.{{block.body}}
      end
    end

    # ========================================
    # MACROS - CALLBACKS
    # ========================================

    macro check_callback_conditions(condition_if, condition_unless, on_condition = nil, current_action = nil)
      {% if condition_if || condition_unless || on_condition %}
        # Check on conditions first
        {% if on_condition && current_action %}
          {% if on_condition.is_a?(ArrayLiteral) %}
            return unless {{on_condition}}.includes?({{current_action}})
          {% else %}
            return unless {{on_condition}} == {{current_action}}
          {% end %}
        {% end %}

        # Check if conditions (all must be true)
        {% if condition_if %}
          {% if condition_if.is_a?(ArrayLiteral) %}
            # Handle array of conditions - all must be true
            {% for condition in condition_if %}
              {% if condition.is_a?(SymbolLiteral) %}
                return unless {{condition.id}}
              {% elsif condition.is_a?(ProcLiteral) %}
                condition_proc = {{condition}}
                {% if condition.args.size > 0 %}
                  # Proc with parameter(s)
                  return unless condition_proc.call(self)
                {% else %}
                  # Proc without parameters
                  return unless condition_proc.call
                {% end %}
              {% else %}
                {% raise "Unsupported callback condition type: #{condition.class_name}. Use Symbol or Proc." %}
              {% end %}
            {% end %}
          {% elsif condition_if.is_a?(SymbolLiteral) %}
            return unless {{condition_if.id}}
          {% elsif condition_if.is_a?(ProcLiteral) %}
            condition_proc = {{condition_if}}
            {% if condition_if.args.size > 0 %}
              # Proc with parameter(s)
              return unless condition_proc.call(self)
            {% else %}
              # Proc without parameters
              return unless condition_proc.call
            {% end %}
          {% else %}
            {% raise "Unsupported callback condition type: #{condition_if.class_name}. Use Symbol or Proc." %}
          {% end %}
        {% end %}

        # Check unless conditions (all must be false)
        {% if condition_unless %}
          {% if condition_unless.is_a?(ArrayLiteral) %}
            # Handle array of conditions - all must be false
            {% for condition in condition_unless %}
              {% if condition.is_a?(SymbolLiteral) %}
                return if {{condition.id}}
              {% elsif condition.is_a?(ProcLiteral) %}
                condition_proc = {{condition}}
                {% if condition.args.size > 0 %}
                  # Proc with parameter(s)
                  return if condition_proc.call(self)
                {% else %}
                  # Proc without parameters
                  return if condition_proc.call
                {% end %}
              {% else %}
                {% raise "Unsupported callback condition type: #{condition.class_name}. Use Symbol or Proc." %}
              {% end %}
            {% end %}
          {% elsif condition_unless.is_a?(SymbolLiteral) %}
            return if {{condition_unless.id}}
          {% elsif condition_unless.is_a?(ProcLiteral) %}
            condition_proc = {{condition_unless}}
            {% if condition_unless.args.size > 0 %}
              # Proc with parameter(s)
              return if condition_proc.call(self)
            {% else %}
              # Proc without parameters
              return if condition_proc.call
            {% end %}
          {% else %}
            {% raise "Unsupported callback condition type: #{condition_unless.class_name}. Use Symbol or Proc." %}
          {% end %}
        {% end %}
      {% end %}
    end

    macro before_save(method_name = nil, if condition_if = nil, unless condition_unless = nil, on on_condition = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("before_save_callback_") }.size %}

      {% if method_name %}
        def before_save_callback_{{callback_num}}
          {% if on_condition %}
            current_action = new_record? ? :create : :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{method_name.id}}
        end
      {% else %}
        def before_save_callback_{{callback_num}}
          {% if on_condition %}
            current_action = new_record? ? :create : :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{block.body}}
        end
      {% end %}
    end

    macro after_save(method_name = nil, if condition_if = nil, unless condition_unless = nil, on on_condition = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_save_callback_") }.size %}

      {% if method_name %}
        def after_save_callback_{{callback_num}}
          {% if on_condition %}
            current_action = @_last_action || (new_record? ? :create : :update)
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{method_name.id}}
        end
      {% else %}
        def after_save_callback_{{callback_num}}
          {% if on_condition %}
            current_action = @_last_action || (new_record? ? :create : :update)
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{block.body}}
        end
      {% end %}
    end

    macro before_create(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("before_create_callback_") }.size %}

      {% if method_name %}
        def before_create_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def before_create_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro after_create(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_create_callback_") }.size %}

      {% if method_name %}
        def after_create_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def after_create_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro before_update(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("before_update_callback_") }.size %}

      {% if method_name %}
        def before_update_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def before_update_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro after_update(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_update_callback_") }.size %}

      {% if method_name %}
        def after_update_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def after_update_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro before_destroy(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("before_destroy_callback_") }.size %}

      {% if method_name %}
        def before_destroy_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def before_destroy_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro after_destroy(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_destroy_callback_") }.size %}

      {% if method_name %}
        def after_destroy_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def after_destroy_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro before_validation(method_name = nil, if condition_if = nil, unless condition_unless = nil, on on_condition = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("before_validation_callback_") }.size %}

      {% if method_name %}
        def before_validation_callback_{{callback_num}}
          {% if on_condition %}
            current_action = new_record? ? :create : :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{method_name.id}}
        end
      {% else %}
        def before_validation_callback_{{callback_num}}
          {% if on_condition %}
            current_action = new_record? ? :create : :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{block.body}}
        end
      {% end %}
    end

    macro after_validation(method_name = nil, if condition_if = nil, unless condition_unless = nil, on on_condition = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_validation_callback_") }.size %}

      {% if method_name %}
        def after_validation_callback_{{callback_num}}
          {% if on_condition %}
            current_action = new_record? ? :create : :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{method_name.id}}
        end
      {% else %}
        def after_validation_callback_{{callback_num}}
          {% if on_condition %}
            current_action = new_record? ? :create : :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{block.body}}
        end
      {% end %}
    end

    # Generic commit callbacks (ActiveRecord pattern)
    macro after_commit(method_name = nil, if condition_if = nil, unless condition_unless = nil, on on_condition = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_commit_callback_") }.size %}

      {% if method_name %}
        def after_commit_callback_{{callback_num}}
          {% if on_condition %}
            # For after_commit, we need to track what action was performed
            current_action = @_last_action || :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{method_name.id}}
        end
      {% else %}
        def after_commit_callback_{{callback_num}}
          {% if on_condition %}
            # For after_commit, we need to track what action was performed
            current_action = @_last_action || :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{block.body}}
        end
      {% end %}
    end

    macro after_rollback(method_name = nil, if condition_if = nil, unless condition_unless = nil, on on_condition = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_rollback_callback_") }.size %}

      {% if method_name %}
        def after_rollback_callback_{{callback_num}}
          {% if on_condition %}
            # For after_rollback, we need to track what action was being performed
            current_action = @_last_action || :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{method_name.id}}
        end
      {% else %}
        def after_rollback_callback_{{callback_num}}
          {% if on_condition %}
            # For after_rollback, we need to track what action was being performed
            current_action = @_last_action || :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{block.body}}
        end
      {% end %}
    end

    # Object lifecycle callbacks (ActiveRecord pattern)
    macro after_initialize(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_initialize_callback_") }.size %}

      {% if method_name %}
        def after_initialize_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def after_initialize_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro after_find(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_find_callback_") }.size %}

      {% if method_name %}
        def after_find_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def after_find_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro after_touch(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_touch_callback_") }.size %}

      {% if method_name %}
        def after_touch_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def after_touch_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    # ========================================
    # MACROS - PROPERTY DEFINITION
    # ========================================

    macro define_property_with_accessors(name, type)
      # Register this column name (without quotes)
      add_column_name({{name.id.stringify}})

      {% if type == Int32 %}
        property {{name.id}} : Int32?
      {% elsif type == Int64 %}
        property {{name.id}} : Int64?
      {% elsif type == String %}
        property {{name.id}} : String?
      {% elsif type == Bool %}
        property {{name.id}} : Bool?
      {% elsif type == Time %}
        property {{name.id}} : Time?
      {% elsif type == Float64 %}
        property {{name.id}} : Float64?
      {% else %}
        property {{name.id}} : {{type}}?
      {% end %}

      # Override setter to track changes and sync with attributes
      def {{name.id}}=(value : {{type}}?)
        old_value = @{{name.id}}
        @{{name.id}} = value

        # Also update the attributes hash
        if value.nil?
          if @persisted
            @attributes[{{name.id.stringify}}] = nil.as(DB::Any)
          else
            @attributes.delete({{name.id.stringify}})
          end
        else
          @attributes[{{name.id.stringify}}] = value.as(DB::Any)
        end

        # Track changes
        if old_value != value
          @changed_attributes << {{name.id.stringify}}
        end
        value
      end

      # Override getter to return from instance variable or attributes
      def {{name.id}}
        if !@{{name.id}}.nil?
          @{{name.id}}
        elsif @attributes.has_key?({{name.id.stringify}})
          value = @attributes[{{name.id.stringify}}]
          {% if type == Int32 %}
            case value
            when Int32
              value
            when Int64
              value.to_i32
            when String
              value.to_i32?
            else
              nil
            end
          {% elsif type == Int64 %}
            case value
            when Int64
              value
            when Int32
              value.to_i64
            when String
              value.to_i64?
            else
              nil
            end
          {% elsif type == String %}
            case value
            when String
              value
            when Nil
              nil
            else
              value.to_s
            end
          {% elsif type == Bool %}
            case value
            when 1, "1", "true", "t", true
              true
            when 0, "0", "false", "f", false
              false
            when nil
              nil
            else
              nil
            end
          {% elsif type == Float64 %}
            case value
            when Float64
              value
            when Int32, Int64
              value.to_f64
            when String
              value.to_f64?
            else
              nil
            end
          {% elsif type == Time %}
            case value
            when Time
              value
            else
              nil
            end
          {% else %}
            value.as?({{type}})
          {% end %}
        else
          nil
        end
      end
    end

    macro assign_instance_variables_from_attributes
      {% begin %}
        case column_name
        {% for ivar in @type.instance_vars %}
          {% excluded_vars = ["attributes", "persisted", "changed_attributes", "validation_errors", "_last_action", "association_cache", "loaded_associations", "readonly", "strict_loading"] %}
          {% unless excluded_vars.includes?(ivar.name.stringify) %}
            when {{ivar.name.stringify}}
              # Extract the type from the instance variable type and do direct assignment
              {% if ivar.type.stringify.includes?("Int32") %}
                @{{ivar.name}} = case value
                when Int32
                  value
                when Int64
                  value.to_i32
                when String
                  value.to_i32?
                else
                  nil
                end
              {% elsif ivar.type.stringify.includes?("Int64") %}
                @{{ivar.name}} = case value
                when Int64
                  value
                when Int32
                  value.to_i64
                when String
                  value.to_i64?
                else
                  nil
                end
              {% elsif ivar.type.stringify.includes?("String") %}
                @{{ivar.name}} = case value
                when String
                  value
                when Nil
                  nil
                else
                  value.to_s
                end
              {% elsif ivar.type.stringify.includes?("Bool") %}
                @{{ivar.name}} = case value
                when 1, "1", "true", "t", true
                  true
                when 0, "0", "false", "f", false
                  false
                when nil
                  nil
                else
                  nil
                end
              {% elsif ivar.type.stringify.includes?("Float64") %}
                @{{ivar.name}} = case value
                when Float64
                  value
                when Int32, Int64
                  value.to_f64
                when String
                  value.to_f64?
                else
                  nil
                end
              {% elsif ivar.type.stringify.includes?("Time") %}
                @{{ivar.name}} = case value
                when Time
                  value
                else
                  nil
                end
              {% else %}
                @{{ivar.name}} = value
              {% end %}
          {% end %}
        {% end %}
        end
      {% end %}
    end

    macro generate_base_model_where_overloads
      {% for type in [Int32, Int64, String, Float32, Float64, Bool, Time] %}
        # ========================================
        # WHERE CLASS METHOD OVERLOADS FOR {{type}}
        # ========================================

        def self.where(condition : String, param : {{type}})
          query.where(condition, param)
        end

        def self.where(condition : String, *params : {{type}})
          query.where(condition, *params)
        end

        def self.where(column : String, values : Array({{type}}))
          query.where(column, values)
        end

        def self.where(column_with_operator : String, value : {{type}})
          query.where(column_with_operator, value)
        end

        # ========================================
        # NOT CLASS METHOD OVERLOADS FOR {{type}}
        # ========================================

        def self.not(condition : String, param : {{type}})
          query.not(condition, param)
        end

        def self.not(condition : String, *params : {{type}})
          query.not(condition, *params)
        end

        def self.not(column_with_operator : String, value : {{type}})
          query.not(column_with_operator, value)
        end

        def self.not(column : String, values : Array({{type}}))
          query.not(column, values)
        end

        # ========================================
        # OR CLASS METHOD OVERLOADS FOR {{type}}
        # ========================================

        def self.or(condition : String, param : {{type}})
          query.or(condition, param)
        end

        def self.or(condition : String, *params : {{type}})
          query.or(condition, *params)
        end

        def self.or(column_with_operator : String, value : {{type}})
          query.or(column_with_operator, value)
        end

        def self.or(column : String, values : Array({{type}}))
          query.or(column, values)
        end
      {% end %}

      # ========================================
      # RANGE CLASS METHOD OVERLOADS
      # ========================================

      {% for type in [Int32, Int64, Float32, Float64, Time, String] %}
        def self.where(column : String, range : Range({{type}}, {{type}}))
          query.where(column, range)
        end

        def self.not(column : String, range : Range({{type}}, {{type}}))
          query.not(column, range)
        end

        def self.or(column : String, range : Range({{type}}, {{type}}))
          query.or(column, range)
        end
      {% end %}
    end

    generate_base_model_where_overloads

    # Helper methods for DRY callback execution
    private def execute_commit_callbacks(action : Symbol)
      @_last_action = action
      run_after_commit_callbacks
    end

    private def execute_rollback_callbacks(action : Symbol)
      @_last_action = action
      run_after_rollback_callbacks
    end

    # ========================================
    # READONLY METHODS
    # ========================================

    # Check if this record is readonly
    def readonly?
      @readonly
    end

    # Mark this record as readonly
    def readonly!
      @readonly = true
      self
    end

    # ========================================
    # STRICT LOADING METHODS
    # ========================================

    # Check if this record has strict loading enabled
    def strict_loading?
      @strict_loading
    end

    # Mark this record as strict loading (prevents N+1 queries)
    def strict_loading!
      @strict_loading = true
      self
    end
  end
end
