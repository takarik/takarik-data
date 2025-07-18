require "set"
require "db"

# ========================================
# GLOBAL SQL UTILITIES
# ========================================

# Sanitizes SQL LIKE wildcards (% and _) to prevent unexpected behavior
# Usage: User.where("title LIKE ?", sanitize_sql_like(params[:title]) + "%")
def sanitize_sql_like(input : String) : String
  input.gsub("%", "\\%").gsub("_", "\\_")
end

module Takarik::Data
  class QueryBuilder(T)
    # Rails compatibility constants
    ORDER_IGNORE_MESSAGE = "Scoped order is ignored, use :cursor with :order to configure custom order."
    DEFAULT_ORDER        = :asc

    @model_class : T.class
    @select_clause : String?
    @where_conditions = [] of String
    @where_params = [] of DB::Any
    @joins = [] of String
    @order_clauses = [] of String
    @group_clause : String?
    @having_conditions = [] of String
    @having_params = [] of DB::Any
    @limit_value : Int32?
    @offset_value : Int32?
    @distinct = false
    @has_joins = false
    @none = false
    @readonly = false
    @includes = [] of String
    @preloads = [] of String
    @eager_loads = [] of String
    @lock_clause : String?
    @strict_loading = false
    @create_with_attributes = {} of String => DB::Any

    def initialize(@model_class : T.class)
    end

    # Create a deep copy of the query builder
    def dup
      new_query = QueryBuilder(T).new(@model_class)
      new_query.set_select(@select_clause)
      new_query.set_where_conditions(@where_conditions.dup, @where_params.dup)
      new_query.set_order_clauses(@order_clauses.dup)
      new_query.set_limit(@limit_value)
      new_query.set_offset(@offset_value)
      new_query.set_distinct(@distinct)
      new_query.set_joins(@joins.dup, @has_joins)
      new_query.set_group(@group_clause)
      new_query.set_having_conditions(@having_conditions.dup, @having_params.dup)
      new_query.set_none(@none)
      new_query.set_readonly(@readonly)
      # Copy other arrays
      new_query.copy_includes(@includes.dup)
      new_query.copy_preloads(@preloads.dup)
      new_query.copy_eager_loads(@eager_loads.dup)
      new_query.set_lock(@lock_clause)
      new_query.set_strict_loading(@strict_loading)
      new_query.set_create_with_attributes(@create_with_attributes.dup)
      new_query
    end

    # ========================================
    # SELECT METHODS
    # ========================================

    def select(*columns : String)
      if columns.size == 1
        column_string = columns[0]

        # Check for empty or whitespace-only strings
        if column_string.strip.empty?
          raise ArgumentError.new("Invalid select clause: cannot select empty string")
        end

        # If it contains commas, treat as comma-separated list
        if column_string.includes?(",")
          @select_clause = sanitize_column_list(column_string)
        else
          # Single column string
          @select_clause = column_string.strip
        end
      else
        # Handle multiple string arguments
        @select_clause = columns.join(", ")
      end
      self
    end

    def select(*columns : Symbol)
      @select_clause = columns.map(&.to_s).join(", ")
      self
    end

    def select(columns : Array(String))
      @select_clause = columns.join(", ")
      self
    end

    def select(columns : Array(Symbol))
      @select_clause = columns.map(&.to_s).join(", ")
      self
    end

    # Private method to clean up comma-separated column lists
    private def sanitize_column_list(column_string : String) : String
      # Split by comma, clean each column, filter out empty ones
      columns = column_string.split(",")
        .map(&.strip)                # Remove whitespace
        .reject(&.empty?)            # Remove empty strings
        .reject { |col| col.blank? } # Remove blank strings

      if columns.empty?
        raise ArgumentError.new("Invalid select clause: no valid columns found in '#{column_string}'")
      end

      columns.join(", ")
    end

    def distinct(value : Bool = true)
      @distinct = value
      self
    end

    # ========================================
    # EXPLAIN METHODS (moved early to avoid method_missing issues)
    # ========================================

    # Run EXPLAIN on the current relation to analyze query execution plan.
    # EXPLAIN output varies for each database adapter.
    #
    # Examples:
    #   Customer.where(id: 1).joins(:orders).explain
    #   Customer.where(id: 1).includes(:orders).explain
    #   User.where("age > 21").explain(:analyze, :verbose)  # PostgreSQL options
    #   Order.joins(:customer).explain(:analyze)            # MySQL/MariaDB options
    #
    # For databases that support it (PostgreSQL, MySQL, MariaDB), you can pass options:
    #   :analyze - Actually execute the query and show real execution statistics
    #   :verbose - Show additional details about the query plan
    #   :buffers - Show buffer usage information (PostgreSQL)
    #   :costs   - Show cost estimates (PostgreSQL, default: true)
    #   :format  - Output format: :text, :json, :xml, :yaml (PostgreSQL)
    #
    # The method executes the query when using includes() since eager loading
    # may trigger multiple queries, and some queries need results from previous ones.
    def explain
      # Handle includes separately since they may execute multiple queries
      if @includes.any?
        explain_with_includes_impl
      else
        explain_single_query_impl
      end
    end

    def explain(*options : Symbol)
      # Handle includes separately since they may execute multiple queries
      if @includes.any?
        explain_with_includes_impl(*options)
      else
        explain_single_query_impl(*options)
      end
    end

    # ========================================
    # PRIVATE EXPLAIN IMPLEMENTATION METHODS
    # ========================================

    private def explain_single_query_impl(*options : Symbol)
      adapter = detect_database_adapter
      explain_sql = build_explain_sql(to_sql, adapter, *options)

      output = String.build do |str|
        str << explain_sql << "\n"

        begin
          Takarik::Data.query_with_logging(@model_class.connection, explain_sql, combined_params, @model_class.name, "EXPLAIN") do |rs|
            case adapter
            when :postgresql
              format_postgresql_explain(rs, str, *options)
            when :mysql, :mariadb
              format_mysql_explain(rs, str, *options)
            when :sqlite
              format_sqlite_explain(rs, str)
            else
              # Generic format for unknown adapters
              format_generic_explain(rs, str)
            end
          end
        rescue ex
          str << "-- Error executing EXPLAIN: #{ex.message}\n"
          str << "-- The query would be: #{to_sql}\n"
        end
      end

      output
    end

    private def explain_single_query_impl
      # Call the version with options but pass no options
      adapter = detect_database_adapter
      explain_sql = build_explain_sql(to_sql, adapter)

      output = String.build do |str|
        str << explain_sql << "\n"

        begin
          Takarik::Data.query_with_logging(@model_class.connection, explain_sql, combined_params, @model_class.name, "EXPLAIN") do |rs|
            case adapter
            when :postgresql
              format_postgresql_explain(rs, str)
            when :mysql, :mariadb
              format_mysql_explain(rs, str)
            when :sqlite
              format_sqlite_explain(rs, str)
            else
              # Generic format for unknown adapters
              format_generic_explain(rs, str)
            end
          end
        rescue ex
          str << "-- Error executing EXPLAIN: #{ex.message}\n"
          str << "-- The query would be: #{to_sql}\n"
        end
      end

      output
    end

    private def explain_with_includes_impl(*options : Symbol)
      adapter = detect_database_adapter
      output = String.build do |str|
        # Show explain for the main query first (avoid executing to_a which might fail)
        main_sql = to_sql
        explain_sql = build_explain_sql(main_sql, adapter, *options)
        str << explain_sql << "\n"

        begin
          Takarik::Data.query_with_logging(@model_class.connection, explain_sql, combined_params, @model_class.name, "EXPLAIN") do |rs|
            case adapter
            when :postgresql
              format_postgresql_explain(rs, str, *options)
            when :mysql, :mariadb
              format_mysql_explain(rs, str, *options)
            when :sqlite
              format_sqlite_explain(rs, str)
            else
              format_generic_explain(rs, str)
            end
          end
        rescue ex
          str << "-- Error executing EXPLAIN: #{ex.message}\n"
          str << "-- The query would be: #{main_sql}\n"
        end

        # Show explain for each preload query that would be executed
        # For includes, we simulate what the preload queries would look like
        @includes.each do |association_name|
          str << "\n"
          explain_simulated_preload_query_with_options(association_name, str, adapter, *options)
        end
      end

      output
    end

    private def explain_with_includes_impl
      # Call the version with options but pass no options
      adapter = detect_database_adapter
      output = String.build do |str|
        # Show explain for the main query first (avoid executing to_a which might fail)
        main_sql = to_sql
        explain_sql = build_explain_sql(main_sql, adapter)
        str << explain_sql << "\n"

        begin
          Takarik::Data.query_with_logging(@model_class.connection, explain_sql, combined_params, @model_class.name, "EXPLAIN") do |rs|
            case adapter
            when :postgresql
              format_postgresql_explain(rs, str)
            when :mysql, :mariadb
              format_mysql_explain(rs, str)
            when :sqlite
              format_sqlite_explain(rs, str)
            else
              format_generic_explain(rs, str)
            end
          end
        rescue ex
          str << "-- Error executing EXPLAIN: #{ex.message}\n"
          str << "-- The query would be: #{main_sql}\n"
        end

        # Show explain for each preload query that would be executed
        # For includes, we simulate what the preload queries would look like
        @includes.each do |association_name|
          str << "\n"
          explain_simulated_preload_query(association_name, str, adapter)
        end
      end

      output
    end

    # Helper methods for explain functionality
    private def detect_database_adapter : Symbol
      # For now, return a default adapter since we don't have access to the actual connection
      # In a real implementation, this would inspect the connection type
      :sqlite
    end

    private def build_explain_sql(sql : String, adapter : Symbol, *options : Symbol) : String
      case adapter
      when :postgresql
        if options.any?
          explain_options = options.map(&.to_s.upcase).join(", ")
          "EXPLAIN (#{explain_options}) #{sql}"
        else
          "EXPLAIN #{sql}"
        end
      when :mysql, :mariadb
        if options.includes?(:analyze)
          "ANALYZE #{sql}"
        else
          "EXPLAIN #{sql}"
        end
      when :sqlite
        if options.includes?(:query_plan)
          "EXPLAIN QUERY PLAN #{sql}"
        else
          "EXPLAIN #{sql}"
        end
      else
        "EXPLAIN #{sql}"
      end
    end

    private def build_explain_sql(sql : String, adapter : Symbol) : String
      case adapter
      when :postgresql
        "EXPLAIN #{sql}"
      when :mysql, :mariadb
        "EXPLAIN #{sql}"
      when :sqlite
        "EXPLAIN #{sql}"
      else
        "EXPLAIN #{sql}"
      end
    end

    private def format_postgresql_explain(rs : DB::ResultSet, output : String::Builder, *options : Symbol)
      while rs.move_next
        plan_line = rs.read.to_s
        output << plan_line << "\n"
      end
    end

    private def format_postgresql_explain(rs : DB::ResultSet, output : String::Builder)
      while rs.move_next
        plan_line = rs.read.to_s
        output << plan_line << "\n"
      end
    end

    private def format_mysql_explain(rs : DB::ResultSet, output : String::Builder, *options : Symbol)
      # MySQL/MariaDB EXPLAIN returns tabular data
      column_names = rs.column_names
      output << format_table_header(column_names)

      while rs.move_next
        row_values = [] of String
        column_names.size.times do
          value = rs.read
          row_values << (value.nil? ? "NULL" : value.to_s)
        end
        output << format_table_row(row_values, column_names.map(&.size))
      end
    end

    private def format_mysql_explain(rs : DB::ResultSet, output : String::Builder)
      # MySQL/MariaDB EXPLAIN returns tabular data
      column_names = rs.column_names
      output << format_table_header(column_names)

      while rs.move_next
        row_values = [] of String
        column_names.size.times do
          value = rs.read
          row_values << (value.nil? ? "NULL" : value.to_s)
        end
        output << format_table_row(row_values, column_names.map(&.size))
      end
    end

    private def format_sqlite_explain(rs : DB::ResultSet, output : String::Builder)
      column_names = rs.column_names

      if column_names.includes?("detail")
        # EXPLAIN QUERY PLAN format
        while rs.move_next
          detail = rs.read.to_s
          output << detail << "\n"
        end
      else
        # Regular EXPLAIN format (opcode listing)
        output << format_table_header(column_names)

        while rs.move_next
          row_values = [] of String
          column_names.size.times do
            value = rs.read
            row_values << (value.nil? ? "NULL" : value.to_s)
          end
          output << format_table_row(row_values, column_names.map(&.size))
        end
      end
    end

    private def format_generic_explain(rs : DB::ResultSet, output : String::Builder)
      column_names = rs.column_names
      output << format_table_header(column_names)

      while rs.move_next
        row_values = [] of String
        column_names.size.times do
          value = rs.read
          row_values << (value.nil? ? "NULL" : value.to_s)
        end
        output << format_table_row(row_values, column_names.map(&.size))
      end
    end

    private def format_table_header(column_names : Array(String)) : String
      header = "+" + column_names.map { |name| "-" * (name.size + 2) }.join("+") + "+\n"
      header += "|" + column_names.map { |name| " #{name} " }.join("|") + "|\n"
      header += "+" + column_names.map { |name| "-" * (name.size + 2) }.join("+") + "+\n"
      header
    end

    private def format_table_row(values : Array(String), column_widths : Array(Int32)) : String
      row = "|"
      values.each_with_index do |value, index|
        width = column_widths[index] > value.size ? column_widths[index] : value.size
        row += " #{value.ljust(width)} |"
      end
      row + "\n"
    end

    private def explain_preload_query_no_options(association_name : String, records : Array(T), output : String::Builder, adapter : Symbol)
      # Simplified version without options for now
      output << "-- Preload query for #{association_name} would be executed here\n"
    end

    private def explain_simulated_preload_query(association_name : String, output : String::Builder, adapter : Symbol)
      # Simulate what a preload query would look like without actually executing the main query
      associations = @model_class.associations
      association = associations.find { |a| a.name == association_name }
      return unless association && association.class_type

      case association.type
      when .belongs_to?
        # Simulate: SELECT * FROM associated_table WHERE primary_key IN (?)
        preload_sql = "SELECT * FROM #{association.class_type.not_nil!.table_name} WHERE #{association.primary_key} IN (?)"
      when .has_many?, .has_one?
        # Simulate: SELECT * FROM associated_table WHERE foreign_key IN (?)
        preload_sql = "SELECT * FROM #{association.class_type.not_nil!.table_name} WHERE #{association.foreign_key} IN (?)"
      else
        output << "-- Unknown association type for #{association_name}\n"
        return
      end

      explain_sql = build_explain_sql(preload_sql, adapter)
      output << explain_sql << "\n"

      # For simulation, we don't actually execute the query, just show what it would be
      output << "-- This query would be executed to preload #{association_name} association\n"
    end

    private def explain_simulated_preload_query_with_options(association_name : String, output : String::Builder, adapter : Symbol, *options : Symbol)
      # Simulate what a preload query would look like without actually executing the main query
      associations = @model_class.associations
      association = associations.find { |a| a.name == association_name }
      return unless association && association.class_type

      case association.type
      when .belongs_to?
        # Simulate: SELECT * FROM associated_table WHERE primary_key IN (?)
        preload_sql = "SELECT * FROM #{association.class_type.not_nil!.table_name} WHERE #{association.primary_key} IN (?)"
      when .has_many?, .has_one?
        # Simulate: SELECT * FROM associated_table WHERE foreign_key IN (?)
        preload_sql = "SELECT * FROM #{association.class_type.not_nil!.table_name} WHERE #{association.foreign_key} IN (?)"
      else
        output << "-- Unknown association type for #{association_name}\n"
        return
      end

      explain_sql = build_explain_sql(preload_sql, adapter, *options)
      output << explain_sql << "\n"

      # For simulation, we don't actually execute the query, just show what it would be
      output << "-- This query would be executed to preload #{association_name} association\n"
    end

    # ========================================
    # WHERE METHODS - SQL INJECTION SAFE
    # ========================================
    #
    # All where methods use parameterized queries to prevent SQL injection attacks.
    # Parameters are always separated from SQL structure and properly escaped by the database driver.
    #
    # ✅ SAFE Examples:
    #   User.where("name = ?", user_input)                    # Parameterized query
    #   User.where("name", user_input)                        # Auto-parameterized
    #   User.where(name: user_input)                          # Hash conditions (auto-parameterized)
    #   User.where("id", [1, 2, 3])                          # Array IN conditions (safe placeholders)
    #   User.where("age > ? AND active = ?", 18, true)       # Multiple parameters
    #
    # ❌ UNSAFE (Not supported - preventing accidental vulnerabilities):
    #   User.where("name = '#{user_input}'")                 # Direct interpolation (NOT SUPPORTED)
    #
    # The QueryBuilder automatically handles parameter escaping and prevents SQL injection
    # by using Crystal's DB module's built-in parameter binding capabilities.

    def where(conditions : Hash(String, DB::Any))
      conditions.each do |column, value|
        if value.nil?
          @where_conditions << column + " IS NULL"
        elsif value.is_a?(Array)
          placeholders = (["?"] * value.size).join(", ")
          @where_conditions << column + " IN (#{placeholders})"
          @where_params.concat(value)
        else
          @where_conditions << column + " = ?"
          @where_params << value
        end
      end
      self
    end

    def where(**conditions)
      # Handle conditions properly, including arrays
      conditions.each do |key, value|
        case value
        when Array
          # Arrays need special handling - call the array overload directly
          where(key.to_s, value.map(&.as(DB::Any)))
        else
          # Handle single values normally
          where({key.to_s => value.as(DB::Any)})
        end
      end
      self
    end

    # Named placeholder conditions - must come before variadic method for proper resolution
    def where(condition : String, **named_params)
      # Handle named parameters properly, including arrays
      processed_params = {} of String => DB::Any
      named_params.each do |key, value|
        case value
        when Array
          # Arrays in named parameters are not supported in this context
          # This would require complex parsing of the condition string
          raise "Arrays are not supported in named parameter conditions. Use Hash conditions instead."
        else
          processed_params[key.to_s] = value.as(DB::Any)
        end
      end
      where(condition, processed_params)
    end

    def where(condition : String, *params : DB::Any)
      @where_conditions << condition
      @where_params.concat(params.to_a)
      self
    end

    def where(column_with_operator : String, value : DB::Any)
      if column_with_operator.includes?("?") || column_with_operator.includes?(" ")
        if column_with_operator.includes?("?")
          @where_conditions << column_with_operator
        else
          @where_conditions << "#{column_with_operator} ?"
        end
        @where_params << value
      else
        if value.nil?
          @where_conditions << "#{column_with_operator} IS NULL"
        else
          @where_conditions << "#{column_with_operator} = ?"
          @where_params << value
        end
      end
      self
    end

    def where(column : String, values : Array(DB::Any))
      placeholders = (["?"] * values.size).join(", ")
      @where_conditions << "#{column} IN (#{placeholders})"
      @where_params.concat(values)
      self
    end

    # ========================================
    # NAMED PLACEHOLDER CONDITIONS
    # ========================================

    # Support for named placeholder conditions like:
    #   User.where("name = :name AND age > :min_age", {name: "John", min_age: 18})
    #   User.where("created_at >= :start_date AND created_at <= :end_date", {start_date: params[:start_date], end_date: params[:end_date]})
    def where(condition : String, named_params : Hash(String, DB::Any))
      # Replace named parameters with ? placeholders
      processed_condition = condition
      param_values = [] of DB::Any

      # Process named parameters in order they appear in the condition string
      # We need to handle duplicate placeholders correctly
      named_params.each do |name, value|
        placeholder = ":#{name}"
        while processed_condition.includes?(placeholder)
          processed_condition = processed_condition.sub(placeholder, "?")
          param_values << value
        end
      end

      @where_conditions << processed_condition
      @where_params.concat(param_values)
      self
    end

    # Support for NamedTuple syntax: User.where("name = :name", {name: "Alice"})
    def where(condition : String, named_params : NamedTuple)
      # Convert NamedTuple to Hash
      hash_params = {} of String => DB::Any
      named_params.each do |key, value|
        hash_params[key.to_s] = value.as(DB::Any)
      end
      where(condition, hash_params)
    end

    # ========================================
    # NOT METHODS (new clean syntax)
    # ========================================

    def not(conditions : Hash(String, DB::Any))
      conditions.each do |column, value|
        if value.nil?
          @where_conditions << "#{column} IS NOT NULL"
        elsif value.is_a?(Array)
          placeholders = (["?"] * value.size).join(", ")
          @where_conditions << "#{column} NOT IN (#{placeholders})"
          @where_params.concat(value)
        else
          @where_conditions << "#{column} != ?"
          @where_params << value
        end
      end
      self
    end

    def not(**conditions)
      # Handle conditions properly, including arrays
      conditions.each do |key, value|
        case value
        when Array
          # Arrays need special handling - call the array overload directly
          not(key.to_s, value.map(&.as(DB::Any)))
        else
          # Handle single values normally
          not({key.to_s => value.as(DB::Any)})
        end
      end
      self
    end

    def not(column : String, values : Array(DB::Any))
      placeholders = (["?"] * values.size).join(", ")
      @where_conditions << "#{column} NOT IN (#{placeholders})"
      @where_params.concat(values)
      self
    end

    def not(condition : String, *params : DB::Any)
      @where_conditions << "NOT (" + condition + ")"
      @where_params.concat(params.to_a)
      self
    end

    def not(column_with_operator : String, value : DB::Any)
      if column_with_operator.includes?("?") || column_with_operator.includes?(" ")
        if column_with_operator.includes?("?")
          @where_conditions << "NOT (" + column_with_operator + ")"
        else
          @where_conditions << "NOT (" + column_with_operator + " ?)"
        end
        @where_params << value.as(DB::Any)
      else
        if value.nil?
          @where_conditions << "NOT (" + column_with_operator + " IS NULL)"
        else
          @where_conditions << "NOT (" + column_with_operator + " = ?)"
          @where_params << value.as(DB::Any)
        end
      end
      self
    end

    # ========================================
    # ASSOCIATION EXISTENCE METHODS
    # ========================================

    def associated(association_name : String | Symbol)
      association_name_str = association_name.to_s
      associations = @model_class.associations
      association = associations.find { |a| a.name == association_name_str }

      unless association
        raise "Association '#{association_name_str}' not found for #{@model_class.name}"
      end

      # Skip polymorphic associations as they can't be joined directly
      if association.polymorphic || association.class_type.nil?
        raise "Cannot use associated with polymorphic association '#{association_name_str}'"
      end

      current_table = @model_class.table_name
      associated_table = association.class_type.not_nil!.table_name

      case association.type
      when .belongs_to?
        on_condition = "#{current_table}.#{association.foreign_key} = #{associated_table}.#{association.primary_key}"
      when .has_many?, .has_one?
        on_condition = "#{current_table}.#{association.primary_key} = #{associated_table}.#{association.foreign_key}"
      else
        raise "Unknown association type: #{association.type}"
      end

      @joins << "INNER JOIN #{associated_table} ON #{on_condition}"
      @where_conditions << "#{associated_table}.#{association.primary_key} IS NOT NULL"
      @has_joins = true
      self
    end

    def missing(association_name : String | Symbol)
      association_name_str = association_name.to_s
      associations = @model_class.associations
      association = associations.find { |a| a.name == association_name_str }

      unless association
        raise "Association '#{association_name_str}' not found for #{@model_class.name}"
      end

      # Skip polymorphic associations as they can't be joined directly
      if association.polymorphic || association.class_type.nil?
        raise "Cannot use missing with polymorphic association '#{association_name_str}'"
      end

      current_table = @model_class.table_name
      associated_table = association.class_type.not_nil!.table_name

      case association.type
      when .belongs_to?
        on_condition = "#{current_table}.#{association.foreign_key} = #{associated_table}.#{association.primary_key}"
      when .has_many?, .has_one?
        on_condition = "#{current_table}.#{association.primary_key} = #{associated_table}.#{association.foreign_key}"
      else
        raise "Unknown association type: #{association.type}"
      end

      @joins << "LEFT OUTER JOIN #{associated_table} ON #{on_condition}"
      @where_conditions << "#{associated_table}.#{association.primary_key} IS NULL"
      @has_joins = true
      self
    end

    # ========================================
    # OR METHODS
    # ========================================

    def or(conditions : Hash(String, DB::Any))
      # Build the OR conditions
      or_conditions = [] of String
      conditions.each do |column, value|
        if value.nil?
          or_conditions << column + " IS NULL"
        elsif value.is_a?(Array)
          placeholders = (["?"] * value.size).join(", ")
          or_conditions << column + " IN (#{placeholders})"
          @where_params.concat(value)
        else
          or_conditions << column + " = ?"
          @where_params << value
        end
      end

      # Add OR conditions
      if or_conditions.any?
        @where_conditions << "OR (" + or_conditions.join(" AND ") + ")"
      end

      self
    end

    def or(**conditions)
      or(conditions.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) })
    end

    def or(condition : String, *params : DB::Any)
      @where_conditions << "OR (" + condition + ")"
      @where_params.concat(params.to_a)
      self
    end

    def or(column_with_operator : String, value : DB::Any)
      if column_with_operator.includes?("?") || column_with_operator.includes?(" ")
        if column_with_operator.includes?("?")
          @where_conditions << "OR (" + column_with_operator + ")"
        else
          @where_conditions << "OR (" + column_with_operator + " ?)"
        end
        @where_params << value.as(DB::Any)
      else
        if value.nil?
          @where_conditions << "OR (" + column_with_operator + " IS NULL)"
        else
          @where_conditions << "OR (" + column_with_operator + " = ?)"
          @where_params << value.as(DB::Any)
        end
      end
      self
    end

    def or(column : String, values : Array(DB::Any))
      placeholders = (["?"] * values.size).join(", ")
      @where_conditions << "OR (" + column + " IN (#{placeholders}))"
      @where_params.concat(values.map(&.as(DB::Any)))
      self
    end

    # ========================================
    # JOIN METHODS
    # ========================================

    # Rails-compatible joins method that supports multiple associations and nested joins
    # Examples:
    #   User.joins(:posts)                           # Single association
    #   User.joins(:posts, :account)                 # Multiple associations
    #   User.joins(posts: [:comments])               # Nested joins
    #   User.joins("LEFT JOIN bookmarks ON ...")     # Custom SQL
    #   Author.joins(books: [{ reviews: { customer: :orders } }, :supplier])  # Complex nested
    def joins(*associations : String | Symbol)
      associations.each do |association|
        @has_joins = true
        add_smart_association_join(association.to_s)
      end
      self
    end

    def joins(associations : Array(String | Symbol))
      associations.each do |association|
        @has_joins = true
        add_smart_association_join(association.to_s)
      end
      self
    end

    # Simple flexible signature that can handle any nested structure
    def joins(nested_associations : Hash)
      nested_associations.each do |parent_association, child_associations|
        # First join the parent association using smart join logic
        @has_joins = true
        add_smart_association_join(parent_association.to_s)

        # Then join the child associations
        process_nested_association_value(parent_association.to_s, child_associations)
      end
      self
    end

    def joins(table : String, on : String)
      # Handle table joins with ON clause
      add_join("JOIN", table, on)
    end

    def joins(custom_sql : String)
      # Handle custom SQL joins like Rails
      # Check if it's a table join with ON clause
      if custom_sql.upcase.includes?(" ON ")
        @has_joins = true
        @joins << custom_sql
      else
        # Treat as single association name
        @has_joins = true
        add_smart_association_join(custom_sql)
      end
      self
    end

    # Support for NamedTuple syntax: User.joins(posts: [:comments])
    def joins(**named_associations)
      # Convert NamedTuple to Hash and delegate to the Hash version
      hash = {} of String => typeof(named_associations.values.first)
      named_associations.each do |key, value|
        hash[key.to_s] = value
      end
      joins(hash)
    end

    def inner_join(association_name : String | Symbol)
      add_join("INNER JOIN", association_name.to_s)
    end

    def inner_join(table : String, on : String)
      add_join("INNER JOIN", table, on)
    end

    def left_join(association_name : String | Symbol)
      add_join("LEFT JOIN", association_name.to_s)
    end

    def left_join(table : String, on : String)
      add_join("LEFT JOIN", table, on)
    end

    def right_join(association_name : String | Symbol)
      add_join("RIGHT JOIN", association_name.to_s)
    end

    def right_join(table : String, on : String)
      add_join("RIGHT JOIN", table, on)
    end

    # ========================================
    # INCLUDES METHODS (Eager Loading)
    # ========================================

    def includes(*association_names : String | Symbol)
      association_names.each do |association_name|
        add_includes(association_name.to_s)
      end
      self
    end

    def includes(association_names : Array(String | Symbol))
      association_names.each do |association_name|
        add_includes(association_name.to_s)
      end
      self
    end

    # ========================================
    # PRELOAD METHODS (Separate Query Eager Loading)
    # ========================================

    def preload(*association_names : String | Symbol)
      association_names.each do |association_name|
        add_preload(association_name.to_s)
      end
      self
    end

    def preload(association_names : Array(String | Symbol))
      association_names.each do |association_name|
        add_preload(association_name.to_s)
      end
      self
    end

    # ========================================
    # EAGER_LOAD METHODS (LEFT OUTER JOIN Eager Loading)
    # ========================================

    def eager_load(*association_names : String | Symbol)
      association_names.each do |association_name|
        add_eager_load(association_name.to_s)
      end
      self
    end

    def eager_load(association_names : Array(String | Symbol))
      association_names.each do |association_name|
        add_eager_load(association_name.to_s)
      end
      self
    end

    # ========================================
    # ORDER METHODS
    # ========================================

    # Order with symbol column (single column, defaults to ASC)
    def order(column : Symbol)
      @order_clauses << "#{column} ASC"
      self
    end

    # Order with string column and optional direction
    def order(column : String, direction : String = "ASC")
      # Handle case where direction is already included in column string
      if column.includes?(" ")
        @order_clauses << column
      elsif column.includes?(",")
        # Handle multiple columns in one string like "title ASC, created_at DESC"
        column.split(",").each do |part|
          @order_clauses << part.strip
        end
      else
        @order_clauses << "#{column} #{direction.upcase}"
      end
      self
    end

    # Order with hash of columns and directions (named parameters)
    def order(**columns)
      columns.each do |column, direction|
        direction_str = case direction
                        when Symbol
                          direction.to_s.upcase
                        when String
                          direction.upcase
                        else
                          direction.to_s.upcase
                        end
        @order_clauses << "#{column} #{direction_str}"
      end
      self
    end

    # Order with hash parameter (for nested table ordering like books: { print_year: :desc })
    def order(order_hash : Hash(Symbol | String, Hash(Symbol | String, Symbol | String) | Symbol | String))
      order_hash.each do |table_or_column, column_or_direction|
        case column_or_direction
        when Hash
          # Handle nested hash like books: { print_year: :desc }
          table_name = table_or_column.to_s
          column_or_direction.each do |column, direction|
            direction_str = direction.to_s.upcase
            @order_clauses << "#{table_name}.#{column} #{direction_str}"
          end
        when Symbol, String
          # Handle simple hash like created_at: :desc
          direction_str = column_or_direction.to_s.upcase
          @order_clauses << "#{table_or_column} #{direction_str}"
        end
      end
      self
    end

    # Order with mixed arguments (symbol + hash) - special case
    def order(first_column : Symbol, **additional_columns)
      # Add the first symbol column
      @order_clauses << "#{first_column} ASC"

      # Add the hash columns
      additional_columns.each do |column, direction|
        direction_str = direction.to_s.upcase
        @order_clauses << "#{column} #{direction_str}"
      end
      self
    end

    def order_by(column : String, direction : String = "ASC")
      order(column, direction)
    end

    # Order with array of strings (for multiple string arguments)
    def order(columns : Array(String))
      columns.each do |column|
        if column.includes?(",")
          # Handle multiple columns in one string like "title ASC, created_at DESC"
          column.split(",").each do |part|
            @order_clauses << part.strip
          end
        elsif column.includes?(" ")
          @order_clauses << column
        else
          @order_clauses << "#{column} ASC"
        end
      end
      self
    end

    # Create a new query with reversed ordering
    def reverse_order
      new_query = dup

      if @order_clauses.empty?
        # If no ordering clause is specified, order by primary key in reverse order
        primary_key = @model_class.primary_key
        new_query.@order_clauses << "#{@model_class.table_name}.#{primary_key} DESC"
      else
        reversed_order = @order_clauses.map do |clause|
          if clause.ends_with?(" ASC")
            clause.gsub(" ASC", " DESC")
          elsif clause.ends_with?(" DESC")
            clause.gsub(" DESC", " ASC")
          else
            # If no explicit direction, assume ASC and reverse to DESC
            "#{clause} DESC"
          end
        end

        # Clear existing order and add reversed order
        new_query.clear_order
        reversed_order.each { |clause| new_query.add_order_clause(clause) }
      end

      new_query
    end

    # Helper method to clear order clauses
    def clear_order
      @order_clauses.clear
      self
    end

    # Helper method to add order clause
    def add_order_clause(clause : String)
      @order_clauses << clause
      self
    end

    # Helper methods for clearing various query parts
    def clear_where
      @where_conditions.clear
      @where_params.clear
      self
    end

    def clear_limit
      @limit_value = nil
      self
    end

    def clear_offset
      @offset_value = nil
      self
    end

    def clear_group
      @group_clause = nil
      self
    end

    def clear_having
      @having_conditions.clear
      @having_params.clear
      self
    end

    def clear_select
      @select_clause = nil
      self
    end

    def clear_distinct
      @distinct = false
      self
    end

    def clear_none
      @none = false
      self
    end

    def set_where_conditions(conditions : Array(String), params : Array(DB::Any))
      @where_conditions = conditions
      @where_params = params
      self
    end

    def set_order_clauses(clauses : Array(String))
      @order_clauses = clauses
      self
    end

    def set_limit(value : Int32?)
      @limit_value = value
      self
    end

    def set_offset(value : Int32?)
      @offset_value = value
      self
    end

    def set_group(clause : String?)
      @group_clause = clause
      self
    end

    def set_having_conditions(conditions : Array(String), params : Array(DB::Any))
      @having_conditions = conditions
      @having_params = params
      self
    end

    def set_select(clause : String?)
      @select_clause = clause
      self
    end

    def set_distinct(value : Bool)
      @distinct = value
      self
    end

    def set_joins(joins : Array(String), has_joins : Bool)
      @joins = joins
      @has_joins = has_joins
      self
    end

    def copy_includes(includes : Array(String))
      @includes = includes
      self
    end

    def copy_preloads(preloads : Array(String))
      @preloads = preloads
      self
    end

    def copy_eager_loads(eager_loads : Array(String))
      @eager_loads = eager_loads
      self
    end

    def set_none(none : Bool)
      @none = none
      self
    end

    def set_readonly(readonly : Bool)
      @readonly = readonly
      self
    end

    def set_lock(lock_clause : String?)
      @lock_clause = lock_clause
      self
    end

    def set_strict_loading(strict_loading : Bool)
      @strict_loading = strict_loading
      self
    end

    def set_create_with_attributes(attributes : Hash(String, DB::Any))
      @create_with_attributes = attributes
      self
    end

    # Getter methods for debugging
    def order_clauses
      @order_clauses
    end

    def where_conditions
      @where_conditions
    end

    # ========================================
    # OVERRIDING CONDITIONS METHODS
    # ========================================

    # Remove specific conditions from query
    def unscope(*clauses : Symbol)
      new_query = dup

      clauses.each do |clause|
        case clause
        when :where
          new_query.clear_where
        when :order
          new_query.clear_order
        when :limit
          new_query.clear_limit
        when :offset
          new_query.clear_offset
        when :group
          new_query.clear_group
        when :having
          new_query.clear_having
        when :select
          new_query.clear_select
        when :distinct
          new_query.clear_distinct
        when :none
          new_query.clear_none
        end
      end

      new_query
    end

    # Remove specific where conditions
    def unscope(*, where clause_name : Symbol)
      new_query = dup
      clause_str = clause_name.to_s

      # Filter out conditions that contain the specified column
      filtered_conditions = [] of String
      filtered_params = [] of DB::Any

      param_index = 0
      @where_conditions.each_with_index do |condition, i|
        if condition.includes?(clause_str)
          # Skip this condition and its parameters
          param_count = condition.count("?")
          param_index += param_count
        else
          # Keep this condition and its parameters
          filtered_conditions << condition
          param_count = condition.count("?")
          param_count.times do
            if param_index < @where_params.size
              filtered_params << @where_params[param_index]
            end
            param_index += 1
          end
        end
      end

      new_query.set_where_conditions(filtered_conditions, filtered_params)
      new_query
    end

    # Keep only specified clauses
    def only(*clauses : Symbol)
      new_query = @model_class.all.as(QueryBuilder(T))

      clauses.each do |clause|
        case clause
        when :where
          new_query.set_where_conditions(@where_conditions.dup, @where_params.dup)
        when :order
          new_query.set_order_clauses(@order_clauses.dup)
        when :limit
          new_query.set_limit(@limit_value)
        when :offset
          new_query.set_offset(@offset_value)
        when :group
          new_query.set_group(@group_clause)
        when :having
          new_query.set_having_conditions(@having_conditions.dup, @having_params.dup)
        when :select
          new_query.set_select(@select_clause)
        when :distinct
          new_query.set_distinct(@distinct)
        when :joins
          new_query.set_joins(@joins.dup, @has_joins)
        end
      end

      new_query
    end

    # Override existing select clause
    def reselect(*columns : String)
      new_query = dup
      new_query.set_select(columns.join(", "))
      new_query
    end

    def reselect(*columns : Symbol)
      new_query = dup
      new_query.set_select(columns.map(&.to_s).join(", "))
      new_query
    end

    def reselect(columns : Array(String))
      new_query = dup
      new_query.set_select(columns.join(", "))
      new_query
    end

    def reselect(columns : Array(Symbol))
      new_query = dup
      new_query.set_select(columns.map(&.to_s).join(", "))
      new_query
    end

    # Override existing order clause
    def reorder(*columns : String)
      new_query = dup
      new_query.clear_order
      columns.each { |column| new_query.add_order_clause(column) }
      new_query
    end

    def reorder(*columns : Symbol)
      new_query = dup
      new_query.clear_order
      columns.each { |column| new_query.add_order_clause(column.to_s) }
      new_query
    end

    def reorder(column : String, direction : String = "ASC")
      new_query = dup
      new_query.clear_order
      new_query.add_order_clause("#{column} #{direction.upcase}")
      new_query
    end

    def reorder(**columns)
      new_query = dup
      new_query.clear_order
      columns.each do |column, direction|
        new_query.add_order_clause("#{column} #{direction.to_s.upcase}")
      end
      new_query
    end

    # Replace existing where conditions
    def rewhere(conditions : Hash(String, DB::Any))
      new_query = dup
      new_query.clear_where
      new_query.where(conditions)
    end

    def rewhere(**conditions)
      new_query = dup
      new_query.clear_where
      new_query.where(**conditions)
    end

    def rewhere(condition : String, *params : DB::Any)
      new_query = dup
      new_query.clear_where
      new_query.where(condition, *params)
    end

    # Replace existing group clause
    def regroup(*columns : String)
      new_query = dup
      new_query.set_group(columns.join(", "))
      new_query
    end

    def regroup(*columns : Symbol)
      new_query = dup
      new_query.set_group(columns.map(&.to_s).join(", "))
      new_query
    end

    def regroup(columns : Array(String))
      new_query = dup
      new_query.set_group(columns.join(", "))
      new_query
    end

    def regroup(columns : Array(Symbol))
      new_query = dup
      new_query.set_group(columns.map(&.to_s).join(", "))
      new_query
    end

    # Return an empty relation that fires no queries
    def none
      new_query = dup
      new_query.set_none(true)
      new_query
    end

    # Return a relation that marks all returned records as readonly
    def readonly
      new_query = dup
      new_query.set_readonly(true)
      new_query
    end

    # Return a relation that enables strict loading to prevent N+1 queries
    # When strict loading is enabled, accessing associations that weren't preloaded will raise an error
    def strict_loading
      new_query = dup
      new_query.set_strict_loading(true)
      new_query
    end

    # ========================================
    # CREATE WITH METHODS
    # ========================================

    # Set default attributes that will be used when creating new records.
    # These attributes are only applied when creating new records, not when finding existing ones.
    #
    # Examples:
    #   Customer.create_with(locked: false).find_or_create_by(first_name: "Andy")
    #   User.create_with(active: true, role: "user").find_or_create_by(email: "test@example.com")
    #
    # The create_with attributes are merged with the find_or_create_by conditions when creating.
    def create_with(attributes : Hash(String, DB::Any))
      new_query = dup
      new_query.set_create_with_attributes(@create_with_attributes.merge(attributes))
      new_query
    end

    def create_with(**attributes)
      processed_attributes = attributes.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) }
      create_with(processed_attributes)
    end

    # Merge another relation's conditions, replacing existing ones where they conflict
    # This is useful for overriding conditions from scopes or previous where clauses
    #
    # Examples:
    #   Book.in_print.merge(Book.out_of_print)  # out_of_print condition wins
    #   User.where(active: true).merge(User.where(active: false))  # active: false wins
    #
    # The merge method replaces conflicting conditions rather than adding them with AND
    def merge(other_relation : QueryBuilder(T))
      new_query = dup

      # Replace where conditions and parameters entirely
      new_query.set_where_conditions(other_relation.@where_conditions.dup, other_relation.@where_params.dup)

      # Replace other query parts if they exist in the other relation
      new_query.set_order_clauses(other_relation.@order_clauses.dup) unless other_relation.@order_clauses.empty?
      new_query.set_group(other_relation.@group_clause) if other_relation.@group_clause
      new_query.set_having_conditions(other_relation.@having_conditions.dup, other_relation.@having_params.dup) unless other_relation.@having_conditions.empty?
      new_query.set_limit(other_relation.@limit_value) if other_relation.@limit_value
      new_query.set_offset(other_relation.@offset_value) if other_relation.@offset_value
      new_query.set_select(other_relation.@select_clause) if other_relation.@select_clause
      new_query.set_distinct(other_relation.@distinct) if other_relation.@distinct
      new_query.set_joins(other_relation.@joins.dup, other_relation.@has_joins) if other_relation.@has_joins
      new_query.copy_includes(other_relation.@includes.dup) unless other_relation.@includes.empty?
      new_query.copy_preloads(other_relation.@preloads.dup) unless other_relation.@preloads.empty?
      new_query.copy_eager_loads(other_relation.@eager_loads.dup) unless other_relation.@eager_loads.empty?
      new_query.set_none(other_relation.@none) if other_relation.@none
      new_query.set_readonly(other_relation.@readonly) if other_relation.@readonly
      new_query.set_lock(other_relation.@lock_clause) if other_relation.@lock_clause
      new_query.set_strict_loading(other_relation.@strict_loading) if other_relation.@strict_loading

      new_query
    end

    # Remove all scoping and return a fresh query builder
    # This is useful for bypassing default scopes or removing all existing conditions
    #
    # Examples:
    #   User.where(active: true).unscoped.all  # Removes the where condition
    #   Book.unscoped.load                     # Bypasses any default scope
    #
    # This method removes all scoping and will do a normal query on the table
    def unscoped
      @model_class.unscoped
    end

    # ========================================
    # LOCKING METHODS
    # ========================================

    # Adds a locking clause to the query for pessimistic locking.
    # This is useful for preventing race conditions when updating records.
    #
    # Examples:
    #   User.lock.first                           # SELECT * FROM users LIMIT 1 FOR UPDATE
    #   User.lock("LOCK IN SHARE MODE").first     # SELECT * FROM users LIMIT 1 LOCK IN SHARE MODE
    #   User.where(active: true).lock.to_a       # SELECT * FROM users WHERE active = 1 FOR UPDATE
    #
    # The lock is automatically released when the transaction completes.
    # It's recommended to wrap locked queries in a transaction:
    #
    #   User.transaction do
    #     user = User.lock.first
    #     user.update(name: "New Name")
    #   end
    def lock(lock_type : String = "FOR UPDATE")
      new_query = dup
      new_query.set_lock(lock_type)
      new_query
    end

    # ========================================
    # GROUP BY METHODS
    # ========================================

    def group(*columns : String)
      @group_clause = columns.join(", ")
      self
    end

    def group(*columns : Symbol)
      @group_clause = columns.map(&.to_s).join(", ")
      self
    end

    def group(columns : Array(String))
      @group_clause = columns.join(", ")
      self
    end

    def group(columns : Array(Symbol))
      @group_clause = columns.map(&.to_s).join(", ")
      self
    end

    # ========================================
    # HAVING METHODS
    # ========================================

    def having(condition : String)
      @having_conditions << condition
      self
    end

    def having(condition : String, *params : DB::Any)
      @having_conditions << condition
      @having_params.concat(params.to_a)
      self
    end

    def having(column : String, value : DB::Any)
      if column.includes?("?")
        @having_conditions << column
        @having_params << value
        return self
      end

      if value.nil?
        @having_conditions << "#{column} IS NULL"
      else
        @having_conditions << "#{column} = ?"
        @having_params << value
      end
      self
    end

    # ========================================
    # LIMIT AND OFFSET METHODS
    # ========================================

    def limit(count : Int32)
      @limit_value = count
      self
    end

    def offset(count : Int32)
      @offset_value = count
      self
    end

    def page(page_number : Int32, per_page : Int32 = 20)
      offset((page_number - 1) * per_page)
      limit(per_page)
    end

    # ========================================
    # QUERY EXECUTION METHODS
    # ========================================

    def to_sql
      # First, determine if includes should use JOIN strategy
      # Always use JOINs for includes to match ActiveRecord behavior
      use_join_for_includes = @includes.any?

      # Add JOINs for includes if needed
      if use_join_for_includes && !@has_joins
        @includes.each do |association_name|
          add_association_join("LEFT JOIN", association_name)
        end
        @has_joins = true
      end

      sql_parts = [] of String

      # SELECT clause
      if (@has_joins || use_join_for_includes) && @select_clause.nil?
        select_part = get_prefixed_columns
      else
        select_part = @select_clause || "*"
      end

      if @distinct
        sql_parts << "SELECT DISTINCT #{select_part}"
      else
        sql_parts << "SELECT #{select_part}"
      end

      # FROM clause
      sql_parts << "FROM #{@model_class.table_name}"

      # JOIN clauses
      unless @joins.empty?
        sql_parts.concat(@joins)
      end

      # WHERE clause
      unless @where_conditions.empty?
        # Check if there are any OR conditions and multiple conditions
        has_or_conditions = @where_conditions.any? { |condition| condition.starts_with?("OR ") }
        has_multiple_conditions = @where_conditions.size > 1

        # Only wrap in parentheses if we have OR conditions or multiple conditions
        where_clause = @where_conditions.map_with_index do |condition, index|
          if index == 0
            if has_or_conditions || has_multiple_conditions
              "(#{condition})"
            else
              condition
            end
          elsif condition.starts_with?("OR ")
            " " + condition
          else
            if has_or_conditions || has_multiple_conditions
              " AND (#{condition})"
            else
              " AND " + condition
            end
          end
        end.join("")
        sql_parts << "WHERE #{where_clause}"
      end

      # GROUP BY clause
      sql_parts << "GROUP BY #{@group_clause}" if @group_clause

      # HAVING clause
      unless @having_conditions.empty?
        sql_parts << "HAVING #{@having_conditions.join(" AND ")}"
      end

      # ORDER BY clause
      unless @order_clauses.empty?
        sql_parts << "ORDER BY #{@order_clauses.join(", ")}"
      end

      # LIMIT clause
      sql_parts << "LIMIT #{@limit_value}" if @limit_value

      # OFFSET clause
      sql_parts << "OFFSET #{@offset_value}" if @offset_value

      # LOCK clause (for pessimistic locking)
      sql_parts << @lock_clause.not_nil! if @lock_clause

      sql_parts.join(" ")
    end

    # Expose query parameters for testing and debugging
    def params
      combined_params
    end

    def to_a
      results = [] of T

      return results if @none

      # Check if includes should use JOIN strategy - always true for includes
      use_join_for_includes = @includes.any?

      Takarik::Data.query_with_logging(@model_class.connection, to_sql, combined_params, @model_class.name, "Load") do |rs|
        rs.each do
          instance = @model_class.new
          if use_join_for_includes
            instance.load_from_result_set_with_includes(rs, @includes)
          elsif @eager_loads.any?
            instance.load_from_result_set_with_includes(rs, @eager_loads)
          elsif @has_joins
            instance.load_from_prefixed_result_set(rs)
          else
            instance.load_from_result_set(rs)
          end

          # Mark instance as readonly if the query is readonly
          if @readonly
            instance.readonly!
          end

          # Mark instance as strict loading if the query has strict loading enabled
          if @strict_loading
            instance.strict_loading!
          end

          results << instance
        end
      end

      # Handle preloading with separate queries after main query
      if @preloads.any?
        perform_preloading(results)
      end

      results
    end

    def first
      limit(1)
      results = to_a
      results.first?
    end

    def first!
      first || raise Takarik::Data::RecordNotFound.new("Couldn't find #{@model_class.model_name}")
    end

    # Find up to the specified number of records with ordering preserved from query.
    def first(limit_count : Int32)
      limit(limit_count).to_a
    end

    # Find up to the specified number of records with ordering. Raises RecordNotFound if no records found.
    def first!(limit_count : Int32)
      results = first(limit_count)
      if results.empty?
        raise Takarik::Data::RecordNotFound.new("Couldn't find #{@model_class.model_name}")
      end
      results
    end

    # Retrieve a record without any implicit ordering. Returns nil if no record found.
    def take
      limit(1)
      results = to_a
      results.first?
    end

    # Retrieve up to the specified number of records without any implicit ordering.
    def take(limit_count : Int32)
      limit(limit_count).to_a
    end

    # Retrieve a record without any implicit ordering. Raises RecordNotFound if no record found.
    def take!
      take || raise Takarik::Data::RecordNotFound.new("Couldn't find #{@model_class.model_name}")
    end

    # Retrieve up to the specified number of records without any implicit ordering. Raises RecordNotFound if no records found.
    def take!(limit_count : Int32)
      results = take(limit_count)
      if results.empty?
        raise Takarik::Data::RecordNotFound.new("Couldn't find #{@model_class.model_name}")
      end
      results
    end

    def last
      reversed_order = @order_clauses.map do |clause|
        if clause.ends_with?(" ASC")
          clause.gsub(" ASC", " DESC")
        elsif clause.ends_with?(" DESC")
          clause.gsub(" DESC", " ASC")
        else
          "#{clause} DESC"
        end
      end

      if reversed_order.empty?
        reversed_order = ["#{@model_class.primary_key} DESC"]
      end

      @order_clauses = reversed_order
      limit(1)
      results = to_a
      results.first?
    end

    def last!
      last || raise Takarik::Data::RecordNotFound.new("Couldn't find #{@model_class.model_name}")
    end

    # Find up to the specified number of records in reverse order.
    def last(limit_count : Int32)
      # Use reverse_order to flip any existing ordering, then apply limit
      reverse_order.limit(limit_count).to_a
    end

    # Find up to the specified number of records in reverse order. Raises RecordNotFound if no records found.
    def last!(limit_count : Int32)
      results = last(limit_count)
      if results.empty?
        raise Takarik::Data::RecordNotFound.new("Couldn't find #{@model_class.model_name}")
      end
      results
    end

    # Count records in the current relation.
    # Returns the number of records matching the current query conditions.
    #
    # Examples:
    #   Customer.count  # => 5
    #   Customer.where(first_name: 'Ryan').count  # => 2
    #   Customer.group(:status).count  # => {"active" => 3, "inactive" => 2}
    #
    # SQL: SELECT COUNT(*) FROM customers
    # SQL: SELECT COUNT(*) FROM customers WHERE (customers.first_name = 'Ryan')
    def count : Int64 | Hash(String, Int64)
      # Return 0 or empty hash if this is a none relation
      if @none
        return @group_clause ? {} of String => Int64 : 0_i64
      end

      # If there's a GROUP BY clause, return a hash of grouped counts
      if @group_clause
        grouped_count
      elsif @limit_value
        # If there's a LIMIT clause, we need to count the actual records that would be returned
        to_a.size.to_i64
      else
        original_select = @select_clause
        if @distinct && @select_clause && !@select_clause.not_nil!.empty? && @select_clause != "*"
          # Use COUNT(DISTINCT column) when distinct is enabled and we have a specific column
          @select_clause = "COUNT(DISTINCT #{@select_clause})"
        else
          @select_clause = "COUNT(*)"
        end
        result = Takarik::Data.scalar_with_logging(@model_class.connection, to_sql, combined_params, @model_class.name, "COUNT").as(Int64)
        @select_clause = original_select
        result
      end
    end

    # Count records by a specific column, counting only non-null values.
    # This is useful for counting records that have a value present in a specific field.
    #
    # Examples:
    #   Customer.count(:title)  # => 3 (only customers with a title)
    #   Customer.where(active: true).count(:email)  # => 5 (active customers with email)
    #
    # SQL: SELECT COUNT(title) FROM customers
    # SQL: SELECT COUNT(email) FROM customers WHERE (customers.active = 1)
    def count(column : String | Symbol) : Int64 | Hash(String, Int64)
      column_name = column.to_s

      # Return 0 or empty hash if this is a none relation
      if @none
        return @group_clause ? {} of String => Int64 : 0_i64
      end

      # If there's a GROUP BY clause, return a hash of grouped counts
      if @group_clause
        grouped_count_by_column(column_name)
      elsif @limit_value
        # If there's a LIMIT clause, we need to count the actual records that would be returned
        # For column counting, we need to check for non-null values
        to_a.count { |record| !record.get_attribute(column_name).nil? }.to_i64
      else
        original_select = @select_clause
        @select_clause = "COUNT(#{column_name})"
        result = Takarik::Data.scalar_with_logging(@model_class.connection, to_sql, combined_params, @model_class.name, "COUNT").as(Int64)
        @select_clause = original_select
        result
      end
    end

    # Private method to handle grouped counting
    private def grouped_count
      # Build SQL like: SELECT COUNT(*) AS count_all, status AS status FROM orders GROUP BY status
      group_columns = @group_clause.not_nil!.split(",").map(&.strip)

      # Create the SELECT clause with COUNT(*) and the group columns
      select_parts = ["COUNT(*) AS count_all"]
      group_columns.each do |col|
        select_parts << "#{col} AS #{col}"
      end

      original_select = @select_clause
      @select_clause = select_parts.join(", ")

      result_hash = {} of String => Int64

      Takarik::Data.query_with_logging(@model_class.connection, to_sql, combined_params, @model_class.name, "COUNT") do |rs|
        rs.each do
          count = rs.read.as(Int64)

          # Handle single or multiple group columns
          if group_columns.size == 1
            group_value = rs.read
            key = group_value.nil? ? "nil" : group_value.to_s
            result_hash[key] = count
          else
            # For multiple columns, create a combined key
            group_values = [] of String
            group_columns.size.times do
              value = rs.read
              group_values << (value.nil? ? "nil" : value.to_s)
            end
            key = group_values.join(", ")
            result_hash[key] = count
          end
        end
      end

      @select_clause = original_select
      result_hash
    end

    # Private method to handle grouped counting by column
    private def grouped_count_by_column(column_name : String)
      # Build SQL like: SELECT COUNT(column_name) AS count_all, status AS status FROM orders GROUP BY status
      group_columns = @group_clause.not_nil!.split(",").map(&.strip)

      # Create the SELECT clause with COUNT(column) and the group columns
      select_parts = ["COUNT(#{column_name}) AS count_all"]
      group_columns.each do |col|
        select_parts << "#{col} AS #{col}"
      end

      original_select = @select_clause
      @select_clause = select_parts.join(", ")

      result_hash = {} of String => Int64

      Takarik::Data.query_with_logging(@model_class.connection, to_sql, combined_params, @model_class.name, "COUNT") do |rs|
        rs.each do
          count = rs.read.as(Int64)

          # Handle single or multiple group columns
          if group_columns.size == 1
            group_value = rs.read
            key = group_value.nil? ? "nil" : group_value.to_s
            result_hash[key] = count
          else
            # For multiple columns, create a combined key
            group_values = [] of String
            group_columns.size.times do
              value = rs.read
              group_values << (value.nil? ? "nil" : value.to_s)
            end
            key = group_values.join(", ")
            result_hash[key] = count
          end
        end
      end

      @select_clause = original_select
      result_hash
    end

    def exists?
      # Return false if this is a none relation
      return false if @none

      # For grouped queries, use count-based approach
      if @group_clause
        result = count
        case result
        when Hash(String, Int64)
          result.any? { |_, v| v > 0 }
        else
          false
        end
      else
        # Use optimized SELECT 1 query for existence check
        original_select = @select_clause
        original_limit = @limit_value
        @select_clause = "1"
        @limit_value = 1

        exists = false
        Takarik::Data.query_with_logging(@model_class.connection, to_sql, combined_params, @model_class.name, "Exists") do |rs|
          exists = rs.move_next
        end

        @select_clause = original_select
        @limit_value = original_limit
        exists
      end
    end

    def empty?
      !exists?
    end

    def any?
      exists?
    end

    # Check if there are more than one record matching the current query.
    # Uses an optimized approach with LIMIT 2 to avoid counting all records.
    #
    # Examples:
    #   Order.many?  # => true if there are 2 or more orders
    #   Order.shipped.many?  # => true if there are 2 or more shipped orders
    #   Book.where(out_of_print: true).many?  # => true if there are 2 or more out of print books
    def many?
      # Return false if this is a none relation
      return false if @none

      # If there's already a limit of 1 or less, can't have many
      if @limit_value.try(&.<= 1)
        return false
      end

      # For grouped queries, use count-based approach
      if @group_clause
        result = count
        case result
        when Hash(String, Int64)
          result.count { |_, v| v > 0 } > 1
        else
          false
        end
      else
        # Use optimized SELECT 1 query with LIMIT 2 (or respect existing limit)
        original_select = @select_clause
        original_limit = @limit_value
        @select_clause = "1"

        # Use the smaller of existing limit or 2
        effective_limit = if @limit_value && @limit_value.not_nil! < 2
                            @limit_value.not_nil!
                          else
                            2
                          end
        @limit_value = effective_limit

        count = 0
        Takarik::Data.query_with_logging(@model_class.connection, to_sql, combined_params, @model_class.name, "Many") do |rs|
          while rs.move_next && count < 2
            count += 1
          end
        end

        @select_clause = original_select
        @limit_value = original_limit
        count > 1
      end
    end

    # ========================================
    # PLUCK METHODS
    # ========================================

    def pluck(column : String)
      @select_clause = column
      results = [] of DB::Any
      Takarik::Data.query_with_logging(@model_class.connection, to_sql, combined_params, @model_class.name, "Pluck") do |rs|
        rs.each do
          results << rs.read
        end
      end
      results
    end

    def pluck(*columns : String)
      @select_clause = columns.join(", ")
      results = [] of Array(DB::Any)
      Takarik::Data.query_with_logging(@model_class.connection, to_sql, combined_params, @model_class.name, "Pluck") do |rs|
        rs.each do
          row = [] of DB::Any
          columns.size.times do
            row << rs.read
          end
          results << row
        end
      end
      results
    end

    # ========================================
    # SELECT_ALL, PICK, AND IDS METHODS
    # ========================================

    # Execute a custom SQL query and return raw results as an array of hashes.
    # This is similar to find_by_sql but returns raw data instead of model instances.
    # This method is equivalent to Rails' lease_connection.select_all.
    #
    # Examples:
    #   Customer.select_all("SELECT first_name, created_at FROM customers WHERE id = '1'")
    #   # => [{"first_name" => "Rafael", "created_at" => "2012-11-10 23:23:45.281189"}]
    #
    # The method always returns an array of hashes, even if the query returns a single record.
    # Returns an empty array if no records are found.
    def select_all(sql : String, params : Array(DB::Any) = [] of DB::Any)
      results = [] of Hash(String, DB::Any)

      Takarik::Data.query_with_logging(@model_class.connection, sql, params, @model_class.name, "Load") do |rs|
        while rs.move_next
          row = {} of String => DB::Any
          rs.column_names.each_with_index do |column_name, index|
            row[column_name] = rs.read
          end
          results << row
        end
      end

      results
    end

    def select_all(sql : String, *params : DB::Any)
      select_all(sql, params.to_a)
    end

    # Pick the value(s) from the named column(s) in the current relation.
    # Returns the first row of the specified column values with corresponding data type.
    # This is a short-hand for relation.limit(1).pluck(*column_names).first.
    #
    # Examples:
    #   Customer.where(id: 1).pick(:id)  # => 1
    #   Customer.where(id: 1).pick(:id, :first_name)  # => [1, "David"]
    #   Customer.where(id: 999).pick(:id)  # => nil
    def pick(column : String)
      result = limit(1).pluck(column)
      result.first?
    end

    def pick(*columns : String)
      result = limit(1).pluck(*columns)
      result.first?
    end

    # Pluck all the IDs for the relation using the table's primary key.
    # This is a convenience method equivalent to pluck(primary_key).
    #
    # Examples:
    #   Customer.ids  # => [1, 2, 3]
    #   Customer.where(active: true).ids  # => [1, 3]
    def ids
      pluck(@model_class.primary_key)
    end

    # ========================================
    # AGGREGATION METHODS
    # ========================================

    # Calculate the sum of values in the specified column.
    # Returns the sum as a number (possibly a floating-point number).
    # Returns 0 if no records match the query or if all values are NULL.
    #
    # Examples:
    #   Order.sum("subtotal")  # => 150.75
    #   Order.where(status: "shipped").sum("subtotal")  # => 89.50
    #
    # SQL: SELECT SUM(subtotal) FROM orders
    # SQL: SELECT SUM(subtotal) FROM orders WHERE (orders.status = 'shipped')
    def sum(column : String | Symbol)
      # Return 0 if this is a none relation
      return 0 if @none

      result = aggregate("SUM", column.to_s)
      # SUM returns NULL when there are no records, convert to 0
      result.nil? ? 0 : result
    end

    # Calculate the average of values in the specified column.
    # Returns the average as a number (possibly a floating-point number).
    # Returns nil if no records match the query or if all values are NULL.
    #
    # Examples:
    #   Order.average("subtotal")  # => 25.125
    #   Order.where(status: "shipped").average("subtotal")  # => 29.83
    #
    # SQL: SELECT AVG(subtotal) FROM orders
    # SQL: SELECT AVG(subtotal) FROM orders WHERE (orders.status = 'shipped')
    def average(column : String | Symbol)
      # Return nil if this is a none relation
      return nil if @none

      aggregate("AVG", column.to_s)
    end

    # Find the minimum value in the specified column.
    # Returns the minimum value with the corresponding data type.
    # Returns nil if no records match the query or if all values are NULL.
    #
    # Examples:
    #   Order.minimum("subtotal")  # => 5.99
    #   Order.where(status: "shipped").minimum("created_at")  # => 2023-01-15 10:30:00
    #
    # SQL: SELECT MIN(subtotal) FROM orders
    # SQL: SELECT MIN(created_at) FROM orders WHERE (orders.status = 'shipped')
    def minimum(column : String | Symbol)
      # Return nil if this is a none relation
      return nil if @none

      aggregate("MIN", column.to_s)
    end

    # Find the maximum value in the specified column.
    # Returns the maximum value with the corresponding data type.
    # Returns nil if no records match the query or if all values are NULL.
    #
    # Examples:
    #   Order.maximum("subtotal")  # => 199.99
    #   Order.where(status: "shipped").maximum("created_at")  # => 2023-12-31 23:59:59
    #
    # SQL: SELECT MAX(subtotal) FROM orders
    # SQL: SELECT MAX(created_at) FROM orders WHERE (orders.status = 'shipped')
    def maximum(column : String | Symbol)
      # Return nil if this is a none relation
      return nil if @none

      aggregate("MAX", column.to_s)
    end

    # Test method to check if methods are being found
    def test_method
      "test method works"
    end

    private def explain_preload_query(association_name : String, records : Array(T), output : String::Builder, adapter : Symbol, *options : Symbol)
      associations = @model_class.associations
      association = associations.find { |a| a.name == association_name }
      return unless association && association.class_type

      case association.type
      when .belongs_to?
        explain_belongs_to_preload(association, records, output, adapter, *options)
      when .has_many?
        explain_has_many_preload(association, records, output, adapter, *options)
      when .has_one?
        explain_has_one_preload(association, records, output, adapter, *options)
      end
    end

    private def explain_belongs_to_preload(association, records : Array(T), output : String::Builder, adapter : Symbol, *options : Symbol)
      # Get foreign key values that would be used in the preload query
      foreign_key_values = records.map { |record| record.get_attribute(association.foreign_key) }
        .reject(&.nil?)
        .uniq

      return if foreign_key_values.empty?

      # Build the preload query
      placeholders = (["?"] * foreign_key_values.size).join(", ")
      preload_sql = "SELECT * FROM #{association.class_type.not_nil!.table_name} WHERE #{association.primary_key} IN (#{placeholders})"
      explain_sql = build_explain_sql(preload_sql, adapter, *options)

      output << explain_sql << "\n"

      Takarik::Data.query_with_logging(@model_class.connection, explain_sql, foreign_key_values, @model_class.name, "EXPLAIN") do |rs|
        case adapter
        when :postgresql
          format_postgresql_explain(rs, output, *options)
        when :mysql, :mariadb
          format_mysql_explain(rs, output, *options)
        when :sqlite
          format_sqlite_explain(rs, output)
        else
          format_generic_explain(rs, output)
        end
      end
    end

    private def explain_has_many_preload(association, records : Array(T), output : String::Builder, adapter : Symbol, *options : Symbol)
      # Get primary key values that would be used in the preload query
      primary_key_values = records.map { |record| record.get_attribute(association.primary_key) }
        .reject(&.nil?)
        .uniq

      return if primary_key_values.empty?

      # Build the preload query
      placeholders = (["?"] * primary_key_values.size).join(", ")
      preload_sql = "SELECT * FROM #{association.class_type.not_nil!.table_name} WHERE #{association.foreign_key} IN (#{placeholders})"
      explain_sql = build_explain_sql(preload_sql, adapter, *options)

      output << explain_sql << "\n"

      Takarik::Data.query_with_logging(@model_class.connection, explain_sql, primary_key_values, @model_class.name, "EXPLAIN") do |rs|
        case adapter
        when :postgresql
          format_postgresql_explain(rs, output, *options)
        when :mysql, :mariadb
          format_mysql_explain(rs, output, *options)
        when :sqlite
          format_sqlite_explain(rs, output)
        else
          format_generic_explain(rs, output)
        end
      end
    end

    private def explain_has_one_preload(association, records : Array(T), output : String::Builder, adapter : Symbol, *options : Symbol)
      # Same as has_many for preloading
      explain_has_many_preload(association, records, output, adapter, *options)
    end

    private def detect_database_adapter : Symbol
      # Get the connection URI to detect the adapter
      connection = @model_class.connection

      # Try to detect from connection class name or URI
      connection_class = connection.class.name

      case connection_class
      when .includes?("SQLite")
        :sqlite
      when .includes?("PostgreSQL"), .includes?("Postgres")
        :postgresql
      when .includes?("MySQL")
        :mysql
      when .includes?("MariaDB")
        :mariadb
      else
        # Try to detect from connection URI if available
        # This is a fallback method - in practice, you might want to store
        # the adapter type when establishing the connection
        :unknown
      end
    end

    private def build_explain_sql(sql : String, adapter : Symbol, *options : Symbol) : String
      case adapter
      when :postgresql
        build_postgresql_explain_sql(sql, *options)
      when :mysql, :mariadb
        build_mysql_explain_sql(sql, *options)
      when :sqlite
        build_sqlite_explain_sql(sql, *options)
      else
        # Generic EXPLAIN for unknown adapters
        "EXPLAIN #{sql}"
      end
    end

    private def build_postgresql_explain_sql(sql : String, *options : Symbol) : String
      explain_options = [] of String

      options.each do |option|
        case option
        when :analyze
          explain_options << "ANALYZE"
        when :verbose
          explain_options << "VERBOSE"
        when :costs
          explain_options << "COSTS"
        when :buffers
          explain_options << "BUFFERS"
        when :format
          # Default to TEXT format, could be extended to support other formats
          explain_options << "FORMAT TEXT"
        end
      end

      if explain_options.any?
        "EXPLAIN (#{explain_options.join(", ")}) #{sql}"
      else
        "EXPLAIN #{sql}"
      end
    end

    private def build_mysql_explain_sql(sql : String, *options : Symbol) : String
      if options.includes?(:analyze)
        # MySQL 8.0+ and MariaDB support ANALYZE
        "ANALYZE #{sql}"
      else
        "EXPLAIN #{sql}"
      end
    end

    private def build_sqlite_explain_sql(sql : String, *options : Symbol) : String
      if options.includes?(:query_plan)
        "EXPLAIN QUERY PLAN #{sql}"
      else
        "EXPLAIN #{sql}"
      end
    end

    private def format_postgresql_explain(rs : DB::ResultSet, output : String::Builder, *options : Symbol)
      # PostgreSQL EXPLAIN returns a single column with the plan
      while rs.move_next
        plan_line = rs.read.to_s
        output << plan_line << "\n"
      end
    end

    private def format_mysql_explain(rs : DB::ResultSet, output : String::Builder, *options : Symbol)
      # MySQL/MariaDB EXPLAIN returns tabular data
      # First, get column names
      column_names = rs.column_names

      # Format header
      output << format_table_header(column_names)

      # Format rows
      while rs.move_next
        row_values = [] of String
        column_names.size.times do
          value = rs.read
          row_values << (value.nil? ? "NULL" : value.to_s)
        end
        output << format_table_row(row_values, column_names.map(&.size))
      end
    end

    private def format_sqlite_explain(rs : DB::ResultSet, output : String::Builder)
      # SQLite EXPLAIN returns different formats depending on the command
      column_names = rs.column_names

      if column_names.includes?("detail")
        # EXPLAIN QUERY PLAN format
        while rs.move_next
          detail = rs.read.to_s
          output << detail << "\n"
        end
      else
        # Regular EXPLAIN format (opcode listing)
        output << format_table_header(column_names)

        while rs.move_next
          row_values = [] of String
          column_names.size.times do
            value = rs.read
            row_values << (value.nil? ? "NULL" : value.to_s)
          end
          output << format_table_row(row_values, column_names.map(&.size))
        end
      end
    end

    private def format_generic_explain(rs : DB::ResultSet, output : String::Builder)
      # Generic tabular format for unknown adapters
      column_names = rs.column_names
      output << format_table_header(column_names)

      while rs.move_next
        row_values = [] of String
        column_names.size.times do
          value = rs.read
          row_values << (value.nil? ? "NULL" : value.to_s)
        end
        output << format_table_row(row_values, column_names.map(&.size))
      end
    end

    private def format_table_header(column_names : Array(String)) : String
      # Create a simple table header
      header = "+" + column_names.map { |name| "-" * (name.size + 2) }.join("+") + "+\n"
      header += "|" + column_names.map { |name| " #{name} " }.join("|") + "|\n"
      header += "+" + column_names.map { |name| "-" * (name.size + 2) }.join("+") + "+\n"
      header
    end

    private def format_table_row(values : Array(String), column_widths : Array(Int32)) : String
      # Format a table row with proper padding
      row = "|"
      values.each_with_index do |value, index|
        width = column_widths[index] > value.size ? column_widths[index] : value.size
        row += " #{value.ljust(width)} |"
      end
      row + "\n"
    end

    # ========================================
    # FIND OR CREATE METHODS
    # ========================================

    # Find the first record matching the current query conditions or create a new one.
    # Uses any create_with attributes when creating new records.
    #
    # Examples:
    #   User.where(active: true).find_or_create_by(name: "Andy")
    #   User.create_with(role: "user").find_or_create_by(email: "test@example.com")
    def find_or_create_by(conditions : Hash(String, DB::Any), &block : T ->)
      record = where(conditions).take
      return record if record

      # Create new record with both conditions and create_with attributes
      all_attributes = @create_with_attributes.merge(conditions)
      instance = @model_class.new
      all_attributes.each do |key, value|
        instance.set_attribute(key, value)
      end

      # Apply any additional attributes from the block
      yield instance

      instance.save
      instance
    end

    def find_or_create_by(conditions : Hash(String, DB::Any))
      record = where(conditions).take
      return record if record

      # Create new record with both conditions and create_with attributes
      all_attributes = @create_with_attributes.merge(conditions)
      @model_class.create(all_attributes)
    end

    def find_or_create_by(**conditions, &block : T ->)
      processed_conditions = conditions.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) }
      find_or_create_by(processed_conditions, &block)
    end

    def find_or_create_by(**conditions)
      processed_conditions = conditions.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) }
      find_or_create_by(processed_conditions)
    end

    def find_or_create_by!(conditions : Hash(String, DB::Any), &block : T ->)
      record = where(conditions).take
      return record if record

      # Create new record with both conditions and create_with attributes
      all_attributes = @create_with_attributes.merge(conditions)
      instance = @model_class.new
      all_attributes.each do |key, value|
        instance.set_attribute(key, value)
      end

      # Apply any additional attributes from the block
      yield instance

      instance.save!
      instance
    end

    def find_or_create_by!(conditions : Hash(String, DB::Any))
      record = where(conditions).take
      return record if record

      # Create new record with both conditions and create_with attributes
      all_attributes = @create_with_attributes.merge(conditions)
      instance = @model_class.new
      all_attributes.each do |key, value|
        instance.set_attribute(key, value)
      end
      instance.save!
      instance
    end

    def find_or_create_by!(**conditions, &block : T ->)
      processed_conditions = conditions.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) }
      find_or_create_by!(processed_conditions, &block)
    end

    def find_or_create_by!(**conditions)
      processed_conditions = conditions.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) }
      find_or_create_by!(processed_conditions)
    end

    def find_or_initialize_by(conditions : Hash(String, DB::Any), &block : T ->)
      record = where(conditions).take
      return record if record

      # Initialize new record with both conditions and create_with attributes
      all_attributes = @create_with_attributes.merge(conditions)
      instance = @model_class.new
      all_attributes.each do |key, value|
        instance.set_attribute(key, value)
      end

      # Apply any additional attributes from the block
      yield instance

      instance
    end

    def find_or_initialize_by(conditions : Hash(String, DB::Any))
      record = where(conditions).take
      return record if record

      # Initialize new record with both conditions and create_with attributes
      all_attributes = @create_with_attributes.merge(conditions)
      instance = @model_class.new
      all_attributes.each do |key, value|
        instance.set_attribute(key, value)
      end
      instance
    end

    def find_or_initialize_by(**conditions, &block : T ->)
      processed_conditions = conditions.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) }
      find_or_initialize_by(processed_conditions, &block)
    end

    def find_or_initialize_by(**conditions)
      processed_conditions = conditions.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) }
      find_or_initialize_by(processed_conditions)
    end

    # ========================================
    # UPDATE AND DELETE METHODS
    # ========================================

    def update_all(attributes : Hash(String, DB::Any))
      return 0 if attributes.empty?

      # If there's a LIMIT or OFFSET, we need to use a subquery approach
      if @limit_value || @offset_value
        # Get the IDs of records that match the limited query
        primary_key = @model_class.primary_key
        ids = pluck(primary_key)
        return 0 if ids.empty?

        # Update only those specific records
        set_clause = attributes.keys.map { |key| key + " = ?" }.join(", ")
        placeholders = ids.map { "?" }.join(", ")
        update_params = attributes.values.to_a + ids

        sql = "UPDATE #{@model_class.table_name} SET #{set_clause} WHERE #{primary_key} IN (#{placeholders})"
        result = Takarik::Data.exec_with_logging(@model_class.connection, sql, update_params, @model_class.name, "Update")
        result.rows_affected
      else
        # Normal update without LIMIT/OFFSET
        set_clause = attributes.keys.map { |key| key + " = ?" }.join(", ")
        update_params = attributes.values.to_a + combined_params

        sql = "UPDATE #{@model_class.table_name} SET #{set_clause}"
        unless @where_conditions.empty?
          # Check if there are any OR conditions and multiple conditions
          has_or_conditions = @where_conditions.any? { |condition| condition.starts_with?("OR ") }
          has_multiple_conditions = @where_conditions.size > 1

          # Only wrap in parentheses if we have OR conditions or multiple conditions
          where_clause = @where_conditions.map_with_index do |condition, index|
            if index == 0
              if has_or_conditions || has_multiple_conditions
                "(#{condition})"
              else
                condition
              end
            elsif condition.starts_with?("OR ")
              " " + condition
            else
              if has_or_conditions || has_multiple_conditions
                " AND (#{condition})"
              else
                " AND " + condition
              end
            end
          end.join("")
          sql += " WHERE #{where_clause}"
        end

        result = Takarik::Data.exec_with_logging(@model_class.connection, sql, update_params, @model_class.name, "Update")
        result.rows_affected
      end
    end

    def update_all(**attributes)
      update_all(attributes.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) })
    end

    def delete_all
      # If there's a LIMIT or OFFSET, we need to use a subquery approach
      if @limit_value || @offset_value
        # Get the IDs of records that match the limited query
        primary_key = @model_class.primary_key
        ids = pluck(primary_key)
        return 0 if ids.empty?

        # Delete only those specific records
        placeholders = ids.map { "?" }.join(", ")
        sql = "DELETE FROM #{@model_class.table_name} WHERE #{primary_key} IN (#{placeholders})"
        result = Takarik::Data.exec_with_logging(@model_class.connection, sql, ids, @model_class.name, "Delete")
        result.rows_affected
      else
        # Normal delete without LIMIT/OFFSET
        sql = "DELETE FROM #{@model_class.table_name}"
        unless @where_conditions.empty?
          # Check if there are any OR conditions and multiple conditions
          has_or_conditions = @where_conditions.any? { |condition| condition.starts_with?("OR ") }
          has_multiple_conditions = @where_conditions.size > 1

          # Only wrap in parentheses if we have OR conditions or multiple conditions
          where_clause = @where_conditions.map_with_index do |condition, index|
            if index == 0
              if has_or_conditions || has_multiple_conditions
                "(#{condition})"
              else
                condition
              end
            elsif condition.starts_with?("OR ")
              " " + condition
            else
              if has_or_conditions || has_multiple_conditions
                " AND (#{condition})"
              else
                " AND " + condition
              end
            end
          end.join("")
          sql += " WHERE #{where_clause}"
        end

        result = Takarik::Data.exec_with_logging(@model_class.connection, sql, combined_params, @model_class.name, "Delete")
        result.rows_affected
      end
    end

    def destroy_all
      records = to_a
      records.each(&.destroy)
      records.size
    end

    def destroy_by(conditions : Hash(String, DB::Any))
      where(conditions).destroy_all
    end

    def destroy_by(**conditions)
      processed_conditions = conditions.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) }
      destroy_by(processed_conditions)
    end

    def delete_by(conditions : Hash(String, DB::Any))
      where(conditions).delete_all
    end

    def delete_by(**conditions)
      processed_conditions = conditions.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) }
      delete_by(processed_conditions)
    end

    # Retrieve records in batches and yield each one to the block.
    # This is efficient for processing large datasets without loading everything into memory.
    # Uses cursor-based batching for better performance than OFFSET-based batching.
    #
    # Examples:
    #   User.where(active: true).find_each do |user|
    #     process_user(user)
    #   end
    #
    #   User.find_each(start: 2000, batch_size: 5000) do |user|
    #     UserMailer.newsletter(user).deliver_now
    #   end
    #
    #   # Composite key ordering
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
    def find_each(start : DB::Any? = nil, finish : DB::Any? = nil, batch_size : Int32 = 1000,
                  error_on_ignore : Bool? = nil, cursor : String | Array(String) = @model_class.primary_key,
                  order : Symbol | Array(Symbol) = DEFAULT_ORDER, &block : T ->)
      # Use find_in_batches and iterate over each record in each batch
      find_in_batches(start: start, finish: finish, batch_size: batch_size,
        error_on_ignore: error_on_ignore, cursor: cursor, order: order) do |records|
        records.each(&block)
      end
    end

    # Returns an Enumerator when no block is given
    def find_each(start : DB::Any? = nil, finish : DB::Any? = nil, batch_size : Int32 = 1000,
                  error_on_ignore : Bool? = nil, cursor : String | Array(String) = @model_class.primary_key,
                  order : Symbol | Array(Symbol) = DEFAULT_ORDER)
      records = [] of T
      find_each(start: start, finish: finish, batch_size: batch_size,
        error_on_ignore: error_on_ignore, cursor: cursor, order: order) do |record|
        records << record
      end
      records.each
    end

    # Yields each batch of records as an array. Uses cursor-based batching for better performance.
    #
    # Examples:
    #   User.find_in_batches do |batch|
    #     batch.each { |user| process_user(user) }
    #   end
    #
    #   User.find_in_batches(start: 1000, batch_size: 500) do |batch|
    #     batch.each { |user| user.update_status }
    #   end
    #
    # Options: Same as find_each - start, finish, batch_size, error_on_ignore, cursor, order
    def find_in_batches(start : DB::Any? = nil, finish : DB::Any? = nil, batch_size : Int32 = 1000,
                        error_on_ignore : Bool? = nil, cursor : String | Array(String) = @model_class.primary_key,
                        order : Symbol | Array(Symbol) = DEFAULT_ORDER, &block : Array(T) ->)
      # Use in_batches with load: true to get the actual records, similar to Rails
      in_batches(of: batch_size, start: start, finish: finish, load: true,
        error_on_ignore: error_on_ignore, cursor: cursor, order: order) do |relation|
        yield relation.to_a
      end
    end

    # Returns an Enumerator when no block is given
    def find_in_batches(start : DB::Any? = nil, finish : DB::Any? = nil, batch_size : Int32 = 1000,
                        error_on_ignore : Bool? = nil, cursor : String | Array(String) = @model_class.primary_key,
                        order : Symbol | Array(Symbol) = DEFAULT_ORDER)
      batches = [] of Array(T)
      find_in_batches(start: start, finish: finish, batch_size: batch_size,
        error_on_ignore: error_on_ignore, cursor: cursor, order: order) do |batch|
        batches << batch
      end
      batches.each
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
    #
    # Options:
    #   :of - Specifies the size of the batch (default: 1000)
    #   :load - Specifies if the relation should be loaded (default: false)
    #   :start - Starting cursor value (inclusive)
    #   :finish - Ending cursor value (inclusive)
    #   :error_on_ignore - Raise error if existing order is present (default: nil)
    #   :cursor - Column(s) to use for batching (default: primary_key)
    #   :order - Cursor order (:asc/:desc or array, default: :asc)
    #   :use_ranges - Use range iteration for better performance (default: nil, auto-detected)
    def in_batches(of batch_size : Int32 = 1000, start : DB::Any? = nil, finish : DB::Any? = nil,
                   load : Bool = false, error_on_ignore : Bool? = nil,
                   cursor : String | Array(String) = @model_class.primary_key, order : Symbol | Array(Symbol) = DEFAULT_ORDER,
                   use_ranges : Bool? = nil, &block : QueryBuilder(T) ->)
      # Validate batch size
      if batch_size <= 0
        raise ArgumentError.new("Batch size must be positive")
      end

      # Normalize cursor to array
      cursor_array = cursor.is_a?(Array) ? cursor : [cursor.as(String)]

      # Validate parameters
      ensure_valid_options_for_batching!(cursor_array, start, finish, order)

      # Check for existing ordering
      unless @order_clauses.empty?
        act_on_ignored_order(error_on_ignore)
      end

      # Validate and normalize order parameter
      order_array = case order
                    when Symbol
                      cursor_array.map { order.as(Symbol) }
                    when Array
                      if order.size != cursor_array.size
                        raise ArgumentError.new("Order array size (#{order.size}) must match cursor columns size (#{cursor_array.size})")
                      end
                      order.as(Array(Symbol))
                    else
                      raise ArgumentError.new("Order must be Symbol or Array(Symbol), got #{order.class}")
                    end

      # Build batch orders hash
      batch_orders = cursor_array.zip(order_array).to_h

      # Create base relation with proper ordering and limits
      relation = dup.clear_order
      batch_orders.each do |column, direction|
        relation = relation.order(column, direction.to_s)
      end
      relation = relation.limit(batch_size)
      relation = apply_limits(relation, cursor_array, start, finish, batch_orders)

      # Enhanced batching implementation
      last_values = nil

      loop do
        current_relation = if last_values
                             build_next_batch_relation(relation, cursor_array, last_values, batch_orders)
                           else
                             relation
                           end

        records = current_relation.to_a
        break if records.empty?

        if load
          # For load: true, create a fresh relation with just the specific records
          cursor_values = records.map { |r| cursor_array.map { |col| r.get_attribute(col) } }
          # Create a completely fresh query with just the IDs we want, preserving order
          if cursor_array.size == 1
            values = cursor_values.map(&.first)
            yielded_relation = @model_class.where(cursor_array[0], values)
            # Preserve the original ordering
            batch_orders.each do |column, direction|
              yielded_relation = yielded_relation.order(column, direction.to_s)
            end
          else
            yielded_relation = @model_class.query
            cursor_values.each do |values|
              conditions = {} of String => DB::Any
              cursor_array.each_with_index do |col, i|
                conditions[col] = values[i]
              end
              yielded_relation = yielded_relation.or(conditions)
            end
            # Preserve the original ordering
            batch_orders.each do |column, direction|
              yielded_relation = yielded_relation.order(column, direction.to_s)
            end
          end
          yield yielded_relation
        else
          # For load: false, yield the relation that would fetch these records
          yield current_relation
        end

        break if records.size < batch_size
        last_values = records.map { |r| cursor_array.map { |col| r.get_attribute(col) } }.last
      end

      self
    end

    # Returns a BatchEnumerator when no block is given
    def in_batches(of batch_size : Int32 = 1000, start : DB::Any? = nil, finish : DB::Any? = nil,
                   load : Bool = false, error_on_ignore : Bool? = nil,
                   cursor : String | Array(String) = @model_class.primary_key, order : Symbol | Array(Symbol) = DEFAULT_ORDER,
                   use_ranges : Bool? = nil)
      # Validate batch size
      if batch_size <= 0
        raise ArgumentError.new("Batch size must be positive")
      end

      BatchEnumerator(T).new(
        of: batch_size,
        start: start,
        finish: finish,
        relation: self,
        cursor: cursor,
        order: order,
        use_ranges: use_ranges
      )
    end

    # ========================================
    # PRIVATE BATCHING HELPER METHODS
    # ========================================

    private def ensure_valid_options_for_batching!(cursor : Array(String), start : DB::Any?, finish : DB::Any?, order : Symbol | Array(Symbol))
      # Validate start parameter
      if start && !start.is_a?(Array) && cursor.size > 1
        raise ArgumentError.new(":start must contain one value per cursor column")
      end

      # Validate finish parameter
      if finish && !finish.is_a?(Array) && cursor.size > 1
        raise ArgumentError.new(":finish must contain one value per cursor column")
      end

      # Validate that cursor includes primary key or unique column
      primary_keys = [@model_class.primary_key].flatten
      unless (primary_keys - cursor).empty?
        # For now, we'll allow it but ideally should check for unique indexes
        # raise ArgumentError.new(":cursor must include a primary key or other unique column(s)")
      end

      # Validate order parameter
      order_array = order.is_a?(Array) ? order : [order]
      valid_orders = [:asc, :desc]
      unless order_array.all? { |o| valid_orders.includes?(o) }
        raise ArgumentError.new("Order must be :asc or :desc")
      end
    end

    private def act_on_ignored_order(error_on_ignore : Bool?)
      # For now, just warn or raise based on error_on_ignore
      if error_on_ignore
        raise ArgumentError.new(ORDER_IGNORE_MESSAGE)
      end
      # In a real implementation, we'd log a warning here
    end

    private def apply_limits(relation : QueryBuilder(T), cursor : Array(String), start : DB::Any?, finish : DB::Any?, batch_orders : Hash(String, Symbol))
      relation = apply_start_limit(relation, cursor, start, batch_orders) if start
      relation = apply_finish_limit(relation, cursor, finish, batch_orders) if finish
      relation
    end

    private def apply_start_limit(relation : QueryBuilder(T), cursor : Array(String), start : DB::Any, batch_orders : Hash(String, Symbol))
      # Build condition for start limit
      start_values = start.is_a?(Array) ? start : [start]
      operators = batch_orders.values.map { |order| order == :desc ? "<=" : ">=" }
      batch_condition(relation, cursor, start_values, operators)
    end

    private def apply_finish_limit(relation : QueryBuilder(T), cursor : Array(String), finish : DB::Any, batch_orders : Hash(String, Symbol))
      # Build condition for finish limit
      finish_values = finish.is_a?(Array) ? finish : [finish]
      operators = batch_orders.values.map { |order| order == :desc ? ">=" : "<=" }
      batch_condition(relation, cursor, finish_values, operators)
    end

    private def batch_condition(relation : QueryBuilder(T), cursor : Array(String), values : Array(DB::Any), operators : Array(String))
      # For simplicity, handle single column case
      if cursor.size == 1
        relation.where("#{cursor[0]} #{operators[0]} ?", values[0])
      else
        # For multiple columns, we'd need more complex logic
        # For now, just use the first column
        relation.where("#{cursor[0]} #{operators[0]} ?", values[0])
      end
    end

    private def build_next_batch_relation(relation : QueryBuilder(T), cursor : Array(String), last_values : Array(DB::Any), batch_orders : Hash(String, Symbol))
      # Build condition for next batch (exclusive of last values)
      if cursor.size == 1
        column = cursor[0]
        order = batch_orders[column]
        operator = order == :desc ? "<" : ">"
        relation.where("#{column} #{operator} ?", last_values[0])
      else
        # For multiple columns, we'd need more complex logic
        # For now, just use the first column
        column = cursor[0]
        order = batch_orders[column]
        operator = order == :desc ? "<" : ">"
        relation.where("#{column} #{operator} ?", last_values[0])
      end
    end

    # ========================================
    # ENUMERABLE-LIKE METHODS
    # ========================================

    def each(&block : T ->)
      to_a.each(&block)
      self
    end

    def map(&block : T -> U) forall U
      to_a.map(&block)
    end

    def select(&block : T -> Bool)
      to_a.select(&block)
    end

    def reject(&block : T -> Bool)
      to_a.reject(&block)
    end

    def find(&block : T -> Bool)
      to_a.find(&block)
    end

    # ========================================
    # PRIVATE HELPER METHODS
    # ========================================

    private def combined_params : Array(DB::Any)
      @where_params + @having_params
    end

    private def add_join(join_type : String, association_name : String)
      @has_joins = true
      add_association_join(join_type, association_name)
    end

    private def add_join(join_type : String, table : String, on : String)
      @has_joins = true
      @joins << "#{join_type} #{table} ON #{on}"
      self
    end

    private def add_association_join(join_type : String, association_name : String)
      associations = @model_class.associations
      association = associations.find { |a| a.name == association_name }

      unless association
        raise "Association '#{association_name}' not found for #{@model_class.name}"
      end

      # Skip polymorphic associations as they can't be joined directly
      if association.polymorphic || association.class_type.nil?
        raise "Cannot join polymorphic association '#{association_name}'"
      end

      current_table = @model_class.table_name
      associated_table = association.class_type.not_nil!.table_name

      case association.type
      when .belongs_to?
        on_condition = "#{current_table}.#{association.foreign_key} = #{associated_table}.#{association.primary_key}"
      when .has_many?, .has_one?
        on_condition = "#{current_table}.#{association.primary_key} = #{associated_table}.#{association.foreign_key}"
      else
        raise "Unknown association type: #{association.type}"
      end

      @joins << "#{join_type} #{associated_table} ON #{on_condition}"
      self
    end

    # Smart association join that automatically chooses the correct join type based on association configuration
    private def add_smart_association_join(association_name : String)
      associations = @model_class.associations
      association = associations.find { |a| a.name == association_name }

      unless association
        raise "Association '#{association_name}' not found for #{@model_class.name}"
      end

      join_type = get_smart_join_type(association)
      add_association_join(join_type, association_name)
    end

    # Extract smart join type logic for reuse in nested associations
    # Choose join type based on association configuration, matching Rails behavior
    private def get_smart_join_type(association)
      case association.type
      when .belongs_to?
        # For belongs_to associations, use the optional parameter to determine join type
        association.optional ? "LEFT JOIN" : "INNER JOIN"
      when .has_many?, .has_one?
        # For has_many/has_one, use INNER JOIN to match Rails behavior
        # This filters to only show parent records that have associated records
        # Users can use left_join() explicitly if they want to include parents without children
        "INNER JOIN"
      else
        raise "Unknown association type: #{association.type}"
      end
    end

    # Handle nested association joins like User.joins(posts: [:comments])
    # Uses smart join logic to choose the best join type based on association configuration
    private def join_nested_association(parent_association : String, child_association : String)
      # Get the parent association to find the intermediate model
      parent_associations = @model_class.associations
      parent_assoc = parent_associations.find { |a| a.name == parent_association }

      unless parent_assoc && parent_assoc.class_type
        raise "Parent association '#{parent_association}' not found or invalid for #{@model_class.name}"
      end

      # Get the intermediate model class (e.g., Post in User -> Post -> Comment)
      intermediate_model = parent_assoc.class_type.not_nil!

      # Find the child association on the intermediate model
      child_associations = intermediate_model.associations
      child_assoc = child_associations.find { |a| a.name == child_association }

      unless child_assoc && child_assoc.class_type
        raise "Child association '#{child_association}' not found on #{intermediate_model.name}"
      end

      # Skip polymorphic associations
      if child_assoc.not_nil!.polymorphic || child_assoc.not_nil!.class_type.nil?
        raise "Cannot join polymorphic association '#{child_association}'"
      end

      # Build the join for the child association
      intermediate_table = intermediate_model.table_name
      child_table = child_assoc.not_nil!.class_type.not_nil!.table_name

      case child_assoc.not_nil!.type
      when .belongs_to?
        on_condition = "#{intermediate_table}.#{child_assoc.not_nil!.foreign_key} = #{child_table}.#{child_assoc.not_nil!.primary_key}"
      when .has_many?, .has_one?
        on_condition = "#{intermediate_table}.#{child_assoc.not_nil!.primary_key} = #{child_table}.#{child_assoc.not_nil!.foreign_key}"
      else
        raise "Unknown association type: #{child_assoc.not_nil!.type}"
      end

      # Use smart join logic to choose the best join type based on association configuration
      join_type = get_smart_join_type(child_assoc.not_nil!)
      @joins << "#{join_type} #{child_table} ON #{on_condition}"
      @has_joins = true
    end

    # Process nested association values recursively to handle complex structures
    private def process_nested_association_value(parent_association : String, value)
      case value
      when Array
        value.each do |item|
          process_nested_association_value(parent_association, item)
        end
      when String, Symbol
        # For simple nested associations, use smart join logic
        join_nested_association(parent_association, value.to_s)
      when Hash, NamedTuple
        # Handle both Hash and NamedTuple the same way
        if value.is_a?(NamedTuple)
          # Convert NamedTuple to Hash-like iteration
          value.each do |nested_parent, nested_child|
            # First join the nested parent association
            join_nested_association(parent_association, nested_parent.to_s)

            # Then recursively process the nested child
            process_deeply_nested_association(parent_association, nested_parent.to_s, nested_child)
          end
        else
          # Handle regular Hash
          value.each do |nested_parent, nested_child|
            # First join the nested parent association
            join_nested_association(parent_association, nested_parent.to_s)

            # Then recursively process the nested child
            process_deeply_nested_association(parent_association, nested_parent.to_s, nested_child)
          end
        end
      else
        raise "Invalid nested association value type: #{value.class}"
      end
    end

    # Handle deeply nested associations like { reviews: { customer: :orders } }
    private def process_deeply_nested_association(root_association : String, current_association : String, value)
      case value
      when String, Symbol
        # Simple case: join the final association
        # We need to join current_association -> value
        join_association_from_model(current_association, value.to_s)
      when Hash, NamedTuple
        # Recursive case: process nested hash or NamedTuple
        if value.is_a?(NamedTuple)
          value.each do |nested_parent, nested_child|
            # Join current_association -> nested_parent
            join_association_from_model(current_association, nested_parent.to_s)

            # Continue recursively with nested_parent as the new current
            process_deeply_nested_association(root_association, nested_parent.to_s, nested_child)
          end
        else
          value.each do |nested_parent, nested_child|
            # Join current_association -> nested_parent
            join_association_from_model(current_association, nested_parent.to_s)

            # Continue recursively with nested_parent as the new current
            process_deeply_nested_association(root_association, nested_parent.to_s, nested_child)
          end
        end
      when Array
        # Array case: process each item
        value.each do |item|
          process_deeply_nested_association(root_association, current_association, item)
        end
      else
        raise "Invalid deeply nested association value type: #{value.class}"
      end
    end

    # Join an association starting from a specific model in the chain
    # This is different from join_nested_association which always starts from @model_class
    # Uses smart join logic to choose the best join type based on association configuration
    private def join_association_from_model(from_association : String, to_association : String)
      # Find the model class for the from_association
      from_model_class = find_model_for_association(from_association)

      # Find the to_association on that model
      associations = from_model_class.associations
      association = associations.find { |a| a.name == to_association }

      unless association && association.class_type
        raise "Association '#{to_association}' not found on #{from_model_class.name}"
      end

      # Skip polymorphic associations
      if association.polymorphic || association.class_type.nil?
        raise "Cannot join polymorphic association '#{to_association}'"
      end

      # Build the join
      from_table = from_model_class.table_name
      to_table = association.class_type.not_nil!.table_name

      case association.type
      when .belongs_to?
        on_condition = "#{from_table}.#{association.foreign_key} = #{to_table}.#{association.primary_key}"
      when .has_many?, .has_one?
        on_condition = "#{from_table}.#{association.primary_key} = #{to_table}.#{association.foreign_key}"
      else
        raise "Unknown association type: #{association.type}"
      end

      # Use smart join logic to choose the best join type based on association configuration
      join_type = get_smart_join_type(association)
      @joins << "#{join_type} #{to_table} ON #{on_condition}"
      @has_joins = true
    end

    # Find the model class for a given association name by traversing the existing joins
    private def find_model_for_association(association_name : String)
      # Collect all models that are part of the join chain
      models_in_chain = [@model_class.as(Takarik::Data::BaseModel.class)]

      # Add models from direct associations
      @model_class.associations.each do |assoc|
        if assoc.class_type
          models_in_chain << assoc.class_type.not_nil!
        end
      end

      # Add models from nested associations (go deeper)
      models_to_check = models_in_chain.dup
      models_to_check.each do |model|
        model.associations.each do |assoc|
          if assoc.class_type && !models_in_chain.includes?(assoc.class_type.not_nil!)
            models_in_chain << assoc.class_type.not_nil!
          end
        end
      end

      # Now search for the association in all these models
      models_in_chain.each do |model|
        associations = model.associations
        association = associations.find { |a| a.name == association_name }
        if association && association.class_type
          return association.class_type.not_nil!
        end
      end

      # If still not found, raise an error
      raise "Could not find model for association '#{association_name}'"
    end

    private def add_includes(association_name : String)
      # Validate the association exists
      associations = @model_class.associations
      association = associations.find { |a| a.name == association_name }

      unless association
        raise "Association '#{association_name}' not found for #{@model_class.name}"
      end

      # Skip polymorphic associations as they can't be eagerly loaded
      if association.polymorphic
        raise "Cannot eagerly load polymorphic association '#{association_name}'"
      end

      # Don't add duplicate includes
      return if @includes.includes?(association_name)

      @includes << association_name
      # JOIN logic is handled in to_sql method based on conditions
    end

    private def get_prefixed_columns
      all_columns = [] of String

      columns = @model_class.column_names
      if columns.empty?
        columns = ["id", "created_at", "updated_at"]
      end

      table_name = @model_class.table_name

      columns.each do |col|
        all_columns << "#{table_name}.#{col} AS #{table_name}_#{col}"
      end

      # Include columns from associated tables when using includes
      @includes.each do |association_name|
        associations = @model_class.associations
        association = associations.find { |a| a.name == association_name }
        next unless association && association.class_type && !association.polymorphic

        associated_columns = association.class_type.not_nil!.column_names
        if associated_columns.empty?
          associated_columns = ["id", "created_at", "updated_at"]
        end

        associated_table = association.class_type.not_nil!.table_name

        associated_columns.each do |col|
          all_columns << "#{associated_table}.#{col} AS #{associated_table}_#{col}"
        end
      end

      # Include columns from associated tables when using eager_load
      @eager_loads.each do |association_name|
        associations = @model_class.associations
        association = associations.find { |a| a.name == association_name }
        next unless association && association.class_type && !association.polymorphic

        associated_columns = association.class_type.not_nil!.column_names
        if associated_columns.empty?
          associated_columns = ["id", "created_at", "updated_at"]
        end

        associated_table = association.class_type.not_nil!.table_name

        associated_columns.each do |col|
          all_columns << "#{associated_table}.#{col} AS #{associated_table}_#{col}"
        end
      end

      all_columns.join(", ")
    end

    private def aggregate(function : String, column : String)
      @select_clause = "#{function}(#{column})"
      Takarik::Data.scalar_with_logging(@model_class.connection, to_sql, combined_params, @model_class.name, function)
    end

    # ========================================
    # PRELOAD HELPER METHODS
    # ========================================

    private def add_preload(association_name : String)
      # Validate the association exists
      associations = @model_class.associations
      association = associations.find { |a| a.name == association_name }

      unless association
        raise "Association '#{association_name}' not found for #{@model_class.name}"
      end

      # Skip polymorphic associations as they can't be preloaded
      if association.polymorphic
        raise "Cannot preload polymorphic association '#{association_name}'"
      end

      # Don't add duplicate preloads
      return if @preloads.includes?(association_name)

      @preloads << association_name
    end

    private def add_eager_load(association_name : String)
      # Validate the association exists
      associations = @model_class.associations
      association = associations.find { |a| a.name == association_name }

      unless association
        raise "Association '#{association_name}' not found for #{@model_class.name}"
      end

      # Skip polymorphic associations as they can't be eagerly loaded
      if association.polymorphic
        raise "Cannot eager load polymorphic association '#{association_name}'"
      end

      # Don't add duplicate eager loads
      return if @eager_loads.includes?(association_name)

      @eager_loads << association_name
      @has_joins = true

      # Add a LEFT JOIN for the association
      add_association_join("LEFT JOIN", association_name)
    end

    # Check if where conditions reference any of the included associations
    private def has_association_conditions?
      return false if @includes.empty? || @where_conditions.empty?

      # Get association table names
      association_tables = Set(String).new
      @includes.each do |association_name|
        associations = @model_class.associations
        association = associations.find { |a| a.name == association_name }
        if association && association.class_type && !association.polymorphic
          table_name = association.class_type.not_nil!.table_name
          association_tables.add(table_name)
        end
      end

      # Check if any where condition references association tables
      @where_conditions.any? do |condition|
        association_tables.any? { |table| condition.includes?(table) }
      end
    end

    # Handle includes with separate queries (same as preload)
    private def perform_preloading_for_includes(records : Array(T))
      return if records.empty?

      @includes.each do |association_name|
        preload_association(records, association_name)
      end
    end

    private def perform_preloading(records : Array(T))
      return if records.empty?

      @preloads.each do |association_name|
        preload_association(records, association_name)
      end
    end

    private def preload_association(records : Array(T), association_name : String)
      associations = @model_class.associations
      association = associations.find { |a| a.name == association_name }
      return unless association && association.class_type

      case association.type
      when .belongs_to?
        preload_belongs_to(records, association)
      when .has_many?
        preload_has_many(records, association)
      when .has_one?
        preload_has_one(records, association)
      end
    end

    private def preload_belongs_to(records : Array(T), association)
      # Get all foreign key values from the records
      foreign_key_values = records.map { |record| record.get_attribute(association.foreign_key) }
        .reject(&.nil?)
        .uniq

      return if foreign_key_values.empty?

      # Query associated records in one query using IN clause
      associated_records = association.class_type.not_nil!.where(association.primary_key, foreign_key_values).to_a

      # Create a hash for quick lookup
      lookup_hash = {} of DB::Any => typeof(associated_records.first)
      associated_records.each do |record|
        key = record.get_attribute(association.primary_key)
        lookup_hash[key] = record if key
      end

      # Cache associations on each record
      records.each do |record|
        foreign_key_value = record.get_attribute(association.foreign_key)
        if foreign_key_value && lookup_hash.has_key?(foreign_key_value)
          record.cache_association(association.name, lookup_hash[foreign_key_value])
        else
          record.cache_association(association.name, nil)
        end
      end
    end

    private def preload_has_many(records : Array(T), association)
      # Get all primary key values from the records
      primary_key_values = records.map { |record| record.get_attribute(association.primary_key) }
        .reject(&.nil?)
        .uniq

      return if primary_key_values.empty?

      # Query associated records in one query using IN clause
      associated_records = association.class_type.not_nil!.where(association.foreign_key, primary_key_values).to_a

      # Group by foreign key
      grouped_records = {} of DB::Any => Array(typeof(associated_records.first))
      associated_records.each do |record|
        foreign_key_value = record.get_attribute(association.foreign_key)
        if foreign_key_value
          grouped_records[foreign_key_value] ||= [] of typeof(associated_records.first)
          grouped_records[foreign_key_value] << record
        end
      end

      # Cache associations on each record (mark as loaded but don't store array)
      records.each do |record|
        primary_key_value = record.get_attribute(association.primary_key)
        if primary_key_value
          # Mark as loaded for has_many (the actual query will use the cached foreign keys)
          record.cache_association(association.name, nil)
        end
      end
    end

    private def preload_has_one(records : Array(T), association)
      # Get all primary key values from the records
      primary_key_values = records.map { |record| record.get_attribute(association.primary_key) }
        .reject(&.nil?)
        .uniq

      return if primary_key_values.empty?

      # Query associated records in one query using IN clause
      associated_records = association.class_type.not_nil!.where(association.foreign_key, primary_key_values).to_a

      # Create a hash for quick lookup
      lookup_hash = {} of DB::Any => typeof(associated_records.first)
      associated_records.each do |record|
        foreign_key_value = record.get_attribute(association.foreign_key)
        lookup_hash[foreign_key_value] = record if foreign_key_value
      end

      # Cache associations on each record
      records.each do |record|
        primary_key_value = record.get_attribute(association.primary_key)
        if primary_key_value && lookup_hash.has_key?(primary_key_value)
          record.cache_association(association.name, lookup_hash[primary_key_value])
        else
          record.cache_association(association.name, nil)
        end
      end
    end

    # ========================================
    # MACROS
    # ========================================

    macro generate_where_overloads
      {% for type in [Int32, Int64, String, Float32, Float64, Bool, Time] %}
        # ========================================
        # WHERE METHOD OVERLOADS FOR {{type}}
        # ========================================

        def where(condition : String, param : {{type}})
          @where_conditions << condition
          @where_params << param.as(DB::Any)
          self
        end

        def where(condition : String, *params : {{type}})
          @where_conditions << condition
          params.each { |param| @where_params << param.as(DB::Any) }
          self
        end

        def where(column_with_operator : String, value : {{type}})
          if column_with_operator.includes?("?") || column_with_operator.includes?(" ")
            if column_with_operator.includes?("?")
              @where_conditions << column_with_operator
            else
              @where_conditions << "#{column_with_operator} ?"
            end
            @where_params << value.as(DB::Any)
          else
            @where_conditions << "#{column_with_operator} = ?"
            @where_params << value.as(DB::Any)
          end
          self
        end

        def where(column : String, values : Array({{type}}))
          where(column, values.map(&.as(DB::Any)))
        end

        # ========================================
        # NOT METHOD OVERLOADS FOR {{type}}
        # ========================================

        def not(condition : String, param : {{type}})
          @where_conditions << "NOT (" + condition + ")"
          @where_params << param.as(DB::Any)
          self
        end

        def not(condition : String, *params : {{type}})
          @where_conditions << "NOT (" + condition + ")"
          params.each { |param| @where_params << param.as(DB::Any) }
          self
        end

        def not(column_with_operator : String, value : {{type}})
          if column_with_operator.includes?("?") || column_with_operator.includes?(" ")
            if column_with_operator.includes?("?")
              @where_conditions << "NOT (" + column_with_operator + ")"
            else
              @where_conditions << "NOT (" + column_with_operator + " ?)"
            end
            @where_params << value.as(DB::Any)
          else
            @where_conditions << "NOT (" + column_with_operator + " = ?)"
            @where_params << value.as(DB::Any)
          end
          self
        end

        def not(column : String, values : Array({{type}}))
          not(column, values.map(&.as(DB::Any)))
        end

        # ========================================
        # OR METHOD OVERLOADS FOR {{type}}
        # ========================================

        def or(condition : String, param : {{type}})
          @where_conditions << "OR (" + condition + ")"
          @where_params << param.as(DB::Any)
          self
        end

        def or(condition : String, *params : {{type}})
          @where_conditions << "OR (" + condition + ")"
          params.each { |param| @where_params << param.as(DB::Any) }
          self
        end

        def or(column_with_operator : String, value : {{type}})
          if column_with_operator.includes?("?") || column_with_operator.includes?(" ")
            if column_with_operator.includes?("?")
              @where_conditions << "OR (" + column_with_operator + ")"
            else
              @where_conditions << "OR (" + column_with_operator + " ?)"
            end
            @where_params << value.as(DB::Any)
          else
            @where_conditions << "OR (" + column_with_operator + " = ?)"
            @where_params << value.as(DB::Any)
          end
          self
        end

        def or(column : String, values : Array({{type}}))
          or(column, values.map(&.as(DB::Any)))
        end
      {% end %}

      # ========================================
      # RANGE METHOD OVERLOADS
      # ========================================

      {% for type in [Int32, Int64, Float32, Float64, Time, String] %}
        def where(column : String, range : Range({{type}}, {{type}}))
          if range.exclusive?
            @where_conditions << "#{column} >= ? AND #{column} < ?"
          else
            @where_conditions << "#{column} BETWEEN ? AND ?"
          end
          @where_params << range.begin.as(DB::Any) << range.end.as(DB::Any)
          self
        end

        def not(column : String, range : Range({{type}}, {{type}}))
          if range.exclusive?
            @where_conditions << "NOT (#{column} >= ? AND #{column} < ?)"
          else
            @where_conditions << "NOT (#{column} BETWEEN ? AND ?)"
          end
          @where_params << range.begin.as(DB::Any) << range.end.as(DB::Any)
          self
        end

        def or(column : String, range : Range({{type}}, {{type}}))
          if range.exclusive?
            @where_conditions << "OR (#{column} >= ? AND #{column} < ?)"
          else
            @where_conditions << "OR (#{column} BETWEEN ? AND ?)"
          end
          @where_params << range.begin.as(DB::Any) << range.end.as(DB::Any)
          self
        end
      {% end %}
    end

    generate_where_overloads

    # Runtime method_missing for dynamic scope delegation
    macro method_missing(call)
      {% array_methods = %w[
           size all? none?
           each_with_index each_with_object
           map_with_index
           find_index includes? index
           sample shuffle reverse
           sort sort_by min max min_by max_by
           join partition group_by
           zip flatten compact uniq
           drop take_while drop_while
           [] []? at at? fetch
         ] %}

      {% query_builder_methods = %w[unscope only reselect reorder rewhere regroup reverse_order] %}
      {% explain_methods = %w[test_method] %}
      {% method_name = call.name.stringify %}

      {% if array_methods.includes?(method_name) %}
        to_a.{{call}}
      {% elsif query_builder_methods.includes?(method_name) %}
        # These methods should not be delegated - they should be handled by QueryBuilder itself
        # If we reach here, it means the method is not properly defined
        raise "Method " + {{method_name}} + " not found on QueryBuilder instance"
      {% elsif explain_methods.includes?(method_name) %}
        # These methods are defined on QueryBuilder but method_missing is intercepting them
        # This should not happen - there might be a compilation issue
        raise "Method " + {{method_name}} + " exists on QueryBuilder but method_missing was called. This indicates a compilation issue."
      {% else %}
        # Try to delegate to model class scope
        begin
          {% if call.args.size > 0 %}
            scope_result = @model_class.{{call.name.id}}({{call.args.splat}})
          {% else %}
            scope_result = @model_class.{{call.name.id}}
          {% end %}

          # If the scope returns a QueryBuilder, merge it with this one
          if scope_result.is_a?(Takarik::Data::QueryBuilder)
            merge_with_scope(scope_result)
          else
            # If scope doesn't return QueryBuilder (e.g., conditional scope returns all),
            # just return self to maintain chainability
            self
          end
        rescue ex
          # If method doesn't exist on model class, raise a more helpful error
          raise "Method " + {{call.name.id.stringify}} + " not found on " + @model_class.name + " or QueryBuilder"
        end
      {% end %}
    end

    # ========================================
    # SCOPE CHAINING METHODS
    # ========================================

    # Helper method to merge another QueryBuilder's state into this one
    private def merge_with_scope(other_query : Takarik::Data::QueryBuilder)
      # Merge where conditions and parameters
      @where_conditions.concat(other_query.@where_conditions)
      @where_params.concat(other_query.@where_params)

      # Merge order clauses
      @order_clauses.concat(other_query.@order_clauses)

      # Merge other query state
      @group_clause = other_query.@group_clause if other_query.@group_clause
      @having_conditions.concat(other_query.@having_conditions)
      @having_params.concat(other_query.@having_params)
      @limit_value = other_query.@limit_value if other_query.@limit_value
      @offset_value = other_query.@offset_value if other_query.@offset_value

      # Note: We don't merge select_clause, joins, or distinct to avoid conflicts
      # Those are typically set at the beginning of a query chain

      self
    end
  end

  # BatchEnumerator provides enumerable methods for batched operations
  # This is returned when in_batches is called without a block
  class BatchEnumerator(T)
    include Enumerable(QueryBuilder(T))

    @of : Int32
    @relation : QueryBuilder(T)
    @start : DB::Any?
    @finish : DB::Any?
    @cursor : String | Array(String)?
    @order : Symbol | Array(Symbol)
    @use_ranges : Bool?

    def initialize(of : Int32, start : DB::Any?, finish : DB::Any?, relation : QueryBuilder(T),
                   cursor : String | Array(String)?, order : Symbol | Array(Symbol), use_ranges : Bool?)
      @of = of
      @relation = relation
      @start = start
      @finish = finish
      @cursor = cursor
      @order = order
      @use_ranges = use_ranges
    end

    # The primary key value from which the BatchEnumerator starts, inclusive of the value.
    getter start

    # The primary key value at which the BatchEnumerator ends, inclusive of the value.
    getter finish

    # The relation from which the BatchEnumerator yields batches.
    getter relation

    # The size of the batches yielded by the BatchEnumerator.
    def batch_size
      @of
    end

    # Looping through a collection of records from the database is very inefficient
    # since it will try to instantiate all the objects at once.
    #
    # In that case, batch processing methods allow you to work with the
    # records in batches, thereby greatly reducing memory consumption.
    #
    #   User.in_batches.each_record do |user|
    #     user.do_awesome_stuff
    #   end
    #
    #   User.where("age > 21").in_batches(of: 10).each_record do |user|
    #     user.party_all_night!
    #   end
    def each_record(&block : T ->)
      @relation.in_batches(of: @of, start: @start, finish: @finish, load: true,
        cursor: @cursor, order: @order, use_ranges: @use_ranges) do |relation|
        relation.each(&block)
      end
    end

    # Returns an Enumerator when no block is given
    def each_record
      records = [] of T
      each_record { |record| records << record }
      records.each
    end

    # Deletes records in batches. Returns the total number of rows affected.
    #
    #   User.in_batches.delete_all
    #
    # See QueryBuilder#delete_all for details of how each batch is deleted.
    def delete_all
      total_deleted = 0_i64
      each do |relation|
        total_deleted += relation.delete_all
      end
      total_deleted
    end

    # Updates records in batches. Returns the total number of rows affected.
    #
    #   User.in_batches.update_all(active: true)
    #
    # See QueryBuilder#update_all for details of how each batch is updated.
    def update_all(attributes : Hash(String, DB::Any))
      total_updated = 0_i64
      each do |relation|
        total_updated += relation.update_all(attributes)
      end
      total_updated
    end

    def update_all(**attributes)
      update_all(attributes.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) })
    end

    # Destroys records in batches. Returns the total number of rows affected.
    #
    #   User.where("age < 10").in_batches.destroy_all
    #
    # See QueryBuilder#destroy_all for details of how each batch is destroyed.
    def destroy_all
      total_destroyed = 0
      each do |relation|
        total_destroyed += relation.destroy_all
      end
      total_destroyed
    end

    # Yields a QueryBuilder object for each batch of records.
    #
    #   User.in_batches.each do |relation|
    #     relation.update_all(awesome: true)
    #   end
    def each(&block : QueryBuilder(T) ->)
      @relation.in_batches(of: @of, start: @start, finish: @finish, load: false,
        cursor: @cursor, order: @order, use_ranges: @use_ranges, &block)
    end

    # Returns an Enumerator when no block is given
    def each
      relations = [] of QueryBuilder(T)
      each { |relation| relations << relation }
      relations.each
    end

    # Enumerate over each batch relation with index
    def each_with_index(&block : QueryBuilder(T), Int32 ->)
      index = 0
      each do |relation|
        yield relation, index
        index += 1
      end
    end

    # Count total records across all batches
    def count
      total_count = 0_i64
      each do |relation|
        total_count += relation.count.as(Int64)
      end
      total_count
    end

    # Check if any batches exist
    def any?
      @relation.any?
    end

    # Check if all batches are empty
    def empty?
      !@relation.any?
    end

    # Pluck values from all batches
    def pluck(column : String)
      results = [] of DB::Any
      each do |relation|
        results.concat(relation.pluck(column))
      end
      results
    end

    def pluck(*columns : String)
      results = [] of Array(DB::Any)
      each do |relation|
        results.concat(relation.pluck(*columns))
      end
      results
    end

    # Sum values across all batches
    def sum(column : String)
      total = 0
      each do |relation|
        batch_sum = relation.sum(column)
        case batch_sum
        when Int32, Int64, Float32, Float64
          total += batch_sum
        end
      end
      total
    end

    # Get average across all batches (weighted by batch size)
    def average(column : String)
      total_sum = 0.0
      total_count = 0_i64

      each do |relation|
        batch_records = relation.to_a
        next if batch_records.empty?

        batch_records.each do |record|
          value = record.get_attribute(column)
          case value
          when Int32, Int64, Float32, Float64
            total_sum += value.to_f
            total_count += 1
          end
        end
      end

      total_count > 0 ? total_sum / total_count : nil
    end

    # Find minimum value across all batches
    def minimum(column : String)
      min_value = nil
      each do |relation|
        batch_min = relation.minimum(column)
        if batch_min
          min_value = batch_min if min_value.nil? || batch_min < min_value
        end
      end
      min_value
    end

    # Find maximum value across all batches
    def maximum(column : String)
      max_value = nil
      each do |relation|
        batch_max = relation.maximum(column)
        if batch_max
          max_value = batch_max if max_value.nil? || batch_max > max_value
        end
      end
      max_value
    end
  end
end
