require "set"

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
    @model_class : T.class
    @select_clause : String?
    @where_conditions = [] of String
    @where_params = [] of DB::Any
    @joins = [] of String
    @order_clauses = [] of String
    @group_clause : String?
    @having_clause : String?
    @having_params = [] of DB::Any
    @limit_value : Int32?
    @offset_value : Int32?
    @distinct = false
    @has_joins = false
    @includes = [] of String
    @preloads = [] of String
    @eager_loads = [] of String

    def initialize(@model_class : T.class)
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
        .map(&.strip)                    # Remove whitespace
        .reject(&.empty?)                # Remove empty strings
        .reject { |col| col.blank? }     # Remove blank strings

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
      where(conditions.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) })
    end

    # Named placeholder conditions - must come before variadic method for proper resolution
    def where(condition : String, **named_params)
      where(condition, named_params.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) })
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
      not(conditions.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) })
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

    def join(association_name : String | Symbol)
      @has_joins = true
      add_smart_association_join(association_name.to_s)
    end

    def join(table : String, on : String)
      add_join("JOIN", table, on)
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

    def having(condition : String, *params : DB::Any)
      @having_clause = condition
      @having_params.concat(params.to_a)
      self
    end

    def having(column : String, value : DB::Any)
      if column.includes?("?")
        @having_clause = column
        @having_params << value
        return self
      end

      if value.nil?
        @having_clause = "#{column} IS NULL"
      else
        @having_clause = "#{column} = ?"
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
      if @group_clause
        sql_parts << "GROUP BY #{@group_clause}"
      end

      # HAVING clause
      if @having_clause
        sql_parts << "HAVING #{@having_clause}"
      end

      # ORDER BY clause
      unless @order_clauses.empty?
        sql_parts << "ORDER BY #{@order_clauses.join(", ")}"
      end

      # LIMIT clause
      if @limit_value
        sql_parts << "LIMIT #{@limit_value}"
      end

      # OFFSET clause
      if @offset_value
        sql_parts << "OFFSET #{@offset_value}"
      end

      sql_parts.join(" ")
    end

    # Expose query parameters for testing and debugging
    def params
      combined_params
    end

    def to_a
      results = [] of T

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

    def count : Int64 | Hash(String, Int64)
      # If there's a GROUP BY clause, return a hash of grouped counts
      if @group_clause
        grouped_count
      elsif @limit_value
        # If there's a LIMIT clause, we need to count the actual records that would be returned
        to_a.size.to_i64
      else
        original_select = @select_clause
        @select_clause = "COUNT(*)"
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

    def exists?
      result = count
      case result
      when Int64
        result > 0
      when Hash(String, Int64)
        result.any? { |_, v| v > 0 }
      else
        false
      end
    end

    def empty?
      result = count
      case result
      when Int64
        result == 0
      when Hash(String, Int64)
        result.all? { |_, v| v == 0 }
      else
        true
      end
    end

    def any?
      exists?
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
    # AGGREGATION METHODS
    # ========================================

    def sum(column : String)
      aggregate("SUM", column)
    end

    def average(column : String)
      aggregate("AVG", column)
    end

    def minimum(column : String)
      aggregate("MIN", column)
    end

    def maximum(column : String)
      aggregate("MAX", column)
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
        result = Takarik::Data.exec_with_logging(@model_class.connection, sql, ids, @model_class.name, "Destroy")
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

        result = Takarik::Data.exec_with_logging(@model_class.connection, sql, combined_params, @model_class.name, "Destroy")
        result.rows_affected
      end
    end

    def destroy_all
      records = to_a
      records.each(&.destroy)
      records.size
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
                  error_on_ignore : Bool? = nil, cursor : String | Array(String)? = nil,
                  order : Symbol | Array(Symbol) = :asc, &block : T ->)
      raise Exception.new("Batch size must be positive") if batch_size <= 0

      # Check for existing ordering
      unless @order_clauses.empty?
        if error_on_ignore
          raise ArgumentError.new("Cannot use find_each with existing order. Use unscoped to remove ordering.")
        else
          # In Rails, this would log a warning. For now, we'll just ignore the order.
        end
      end

      # Determine cursor columns
      cursor_columns = if cursor
                         cursor.is_a?(Array) ? cursor : [cursor.as(String)]
                       else
                         # Use primary key
                         primary_key = @model_class.primary_key
                         if primary_key.includes?(",")
                           primary_key.split(",").map(&.strip)
                         else
                           [primary_key]
                         end
                       end

      # Validate and normalize order parameter
      order_array = case order
                    when Symbol
                      # Single order applies to all cursor columns
                      cursor_columns.map { order.as(Symbol) }
                    when Array
                      if order.size != cursor_columns.size
                        raise ArgumentError.new("Order array size (#{order.size}) must match cursor columns size (#{cursor_columns.size})")
                      end
                      order.as(Array(Symbol))
                    else
                      raise ArgumentError.new("Order must be Symbol or Array(Symbol), got #{order.class}")
                    end

      # Validate each order direction
      order_array.each do |dir|
        unless [:asc, :desc].includes?(dir)
          raise ArgumentError.new("Order must be :asc or :desc, got #{dir}")
        end
      end

      # For simplicity in this implementation, use the first cursor column for batching
      batch_key = cursor_columns.first
      batch_order = order_array.first

      # Use regular dup (this preserves WHERE conditions but allows us to add our own ordering)
      base_query = dup

      # Apply start/finish filters to base query
      if start
        if batch_order == :asc
          base_query = base_query.where("#{batch_key} >=", start)
        else
          base_query = base_query.where("#{batch_key} <=", start)
        end
      end

      if finish
        if batch_order == :asc
          base_query = base_query.where("#{batch_key} <=", finish)
        else
          base_query = base_query.where("#{batch_key} >=", finish)
        end
      end

      # Set up ordering for batching using cursor columns and their orders
      cursor_columns.each_with_index do |col, i|
        base_query = base_query.order(col, order_array[i].to_s.upcase)
      end

      # Start batching
      last_id = start

      loop do
        # Build query for this batch
        current_query = base_query.dup

        # Add condition to get next batch
        if last_id && last_id != start
          if batch_order == :asc
            current_query = current_query.where("#{batch_key} >", last_id)
          else
            current_query = current_query.where("#{batch_key} <", last_id)
          end
        end

        # Get batch of records
        batch = current_query.limit(batch_size).to_a

        # Break if no more records
        break if batch.empty?

        # Yield each record to the block
        batch.each do |record|
          yield record
        end

        # Update last_id for next batch
        # Break if we got fewer records than batch_size (last batch)
        if batch.size < batch_size
          break
        else
          last_id = batch.last.get_attribute(batch_key)
        end
      end

      self
    end

    # Returns an Enumerator when no block is given
    def find_each(start : DB::Any? = nil, finish : DB::Any? = nil, batch_size : Int32 = 1000,
                  error_on_ignore : Bool? = nil, cursor : String | Array(String)? = nil,
                  order : Symbol | Array(Symbol) = :asc)
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
                        error_on_ignore : Bool? = nil, cursor : String | Array(String)? = nil,
                        order : Symbol | Array(Symbol) = :asc, &block : Array(T) ->)
      raise Exception.new("Batch size must be positive") if batch_size <= 0

      # Use the same logic as find_each but collect records into batches
      current_batch = [] of T

      find_each(start: start, finish: finish, batch_size: batch_size, error_on_ignore: error_on_ignore, cursor: cursor, order: order) do |record|
        current_batch << record
        if current_batch.size >= batch_size
          yield current_batch
          current_batch.clear
        end
      end

      # Yield any remaining records in the final batch
      unless current_batch.empty?
        yield current_batch
      end

      self
    end

    # Returns an Enumerator when no block is given
    def find_in_batches(start : DB::Any? = nil, finish : DB::Any? = nil, batch_size : Int32 = 1000,
                        error_on_ignore : Bool? = nil, cursor : String | Array(String)? = nil,
                        order : Symbol | Array(Symbol) = :asc)
      batches = [] of Array(T)
      find_in_batches(start: start, finish: finish, batch_size: batch_size,
        error_on_ignore: error_on_ignore, cursor: cursor, order: order) do |batch|
        batches << batch
      end
      batches.each
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

      # Choose join type based on association configuration
      join_type = case association.type
                  when .belongs_to?
                    # For belongs_to associations, use the optional parameter to determine join type
                    association.optional ? "LEFT JOIN" : "INNER JOIN"
                  when .has_many?, .has_one?
                    # For has_many/has_one, typically use LEFT JOIN since the parent might not have children
                    "LEFT JOIN"
                  else
                    raise "Unknown association type: #{association.type}"
                  end

      add_association_join(join_type, association_name)
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

      {% method_name = call.name.stringify %}

      {% if array_methods.includes?(method_name) %}
        to_a.{{call}}
      {% else %}
        # Try to delegate to model class scope
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
      @having_clause = other_query.@having_clause if other_query.@having_clause
      @having_params.concat(other_query.@having_params)
      @limit_value = other_query.@limit_value if other_query.@limit_value
      @offset_value = other_query.@offset_value if other_query.@offset_value

      # Note: We don't merge select_clause, joins, or distinct to avoid conflicts
      # Those are typically set at the beginning of a query chain

      self
    end
  end
end
