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
    @has_joins = false

    def initialize(@model_class : T.class)
    end

    # Select specific columns
    def select(*columns : String)
      # Quote column references that look like table.column
      quoted_columns = columns.map do |col|
        if col.includes?(".")
          parts = col.split(".")
          if parts.size == 2
            "\"#{parts[0]}\".\"#{parts[1]}\""
          else
            col # Complex expression, leave as-is
          end
        elsif col.includes?(" ") || col.includes?("(") || col.includes?("*")
          # SQL expression, function call, or wildcard - don't quote
          col
        else
          "\"#{col}\"" # Simple column name, quote it
        end
      end
      @select_clause = quoted_columns.join(", ")
      self
    end

    def select(columns : Array(String))
      # Quote column references that look like table.column
      quoted_columns = columns.map do |col|
        if col.includes?(".")
          parts = col.split(".")
          if parts.size == 2
            "\"#{parts[0]}\".\"#{parts[1]}\""
          else
            col # Complex expression, leave as-is
          end
        elsif col.includes?(" ") || col.includes?("(") || col.includes?("*")
          # SQL expression, function call, or wildcard - don't quote
          col
        else
          "\"#{col}\"" # Simple column name, quote it
        end
      end
      @select_clause = quoted_columns.join(", ")
      self
    end

    # Enhanced where method with multiple signatures
    def where(conditions : Hash(String, DB::Any))
      conditions.each do |column, value|
        if value.nil?
          # Handle NULL checks
          @where_conditions << "\"#{column}\" IS NULL"
        elsif value.is_a?(Array)
          # Handle IN clauses
          placeholders = (["?"] * value.size).join(", ")
          @where_conditions << "\"#{column}\" IN (#{placeholders})"
          @where_params.concat(value)
        else
          @where_conditions << "\"#{column}\" = ?"
          @where_params << value
        end
      end
      self
    end

    def where(**conditions)
      where(conditions.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) })
    end

    def where(condition : String, *params : DB::Any)
      @where_conditions << condition
      @where_params.concat(params.to_a)
      self
    end

    # Enhanced where with operator support for DB::Any
    def where(column_with_operator : String, value : DB::Any)
      if column_with_operator.includes?("?") || column_with_operator.includes?(" ")
        # This is a SQL expression with placeholders or operators, don't quote
        if column_with_operator.includes?("?")
          @where_conditions << column_with_operator
        else
          @where_conditions << "#{column_with_operator} ?"
        end
        @where_params << value
      else
        # Check if it's a table.column reference
        quoted_column = if column_with_operator.includes?(".")
          parts = column_with_operator.split(".")
          if parts.size == 2
            "\"#{parts[0]}\".\"#{parts[1]}\""
          else
            column_with_operator # Complex expression, leave as-is
          end
        else
          "\"#{column_with_operator}\"" # Simple column name, quote it
        end

        if value.nil?
          @where_conditions << "#{quoted_column} IS NULL"
        else
          @where_conditions << "#{quoted_column} = ?"
          @where_params << value
        end
      end
      self
    end

    def where_not(conditions : Hash(String, DB::Any))
      conditions.each do |column, value|
        # Check if it's a table.column reference
        quoted_column = if column.includes?(".")
          parts = column.split(".")
          if parts.size == 2
            "\"#{parts[0]}\".\"#{parts[1]}\""
          else
            column # Complex expression, leave as-is
          end
        else
          "\"#{column}\"" # Simple column name, quote it
        end

        if value.nil?
          @where_conditions << "#{quoted_column} IS NOT NULL"
        elsif value.is_a?(Array)
          placeholders = (["?"] * value.size).join(", ")
          @where_conditions << "#{quoted_column} NOT IN (#{placeholders})"
          @where_params.concat(value)
        else
          @where_conditions << "#{quoted_column} != ?"
          @where_params << value
        end
      end
      self
    end

    def where_not(**conditions)
      where_not(conditions.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) })
    end

    def where_not(column : String, values : Array(DB::Any))
      # Check if it's a table.column reference
      quoted_column = if column.includes?(".")
        parts = column.split(".")
        if parts.size == 2
          "\"#{parts[0]}\".\"#{parts[1]}\""
        else
          column # Complex expression, leave as-is
        end
      else
        "\"#{column}\"" # Simple column name, quote it
      end

      placeholders = (["?"] * values.size).join(", ")
      @where_conditions << "#{quoted_column} NOT IN (#{placeholders})"
      @where_params.concat(values)
      self
    end

    # Macro to generate where method overloads for different parameter types
    macro generate_where_overloads
      {% for type in [Int32, Int64, String, Float32, Float64, Bool, Time] %}
        # Single parameter overload for {{type}}
        def where(condition : String, param : {{type}})
          @where_conditions << condition
          @where_params << param.as(DB::Any)
          self
        end

        # Variadic parameters overload for {{type}}
        def where(condition : String, *params : {{type}})
          @where_conditions << condition
          params.each { |param| @where_params << param.as(DB::Any) }
          self
        end

        # Column with operator overload for {{type}}
        def where(column_with_operator : String, value : {{type}})
          if column_with_operator.includes?("?") || column_with_operator.includes?(" ")
            # This is a SQL expression, don't quote
            if column_with_operator.includes?("?")
              @where_conditions << column_with_operator
            else
              @where_conditions << "#{column_with_operator} ?"
            end
            @where_params << value.as(DB::Any)
          else
            # Check if it's a table.column reference
            quoted_column = if column_with_operator.includes?(".")
              parts = column_with_operator.split(".")
              if parts.size == 2
                "\"#{parts[0]}\".\"#{parts[1]}\""
              else
                column_with_operator # Complex expression, leave as-is
              end
            else
              "\"#{column_with_operator}\"" # Simple column name, quote it
            end

            @where_conditions << "#{quoted_column} = ?"
            @where_params << value.as(DB::Any)
          end
          self
        end

        # Array overload for {{type}}
        def where(column : String, values : Array({{type}}))
          where(column, values.map(&.as(DB::Any)))
        end

        # where_not array overload for {{type}}
        def where_not(column : String, values : Array({{type}}))
          where_not(column, values.map(&.as(DB::Any)))
        end
      {% end %}

      # Range overloads for numeric and comparable types
      {% for type in [Int32, Int64, Float32, Float64, Time, String] %}
        def where(column : String, range : Range({{type}}, {{type}}))
          # Check if it's a table.column reference
          quoted_column = if column.includes?(".")
            parts = column.split(".")
            if parts.size == 2
              "\"#{parts[0]}\".\"#{parts[1]}\""
            else
              column # Complex expression, leave as-is
            end
          else
            "\"#{column}\"" # Simple column name, quote it
          end

          if range.exclusive?
            @where_conditions << "#{quoted_column} >= ? AND #{quoted_column} < ?"
          else
            @where_conditions << "#{quoted_column} BETWEEN ? AND ?"
          end
          @where_params << range.begin.as(DB::Any) << range.end.as(DB::Any)
          self
        end
      {% end %}
    end

    # Generate all the where method overloads
    generate_where_overloads

    # Joins with automatic condition generation based on associations
    def join(association_name : String)
      @has_joins = true
      add_association_join("JOIN", association_name)
    end

    def inner_join(association_name : String)
      @has_joins = true
      add_association_join("INNER JOIN", association_name)
    end

    def left_join(association_name : String)
      @has_joins = true
      add_association_join("LEFT JOIN", association_name)
    end

    def right_join(association_name : String)
      @has_joins = true
      add_association_join("RIGHT JOIN", association_name)
    end

    # Manual joins (keep existing functionality)
    def join(table : String, on : String)
      @has_joins = true
      @joins << "JOIN #{table} ON #{on}"
      self
    end

    def left_join(table : String, on : String)
      @has_joins = true
      @joins << "LEFT JOIN #{table} ON #{on}"
      self
    end

    def right_join(table : String, on : String)
      @has_joins = true
      @joins << "RIGHT JOIN #{table} ON #{on}"
      self
    end

    def inner_join(table : String, on : String)
      @has_joins = true
      @joins << "INNER JOIN #{table} ON #{on}"
      self
    end

    private def add_association_join(join_type : String, association_name : String)
      # Get association metadata
      associations = @model_class.associations
      association = associations.find { |a| a.name == association_name }

      unless association
        raise "Association '#{association_name}' not found for #{@model_class.name}"
      end

      # Generate table names
      current_table = @model_class.table_name

      # Generate associated table name from class name using Wordsmith
      # Remove quotes from class name if present
      clean_class_name = association.class_name.gsub("\"", "")
      associated_table = Wordsmith::Inflector.tableize(clean_class_name)

      # Generate join condition based on association type
      case association.type
      when .belongs_to?
        # For belongs_to: current_table.foreign_key = associated_table.primary_key
        on_condition = "\"#{current_table}\".\"#{association.foreign_key}\" = \"#{associated_table}\".\"#{association.primary_key}\""
      when .has_many?, .has_one?
        # For has_many/has_one: current_table.primary_key = associated_table.foreign_key
        on_condition = "\"#{current_table}\".\"#{association.primary_key}\" = \"#{associated_table}\".\"#{association.foreign_key}\""
      else
        raise "Unknown association type: #{association.type}"
      end

      @joins << "#{join_type} \"#{associated_table}\" ON #{on_condition}"
      self
    end

    # Get all column names for the main table with table prefix
    private def get_prefixed_columns
      table_name = @model_class.table_name
      table_name_clean = table_name.gsub("\"", "")

      # Get columns dynamically from the model class
      columns = @model_class.column_names

      # Fallback to common columns if no columns are defined
      if columns.empty?
        columns = ["id", "created_at", "updated_at"]
      end

      # Generate properly quoted column names with correct alias format
      # Always quote the table name and column names for consistency
      quoted_table_name = table_name_clean.starts_with?("\"") ? table_name : "\"#{table_name_clean}\""
      columns.map { |col| "#{quoted_table_name}.\"#{col}\" AS #{table_name_clean}_#{col}" }.join(", ")
    end

    # Ordering
    def order(column : String, direction : String = "ASC")
      quoted_column = if column.includes?(".")
        parts = column.split(".")
        if parts.size == 2
          "\"#{parts[0]}\".\"#{parts[1]}\""
        else
          column # Complex expression, leave as-is
        end
      elsif column.includes?(" ") || column.includes?("(")
        # SQL expression or function call, don't quote
        column
      else
        "\"#{column}\"" # Simple column name, quote it
      end
      @order_clauses << "#{quoted_column} #{direction.upcase}"
      self
    end

    def order(**columns)
      columns.each do |column, direction|
        order(column.to_s, direction.to_s)
      end
      self
    end

    def order_by(column : String, direction : String = "ASC")
      order(column, direction)
    end

    # Grouping
    def group(*columns : String)
      quoted_columns = columns.map do |col|
        if col.includes?(".")
          parts = col.split(".")
          if parts.size == 2
            "\"#{parts[0]}\".\"#{parts[1]}\""
          else
            col # Complex expression, leave as-is
          end
        elsif col.includes?(" ") || col.includes?("(")
          # SQL expression or function call, don't quote
          col
        else
          "\"#{col}\"" # Simple column name, quote it
        end
      end
      @group_clause = quoted_columns.join(", ")
      self
    end

    def group(columns : Array(String))
      quoted_columns = columns.map do |col|
        if col.includes?(".")
          parts = col.split(".")
          if parts.size == 2
            "\"#{parts[0]}\".\"#{parts[1]}\""
          else
            col # Complex expression, leave as-is
          end
        elsif col.includes?(" ") || col.includes?("(")
          # SQL expression or function call, don't quote
          col
        else
          "\"#{col}\"" # Simple column name, quote it
        end
      end
      @group_clause = quoted_columns.join(", ")
      self
    end

    # Having
    def having(condition : String, *params : DB::Any)
      @having_clause = condition
      @having_params.concat(params.to_a)
      self
    end

    # Having with column and value (similar to where)
    def having(column : String, value : DB::Any)
      # If the column contains a placeholder (?), treat it as a raw SQL condition
      if column.includes?("?")
        @having_clause = column
        @having_params << value
        return self
      end

      # Check if it's a table.column reference
      quoted_column = if column.includes?(".")
        parts = column.split(".")
        if parts.size == 2
          "\"#{parts[0]}\".\"#{parts[1]}\""
        else
          column # Complex expression, leave as-is
        end
      elsif column.includes?(" ") || column.includes?("(")
        # SQL expression or function call, don't quote
        column
      else
        "\"#{column}\"" # Simple column name, quote it
      end

      if value.nil?
        @having_clause = "#{quoted_column} IS NULL"
      else
        @having_clause = "#{quoted_column} = ?"
        @having_params << value
      end
      self
    end

    # Limit and Offset
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

    # Execution methods
    def to_sql
      sql_parts = [] of String

      # SELECT clause - use prefixed columns when joins are present
      if @has_joins && @select_clause.nil?
        # When joins are present and no explicit select, use prefixed columns for main table only
        select_part = get_prefixed_columns
      else
        select_part = @select_clause || "*"
      end
      sql_parts << "SELECT #{select_part}"

      # FROM clause
      sql_parts << "FROM \"#{@model_class.table_name}\""

      # JOIN clauses
      unless @joins.empty?
        sql_parts.concat(@joins)
      end

      # WHERE clause
      unless @where_conditions.empty?
        sql_parts << "WHERE #{@where_conditions.join(" AND ")}"
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

    def to_a
      results = [] of T

      # Combine parameters in the correct order: WHERE params first, then HAVING params
      all_params = @where_params + @having_params

      @model_class.connection.query(to_sql, args: all_params) do |rs|
        rs.each do
          instance = @model_class.new
          if @has_joins
            instance.load_from_prefixed_result_set(rs)
          else
            instance.load_from_result_set(rs)
          end
          results << instance
        end
      end

      results
    end

    def all
      to_a
    end

    def first
      limit(1)
      results = to_a
      results.first?
    end

    def first!
      first || raise "No records found"
    end

    def last
      # Reverse the order and get first
      reversed_order = @order_clauses.map do |clause|
        if clause.ends_with?(" ASC")
          clause.gsub(" ASC", " DESC")
        elsif clause.ends_with?(" DESC")
          clause.gsub(" DESC", " ASC")
        else
          "#{clause} DESC"
        end
      end

      # If no order specified, order by primary key DESC
      if reversed_order.empty?
        reversed_order = ["#{@model_class.primary_key} DESC"]
      end

      @order_clauses = reversed_order
      limit(1)
      results = to_a
      results.first?
    end

    def count
      original_select = @select_clause
      @select_clause = "COUNT(*)"

      # Combine parameters in the correct order: WHERE params first, then HAVING params
      all_params = @where_params + @having_params
      result = @model_class.connection.scalar(to_sql, args: all_params).as(Int64)

      @select_clause = original_select
      result
    end

    def exists?
      count > 0
    end

    def empty?
      count == 0
    end

    def any?
      exists?
    end

    def pluck(column : String)
      @select_clause = column
      results = [] of DB::Any

      # Combine parameters in the correct order: WHERE params first, then HAVING params
      all_params = @where_params + @having_params
      @model_class.connection.query(to_sql, args: all_params) do |rs|
        rs.each do
          results << rs.read
        end
      end

      results
    end

    def pluck(*columns : String)
      @select_clause = columns.join(", ")
      results = [] of Array(DB::Any)

      # Combine parameters in the correct order: WHERE params first, then HAVING params
      all_params = @where_params + @having_params
      @model_class.connection.query(to_sql, args: all_params) do |rs|
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

    # Aggregation methods
    def sum(column : String)
      @select_clause = "SUM(\"#{column}\")"
      all_params = @where_params + @having_params
      @model_class.connection.scalar(to_sql, args: all_params)
    end

    def average(column : String)
      @select_clause = "AVG(\"#{column}\")"
      all_params = @where_params + @having_params
      @model_class.connection.scalar(to_sql, args: all_params)
    end

    def minimum(column : String)
      @select_clause = "MIN(\"#{column}\")"
      all_params = @where_params + @having_params
      @model_class.connection.scalar(to_sql, args: all_params)
    end

    def maximum(column : String)
      @select_clause = "MAX(\"#{column}\")"
      all_params = @where_params + @having_params
      @model_class.connection.scalar(to_sql, args: all_params)
    end

    # Update and delete operations
    def update_all(attributes : Hash(String, DB::Any))
      return 0 if attributes.empty?

      set_clause = attributes.keys.map { |key| "\"#{key}\" = ?" }.join(", ")
      # Combine parameters: attributes values first, then WHERE params, then HAVING params
      update_params = attributes.values.to_a + @where_params + @having_params

      sql = "UPDATE \"#{@model_class.table_name}\" SET #{set_clause}"
      unless @where_conditions.empty?
        sql += " WHERE #{@where_conditions.join(" AND ")}"
      end

      result = @model_class.connection.exec(sql, args: update_params)
      result.rows_affected
    end

    def update_all(**attributes)
      update_all(attributes.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) })
    end

    def delete_all
      sql = "DELETE FROM \"#{@model_class.table_name}\""
      unless @where_conditions.empty?
        sql += " WHERE #{@where_conditions.join(" AND ")}"
      end

      # Combine parameters in the correct order: WHERE params first, then HAVING params
      all_params = @where_params + @having_params
      result = @model_class.connection.exec(sql, args: all_params)
      result.rows_affected
    end

    def destroy_all
      records = to_a
      records.each(&.destroy)
      records.size
    end

    # Chainable methods return self for method chaining
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

    # Method missing to delegate array-like methods to to_a automatically
    # This allows calling .size, .empty?, .any?, etc. directly on QueryBuilder
    macro method_missing(call)
      # List of methods that should be delegated to the array result
      # Only including methods that are NOT already implemented in QueryBuilder
      {% array_methods = %w[
           size all? none?
           each_with_index each_with_object
           map_with_index
           find_index includes? index
           sample shuffle reverse
           sort sort_by min max min_by max_by
           join partition group_by
           zip flatten compact uniq
           take drop take_while drop_while
           [] []? at at? fetch
         ] %}

      {% method_name = call.name.stringify %}

      {% if array_methods.includes?(method_name) %}
        # Delegate to the array result
        to_a.{{call}}
      {% else %}
        # Call the original method_missing
        super
      {% end %}
    end

    # Handle IN clauses with arrays
    def where(column : String, values : Array(DB::Any))
      # Check if it's a table.column reference
      quoted_column = if column.includes?(".")
        parts = column.split(".")
        if parts.size == 2
          "\"#{parts[0]}\".\"#{parts[1]}\""
        else
          column # Complex expression, leave as-is
        end
      else
        "\"#{column}\"" # Simple column name, quote it
      end

      placeholders = (["?"] * values.size).join(", ")
      @where_conditions << "#{quoted_column} IN (#{placeholders})"
      @where_params.concat(values)
      self
    end
  end
end
