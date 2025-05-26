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
    @limit_value : Int32?
    @offset_value : Int32?

    def initialize(@model_class : T.class)
    end

    # Select specific columns
    def select(*columns : String)
      @select_clause = columns.join(", ")
      self
    end

    def select(columns : Array(String))
      @select_clause = columns.join(", ")
      self
    end

    # Where conditions
    def where(conditions : Hash(String, DB::Any))
      conditions.each do |column, value|
        @where_conditions << "#{column} = ?"
        @where_params << value
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

    # Advanced where conditions
    def where_not(conditions : Hash(String, DB::Any))
      conditions.each do |column, value|
        @where_conditions << "#{column} != ?"
        @where_params << value
      end
      self
    end

    def where_not(**conditions)
      where_not(conditions.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) })
    end

    def where_in(column : String, values : Array(DB::Any))
      placeholders = (["?"] * values.size).join(", ")
      @where_conditions << "#{column} IN (#{placeholders})"
      @where_params.concat(values)
      self
    end

    def where_not_in(column : String, values : Array(DB::Any))
      placeholders = (["?"] * values.size).join(", ")
      @where_conditions << "#{column} NOT IN (#{placeholders})"
      @where_params.concat(values)
      self
    end

    def where_like(column : String, pattern : String)
      @where_conditions << "#{column} LIKE ?"
      @where_params << pattern.as(DB::Any)
      self
    end

    def where_ilike(column : String, pattern : String)
      @where_conditions << "#{column} ILIKE ?"
      @where_params << pattern.as(DB::Any)
      self
    end

    def where_between(column : String, start_value : DB::Any, end_value : DB::Any)
      @where_conditions << "#{column} BETWEEN ? AND ?"
      @where_params << start_value << end_value
      self
    end

    def where_null(column : String)
      @where_conditions << "#{column} IS NULL"
      self
    end

    def where_not_null(column : String)
      @where_conditions << "#{column} IS NOT NULL"
      self
    end

    def where_gt(column : String, value : DB::Any)
      @where_conditions << "#{column} > ?"
      @where_params << value
      self
    end

    def where_gte(column : String, value : DB::Any)
      @where_conditions << "#{column} >= ?"
      @where_params << value
      self
    end

    def where_lt(column : String, value : DB::Any)
      @where_conditions << "#{column} < ?"
      @where_params << value
      self
    end

    def where_lte(column : String, value : DB::Any)
      @where_conditions << "#{column} <= ?"
      @where_params << value
      self
    end

    # Joins
    def join(table : String, on : String)
      @joins << "JOIN #{table} ON #{on}"
      self
    end

    def left_join(table : String, on : String)
      @joins << "LEFT JOIN #{table} ON #{on}"
      self
    end

    def right_join(table : String, on : String)
      @joins << "RIGHT JOIN #{table} ON #{on}"
      self
    end

    def inner_join(table : String, on : String)
      @joins << "INNER JOIN #{table} ON #{on}"
      self
    end

    # Ordering
    def order(column : String, direction : String = "ASC")
      @order_clauses << "#{column} #{direction.upcase}"
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
      @group_clause = columns.join(", ")
      self
    end

    def group(columns : Array(String))
      @group_clause = columns.join(", ")
      self
    end

    # Having
    def having(condition : String, *params : DB::Any)
      @having_clause = condition
      @where_params.concat(params.to_a)
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

      # SELECT clause
      select_part = @select_clause || "*"
      sql_parts << "SELECT #{select_part}"

      # FROM clause
      sql_parts << "FROM #{@model_class.table_name}"

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

      @model_class.connection.query(to_sql, args: @where_params) do |rs|
        rs.each do
          instance = @model_class.new
          instance.load_from_result_set(rs)
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

      result = @model_class.connection.scalar(to_sql, args: @where_params).as(Int64)

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

      @model_class.connection.query(to_sql, args: @where_params) do |rs|
        rs.each do
          results << rs.read
        end
      end

      results
    end

    def pluck(*columns : String)
      @select_clause = columns.join(", ")
      results = [] of Array(DB::Any)

      @model_class.connection.query(to_sql, args: @where_params) do |rs|
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
      @select_clause = "SUM(#{column})"
      @model_class.connection.scalar(to_sql, args: @where_params)
    end

    def average(column : String)
      @select_clause = "AVG(#{column})"
      @model_class.connection.scalar(to_sql, args: @where_params)
    end

    def minimum(column : String)
      @select_clause = "MIN(#{column})"
      @model_class.connection.scalar(to_sql, args: @where_params)
    end

    def maximum(column : String)
      @select_clause = "MAX(#{column})"
      @model_class.connection.scalar(to_sql, args: @where_params)
    end

    # Update and delete operations
    def update_all(attributes : Hash(String, DB::Any))
      return 0 if attributes.empty?

      set_clause = attributes.keys.map { |key| "#{key} = ?" }.join(", ")
      update_params = attributes.values.to_a + @where_params

      sql = "UPDATE #{@model_class.table_name} SET #{set_clause}"
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
      sql = "DELETE FROM #{@model_class.table_name}"
      unless @where_conditions.empty?
        sql += " WHERE #{@where_conditions.join(" AND ")}"
      end

      result = @model_class.connection.exec(sql, args: @where_params)
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
  end
end
