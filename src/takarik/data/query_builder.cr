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

    # ========================================
    # SELECT METHODS
    # ========================================

    def select(*columns : String)
      @select_clause = columns.join(", ")
      self
    end

    def select(columns : Array(String))
      @select_clause = columns.join(", ")
      self
    end

    # ========================================
    # WHERE METHODS
    # ========================================

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
    # ORDER METHODS
    # ========================================

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

    # ========================================
    # GROUP BY METHODS
    # ========================================

    def group(*columns : String)
      @group_clause = columns.join(", ")
      self
    end

    def group(columns : Array(String))
      @group_clause = columns.join(", ")
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
      sql_parts = [] of String

      # SELECT clause
      if @has_joins && @select_clause.nil?
        select_part = get_prefixed_columns
      else
        select_part = @select_clause || "*"
      end
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
      @model_class.connection.query(to_sql, args: combined_params) do |rs|
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

    def first
      limit(1)
      results = to_a
      results.first?
    end

    def first!
      first || raise "No records found"
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

    def count
      original_select = @select_clause
      @select_clause = "COUNT(*)"
      result = @model_class.connection.scalar(to_sql, args: combined_params).as(Int64)
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

    # ========================================
    # PLUCK METHODS
    # ========================================

    def pluck(column : String)
      @select_clause = column
      results = [] of DB::Any
      @model_class.connection.query(to_sql, args: combined_params) do |rs|
        rs.each do
          results << rs.read
        end
      end
      results
    end

    def pluck(*columns : String)
      @select_clause = columns.join(", ")
      results = [] of Array(DB::Any)
      @model_class.connection.query(to_sql, args: combined_params) do |rs|
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

      set_clause = attributes.keys.map { |key| key + " = ?" }.join(", ")
      update_params = attributes.values.to_a + combined_params

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

      result = @model_class.connection.exec(sql, args: combined_params)
      result.rows_affected
    end

    def destroy_all
      records = to_a
      records.each(&.destroy)
      records.size
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

    private def get_prefixed_columns
      table_name = @model_class.table_name
      table_name_clean = table_name.gsub("\"", "")

      columns = @model_class.column_names
      if columns.empty?
        columns = ["id", "created_at", "updated_at"]
      end

      columns.map { |col| "#{table_name_clean}.#{col} AS #{table_name_clean}_#{col}" }.join(", ")
    end

    private def aggregate(function : String, column : String)
      @select_clause = "#{function}(#{column})"
      @model_class.connection.scalar(to_sql, args: combined_params)
    end

    # ========================================
    # MACROS
    # ========================================

    macro generate_where_overloads
      {% for type in [Int32, Int64, String, Float32, Float64, Bool, Time] %}
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

        # New not method overloads
        def not(column : String, values : Array({{type}}))
          not(column, values.map(&.as(DB::Any)))
        end
      {% end %}

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
      {% end %}
    end

    generate_where_overloads

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
           take drop take_while drop_while
           [] []? at at? fetch
         ] %}

      {% method_name = call.name.stringify %}

      {% if array_methods.includes?(method_name) %}
        to_a.{{call}}
      {% else %}
        super
      {% end %}
    end
  end
end
