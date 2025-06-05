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
    @includes = [] of String

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

    def to_a
      results = [] of T
      @model_class.connection.query(to_sql, args: combined_params) do |rs|
        rs.each do
          instance = @model_class.new
          if @includes.any?
            instance.load_from_result_set_with_includes(rs, @includes)
          elsif @has_joins
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

      result = @model_class.connection.exec(sql, args: update_params)
      result.rows_affected
    end

    def update_all(**attributes)
      update_all(attributes.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) })
    end

    def delete_all
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

      result = @model_class.connection.exec(sql, args: combined_params)
      result.rows_affected
    end

    def destroy_all
      records = to_a
      records.each(&.destroy)
      records.size
    end

    # Memory-efficient method to iterate over records in batches
    def find_each(batch_size : Int32 = 1000, &block : T ->)
      raise ArgumentError.new("Batch size must be positive") if batch_size <= 0

      offset_value = 0

      loop do
        # Store original limit and offset
        original_limit = @limit_value
        original_offset = @offset_value

        # Set batch limit and offset
        @limit_value = batch_size
        @offset_value = offset_value

        batch_records = to_a

        # Restore original limit and offset
        @limit_value = original_limit
        @offset_value = original_offset

        break if batch_records.empty?

        batch_records.each(&block)

        # Break if we got fewer records than the batch size (last batch)
        break if batch_records.size < batch_size

        offset_value += batch_size
      end

      self
    end

    # Memory-efficient method to iterate over records in batches, yielding batches
    def find_in_batches(batch_size : Int32 = 1000, &block : Array(T) ->)
      raise ArgumentError.new("Batch size must be positive") if batch_size <= 0

      offset_value = 0

      loop do
        # Store original limit and offset
        original_limit = @limit_value
        original_offset = @offset_value

        # Set batch limit and offset
        @limit_value = batch_size
        @offset_value = offset_value

        batch_records = to_a

        # Restore original limit and offset
        @limit_value = original_limit
        @offset_value = original_offset

        break if batch_records.empty?

        yield batch_records

        # Break if we got fewer records than the batch size (last batch)
        break if batch_records.size < batch_size

        offset_value += batch_size
      end

      self
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
      @has_joins = true

      # Add a LEFT JOIN for the association
      add_association_join("LEFT JOIN", association_name)
    end

    private def get_prefixed_columns
      all_columns = [] of String

      # Main table columns
      table_name = @model_class.table_name
      table_name_clean = table_name.gsub("\"", "")

      columns = @model_class.column_names
      if columns.empty?
        columns = ["id", "created_at", "updated_at"]
      end

      columns.each do |col|
        all_columns << "#{table_name_clean}.#{col} AS #{table_name_clean}_#{col}"
      end

      # Include columns from associated tables when using includes
      @includes.each do |association_name|
        associations = @model_class.associations
        association = associations.find { |a| a.name == association_name }
        next unless association && association.class_type && !association.polymorphic

        associated_table = association.class_type.not_nil!.table_name
        associated_table_clean = associated_table.gsub("\"", "")

        associated_columns = association.class_type.not_nil!.column_names
        if associated_columns.empty?
          associated_columns = ["id", "created_at", "updated_at"]
        end

        associated_columns.each do |col|
          all_columns << "#{associated_table_clean}.#{col} AS #{associated_table_clean}_#{col}"
        end
      end

      all_columns.join(", ")
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
           take drop take_while drop_while
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
