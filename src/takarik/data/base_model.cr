require "db"
require "json"
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

  # Forward declaration for QueryBuilder
  class QueryBuilder(T)
  end

  # Base class for all ORM models, providing ActiveRecord-like functionality
  # but designed specifically for Crystal language features
  abstract class BaseModel
    include JSON::Serializable
    include Validations
    include Associations

    # Class variable to store column names for each model
    @@column_names = {} of String => Array(String)

    # Instance variables
    @attributes = {} of String => DB::Any
    @persisted = false
    @changed_attributes = Set(String).new

    # Class method to get column names for this model
    def self.column_names
      @@column_names[self.name]? || [] of String
    end

    # Class method to add a column name
    def self.add_column_name(name : String)
      @@column_names[self.name] ||= [] of String
      @@column_names[self.name] << name unless @@column_names[self.name].includes?(name)
    end

    # Add query builder methods to the class - make them more direct
    def self.query
      QueryBuilder(self).new(self)
    end

    # Shared macro for property definition with getter/setter
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
          @attributes.delete({{name.id.stringify}})
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
        if @{{name.id}}
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
            when 0, "0", "false", "f", false, nil
              false
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
            value.is_a?(Time) ? value : nil
          {% else %}
            value.as?({{type}})
          {% end %}
        else
          nil
        end
      end
    end

    # Shared macro for instance variable assignment in loops
    macro assign_instance_variables_from_attributes
      {% begin %}
        case column_name
        {% for ivar in @type.instance_vars %}
          {% if ivar.name.stringify != "attributes" && ivar.name.stringify != "persisted" && ivar.name.stringify != "changed_attributes" && ivar.name.stringify != "validation_errors" %}
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
                when 0, "0", "false", "f", false, nil
                  false
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
                @{{ivar.name}} = value.is_a?(Time) ? value : nil
              {% else %}
                @{{ivar.name}} = value.as?({{ivar.type}})
              {% end %}
          {% end %}
        {% end %}
        end
      {% end %}
    end

    # Helper method to set a single instance variable
    private def set_single_instance_variable(column_name : String, value : DB::Any)
      assign_instance_variables_from_attributes
    end

    # Macro to generate where method overloads for BaseModel
    macro generate_base_model_where_overloads
      {% for type in [Int32, Int64, String, Float32, Float64, Bool, Time, DB::Any] %}
        # Variadic parameters overload for {{type}}
        def self.where(condition : String, *params : {{type}})
          query.where(condition, *params)
        end

        # Array overload for {{type}}
        def self.where(column : String, values : Array({{type}}))
          query.where(column, values)
        end

        # Column with operator overload for {{type}}
        def self.where(column_with_operator : String, value : {{type}})
          query.where(column_with_operator, value)
        end

        # where_not array overload for {{type}}
        def self.where_not(column : String, values : Array({{type}}))
          query.where_not(column, values)
        end
      {% end %}

      # Range overloads
      {% for type in [Int32, Int64, Float32, Float64, Time, String] %}
        def self.where(column : String, range : Range({{type}}, {{type}}))
          query.where(column, range)
        end
      {% end %}
    end

    # Generate all the where method overloads for BaseModel
    generate_base_model_where_overloads

    # Chainable query methods - return QueryBuilder for chaining
    def self.where_not(**conditions)
      query.where_not(**conditions)
    end

    def self.select(*columns : String)
      query.select(*columns)
    end

    def self.select(columns : Array(String))
      query.select(columns)
    end

    def self.order(column : String, direction : String = "ASC")
      query.order(column, direction)
    end

    def self.order(**columns)
      query.order(**columns)
    end

    def self.limit(count : Int32)
      query.limit(count)
    end

    def self.offset(count : Int32)
      query.offset(count)
    end

    def self.joins(table : String, on : String)
      query.join(table, on)
    end

    def self.joins(association_name : String)
      query.join(association_name)
    end

    def self.inner_join(table : String, on : String)
      query.inner_join(table, on)
    end

    def self.inner_join(association_name : String)
      query.inner_join(association_name)
    end

    def self.left_join(table : String, on : String)
      query.left_join(table, on)
    end

    def self.left_join(association_name : String)
      query.left_join(association_name)
    end

    def self.right_join(table : String, on : String)
      query.right_join(table, on)
    end

    def self.right_join(association_name : String)
      query.right_join(association_name)
    end

    def self.group(*columns : String)
      query.group(*columns)
    end

    def self.having(condition : String, *params : DB::Any)
      query.having(condition, *params)
    end

    def self.page(page_number : Int32, per_page : Int32)
      query.page(page_number, per_page)
    end

    # Aggregation methods
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

    # Scopes support - now returns QueryBuilder for chaining
    macro scope(name, &block)
      def self.{{name.id}}
        {{block.body}}
      end
    end

    # Callbacks support - refactored to support multiple callbacks
    # Simple approach: use incremental counters for each callback type

    # Helper macro to generate conditional checking logic
    macro check_callback_conditions(condition_if, condition_unless)
      {% if condition_if %}
        {% if condition_if.is_a?(SymbolLiteral) %}
          condition_if_result = {{condition_if.id}}
        {% else %}
          condition_if_result = ({{condition_if}}).call
        {% end %}
        return unless condition_if_result
      {% end %}
      {% if condition_unless %}
        {% if condition_unless.is_a?(SymbolLiteral) %}
          condition_unless_result = {{condition_unless.id}}
        {% else %}
          condition_unless_result = ({{condition_unless}}).call
        {% end %}
        return if condition_unless_result
      {% end %}
    end

    # Callback registration macros - support both blocks and method names with conditional options
    macro before_save(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {%
        callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("before_save_callback_") }.size
      %}

      {% if method_name %}
        def before_save_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def before_save_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro after_save(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {%
        callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_save_callback_") }.size
      %}

      {% if method_name %}
        def after_save_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def after_save_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro before_create(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {%
        callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("before_create_callback_") }.size
      %}

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
      {%
        callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_create_callback_") }.size
      %}

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
      {%
        callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("before_update_callback_") }.size
      %}

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
      {%
        callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_update_callback_") }.size
      %}

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
      {%
        callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("before_destroy_callback_") }.size
      %}

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
      {%
        callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_destroy_callback_") }.size
      %}

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

    macro before_validation(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {%
        callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("before_validation_callback_") }.size
      %}

      {% if method_name %}
        def before_validation_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def before_validation_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro after_validation(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {%
        callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_validation_callback_") }.size
      %}

      {% if method_name %}
        def after_validation_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def after_validation_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    # Methods to execute callbacks by calling all numbered callback methods
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

    # Timestamps support
    macro timestamps
      column created_at, Time
      column updated_at, Time

      before_create do
        self.created_at = Time.utc
        self.updated_at = Time.utc
      end

      before_update do
        self.updated_at = Time.utc
      end
    end

    # Macro to define the primary key column
    macro primary_key(name = "id", type = Int64, **options)
      define_property_with_accessors({{name}}, {{type}})

      # Override the class method to return the primary key name
      def self.primary_key
        {% if name.is_a?(SymbolLiteral) %}
          {{name.id.stringify}}
        {% else %}
          {{name}}
        {% end %}
      end
    end

    # Macro to define database columns with types
    macro column(name, type, **options)
      define_property_with_accessors({{name}}, {{type}})

      # Add to JSON serialization
      def {{name.id}}_json
        @{{name.id}}.try(&.to_json) || "null"
      end
    end

    # Macro to set table name
    macro table_name(name)
      def self.table_name
        {% if name.is_a?(SymbolLiteral) %}
          {{name.id.stringify}}
        {% else %}
          {{name}}
        {% end %}
      end
    end

    # Class methods for database operations
    def self.connection
      Takarik::Data.connection
    end

    def self.table_name
      self.name.split("::").last.underscore.pluralize
    end

    # Query methods
    def self.all
      query = "SELECT * FROM #{table_name}"
      results = [] of self

      connection.query(query) do |rs|
        rs.each do
          instance = new
          instance.load_from_result_set(rs)
          results << instance
        end
      end

      results
    end

    def self.find(id)
      query = "SELECT * FROM #{table_name} WHERE #{primary_key} = ?"

      connection.query_one?(query, id) do |rs|
        instance = new
        instance.load_from_result_set(rs)
        instance
      end
    end

    def self.find!(id)
      find(id) || raise "Record not found with #{primary_key}=#{id}"
    end

    def self.where(conditions : Hash(String, DB::Any))
      QueryBuilder(self).new(self).where(conditions)
    end

    def self.where(**conditions)
      QueryBuilder(self).new(self).where(**conditions)
    end

    def self.first
      query = "SELECT * FROM #{table_name} LIMIT 1"

      connection.query_one?(query) do |rs|
        instance = new
        instance.load_from_result_set(rs)
        instance
      end
    end

    def self.first!
      first || raise "No records found"
    end

    def self.last
      query = "SELECT * FROM #{table_name} ORDER BY #{primary_key} DESC LIMIT 1"

      connection.query_one?(query) do |rs|
        instance = new
        instance.load_from_result_set(rs)
        instance
      end
    end

    def self.count
      query = "SELECT COUNT(*) FROM #{table_name}"
      connection.scalar(query).as(Int64)
    end

    def self.create(attributes : Hash(String, DB::Any))
      instance = new
      attributes.each do |key, value|
        instance.set_attribute(key, value)
      end
      instance.save
      instance
    end

    def self.create(**attributes)
      create(attributes.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) })
    end

    # Instance methods
    def initialize
      @attributes = {} of String => DB::Any
      @persisted = false
      @changed_attributes = Set(String).new
    end

    def persisted?
      @persisted
    end

    def new_record?
      !@persisted
    end

    def changed?
      !@changed_attributes.empty?
    end

    def changed_attributes
      @changed_attributes.to_a
    end

    def set_attribute(name : String, value : DB::Any)
      @attributes[name] = value
      @changed_attributes << name

      # Also set the instance variable if it exists
      set_single_instance_variable(name, value)
    end

    def get_attribute(name : String)
      @attributes[name]?
    end

    def save
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
      attributes.each do |key, value|
        set_attribute(key, value)
      end
      save
    end

    def update(**attributes)
      update(attributes.to_h.transform_keys(&.to_s).transform_values { |v| v.as(DB::Any) })
    end

    def destroy
      return false if new_record?

      # Run before_destroy callbacks
      run_before_destroy_callbacks

      query = "DELETE FROM #{self.class.table_name} WHERE #{self.class.primary_key} = ?"
      id_value = get_attribute(self.class.primary_key)

      result = self.class.connection.exec(query, id_value)
      success = result.rows_affected > 0

      if success
        @persisted = false
        # Run after_destroy callbacks
        run_after_destroy_callbacks
      end

      success
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

    def to_h
      @attributes.dup
    end

    def ==(other : BaseModel)
      return false unless other.class == self.class
      return false if new_record? || other.new_record?

      self_id = get_attribute(self.class.primary_key)
      other_id = other.get_attribute(self.class.primary_key)
      self_id == other_id
    end

    # Helper method to sync instance variables from attributes
    private def sync_instance_variables_from_attributes
      @attributes.each do |column_name, value|
        assign_instance_variables_from_attributes
      end
    end

    # Protected methods for internal use
    protected def load_from_result_set(rs : DB::ResultSet)
      rs.column_names.each_with_index do |column_name, index|
        value = rs.read
        @attributes[column_name] = value
      end

      sync_instance_variables_from_attributes
      @persisted = true
      @changed_attributes.clear
    end

    # Load from result set with prefixed column names (for join queries)
    protected def load_from_prefixed_result_set(rs : DB::ResultSet)
      table_prefix = "#{self.class.table_name.gsub("\"", "")}_"

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
    end

    private def insert_record
      # Run before_save callbacks first
      run_before_save_callbacks

      # Run before_create callbacks
      run_before_create_callbacks

      columns = @attributes.keys
      return false if columns.empty?

      placeholders = (["?"] * columns.size).join(", ")
      query = "INSERT INTO #{self.class.table_name} (#{columns.join(", ")}) VALUES (#{placeholders})"

      result = self.class.connection.exec(query, args: @attributes.values.to_a)

      if result.rows_affected > 0
        # Get the inserted ID if it's an auto-increment primary key
        if self.class.primary_key == "id" && !@attributes.has_key?("id")
          id_value = result.last_insert_id
          @attributes["id"] = id_value.as(DB::Any)
          # Also set the instance variable directly without going through set_attribute
          # to avoid adding it to changed_attributes
          set_single_instance_variable("id", id_value.as(DB::Any))
        end
        @persisted = true
        @changed_attributes.clear

        # Run after_create callbacks
        run_after_create_callbacks

        # Run after_save callbacks
        run_after_save_callbacks

        true
      else
        false
      end
    end

    private def update_record
      return false if @changed_attributes.empty?

      # Run before_save callbacks first
      run_before_save_callbacks

      # Run before_update callbacks
      run_before_update_callbacks

      set_clause = @changed_attributes.map { |attr| "#{attr} = ?" }.join(", ")
      query = "UPDATE #{self.class.table_name} SET #{set_clause} WHERE #{self.class.primary_key} = ?"

      changed_values = @changed_attributes.map { |attr| @attributes[attr] }.to_a
      id_value = get_attribute(self.class.primary_key)
      args = changed_values + [id_value]

      result = self.class.connection.exec(query, args: args)

      if result.rows_affected > 0
        @changed_attributes.clear

        # Run after_update callbacks
        run_after_update_callbacks

        # Run after_save callbacks
        run_after_save_callbacks

        true
      else
        false
      end
    end
  end
end
