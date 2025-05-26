require "db"
require "json"

module Takarik::Data
  # Forward declaration for QueryBuilder
  class QueryBuilder(T)
  end

  # Base class for all ORM models, providing ActiveRecord-like functionality
  # but designed specifically for Crystal language features
  abstract class BaseModel
    include JSON::Serializable

    # Class-level configuration
    @@connection : DB::Database?

    # Instance variables
    @attributes = {} of String => DB::Any
    @persisted = false
    @changed_attributes = Set(String).new

    # Macro to define database columns with types
    macro column(name, type, **options)
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
          @attributes.delete({{name.stringify}})
        else
          @attributes[{{name.stringify}}] = value.as(DB::Any)
        end

        # Track changes
        if old_value != value
          @changed_attributes << {{name.stringify}}
        end
        value
      end

      # Override getter to return from instance variable or attributes
      def {{name.id}}
        if @{{name.id}}
          @{{name.id}}
        elsif @attributes.has_key?({{name.stringify}})
          value = @attributes[{{name.stringify}}]
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

      # Add to JSON serialization
      def {{name.id}}_json
        @{{name.id}}.try(&.to_json) || "null"
      end
    end

    # Macro to set table name
    macro table_name(name)
      def self.table_name
        {{name.stringify}}
      end
    end

    # Macro to set primary key
    macro primary_key(name)
      def self.primary_key
        {{name.stringify}}
      end
    end

    # Class methods for database operations
    def self.establish_connection(database_url : String)
      @@connection = DB.open(database_url)
    end

    def self.connection
      @@connection || raise "Database connection not established. Call establish_connection first."
    end

    def self.table_name
      self.name.split("::").last.underscore.pluralize
    end

    def self.primary_key
      "id"
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
      {% begin %}
        case name
        {% for ivar in @type.instance_vars %}
          {% if ivar.name.stringify != "attributes" && ivar.name.stringify != "persisted" && ivar.name.stringify != "changed_attributes" && ivar.name.stringify != "validation_errors" %}
            when {{ivar.name.stringify}}
              # Handle type conversion for common database types
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

    def get_attribute(name : String)
      @attributes[name]?
    end

    def save
      # Run validation if the model includes validations
      {% if @type.ancestors.any? { |a| a.name.stringify.includes?("Validations") } %}
        return false unless valid?
      {% end %}

      # Run before_save callback if defined
      {% if @type.methods.any? { |m| m.name.stringify == "before_save_callback" } %}
        before_save_callback
      {% end %}

      result = if new_record?
        insert_record
      else
        update_record if changed?
      end

      # Run after_save callback if defined and save was successful
      {% if @type.methods.any? { |m| m.name.stringify == "after_save_callback" } %}
        after_save_callback if result
      {% end %}

      result
    end

    def save!
      # Run validation if the model includes validations
      {% if @type.ancestors.any? { |a| a.name.stringify.includes?("Validations") } %}
        unless valid?
          raise Takarik::Data::Validations::ValidationError.new(@validation_errors)
        end
      {% end %}

      save || raise "Failed to save record"
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

      query = "DELETE FROM #{self.class.table_name} WHERE #{self.class.primary_key} = ?"
      id_value = get_attribute(self.class.primary_key)

      result = self.class.connection.exec(query, id_value)
      @persisted = false if result.rows_affected > 0
      result.rows_affected > 0
    end

    def reload
      return self if new_record?

      id_value = get_attribute(self.class.primary_key)
      fresh_record = self.class.find(id_value)

      if fresh_record
        @attributes = fresh_record.@attributes

        # Also sync all instance variables from the fresh record
        @attributes.each do |column_name, value|
          # Also set the instance variable if it exists
          {% begin %}
            case column_name
            {% for ivar in @type.instance_vars %}
              {% if ivar.name.stringify != "attributes" && ivar.name.stringify != "persisted" && ivar.name.stringify != "changed_attributes" && ivar.name.stringify != "validation_errors" %}
                when {{ivar.name.stringify}}
                  # Handle type conversion for common database types
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

    # Protected methods for internal use
    protected def load_from_result_set(rs : DB::ResultSet)
      rs.column_names.each_with_index do |column_name, index|
        value = rs.read
        @attributes[column_name] = value
      end

      # Now set all instance variables from the attributes
      @attributes.each do |column_name, value|
        # Also set the instance variable if it exists
        {% begin %}
          case column_name
          {% for ivar in @type.instance_vars %}
            {% if ivar.name.stringify != "attributes" && ivar.name.stringify != "persisted" && ivar.name.stringify != "changed_attributes" && ivar.name.stringify != "validation_errors" %}
              when {{ivar.name.stringify}}
                # Handle type conversion for common database types
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

      @persisted = true
      @changed_attributes.clear
    end

    private def insert_record
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
          # Also set the instance variable
          set_attribute("id", id_value.as(DB::Any))
        end
        @persisted = true
        @changed_attributes.clear
        true
      else
        false
      end
    end

    private def update_record
      return false if @changed_attributes.empty?

      set_clause = @changed_attributes.map { |attr| "#{attr} = ?" }.join(", ")
      query = "UPDATE #{self.class.table_name} SET #{set_clause} WHERE #{self.class.primary_key} = ?"

      changed_values = @changed_attributes.map { |attr| @attributes[attr] }.to_a
      id_value = get_attribute(self.class.primary_key)
      args = changed_values + [id_value]

      result = self.class.connection.exec(query, args: args)

      if result.rows_affected > 0
        @changed_attributes.clear
        true
      else
        false
      end
    end
  end
end
