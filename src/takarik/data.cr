require "./data/base_model"
require "./data/validations"
require "./data/associations"
require "./data/query_builder"

# String extensions for inflection
class String
  def underscore
    self.gsub(/([A-Z]+)([A-Z][a-z])/, "\\1_\\2")
        .gsub(/([a-z\d])([A-Z])/, "\\1_\\2")
        .downcase
  end

  def camelcase
    self.split('_').map(&.capitalize).join
  end

  def pluralize
    # Simple pluralization rules - in a real implementation you'd want a more robust solution
    case self
    when .ends_with?("y")
      self[0..-2] + "ies"
    when .ends_with?("s"), .ends_with?("sh"), .ends_with?("ch"), .ends_with?("x"), .ends_with?("z")
      self + "es"
    else
      self + "s"
    end
  end

  def singularize
    # Simple singularization rules
    case self
    when .ends_with?("ies")
      self[0..-4] + "y"
    when .ends_with?("es")
      if self.ends_with?("ses") || self.ends_with?("shes") || self.ends_with?("ches") || self.ends_with?("xes") || self.ends_with?("zes")
        self[0..-3]
      else
        self[0..-2]
      end
    else
      if (self.ends_with?("s") && !self.ends_with?("ss"))
        self[0..-2]
      else
        self
      end
    end
  end
end

module Takarik::Data
  VERSION = "0.1.0"

  # Enhanced BaseModel with query builder integration
  abstract class BaseModel
    include Validations
    include Associations

    # Add query builder methods to the class - make them more direct
    def self.query
      QueryBuilder(self).new(self)
    end

    # Chainable query methods - return QueryBuilder for chaining
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

    # Convenient overloads for common array types
    def self.where(column : String, values : Array(Int32))
      query.where(column, values)
    end

    def self.where(column : String, values : Array(Int64))
      query.where(column, values)
    end

    def self.where(column : String, values : Array(String))
      query.where(column, values)
    end

    def self.where(column : String, values : Array(Float32))
      query.where(column, values)
    end

    def self.where(column : String, values : Array(Float64))
      query.where(column, values)
    end

    def self.where(column : String, values : Array(Bool))
      query.where(column, values)
    end

    def self.where(column : String, range : Range(Int32, Int32))
      query.where(column, range)
    end

    def self.where(column : String, range : Range(Int64, Int64))
      query.where(column, range)
    end

    def self.where(column : String, range : Range(Float32, Float32))
      query.where(column, range)
    end

    def self.where(column : String, range : Range(Float64, Float64))
      query.where(column, range)
    end

    def self.where(column : String, range : Range(Time, Time))
      query.where(column, range)
    end

    def self.where(column : String, range : Range(String, String))
      query.where(column, range)
    end

    def self.where_not(conditions : Hash(String, DB::Any))
      query.where_not(conditions)
    end

    def self.where_not(**conditions)
      query.where_not(**conditions)
    end

    def self.where_not(column : String, values : Array(DB::Any))
      query.where_not(column, values)
    end

    # Convenient overloads for common array types
    def self.where_not(column : String, values : Array(Int32))
      query.where_not(column, values)
    end

    def self.where_not(column : String, values : Array(Int64))
      query.where_not(column, values)
    end

    def self.where_not(column : String, values : Array(String))
      query.where_not(column, values)
    end

    def self.where_not(column : String, values : Array(Float32))
      query.where_not(column, values)
    end

    def self.where_not(column : String, values : Array(Float64))
      query.where_not(column, values)
    end

    def self.where_not(column : String, values : Array(Bool))
      query.where_not(column, values)
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

    def self.inner_join(table : String, on : String)
      query.inner_join(table, on)
    end

    def self.left_join(table : String, on : String)
      query.left_join(table, on)
    end

    def self.right_join(table : String, on : String)
      query.right_join(table, on)
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

    # Callbacks support
    macro before_save(&block)
      def before_save_callback
        {{block.body}}
      end
    end

    macro after_save(&block)
      def after_save_callback
        {{block.body}}
      end
    end

    macro before_create(&block)
      def before_create_callback
        {{block.body}}
      end

      private def insert_record
        before_create_callback if new_record?
        super
      end
    end

    macro after_create(&block)
      def after_create_callback
        {{block.body}}
      end

      private def insert_record
        result = super
        after_create_callback if result && new_record?
        result
      end
    end

    macro before_update(&block)
      def before_update_callback
        {{block.body}}
      end

      private def update_record
        before_update_callback unless new_record?
        super
      end
    end

    macro after_update(&block)
      def after_update_callback
        {{block.body}}
      end

      private def update_record
        result = super
        after_update_callback if result && !new_record?
        result
      end
    end

    macro before_destroy(&block)
      def before_destroy_callback
        {{block.body}}
      end

      def destroy
        before_destroy_callback
        super
      end
    end

    macro after_destroy(&block)
      def after_destroy_callback
        {{block.body}}
      end

      def destroy
        result = super
        after_destroy_callback if result
        result
      end
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
  end

  # Migration support
  abstract class Migration
    abstract def up
    abstract def down

    def self.connection
      BaseModel.connection
    end

    def create_table(name : String, &block)
      table_builder = TableBuilder.new(name)
      yield table_builder
      connection.exec(table_builder.to_sql)
    end

    def drop_table(name : String)
      connection.exec("DROP TABLE #{name}")
    end

    def add_column(table : String, name : String, type : String, **options)
      sql = "ALTER TABLE #{table} ADD COLUMN #{name} #{type}"

      if options[:null] == false
        sql += " NOT NULL"
      end

      if default = options[:default]
        sql += " DEFAULT #{default}"
      end

      connection.exec(sql)
    end

    def remove_column(table : String, name : String)
      connection.exec("ALTER TABLE #{table} DROP COLUMN #{name}")
    end

    def add_index(table : String, columns : String | Array(String), **options)
      column_list = columns.is_a?(Array) ? columns.join(", ") : columns
      index_name = options[:name]? || "idx_#{table}_#{column_list.gsub(", ", "_")}"

      sql = "CREATE"
      sql += " UNIQUE" if options[:unique]
      sql += " INDEX #{index_name} ON #{table} (#{column_list})"

      connection.exec(sql)
    end

    def remove_index(table : String, name : String)
      connection.exec("DROP INDEX #{name}")
    end
  end

  # Table builder for migrations
  class TableBuilder
    @columns = [] of String
    @table_name : String

    def initialize(@table_name : String)
    end

    def column(name : String, type : String, **options)
      column_def = "#{name} #{type}"

      if options[:null] == false
        column_def += " NOT NULL"
      end

      if default = options[:default]
        column_def += " DEFAULT #{default}"
      end

      if options[:primary_key]
        column_def += " PRIMARY KEY"
      end

      if options[:auto_increment]
        column_def += " AUTOINCREMENT"
      end

      @columns << column_def
    end

    def primary_key(name : String = "id", type : String = "INTEGER")
      column(name, type, primary_key: true, auto_increment: true)
    end

    def string(name : String, **options)
      limit = options[:limit]? || 255
      column(name, "VARCHAR(#{limit})", **options)
    end

    def text(name : String, **options)
      column(name, "TEXT", **options)
    end

    def integer(name : String, **options)
      column(name, "INTEGER", **options)
    end

    def bigint(name : String, **options)
      column(name, "BIGINT", **options)
    end

    def float(name : String, **options)
      column(name, "REAL", **options)
    end

    def boolean(name : String, **options)
      column(name, "BOOLEAN", **options)
    end

    def datetime(name : String, **options)
      column(name, "DATETIME", **options)
    end

    def timestamps
      datetime("created_at", null: false)
      datetime("updated_at", null: false)
    end

    def to_sql
      "CREATE TABLE #{@table_name} (#{@columns.join(", ")})"
    end
  end
end
