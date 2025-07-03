require "./base_model"

module Takarik::Data
  # ========================================
  # TYPE ALIASES
  # ========================================

  alias PrimaryKeyConfig = NamedTuple(name: String?, type: String, auto_increment: Bool)
  alias PrimaryKeyShorthand = Hash(String, String)

  # ========================================
  # MIGRATION CLASS
  # ========================================

  abstract class Migration
    # ========================================
    # ABSTRACT METHODS
    # ========================================

    abstract def up
    abstract def down

    # ========================================
    # CLASS METHODS
    # ========================================

    def self.connection
      BaseModel.connection
    end

    # ========================================
    # INSTANCE METHODS - TRANSACTION WRAPPER
    # ========================================

    def run_up
      transaction do
        up
      end
    end

    def run_down
      transaction do
        down
      end
    end

    # ========================================
    # INSTANCE METHODS - TABLE OPERATIONS
    # ========================================

    def create_table(name : String, primary_key : String | PrimaryKeyConfig | PrimaryKeyShorthand | Nil = "id", &block)
      table_builder = TableBuilder.new(name, primary_key)
      yield table_builder
      Takarik::Data.exec_with_logging(connection, table_builder.to_sql)
    end

    def drop_table(name : String)
      Takarik::Data.exec_with_logging(connection, "DROP TABLE #{name}")
    end

    # ========================================
    # INSTANCE METHODS - COLUMN OPERATIONS
    # ========================================

    def add_column(table : String, name : String, type : String, **options)
      sql = "ALTER TABLE #{table} ADD COLUMN #{name} #{type}"

      null_value = options[:null]?
      if null_value == false
        sql += " NOT NULL"
      end

      if default = options[:default]?
        sql += " DEFAULT #{default}"
      end

      Takarik::Data.exec_with_logging(connection, sql)
    end

    def remove_column(table : String, name : String)
      Takarik::Data.exec_with_logging(connection, "ALTER TABLE #{table} DROP COLUMN #{name}")
    end

    # ========================================
    # INSTANCE METHODS - INDEX OPERATIONS
    # ========================================

    def add_index(table : String, columns : String | Array(String), **options)
      column_list = columns.is_a?(Array) ? columns.join(", ") : columns
      index_name = options[:name]? || "idx_#{table}_#{column_list.gsub(", ", "_")}"

      sql = "CREATE"
      sql += " UNIQUE" if options[:unique]?
      sql += " INDEX #{index_name} ON #{table} (#{column_list})"

      Takarik::Data.exec_with_logging(connection, sql)
    end

    def remove_index(table : String, name : String)
      Takarik::Data.exec_with_logging(connection, "DROP INDEX #{name}")
    end

    # ========================================
    # PRIVATE METHODS
    # ========================================

    private def connection
      self.class.connection
    end

    private def transaction(&block)
      begin
        Takarik::Data.exec_with_logging(connection, "BEGIN TRANSACTION")
        yield
        Takarik::Data.exec_with_logging(connection, "COMMIT")
      rescue ex
        Takarik::Data.exec_with_logging(connection, "ROLLBACK")
        raise ex
      end
    end
  end

  # ========================================
  # TABLE BUILDER CLASS
  # ========================================

  class TableBuilder
    # ========================================
    # INSTANCE VARIABLES
    # ========================================

    @columns = [] of String
    @table_name : String
    @primary_key : String | PrimaryKeyConfig | PrimaryKeyShorthand | Nil

    # ========================================
    # INITIALIZE
    # ========================================

    def initialize(@table_name : String, @primary_key : String | PrimaryKeyConfig | PrimaryKeyShorthand | Nil = "id")
      setup_primary_key
    end

    # ========================================
    # INSTANCE METHODS - COLUMN DEFINITION
    # ========================================

    def column(name : String, type : String, **options)
      column_def = "#{name} #{type}"

      null_value = options[:null]?
      if null_value == false
        column_def += " NOT NULL"
      end

      if default = options[:default]?
        column_def += " DEFAULT #{default}"
      end

      if options[:primary_key]?
        column_def += " PRIMARY KEY"
      end

      if options[:auto_increment]?
        column_def += " AUTOINCREMENT"
      end

      @columns << column_def
    end

    # ========================================
    # INSTANCE METHODS - TYPE HELPERS
    # ========================================

    def string(name : String, **options)
      limit = options[:limit]? || 255
      column(name, "VARCHAR(#{limit})", **options)
    end

    def char(name : String, **options)
      limit = options[:limit]? || 1
      column(name, "CHAR(#{limit})", **options)
    end

    def text(name : String, **options)
      column(name, "TEXT", **options)
    end

    def mediumtext(name : String, **options)
      column(name, "MEDIUMTEXT", **options)
    end

    def longtext(name : String, **options)
      column(name, "LONGTEXT", **options)
    end

    def integer(name : String, **options)
      column(name, "INTEGER", **options)
    end

    def int(name : String, **options)
      column(name, "INT", **options)
    end

    def tinyint(name : String, **options)
      column(name, "TINYINT", **options)
    end

    def smallint(name : String, **options)
      column(name, "SMALLINT", **options)
    end

    def mediumint(name : String, **options)
      column(name, "MEDIUMINT", **options)
    end

    def bigint(name : String, **options)
      column(name, "BIGINT", **options)
    end

    def serial(name : String, **options)
      column(name, "SERIAL", **options)
    end

    def bigserial(name : String, **options)
      column(name, "BIGSERIAL", **options)
    end

    def decimal(name : String, **options)
      precision = options[:precision]?
      scale = options[:scale]?

      type = if precision && scale
        "DECIMAL(#{precision},#{scale})"
      elsif precision
        "DECIMAL(#{precision})"
      else
        "DECIMAL"
      end

      column(name, type, **options)
    end

    def numeric(name : String, **options)
      precision = options[:precision]?
      scale = options[:scale]?

      type = if precision && scale
        "NUMERIC(#{precision},#{scale})"
      elsif precision
        "NUMERIC(#{precision})"
      else
        "NUMERIC"
      end

      column(name, type, **options)
    end

    def float(name : String, **options)
      column(name, "REAL", **options)
    end

    def double(name : String, **options)
      column(name, "DOUBLE PRECISION", **options)
    end

    def boolean(name : String, **options)
      column(name, "BOOLEAN", **options)
    end

    def date(name : String, **options)
      column(name, "DATE", **options)
    end

    def time(name : String, **options)
      column(name, "TIME", **options)
    end

    def datetime(name : String, **options)
      column(name, "DATETIME", **options)
    end

    def timestamp(name : String, **options)
      column(name, "TIMESTAMP", **options)
    end

    def timestamptz(name : String, **options)
      column(name, "TIMESTAMPTZ", **options)
    end

    def interval(name : String, **options)
      column(name, "INTERVAL", **options)
    end

    def year(name : String, **options)
      column(name, "YEAR", **options)
    end

    def json(name : String, **options)
      column(name, "JSON", **options)
    end

    def jsonb(name : String, **options)
      column(name, "JSONB", **options)
    end

    def uuid(name : String, **options)
      column(name, "UUID", **options)
    end

    def binary(name : String, **options)
      limit = options[:limit]?
      type = limit ? "BINARY(#{limit})" : "BINARY"
      column(name, type, **options)
    end

    def varbinary(name : String, **options)
      limit = options[:limit]? || 255
      column(name, "VARBINARY(#{limit})", **options)
    end

    def blob(name : String, **options)
      column(name, "BLOB", **options)
    end

    def mediumblob(name : String, **options)
      column(name, "MEDIUMBLOB", **options)
    end

    def longblob(name : String, **options)
      column(name, "LONGBLOB", **options)
    end

    def bytea(name : String, **options)
      column(name, "BYTEA", **options)
    end

    def inet(name : String, **options)
      column(name, "INET", **options)
    end

    def cidr(name : String, **options)
      column(name, "CIDR", **options)
    end

    def macaddr(name : String, **options)
      column(name, "MACADDR", **options)
    end

    def array(name : String, base_type : String, **options)
      column(name, "#{base_type}[]", **options)
    end

    def enum(name : String, values : Array(String), **options)
      enum_values = values.map { |v| "'#{v}'" }.join(", ")
      column(name, "ENUM(#{enum_values})", **options)
    end

    def set(name : String, values : Array(String), **options)
      set_values = values.map { |v| "'#{v}'" }.join(", ")
      column(name, "SET(#{set_values})", **options)
    end

    def geometry(name : String, **options)
      column(name, "GEOMETRY", **options)
    end

    def point(name : String, **options)
      column(name, "POINT", **options)
    end

    def linestring(name : String, **options)
      column(name, "LINESTRING", **options)
    end

    def polygon(name : String, **options)
      column(name, "POLYGON", **options)
    end

    def timestamps
      datetime("created_at", null: false)
      datetime("updated_at", null: false)
    end

    def timestamps_tz
      timestamptz("created_at", null: false)
      timestamptz("updated_at", null: false)
    end

    def soft_deletes
      timestamp("deleted_at")
    end

    def references(name : String, **options)
      column_name = "#{name}_id"
      column_type = "BIGINT"

      # Handle null constraint
      null_option = options[:null]?
      if null_option == false
        column(column_name, column_type, null: false)
      else
        column(column_name, column_type)
      end
    end

    def references(name : String, to table : String, **options)
      column_name = "#{name}_id"
      column_type = "BIGINT"

      # Handle null constraint
      null_option = options[:null]?
      if null_option == false
        column(column_name, column_type, null: false)
      else
        column(column_name, column_type)
      end
    end

    def references(name : String, column col_name : String, **options)
      column_type = "BIGINT"

      # Handle null constraint
      null_option = options[:null]?
      if null_option == false
        column(col_name, column_type, null: false)
      else
        column(col_name, column_type)
      end
    end

    def references(name : String, type col_type : String, **options)
      column_name = "#{name}_id"

      # Handle null constraint
      null_option = options[:null]?
      if null_option == false
        column(column_name, col_type, null: false)
      else
        column(column_name, col_type)
      end
    end

    def raw(name : String, sql_type : String, **options)
      column(name, sql_type, **options)
    end

    # ========================================
    # INSTANCE METHODS - SQL GENERATION
    # ========================================

    def to_sql
      "CREATE TABLE #{@table_name} (#{@columns.join(", ")})"
    end

    # ========================================
    # PRIVATE METHODS
    # ========================================

    private def setup_primary_key
      return if @primary_key.nil?

      case @primary_key
      when String
        # Simple string like "id" - use default INTEGER type with autoincrement
        pk_name = @primary_key.as(String)
        column(pk_name, "INTEGER", primary_key: true, auto_increment: true)
      when PrimaryKeyConfig
        # Named tuple like {name: "uuid", type: "UUID", auto_increment: false}
        pk_config = @primary_key.as(PrimaryKeyConfig)
        pk_name = pk_config[:name]? || "id"
        pk_type = pk_config[:type]
        pk_auto_increment = pk_config[:auto_increment] || false

        if pk_auto_increment
          column(pk_name, pk_type, primary_key: true, auto_increment: true)
        else
          column(pk_name, pk_type, primary_key: true)
        end
      when PrimaryKeyShorthand
        # Hash like {"id" => "UUID"} - shorthand for type change
        pk_shorthand = @primary_key.as(PrimaryKeyShorthand)
        pk_name = pk_shorthand.keys.first
        pk_type = pk_shorthand.values.first

        # Auto-increment is false for non-INTEGER types, true for INTEGER
        pk_auto_increment = pk_type.upcase == "INTEGER"

        if pk_auto_increment
          column(pk_name, pk_type, primary_key: true, auto_increment: true)
        else
          column(pk_name, pk_type, primary_key: true)
        end
      end
    end
  end
end
