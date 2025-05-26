require "./base_model"

module Takarik::Data
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
