require "./spec_helper"

# Test migration classes for testing purposes
class CreateUsersTable < Takarik::Data::Migration
  def up
    create_table("test_users") do |t|
      t.string("name", null: false)
      t.string("email", null: false)
      t.integer("age")
      t.boolean("active", default: true)
      t.timestamps
    end

    add_index("test_users", "email", unique: true)
    add_index("test_users", ["name", "active"])
  end

  def down
    remove_index("test_users", "idx_test_users_email")
    remove_index("test_users", "idx_test_users_name_active")
    drop_table("test_users")
  end
end

class AddPhoneToUsers < Takarik::Data::Migration
  def up
    add_column("test_users", "phone", "VARCHAR(20)")
    add_column("test_users", "address", "TEXT", null: false, default: "'N/A'")
  end

  def down
    remove_column("test_users", "address")
    remove_column("test_users", "phone")
  end
end

class CreateProductsTable < Takarik::Data::Migration
  def up
    create_table("test_products", {name: "product_id", type: "INTEGER", auto_increment: true}) do |t|
      t.string("name", null: false, limit: 100)
      t.text("description")
      t.float("price", null: false)
      t.integer("stock", default: 0)
      t.timestamps
    end

    add_index("test_products", "name")
    add_index("test_products", "price", name: "idx_product_price")
  end

  def down
    remove_index("test_products", "idx_product_price")
    remove_index("test_products", "idx_test_products_name")
    drop_table("test_products")
  end
end

class TestTableBuilder < Takarik::Data::Migration
  def up
    create_table("test_builder") do |t|
      t.string("title")
      t.text("content")
      t.integer("views", default: 0)
      t.boolean("published", default: false)
      t.datetime("published_at")
      t.timestamps
    end
  end

  def down
    drop_table("test_builder")
  end
end

class CreateUUIDTable < Takarik::Data::Migration
  def up
    create_table("test_uuid", {name: "uuid", type: "UUID", auto_increment: false}) do |t|
      t.string("title", null: false)
      t.text("content")
      t.timestamps
    end

    add_index("test_uuid", "title")
  end

  def down
    remove_index("test_uuid", "idx_test_uuid_title")
    drop_table("test_uuid")
  end
end

class CreateCustomPrimaryKeyTable < Takarik::Data::Migration
  def up
    create_table("test_custom_pk", {name: "custom_id", type: "BIGINT", auto_increment: false}) do |t|
      t.string("name", null: false)
      t.integer("value")
      t.timestamps
    end
  end

  def down
    drop_table("test_custom_pk")
  end
end

class CreateNoPrimaryKeyTable < Takarik::Data::Migration
  def up
    create_table("test_no_pk", nil) do |t|
      t.string("name", null: false)
      t.integer("value")
      t.timestamps
    end
  end

  def down
    drop_table("test_no_pk")
  end
end

class FailingMigration < Takarik::Data::Migration
  def up
    create_table("test_failing") do |t|
      t.string("name")
    end

    # This should cause an error - table already exists
    create_table("test_failing") do |t|
      t.string("other_name")
    end
  end

  def down
    drop_table("test_failing")
  end
end

class PartiallyFailingMigration < Takarik::Data::Migration
  def up
    create_table("test_partial") do |t|
      t.string("name")
    end

    # This should succeed
    add_column("test_partial", "email", "VARCHAR(255)")

    # This should fail - invalid SQL
    Takarik::Data.exec_with_logging(connection, "INVALID SQL COMMAND")
  end

  def down
    drop_table("test_partial")
  end
end

class CreateDefaultNameTable < Takarik::Data::Migration
  def up
    create_table("test_default_name", {"id" => "UUID"}) do |t|
      t.string("title", null: false)
      t.text("content")
      t.timestamps
    end
  end

  def down
    drop_table("test_default_name")
  end
end

class CreateUUIDShorthandTable < Takarik::Data::Migration
  def up
    create_table("test_uuid_shorthand", {"id" => "UUID"}) do |t|
      t.string("title", null: false)
      t.text("content")
      t.timestamps
    end
  end

  def down
    drop_table("test_uuid_shorthand")
  end
end

class CreateCustomShorthandTable < Takarik::Data::Migration
  def up
    create_table("test_custom_shorthand", {"user_id" => "BIGINT"}) do |t|
      t.string("name", null: false)
      t.integer("age")
      t.timestamps
    end
  end

  def down
    drop_table("test_custom_shorthand")
  end
end

class CreateMySQLTypesTable < Takarik::Data::Migration
  def up
    create_table("test_mysql_types") do |t|
      # String types
      t.char("code", limit: 10)
      t.string("name", limit: 100)
      t.text("description")
      t.mediumtext("content")
      t.longtext("document")

      # Integer types
      t.tinyint("status")
      t.smallint("priority")
      t.mediumint("count")
      t.int("views")
      t.bigint("large_number")

      # Decimal types
      t.decimal("price", precision: 8, scale: 2)
      t.numeric("score", precision: 5, scale: 3)
      t.float("ratio")
      t.double("precise_value")

      # Date/Time types
      t.date("birth_date")
      t.time("event_time")
      t.datetime("created_at")
      t.timestamp("updated_at")
      t.year("year_value")

      # Binary types
      t.binary("data", limit: 16)
      t.varbinary("variable_data", limit: 255)
      t.blob("file_data")

      # Special types (SQLite compatible)
      t.json("metadata")
      t.boolean("active")

      # Note: ENUM and SET are MySQL-specific and not supported by SQLite
      # These would work in actual MySQL deployments:
      # t.enum("category", ["A", "B", "C"])
      # t.set("tags", ["tag1", "tag2", "tag3"])
    end
  end

  def down
    drop_table("test_mysql_types")
  end
end

class CreatePostgreSQLTypesTable < Takarik::Data::Migration
  def up
    create_table("test_postgresql_types", {"id" => "SERIAL"}) do |t|
      # String types
      t.char("code", limit: 10)
      t.string("name", limit: 100)
      t.text("description")

      # Integer types
      t.smallint("priority")
      t.int("views")
      t.bigint("large_number")
      t.serial("sequence_id")
      t.bigserial("big_sequence_id")

      # Decimal types
      t.decimal("price", precision: 8, scale: 2)
      t.numeric("score", precision: 5, scale: 3)
      t.float("ratio")
      t.double("precise_value")

      # Date/Time types
      t.date("birth_date")
      t.time("event_time")
      t.timestamp("created_at")
      t.timestamptz("updated_at")
      t.interval("duration")

      # Binary types
      t.bytea("binary_data")

      # Special types
      t.json("metadata")
      t.jsonb("structured_data")
      t.uuid("identifier")
      t.boolean("active")

      # Network types
      t.inet("ip_address")
      t.cidr("network")
      t.macaddr("mac_address")

      # Array types
      t.array("tags", "TEXT")
      t.array("numbers", "INTEGER")

      # Geometry types
      t.geometry("location")
      t.point("coordinates")
    end
  end

  def down
    drop_table("test_postgresql_types")
  end
end

class CreateConvenienceMethodsTable < Takarik::Data::Migration
  def up
    create_table("test_convenience") do |t|
      t.string("name", null: false)
      t.timestamps_tz
      t.soft_deletes
      t.references("user")
      t.raw("custom_field", "CUSTOM_TYPE(100)")
    end
  end

  def down
    drop_table("test_convenience")
  end
end

class CreateGeometryTypesTable < Takarik::Data::Migration
  def up
    create_table("test_geometry") do |t|
      t.string("name", null: false)
      t.geometry("shape")
      t.point("location")
      t.linestring("path")
      t.polygon("boundary")
      t.timestamps
    end
  end

  def down
    drop_table("test_geometry")
  end
end

# Helper method to check if table exists
def table_exists?(table_name : String)
  result = Takarik::Data.connection.query_one?("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name=?", table_name, as: Int32)
  result ? result > 0 : false
end

# Helper method to check if column exists
def column_exists?(table_name : String, column_name : String)
  begin
    Takarik::Data.connection.query_one?("SELECT #{column_name} FROM #{table_name} LIMIT 1", as: String)
    true
  rescue
    false
  end
end

# Helper method to check if index exists
def index_exists?(index_name : String)
  result = Takarik::Data.connection.query_one?("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name=?", index_name, as: Int32)
  result ? result > 0 : false
end

describe Takarik::Data::Migration do
  before_each do
    # Clean up any test tables that might exist
    ["test_users", "test_products", "test_builder", "test_failing", "test_partial", "test_uuid", "test_custom_pk", "test_no_pk", "test_default_name", "test_uuid_shorthand", "test_custom_shorthand", "test_mysql_types", "test_postgresql_types", "test_convenience", "test_geometry"].each do |table|
      begin
        Takarik::Data.connection.exec "DROP TABLE IF EXISTS #{table}"
      rescue
        # Ignore errors if table doesn't exist
      end
    end

    # Clean up any test indexes
    ["idx_test_users_email", "idx_test_users_name_active", "idx_test_products_name", "idx_product_price", "idx_test_uuid_title"].each do |index|
      begin
        Takarik::Data.connection.exec "DROP INDEX IF EXISTS #{index}"
      rescue
        # Ignore errors if index doesn't exist
      end
    end
  end

  describe "basic migration functionality" do
    it "has connection method" do
      migration = CreateUsersTable.new
      migration.class.connection.should eq(Takarik::Data::BaseModel.connection)
    end

    it "requires up and down methods to be implemented" do
      migration = CreateUsersTable.new
      migration.responds_to?(:up).should be_true
      migration.responds_to?(:down).should be_true
    end

    it "provides transaction-wrapped run_up and run_down methods" do
      migration = CreateUsersTable.new
      migration.responds_to?(:run_up).should be_true
      migration.responds_to?(:run_down).should be_true
    end
  end

  describe "primary key configuration" do
    it "creates table with default primary key (id)" do
      migration = CreateUsersTable.new

      # Run the up migration
      migration.run_up

      # Verify table and default primary key column exist
      table_exists?("test_users").should be_true
      column_exists?("test_users", "id").should be_true
    end

    it "creates table with custom primary key name and type" do
      migration = CreateProductsTable.new

      # Run the up migration
      migration.run_up

      # Verify table and custom primary key column exist
      table_exists?("test_products").should be_true
      column_exists?("test_products", "product_id").should be_true
      column_exists?("test_products", "id").should be_false  # Default id should not exist
    end

    it "creates table with UUID primary key" do
      migration = CreateUUIDTable.new

      # Run the up migration
      migration.run_up

      # Verify table and UUID primary key column exist
      table_exists?("test_uuid").should be_true
      column_exists?("test_uuid", "uuid").should be_true
      column_exists?("test_uuid", "id").should be_false  # Default id should not exist
    end

    it "creates table with custom BIGINT primary key" do
      migration = CreateCustomPrimaryKeyTable.new

      # Run the up migration
      migration.run_up

      # Verify table and custom primary key column exist
      table_exists?("test_custom_pk").should be_true
      column_exists?("test_custom_pk", "custom_id").should be_true
      column_exists?("test_custom_pk", "id").should be_false  # Default id should not exist
    end

    it "creates table without primary key when nil is passed" do
      migration = CreateNoPrimaryKeyTable.new

      # Run the up migration
      migration.run_up

      # Verify table exists but no primary key columns
      table_exists?("test_no_pk").should be_true
      column_exists?("test_no_pk", "id").should be_false
      column_exists?("test_no_pk", "name").should be_true
    end

    it "creates table with UUID primary key using shorthand syntax" do
      migration = CreateUUIDShorthandTable.new

      # Run the up migration
      migration.run_up

      # Verify table and UUID primary key column exist
      table_exists?("test_uuid_shorthand").should be_true
      column_exists?("test_uuid_shorthand", "id").should be_true
      column_exists?("test_uuid_shorthand", "title").should be_true
    end

    it "creates table with custom primary key using shorthand syntax" do
      migration = CreateCustomShorthandTable.new

      # Run the up migration
      migration.run_up

      # Verify table and custom primary key column exist
      table_exists?("test_custom_shorthand").should be_true
      column_exists?("test_custom_shorthand", "user_id").should be_true
      column_exists?("test_custom_shorthand", "id").should be_false  # Default id should not exist
      column_exists?("test_custom_shorthand", "name").should be_true
    end

    it "creates table with default name using shorthand syntax" do
      migration = CreateDefaultNameTable.new

      # Run the up migration
      migration.run_up

      # Verify table and default name primary key column exist
      table_exists?("test_default_name").should be_true
      column_exists?("test_default_name", "id").should be_true
      column_exists?("test_default_name", "title").should be_true
    end
  end

  describe "transaction-wrapped migrations" do
    it "runs up migration in transaction to create table with columns and indexes" do
      migration = CreateUsersTable.new

      # Verify table doesn't exist initially
      table_exists?("test_users").should be_false

      # Run the up migration in transaction
      migration.run_up

      # Verify table was created
      table_exists?("test_users").should be_true

      # Verify columns exist
      column_exists?("test_users", "id").should be_true
      column_exists?("test_users", "name").should be_true
      column_exists?("test_users", "email").should be_true
      column_exists?("test_users", "age").should be_true
      column_exists?("test_users", "active").should be_true
      column_exists?("test_users", "created_at").should be_true
      column_exists?("test_users", "updated_at").should be_true

      # Verify indexes were created
      index_exists?("idx_test_users_email").should be_true
      index_exists?("idx_test_users_name_active").should be_true
    end

    it "runs down migration in transaction to remove table and indexes" do
      migration = CreateUsersTable.new

      # First run up migration
      migration.run_up
      table_exists?("test_users").should be_true
      index_exists?("idx_test_users_email").should be_true

      # Then run down migration in transaction
      migration.run_down

      # Verify table and indexes were removed
      table_exists?("test_users").should be_false
      index_exists?("idx_test_users_email").should be_false
      index_exists?("idx_test_users_name_active").should be_false
    end
  end

  describe "transaction rollback behavior" do
    it "rolls back entire migration when error occurs" do
      migration = FailingMigration.new

      # Verify no tables exist initially
      table_exists?("test_failing").should be_false

      # Run migration that should fail
      expect_raises(Exception) do
        migration.run_up
      end

      # Verify that no tables were created due to rollback
      table_exists?("test_failing").should be_false
    end

    it "rolls back partially completed migration on error" do
      migration = PartiallyFailingMigration.new

      # Verify no tables exist initially
      table_exists?("test_partial").should be_false

      # Run migration that should fail partway through
      expect_raises(Exception) do
        migration.run_up
      end

      # Verify that no tables or columns were created due to rollback
      table_exists?("test_partial").should be_false
    end

    it "allows successful migration after failed transaction" do
      failing_migration = FailingMigration.new
      success_migration = CreateUsersTable.new

      # First try failing migration
      expect_raises(Exception) do
        failing_migration.run_up
      end

      # Verify nothing was created
      table_exists?("test_failing").should be_false
      table_exists?("test_users").should be_false

      # Now try successful migration
      success_migration.run_up

      # Verify it worked
      table_exists?("test_users").should be_true
      column_exists?("test_users", "name").should be_true
    end
  end

  describe "column addition migration" do
    it "adds columns in up migration with transaction" do
      # First create the base table
      create_users_migration = CreateUsersTable.new
      create_users_migration.run_up

      # Verify new columns don't exist initially
      column_exists?("test_users", "phone").should be_false
      column_exists?("test_users", "address").should be_false

      # Run the column addition migration
      add_columns_migration = AddPhoneToUsers.new
      add_columns_migration.run_up

      # Verify columns were added
      column_exists?("test_users", "phone").should be_true
      column_exists?("test_users", "address").should be_true
    end

    it "removes columns in down migration with transaction" do
      # Set up: create table and add columns
      create_users_migration = CreateUsersTable.new
      create_users_migration.run_up

      add_columns_migration = AddPhoneToUsers.new
      add_columns_migration.run_up

      # Verify columns exist
      column_exists?("test_users", "phone").should be_true
      column_exists?("test_users", "address").should be_true

      # Run down migration
      add_columns_migration.run_down

      # Verify columns were removed
      column_exists?("test_users", "phone").should be_false
      column_exists?("test_users", "address").should be_false
    end
  end

  describe "custom primary key migration" do
    it "creates table with custom primary key in transaction" do
      migration = CreateProductsTable.new

      # Run up migration
      migration.run_up

      # Verify table and custom primary key column exist
      table_exists?("test_products").should be_true
      column_exists?("test_products", "product_id").should be_true
      column_exists?("test_products", "name").should be_true
      column_exists?("test_products", "description").should be_true
      column_exists?("test_products", "price").should be_true
      column_exists?("test_products", "stock").should be_true

      # Verify indexes
      index_exists?("idx_test_products_name").should be_true
      index_exists?("idx_product_price").should be_true
    end

    it "removes table and indexes in down migration with transaction" do
      migration = CreateProductsTable.new

      # Run up then down
      migration.run_up
      table_exists?("test_products").should be_true

      migration.run_down

      # Verify everything was cleaned up
      table_exists?("test_products").should be_false
      index_exists?("idx_test_products_name").should be_false
      index_exists?("idx_product_price").should be_false
    end
  end

  describe "TableBuilder functionality" do
    it "creates table with all column types in transaction" do
      migration = TestTableBuilder.new

      # Run up migration
      migration.run_up

      # Verify table and all columns exist
      table_exists?("test_builder").should be_true
      column_exists?("test_builder", "id").should be_true
      column_exists?("test_builder", "title").should be_true
      column_exists?("test_builder", "content").should be_true
      column_exists?("test_builder", "views").should be_true
      column_exists?("test_builder", "published").should be_true
      column_exists?("test_builder", "published_at").should be_true
      column_exists?("test_builder", "created_at").should be_true
      column_exists?("test_builder", "updated_at").should be_true
    end

    it "generates proper SQL from TableBuilder with default primary key" do
      builder = Takarik::Data::TableBuilder.new("test_table")
      builder.string("name")
      builder.integer("age")

      sql = builder.to_sql
      sql.should_not be_nil
      sql.should be_a(String)
      sql.should eq("CREATE TABLE test_table (id INTEGER PRIMARY KEY AUTOINCREMENT, name VARCHAR(255), age INTEGER)")
    end

    it "generates proper SQL from TableBuilder with custom primary key" do
      builder = Takarik::Data::TableBuilder.new("test_table", {name: "uuid", type: "UUID", auto_increment: false})
      builder.string("name")
      builder.integer("age")

      sql = builder.to_sql
      sql.should_not be_nil
      sql.should be_a(String)
      sql.should eq("CREATE TABLE test_table (uuid UUID PRIMARY KEY, name VARCHAR(255), age INTEGER)")
    end

    it "generates proper SQL from TableBuilder with no primary key" do
      builder = Takarik::Data::TableBuilder.new("test_table", nil)
      builder.string("name")
      builder.integer("age")

      sql = builder.to_sql
      sql.should_not be_nil
      sql.should be_a(String)
      sql.should eq("CREATE TABLE test_table (name VARCHAR(255), age INTEGER)")
    end

    it "generates proper SQL from TableBuilder with shorthand syntax" do
      builder = Takarik::Data::TableBuilder.new("test_table", {"id" => "UUID"})
      builder.string("name")
      builder.integer("age")

      sql = builder.to_sql
      sql.should_not be_nil
      sql.should be_a(String)
      sql.should eq("CREATE TABLE test_table (id UUID PRIMARY KEY, name VARCHAR(255), age INTEGER)")
    end

    it "generates proper SQL from TableBuilder with custom shorthand syntax" do
      builder = Takarik::Data::TableBuilder.new("test_table", {"user_id" => "BIGINT"})
      builder.string("name")
      builder.integer("age")

      sql = builder.to_sql
      sql.should_not be_nil
      sql.should be_a(String)
      sql.should eq("CREATE TABLE test_table (user_id BIGINT PRIMARY KEY, name VARCHAR(255), age INTEGER)")
    end
  end

  describe "migration workflow" do
    it "handles complete migration lifecycle with transactions" do
      # Step 1: Create users table
      create_migration = CreateUsersTable.new
      create_migration.run_up

      table_exists?("test_users").should be_true
      column_exists?("test_users", "name").should be_true
      index_exists?("idx_test_users_email").should be_true

      # Step 2: Add columns
      add_columns_migration = AddPhoneToUsers.new
      add_columns_migration.run_up

      column_exists?("test_users", "phone").should be_true
      column_exists?("test_users", "address").should be_true

      # Step 3: Rollback column addition
      add_columns_migration.run_down

      column_exists?("test_users", "phone").should be_false
      column_exists?("test_users", "address").should be_false
      table_exists?("test_users").should be_true  # Table should still exist

      # Step 4: Rollback table creation
      create_migration.run_down

      table_exists?("test_users").should be_false
      index_exists?("idx_test_users_email").should be_false
    end

    it "handles multiple independent migrations with transactions" do
      # Create two different tables
      users_migration = CreateUsersTable.new
      products_migration = CreateProductsTable.new

      # Run both up migrations
      users_migration.run_up
      products_migration.run_up

      # Verify both tables exist
      table_exists?("test_users").should be_true
      table_exists?("test_products").should be_true

      # Rollback one table
      users_migration.run_down

      # Verify only one table was affected
      table_exists?("test_users").should be_false
      table_exists?("test_products").should be_true

      # Rollback the other
      products_migration.run_down

      table_exists?("test_products").should be_false
    end
  end

  describe "error handling" do
    it "handles running same migration twice with transactions" do
      migration = CreateUsersTable.new

      # First run should succeed
      migration.run_up
      table_exists?("test_users").should be_true

      # Second run should raise an error
      expect_raises(Exception) do
        migration.run_up
      end

      # Table should still exist after failed second run
      table_exists?("test_users").should be_true
    end

    it "handles rollback of non-existent migration with transactions" do
      migration = CreateUsersTable.new

      # Trying to rollback without running up should raise an error
      expect_raises(Exception) do
        migration.run_down
      end
    end

    it "maintains database consistency after transaction rollback" do
      migration = FailingMigration.new

      # Record initial state
      initial_table_count = Takarik::Data.connection.query_one("SELECT COUNT(*) FROM sqlite_master WHERE type='table'", as: Int32)

      # Run failing migration
      expect_raises(Exception) do
        migration.run_up
      end

      # Verify database state is unchanged
      final_table_count = Takarik::Data.connection.query_one("SELECT COUNT(*) FROM sqlite_master WHERE type='table'", as: Int32)
      final_table_count.should eq(initial_table_count)
    end
  end

  describe "MySQL data types support" do
    it "creates table with MySQL-specific data types" do
      migration = CreateMySQLTypesTable.new

      # Run the up migration
      migration.run_up

      # Verify table was created
      table_exists?("test_mysql_types").should be_true

      # Verify various column types exist
      column_exists?("test_mysql_types", "code").should be_true
      column_exists?("test_mysql_types", "name").should be_true
      column_exists?("test_mysql_types", "description").should be_true
      column_exists?("test_mysql_types", "content").should be_true
      column_exists?("test_mysql_types", "document").should be_true
      column_exists?("test_mysql_types", "status").should be_true
      column_exists?("test_mysql_types", "priority").should be_true
      column_exists?("test_mysql_types", "count").should be_true
      column_exists?("test_mysql_types", "views").should be_true
      column_exists?("test_mysql_types", "large_number").should be_true
      column_exists?("test_mysql_types", "price").should be_true
      column_exists?("test_mysql_types", "score").should be_true
      column_exists?("test_mysql_types", "ratio").should be_true
      column_exists?("test_mysql_types", "precise_value").should be_true
      column_exists?("test_mysql_types", "birth_date").should be_true
      column_exists?("test_mysql_types", "event_time").should be_true
      column_exists?("test_mysql_types", "created_at").should be_true
      column_exists?("test_mysql_types", "updated_at").should be_true
      column_exists?("test_mysql_types", "year_value").should be_true
      column_exists?("test_mysql_types", "data").should be_true
      column_exists?("test_mysql_types", "variable_data").should be_true
      column_exists?("test_mysql_types", "file_data").should be_true
      column_exists?("test_mysql_types", "metadata").should be_true
      column_exists?("test_mysql_types", "active").should be_true
    end

    it "generates correct SQL for MySQL decimal types" do
      builder = Takarik::Data::TableBuilder.new("test_decimal")
      builder.decimal("price", precision: 8, scale: 2)
      builder.numeric("score", precision: 5, scale: 3)
      builder.decimal("simple_decimal")

      sql = builder.to_sql
      sql.should contain("price DECIMAL(8,2)")
      sql.should contain("score NUMERIC(5,3)")
      sql.should contain("simple_decimal DECIMAL")
    end

    it "generates correct SQL for MySQL enum and set types" do
      builder = Takarik::Data::TableBuilder.new("test_enum_set")
      builder.enum("status", ["active", "inactive", "pending"])
      builder.set("permissions", ["read", "write", "admin"])

      sql = builder.to_sql
      sql.should contain("status ENUM('active', 'inactive', 'pending')")
      sql.should contain("permissions SET('read', 'write', 'admin')")
    end
  end

  describe "PostgreSQL data types support" do
    it "creates table with PostgreSQL-specific data types" do
      migration = CreatePostgreSQLTypesTable.new

      # Run the up migration
      migration.run_up

      # Verify table was created
      table_exists?("test_postgresql_types").should be_true

      # Verify various column types exist
      column_exists?("test_postgresql_types", "id").should be_true
      column_exists?("test_postgresql_types", "code").should be_true
      column_exists?("test_postgresql_types", "name").should be_true
      column_exists?("test_postgresql_types", "description").should be_true
      column_exists?("test_postgresql_types", "priority").should be_true
      column_exists?("test_postgresql_types", "views").should be_true
      column_exists?("test_postgresql_types", "large_number").should be_true
      column_exists?("test_postgresql_types", "sequence_id").should be_true
      column_exists?("test_postgresql_types", "big_sequence_id").should be_true
      column_exists?("test_postgresql_types", "price").should be_true
      column_exists?("test_postgresql_types", "score").should be_true
      column_exists?("test_postgresql_types", "ratio").should be_true
      column_exists?("test_postgresql_types", "precise_value").should be_true
      column_exists?("test_postgresql_types", "birth_date").should be_true
      column_exists?("test_postgresql_types", "event_time").should be_true
      column_exists?("test_postgresql_types", "created_at").should be_true
      column_exists?("test_postgresql_types", "updated_at").should be_true
      column_exists?("test_postgresql_types", "duration").should be_true
      column_exists?("test_postgresql_types", "binary_data").should be_true
      column_exists?("test_postgresql_types", "metadata").should be_true
      column_exists?("test_postgresql_types", "structured_data").should be_true
      column_exists?("test_postgresql_types", "identifier").should be_true
      column_exists?("test_postgresql_types", "active").should be_true
      column_exists?("test_postgresql_types", "ip_address").should be_true
      column_exists?("test_postgresql_types", "network").should be_true
      column_exists?("test_postgresql_types", "mac_address").should be_true
      column_exists?("test_postgresql_types", "tags").should be_true
      column_exists?("test_postgresql_types", "numbers").should be_true
      column_exists?("test_postgresql_types", "location").should be_true
      column_exists?("test_postgresql_types", "coordinates").should be_true
    end

    it "generates correct SQL for PostgreSQL array types" do
      builder = Takarik::Data::TableBuilder.new("test_arrays")
      builder.array("tags", "TEXT")
      builder.array("numbers", "INTEGER")

      sql = builder.to_sql
      sql.should contain("tags TEXT[]")
      sql.should contain("numbers INTEGER[]")
    end

    it "generates correct SQL for PostgreSQL network types" do
      builder = Takarik::Data::TableBuilder.new("test_network")
      builder.inet("ip_address")
      builder.cidr("network")
      builder.macaddr("mac_address")

      sql = builder.to_sql
      sql.should contain("ip_address INET")
      sql.should contain("network CIDR")
      sql.should contain("mac_address MACADDR")
    end
  end

  describe "convenience methods" do
    it "creates table with convenience methods" do
      migration = CreateConvenienceMethodsTable.new

      # Run the up migration
      migration.run_up

      # Verify table was created
      table_exists?("test_convenience").should be_true

      # Verify convenience method columns exist
      column_exists?("test_convenience", "name").should be_true
      column_exists?("test_convenience", "created_at").should be_true
      column_exists?("test_convenience", "updated_at").should be_true
      column_exists?("test_convenience", "deleted_at").should be_true
      column_exists?("test_convenience", "user_id").should be_true
      column_exists?("test_convenience", "custom_field").should be_true
    end

    it "generates correct SQL for timestamps_tz" do
      builder = Takarik::Data::TableBuilder.new("test_timestamps_tz")
      builder.timestamps_tz

      sql = builder.to_sql
      sql.should contain("created_at TIMESTAMPTZ NOT NULL")
      sql.should contain("updated_at TIMESTAMPTZ NOT NULL")
    end

    it "generates correct SQL for soft_deletes" do
      builder = Takarik::Data::TableBuilder.new("test_soft_deletes")
      builder.soft_deletes

      sql = builder.to_sql
      sql.should contain("deleted_at TIMESTAMP")
    end

    it "generates correct SQL for references" do
      builder = Takarik::Data::TableBuilder.new("test_references")
      builder.references("user")

      sql = builder.to_sql
      sql.should contain("user_id BIGINT")
    end

    it "generates correct SQL for raw types" do
      builder = Takarik::Data::TableBuilder.new("test_raw")
      builder.raw("custom_field", "CUSTOM_TYPE(100)")

      sql = builder.to_sql
      sql.should contain("custom_field CUSTOM_TYPE(100)")
    end
  end

  describe "geometry types support" do
    it "creates table with geometry types" do
      migration = CreateGeometryTypesTable.new

      # Run the up migration
      migration.run_up

      # Verify table was created
      table_exists?("test_geometry").should be_true

      # Verify geometry columns exist
      column_exists?("test_geometry", "name").should be_true
      column_exists?("test_geometry", "shape").should be_true
      column_exists?("test_geometry", "location").should be_true
      column_exists?("test_geometry", "path").should be_true
      column_exists?("test_geometry", "boundary").should be_true
      column_exists?("test_geometry", "created_at").should be_true
      column_exists?("test_geometry", "updated_at").should be_true
    end

    it "generates correct SQL for geometry types" do
      builder = Takarik::Data::TableBuilder.new("test_geo")
      builder.geometry("shape")
      builder.point("location")
      builder.linestring("path")
      builder.polygon("boundary")

      sql = builder.to_sql
      sql.should contain("shape GEOMETRY")
      sql.should contain("location POINT")
      sql.should contain("path LINESTRING")
      sql.should contain("boundary POLYGON")
    end
  end
end
