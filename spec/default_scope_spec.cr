require "./spec_helper"

# Test models for default scope
class ProductDefaultScope < Takarik::Data::BaseModel
  table_name "products_default_scope"
  column :name, String
  column :price, Float64
  column :active, Bool
  column :category, String

  timestamps

  # Set a default scope to only show active products
  default_scope do
    where(active: true)
  end

  # Named scope for expensive products
  scope :expensive do
    where("price >", 100.0)
  end
end

class UserNoDefaultScope < Takarik::Data::BaseModel
  table_name "users_no_default_scope"
  column :name, String
  column :email, String
  column :active, Bool

  # No default scope defined

  scope :active do
    where(active: true)
  end
end

describe "Default Scope" do
  before_each do
    # Set up products table
    Takarik::Data::BaseModel.connection.exec "DROP TABLE IF EXISTS products_default_scope"
    Takarik::Data::BaseModel.connection.exec <<-SQL
      CREATE TABLE products_default_scope (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        price REAL,
        active INTEGER DEFAULT 1,
        category TEXT,
        created_at TEXT,
        updated_at TEXT
      )
    SQL

    # Set up users table
    Takarik::Data::BaseModel.connection.exec "DROP TABLE IF EXISTS users_no_default_scope"
    Takarik::Data::BaseModel.connection.exec <<-SQL
      CREATE TABLE users_no_default_scope (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        email TEXT,
        active INTEGER DEFAULT 1
      )
    SQL

    # Create test data using boolean values (false becomes nil in SQLite)
    ProductDefaultScope.create(name: "Active Laptop", price: 999.99, active: true, category: "Electronics")
    ProductDefaultScope.create(name: "Active Mouse", price: 25.99, active: true, category: "Electronics")
    ProductDefaultScope.create(name: "Inactive Phone", price: 50.0, active: false, category: "Electronics")

    UserNoDefaultScope.create(name: "Active User", email: "active@test.com", active: true)
    UserNoDefaultScope.create(name: "Inactive User", email: "inactive@test.com", active: false)
  end

  describe "with default scope defined" do
    it "applies default scope to .all queries" do
      products = ProductDefaultScope.all.to_a
      products.size.should eq(2) # Only active products
      products.all? { |p| p.active == true }.should be_true
    end

    it "applies default scope to .count" do
      ProductDefaultScope.count.should eq(2)
    end

    it "applies default scope to .first" do
      first_product = ProductDefaultScope.first
      first_product.should_not be_nil
      first_product.try(&.active).should eq(true)
    end

    it "applies default scope to .where queries" do
      expensive_products = ProductDefaultScope.where("price >", 100.0).to_a
      expensive_products.size.should eq(1) # Only active expensive products
      expensive_products.all? { |p| p.active == true }.should be_true
      expensive_products.all? { |p| p.price.try(&.>(100.0)) }.should be_true
    end

    it "applies default scope to named scopes" do
      expensive_products = ProductDefaultScope.expensive.to_a
      expensive_products.size.should eq(1) # Only active expensive products
      expensive_products.all? { |p| p.active == true }.should be_true
    end

    it "generates SQL with default scope conditions" do
      sql = ProductDefaultScope.all.to_sql
      sql.should contain("WHERE active = ?")
    end

    it "bypasses default scope with .unscoped" do
      all_products = ProductDefaultScope.unscoped.to_a
      all_products.size.should eq(3) # All products including inactive

      active_count = all_products.count { |p| p.active == true }
      inactive_count = all_products.count { |p| p.active.nil? }
      active_count.should eq(2)
      inactive_count.should eq(1)
    end

    it "generates clean SQL with .unscoped" do
      sql = ProductDefaultScope.unscoped.to_sql
      sql.should_not contain("active")
      sql.should eq("SELECT * FROM products_default_scope")
    end

    it "allows chaining after .unscoped" do
      # Note: false values are stored as 0 in SQLite but loaded as nil
      # We need to query for records where active = 0
      inactive_products = ProductDefaultScope.unscoped.where("active = ?", 0).to_a
      inactive_products.size.should eq(1)
      inactive_products.all? { |p| p.active.nil? }.should be_true
    end

    it "applies default scope to .find method" do
      # Get an inactive product ID (where active = 0)
      inactive_product = ProductDefaultScope.unscoped.where("active = ?", 0).first
      inactive_product.should_not be_nil

      if inactive_product
        # find with default scope should not find inactive product
        found_product = ProductDefaultScope.find(inactive_product.id)
        found_product.should be_nil

        # unscoped where should find it
        unscoped_found = ProductDefaultScope.unscoped.where(id: inactive_product.id).first
        unscoped_found.should_not be_nil
      end
    end

    it "works with complex chaining" do
      complex_query = ProductDefaultScope.where(category: "Electronics").where("price >", 50.0).to_a
      complex_query.size.should eq(1) # Only active electronics > $50
      complex_query.all? { |p| p.active == true }.should be_true
      complex_query.all? { |p| p.category == "Electronics" }.should be_true
      complex_query.all? { |p| p.price.try(&.>(50.0)) }.should be_true
    end
  end

  describe "without default scope defined" do
    it "returns all records with .all" do
      users = UserNoDefaultScope.all.to_a
      users.size.should eq(2) # All users
    end

    it "generates clean SQL without default scope" do
      sql = UserNoDefaultScope.all.to_sql
      sql.should eq("SELECT * FROM users_no_default_scope")
    end

    it ".unscoped works the same as .all" do
      all_users = UserNoDefaultScope.all.to_a
      unscoped_users = UserNoDefaultScope.unscoped.to_a

      all_users.size.should eq(unscoped_users.size)
      all_users.size.should eq(2)
    end

    it "works with named scopes normally" do
      active_users = UserNoDefaultScope.active.to_a
      active_users.size.should eq(1)
      active_users.all? { |u| u.active == true }.should be_true
    end
  end

  describe "SQL generation comparison" do
    it "shows difference between scoped and unscoped SQL" do
      scoped_sql = ProductDefaultScope.all.to_sql
      unscoped_sql = ProductDefaultScope.unscoped.to_sql

      scoped_sql.should contain("WHERE active = ?")
      unscoped_sql.should_not contain("WHERE")
      unscoped_sql.should eq("SELECT * FROM products_default_scope")
    end

    it "shows chaining after unscoped maintains clean queries" do
      chained_sql = ProductDefaultScope.unscoped.where(category: "Electronics").to_sql
      chained_sql.should contain("WHERE category = ?")
      chained_sql.should_not contain("active")
    end
  end
end
