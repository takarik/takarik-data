require "./spec_helper"

describe "Scope Chaining" do
  before_each do
    # Set up test table
    Takarik::Data::BaseModel.connection.exec "DROP TABLE IF EXISTS chainable_products"
    Takarik::Data::BaseModel.connection.exec <<-SQL
      CREATE TABLE chainable_products (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        price REAL,
        active INTEGER DEFAULT 1,
        category TEXT,
        stock_count INTEGER DEFAULT 0,
        published_year INTEGER,
        created_at TEXT,
        updated_at TEXT
      )
    SQL
  end

  describe "direct scope chaining" do
    it "chains simple scopes without arguments" do
      ChainableProduct.create(name: "Laptop", price: 1200.0, active: 1, category: "Electronics", stock_count: 5)
      ChainableProduct.create(name: "Mouse", price: 25.0, active: 1, category: "Electronics", stock_count: 50)
      ChainableProduct.create(name: "Expensive Book", price: 150.0, active: 1, category: "Books", stock_count: 1)
      ChainableProduct.create(name: "Inactive Laptop", price: 1500.0, active: 0, category: "Electronics", stock_count: 0)

      # Test simple chaining: active.expensive
      result = ChainableProduct.active.expensive.to_a
      result.size.should eq(2)
      result.map(&.name).should contain("Laptop")
      result.map(&.name).should contain("Expensive Book")

      # Verify SQL generation
      sql = ChainableProduct.active.expensive.to_sql
      sql.should contain("WHERE")
      sql.should contain("active = ?")
      sql.should contain("price > ?")
    end

    it "chains scopes with arguments" do
      ChainableProduct.create(name: "Laptop", price: 1200.0, active: 1, category: "Electronics", stock_count: 5)
      ChainableProduct.create(name: "Mouse", price: 25.0, active: 1, category: "Electronics", stock_count: 50)
      ChainableProduct.create(name: "Book", price: 15.0, active: 1, category: "Books", stock_count: 2)

      # Test parameterized chaining: active.in_category("Electronics").costs_more_than(100.0)
      result = ChainableProduct.active.in_category("Electronics").costs_more_than(100.0).to_a
      result.size.should eq(1)
      result.first.name.should eq("Laptop")

      # Verify SQL generation
      sql = ChainableProduct.active.in_category("Electronics").costs_more_than(100.0).to_sql
      sql.should contain("WHERE")
      sql.should contain("active = ?")
      sql.should contain("category = ?")
      sql.should contain("price > ?")
    end

    it "chains multiple parameterized scopes" do
      ChainableProduct.create(name: "Laptop", price: 1200.0, active: 1, category: "Electronics", stock_count: 5)
      ChainableProduct.create(name: "Expensive Book", price: 150.0, active: 1, category: "Books", stock_count: 1)
      ChainableProduct.create(name: "Cheap Book", price: 15.0, active: 1, category: "Books", stock_count: 20)

      # Test multiple parameterized scopes: costs_more_than(100.0).low_stock(10)
      result = ChainableProduct.costs_more_than(100.0).low_stock(10).to_a
      result.size.should eq(2)
      result.map(&.name).should contain("Laptop")
      result.map(&.name).should contain("Expensive Book")

      # Verify SQL generation - should have both conditions
      sql = ChainableProduct.costs_more_than(100.0).low_stock(10).to_sql
      sql.should contain("WHERE")
      sql.should contain("price > ?")
      sql.should contain("stock_count < ?")
      sql.should contain("AND")  # Should have both conditions joined with AND
    end

    it "mixes scope chaining with QueryBuilder methods" do
      ChainableProduct.create(name: "Laptop", price: 1200.0, active: 1, category: "Electronics", stock_count: 5)
      ChainableProduct.create(name: "Expensive Book", price: 150.0, active: 1, category: "Books", stock_count: 1)
      ChainableProduct.create(name: "Out of Stock Item", price: 200.0, active: 1, category: "Electronics", stock_count: 0)

      # Test scope + QueryBuilder methods: active.expensive.where("stock_count > ?", 0).order(:price)
      result = ChainableProduct.active.expensive.where("stock_count > ?", 0).order(:price).to_a
      result.size.should eq(2)
      result.first.name.should eq("Expensive Book")  # Lower price, should be first
      result.last.name.should eq("Laptop")

      # Verify SQL generation
      sql = ChainableProduct.active.expensive.where("stock_count > ?", 0).order(:price).to_sql
      sql.should contain("WHERE")
      sql.should contain("active = ?")
      sql.should contain("price > ?")
      sql.should contain("stock_count > ?")
      sql.should contain("ORDER BY price ASC")
    end

    it "handles complex multi-scope chains" do
      ChainableProduct.create(name: "Laptop", price: 1200.0, active: 1, category: "Electronics", stock_count: 5)
      ChainableProduct.create(name: "Expensive Book", price: 150.0, active: 1, category: "Books", stock_count: 1)
      ChainableProduct.create(name: "Cheap Book", price: 15.0, active: 1, category: "Books", stock_count: 2)
      ChainableProduct.create(name: "Inactive Book", price: 200.0, active: 0, category: "Books", stock_count: 5)

      # Test complex chaining: active.in_category("Books").costs_more_than(50.0).in_stock
      result = ChainableProduct.active.in_category("Books").costs_more_than(50.0).in_stock.to_a
      result.size.should eq(1)
      result.first.name.should eq("Expensive Book")

      # Verify SQL generation
      sql = ChainableProduct.active.in_category("Books").costs_more_than(50.0).in_stock.to_sql
      sql.should contain("WHERE")
      sql.should contain("active = ?")
      sql.should contain("category = ?")
      sql.should contain("price > ?")
      sql.should contain("stock_count > ?")
    end

    it "works with conditional scopes" do
      ChainableProduct.create(name: "Book", price: 15.0, active: 1, category: "Books", stock_count: 2, published_year: 2015)

      # Test conditional scope chaining
      result1 = ChainableProduct.active.published_before(2020).to_a
      result1.size.should eq(1)  # Should find the book published in 2015

      result2 = ChainableProduct.active.published_before(nil).to_a
      result2.size.should eq(1)  # Should return all since condition is nil
    end

    it "maintains chainability even when scopes return all" do
      ChainableProduct.create(name: "Book", price: 15.0, active: 1, category: "Books", stock_count: 2)

      # Test that chaining still works when a scope returns all
      result = ChainableProduct.published_before(nil).active.to_a
      result.size.should eq(1)

      # Verify SQL shows the active condition
      sql = ChainableProduct.published_before(nil).active.to_sql
      sql.should contain("WHERE")
      sql.should contain("active = ?")
    end
  end
end

class ChainableProduct < Takarik::Data::BaseModel
  table_name "chainable_products"
  column :name, String
  column :price, Float64
  column :active, Bool
  column :category, String
  column :stock_count, Int32
  column :published_year, Int32

  timestamps

  scope :active do
    where(active: true)
  end

  scope :expensive do
    where("price > ?", 100.0)
  end

  scope :in_category do |category|
    where(category: category)
  end

  scope :costs_more_than do |amount|
    where("price > ?", amount)
  end

  scope :in_stock do
    where("stock_count > ?", 0)
  end

  scope :low_stock do |threshold|
    actual_threshold = threshold || 10
    return where("stock_count < ?", actual_threshold)
  end

  scope :published_before do |year|
    if year && year > 0
      where("published_year < ?", year)
    else
      # Return all records when condition is not met
      all
    end
  end
end
