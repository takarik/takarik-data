require "./spec_helper"

# Test model for order functionality
class ProductOrder < Takarik::Data::BaseModel
  table_name "products_order"
  column :name, String
  column :price, Float64
  column :category, String
  column :rating, Int32
  column :stock, Int32

  timestamps
end

describe "Enhanced Order Methods" do
  before_each do
    # Set up products table
    Takarik::Data::BaseModel.connection.exec "DROP TABLE IF EXISTS products_order"
    Takarik::Data::BaseModel.connection.exec <<-SQL
      CREATE TABLE products_order (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT,
        price REAL,
        category TEXT,
        rating INTEGER,
        stock INTEGER,
        created_at TEXT,
        updated_at TEXT
      )
    SQL

    # Create test data with specific order for testing
    ProductOrder.create(name: "Laptop", price: 999.99, category: "Electronics", rating: 5, stock: 10)
    ProductOrder.create(name: "Mouse", price: 25.99, category: "Electronics", rating: 4, stock: 50)
    ProductOrder.create(name: "Book", price: 15.99, category: "Books", rating: 5, stock: 100)
    ProductOrder.create(name: "Tablet", price: 299.99, category: "Electronics", rating: 4, stock: 25)
    ProductOrder.create(name: "Novel", price: 12.99, category: "Books", rating: 3, stock: 75)
  end

  describe "order by single column with Symbol" do
    it "orders by Symbol column ascending by default" do
      products = ProductOrder.order(:price).to_a
      products.size.should eq(5)
      products[0].price.should eq(12.99)  # Novel
      products[1].price.should eq(15.99)  # Book
      products[2].price.should eq(25.99)  # Mouse
      products[3].price.should eq(299.99) # Tablet
      products[4].price.should eq(999.99) # Laptop
    end

    it "generates correct SQL for Symbol ordering" do
      sql = ProductOrder.order(:price).to_sql
      sql.should contain("ORDER BY price ASC")
    end
  end

  describe "order by single column with String and direction" do
    it "orders ascending" do
      products = ProductOrder.order("price", "ASC").to_a
      products[0].price.should eq(12.99)
      products[4].price.should eq(999.99)
    end

    it "orders descending" do
      products = ProductOrder.order("price", "DESC").to_a
      products[0].price.should eq(999.99) # Laptop
      products[4].price.should eq(12.99)  # Novel
    end

    it "generates correct SQL for string ordering" do
      asc_sql = ProductOrder.order("price", "ASC").to_sql
      desc_sql = ProductOrder.order("price", "DESC").to_sql

      asc_sql.should contain("ORDER BY price ASC")
      desc_sql.should contain("ORDER BY price DESC")
    end
  end

  describe "order by multiple columns using keyword arguments" do
    it "orders by multiple columns correctly" do
      products = ProductOrder.order(category: "ASC", price: "DESC").to_a

      # Should be ordered by category ASC, then price DESC within each category
      # Books: Novel (12.99), Book (15.99)
      # Electronics: Laptop (999.99), Tablet (299.99), Mouse (25.99)
      products[0].category.should eq("Books")
      products[0].price.should eq(15.99) # Book (higher price in Books)

      products[1].category.should eq("Books")
      products[1].price.should eq(12.99) # Novel (lower price in Books)

      products[2].category.should eq("Electronics")
      products[2].price.should eq(999.99) # Laptop (highest price in Electronics)
    end

    it "generates correct SQL for multiple column ordering" do
      sql = ProductOrder.order(category: "ASC", price: "DESC").to_sql
      sql.should contain("ORDER BY category ASC, price DESC")
    end
  end

  describe "order by Hash with complex syntax" do
    it "handles simple hash syntax" do
      products = ProductOrder.order({"rating" => "DESC", "price" => "ASC"}).to_a

      # Should order by rating DESC, then price ASC
      # Rating 5: Book (15.99), Laptop (999.99)
      # Rating 4: Mouse (25.99), Tablet (299.99)
      # Rating 3: Novel (12.99)
      products[0].rating.should eq(5)
      products[0].price.should eq(15.99) # Book (lowest price among rating 5)

      products[1].rating.should eq(5)
      products[1].price.should eq(999.99) # Laptop
    end

    it "generates correct SQL for hash ordering" do
      simple_hash_sql = ProductOrder.order({"rating" => "DESC", "price" => "ASC"}).to_sql
      simple_hash_sql.should contain("ORDER BY rating DESC, price ASC")
    end
  end

  describe "order by first column with additional keyword arguments" do
    it "combines first column with additional columns" do
      products = ProductOrder.order(:category, price: "DESC", rating: "ASC").to_a

      # Category ASC (first), then price DESC, then rating ASC
      products[0].category.should eq("Books")
      products[2].category.should eq("Electronics")
    end

    it "generates correct SQL for mixed ordering" do
      sql = ProductOrder.order(:category, price: "DESC", rating: "ASC").to_sql
      sql.should contain("ORDER BY category ASC, price DESC, rating ASC")
    end
  end

  describe "order by array of columns" do
    it "orders by array of column names" do
      products = ProductOrder.order(["category", "price"]).to_a

      # Should order by category ASC, price ASC (default)
      products[0].category.should eq("Books")
      products[0].price.should eq(12.99) # Novel (cheaper book)

      products[1].category.should eq("Books")
      products[1].price.should eq(15.99) # Book

      products[2].category.should eq("Electronics")
    end

    it "generates correct SQL for array ordering" do
      sql = ProductOrder.order(["category", "price"]).to_sql
      sql.should contain("ORDER BY category ASC, price ASC")
    end
  end

  describe "order method chaining" do
    it "allows chaining with where clauses" do
      products = ProductOrder.where(category: "Electronics").order(:price).to_a
      products.size.should eq(3)
      products[0].price.should eq(25.99)  # Mouse
      products[1].price.should eq(299.99) # Tablet
      products[2].price.should eq(999.99) # Laptop
    end

    it "allows chaining after order" do
      products = ProductOrder.order(:price).where(category: "Electronics").to_a
      products.size.should eq(3)
      # Order should still be by price even though where comes after
      products[0].price.should eq(25.99)
    end

    it "generates correct SQL for chained ordering" do
      sql = ProductOrder.where(category: "Electronics").order(:price).to_sql
      sql.should contain("WHERE category = ?")
      sql.should contain("ORDER BY price ASC")
    end
  end

  describe "order with limit and offset" do
    it "works with limit" do
      products = ProductOrder.order(:price).limit(3).to_a
      products.size.should eq(3)
      products[0].price.should eq(12.99)
      products[2].price.should eq(25.99)
    end

    it "works with offset and limit" do
      products = ProductOrder.order(:price).offset(2).limit(2).to_a
      products.size.should eq(2)
      products[0].price.should eq(25.99)  # Mouse (3rd in order)
      products[1].price.should eq(299.99) # Tablet (4th in order)
    end

    it "generates correct SQL with limit and offset" do
      sql = ProductOrder.order(:price).offset(2).limit(2).to_sql
      sql.should contain("ORDER BY price ASC")
      sql.should contain("LIMIT 2 OFFSET 2")
    end
  end

  describe "order edge cases" do
    it "handles empty result sets" do
      products = ProductOrder.where(category: "NonExistent").order(:price).to_a
      products.size.should eq(0)
    end

    it "handles null/nil values correctly" do
      # Create product with nil rating
      ProductOrder.create(name: "Test", price: 50.0, category: "Test", rating: nil, stock: 0)

      products = ProductOrder.order(:rating).to_a
      products.size.should eq(6)
      # Nil values should come first in ASC order (SQLite behavior)
      products[0].rating.should be_nil
    end

    it "handles case-insensitive string ordering" do
      # Test that string ordering works as expected
      products = ProductOrder.order(:name).to_a
      # Should be alphabetical: Book, Laptop, Mouse, Novel, Tablet
      products[0].name.should eq("Book")
      products[1].name.should eq("Laptop")
      products[2].name.should eq("Mouse")
      products[3].name.should eq("Novel")
      products[4].name.should eq("Tablet")
    end
  end

  describe "SQL generation verification" do
    it "generates clean SQL for all order variants" do
      # Symbol ordering
      symbol_sql = ProductOrder.order(:price).to_sql
      symbol_sql.should eq("SELECT * FROM products_order ORDER BY price ASC")

      # String with direction
      string_sql = ProductOrder.order("price", "DESC").to_sql
      string_sql.should eq("SELECT * FROM products_order ORDER BY price DESC")

      # Multiple columns with keywords
      multi_sql = ProductOrder.order(category: "ASC", price: "DESC").to_sql
      multi_sql.should eq("SELECT * FROM products_order ORDER BY category ASC, price DESC")

      # Array of columns
      array_sql = ProductOrder.order(["category", "price"]).to_sql
      array_sql.should eq("SELECT * FROM products_order ORDER BY category ASC, price ASC")
    end
  end

  describe "order_by method (alias)" do
    it "works as alias for order" do
      products1 = ProductOrder.order("price", "DESC").to_a
      products2 = ProductOrder.all.order_by("price", "DESC").to_a

      products1.size.should eq(products2.size)
      products1[0].price.should eq(products2[0].price)
    end

    it "generates same SQL as order method" do
      order_sql = ProductOrder.order("price", "DESC").to_sql
      order_by_sql = ProductOrder.all.order_by("price", "DESC").to_sql

      order_sql.should eq(order_by_sql)
    end
  end
end
