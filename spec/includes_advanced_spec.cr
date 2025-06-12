require "./spec_helper"

# Additional models for testing complex includes scenarios
class Customer < Takarik::Data::BaseModel
  table_name "customers"

  column name, String
  column email, String

  has_many orders, class_name: Order, foreign_key: :customer_id
  has_many reviews, class_name: Review, foreign_key: :customer_id

  timestamps
end

class Order < Takarik::Data::BaseModel
  table_name "orders"

  column total, Float64
  column customer_id, Int64

  belongs_to customer, class_name: Customer, foreign_key: :customer_id
  has_many order_items, class_name: OrderItem, foreign_key: :order_id

  timestamps
end

class OrderItem < Takarik::Data::BaseModel
  table_name "order_items"

  column quantity, Int32
  column price, Float64
  column order_id, Int64
  column book_id, Int64

  belongs_to order, class_name: Order, foreign_key: :order_id
  belongs_to book, class_name: BookAdvanced, foreign_key: :book_id

  timestamps
end

class BookAdvanced < Takarik::Data::BaseModel
  table_name "books_advanced"

  column title, String
  column out_of_print, Bool
  column author_id, Int64
  column supplier_id, Int64

  belongs_to author, class_name: AuthorAdvanced, foreign_key: :author_id
  belongs_to supplier, class_name: Supplier, foreign_key: :supplier_id
  has_many order_items, class_name: OrderItem, foreign_key: :book_id
  has_many reviews, class_name: Review, foreign_key: :book_id

  timestamps
end

class AuthorAdvanced < Takarik::Data::BaseModel
  table_name "authors_advanced"

  column first_name, String
  column last_name, String

  has_many books, class_name: BookAdvanced, foreign_key: :author_id

  timestamps
end

class Supplier < Takarik::Data::BaseModel
  table_name "suppliers"

  column name, String
  column address, String

  has_many books, class_name: BookAdvanced, foreign_key: :supplier_id

  timestamps
end

class Review < Takarik::Data::BaseModel
  table_name "reviews"

  column rating, Int32
  column comment, String
  column customer_id, Int64
  column book_id, Int64

  belongs_to customer, class_name: Customer, foreign_key: :customer_id
  belongs_to book, class_name: BookAdvanced, foreign_key: :book_id

  timestamps
end

describe "Advanced Includes Testing - ActiveRecord Compliance" do
  before_each do
    # Create all necessary tables
    create_test_tables

    # Clean up existing data
    clean_test_data
  end

  describe "Basic includes per ActiveRecord specification" do
    it "should work with the exact ActiveRecord pattern" do
      # Create test data
      author1 = AuthorAdvanced.create(first_name: "J.K.", last_name: "Rowling")
      author2 = AuthorAdvanced.create(first_name: "Stephen", last_name: "King")

      supplier = Supplier.create(name: "Book Supplier", address: "123 Main St")

      books = [
        BookAdvanced.create(title: "Harry Potter 1", author_id: author1.id, supplier_id: supplier.id, out_of_print: false),
        BookAdvanced.create(title: "Harry Potter 2", author_id: author1.id, supplier_id: supplier.id, out_of_print: false),
        BookAdvanced.create(title: "The Shining", author_id: author2.id, supplier_id: supplier.id, out_of_print: false),
        BookAdvanced.create(title: "It", author_id: author2.id, supplier_id: supplier.id, out_of_print: true),
        BookAdvanced.create(title: "Pet Sematary", author_id: author2.id, supplier_id: supplier.id, out_of_print: false),
      ]

      # Execute the includes query
      books_with_includes = BookAdvanced.includes(:author).limit(5)

      # Check the generated SQL
      sql = books_with_includes.to_sql

      # Execute and check results
      books_loaded = books_with_includes.to_a

      # Access authors and verify behavior
      author_names = [] of String
      books_loaded.each_with_index do |book, index|
        author = book.author
        if author
          author_name = author.last_name
          author_names << author_name.to_s if author_name
        end
      end

      # Verify we got the expected data
      books_loaded.size.should eq(5)
      author_names.size.should eq(5)
      author_names.should contain("Rowling")
      author_names.should contain("King")
    end
  end

  describe "Multiple associations loading" do
    it "should support Customer.includes(:orders, :reviews)" do
      # Create test data
      customer = Customer.create(name: "John Doe", email: "john@example.com")

      # Create a book for the reviews (since Review model has book_id)
      author = AuthorAdvanced.create(first_name: "Test", last_name: "Author")
      supplier = Supplier.create(name: "Test Supplier", address: "123 Test St")
      book = BookAdvanced.create(
        title: "Test Book",
        author_id: author.id,
        supplier_id: supplier.id,
        out_of_print: false
      )

      # Create orders
      order1 = Order.create(total: 100.0, customer_id: customer.id)
      order2 = Order.create(total: 200.0, customer_id: customer.id)

      # Create reviews with both customer_id and book_id
      review1 = Review.create(rating: 5, comment: "Great!", customer_id: customer.id, book_id: book.id)
      review2 = Review.create(rating: 4, comment: "Good", customer_id: customer.id, book_id: book.id)

      # Test multiple includes
      customers = Customer.includes(:orders, :reviews).to_a
      customer_loaded = customers.first

      customer_loaded.should_not be_nil
      customer_loaded.name.should eq("John Doe")

      # Access associations
      orders = customer_loaded.orders.to_a
      reviews = customer_loaded.reviews.to_a

      orders.size.should eq(2)
      reviews.size.should eq(2)
    end
  end

  describe "Performance comparison" do
    it "demonstrates includes efficiency vs N+1 problem" do
      # Create test data at scale
      authors = [
        AuthorAdvanced.create(first_name: "Author", last_name: "One"),
        AuthorAdvanced.create(first_name: "Author", last_name: "Two"),
        AuthorAdvanced.create(first_name: "Author", last_name: "Three"),
      ]

      supplier = Supplier.create(name: "Big Publisher", address: "Main Street")

      # Create 15 books distributed among authors
      15.times do |i|
        author = authors[i % authors.size]
        BookAdvanced.create(
          title: "Book #{i + 1}",
          author_id: author.id,
          supplier_id: supplier.id,
          out_of_print: i % 4 == 0 # Every 4th book is out of print
        )
      end

      # Simulate N+1
      books_n1 = BookAdvanced.limit(15).to_a
      author_names_n1 = [] of String
      books_n1.each do |book|
        author = book.author
        if author
          author_name = author.last_name
          author_names_n1 << author_name.to_s if author_name
        end
      end

      # Use includes
      books_includes = BookAdvanced.includes(:author).limit(15).to_a
      author_names_includes = [] of String
      books_includes.each do |book|
        author = book.author
        if author
          author_name = author.last_name
          author_names_includes << author_name.to_s if author_name
        end
      end

      # Verify both approaches give same results
      books_n1.size.should eq(books_includes.size)
      author_names_n1.sort.should eq(author_names_includes.sort)
    end
  end

  describe "Complex nested joins (Rails compatibility)" do
    it "should handle Author.joins(books: [{ reviews: { customer: :orders } }, :supplier])" do
      # Create comprehensive test data
      author = AuthorAdvanced.create(first_name: "Complex", last_name: "Author")
      supplier = Supplier.create(name: "Test Supplier", address: "123 Test St")

      book = BookAdvanced.create(
        title: "Complex Book",
        author_id: author.id,
        supplier_id: supplier.id,
        out_of_print: false
      )

      customer = Customer.create(name: "Test Customer", email: "test@example.com")

      order = Order.create(total: 99.99, customer_id: customer.id)

      review = Review.create(
        rating: 5,
        comment: "Great book!",
        customer_id: customer.id,
        book_id: book.id
      )

      # Let's test step by step to understand the expected behavior

      # Step 1: Simple join
      simple_query = AuthorAdvanced.joins(:books)

      # Step 2: Two level nested
      two_level_query = AuthorAdvanced.joins(books: :reviews)

      # Step 3: Three level nested (this should work)
      three_level_query = AuthorAdvanced.joins(books: {reviews: :customer})

      # Step 4: Four level nested (this should work now!)
      four_level_query = AuthorAdvanced.joins(books: {reviews: {customer: :orders}})

      # Step 5: Test the full complex nested join with array
      full_query = AuthorAdvanced.joins(books: [{reviews: {customer: :orders}}, :supplier])

      # Test that all work
      simple_query.to_a.size.should eq(1)
      two_level_query.to_a.size.should eq(1)
      three_level_query.to_a.size.should eq(1)
      four_level_query.to_a.size.should eq(1)
      full_query.to_a.size.should eq(1)
    end

    it "should handle simpler nested joins like User.joins(posts: [:comments])" do
      # This should still work with our enhanced implementation
      author = AuthorAdvanced.create(first_name: "Simple", last_name: "Author")
      supplier = Supplier.create(name: "Simple Supplier", address: "456 Simple St")

      book = BookAdvanced.create(
        title: "Simple Book",
        author_id: author.id,
        supplier_id: supplier.id,
        out_of_print: false
      )

      customer = Customer.create(name: "Simple Customer", email: "simple@example.com")

      review = Review.create(
        rating: 4,
        comment: "Good book",
        customer_id: customer.id,
        book_id: book.id
      )

      # Test simple nested join
      query = AuthorAdvanced.joins(books: [:reviews])

      sql = query.to_sql

      sql.should contain("INNER JOIN books_advanced ON")
      sql.should contain("INNER JOIN reviews ON")

      results = query.to_a
      results.size.should eq(1)
      results.first.last_name.should eq("Author")
    end

    it "should handle mixed array content like books: [:supplier, { reviews: :customer }]" do
      # Create test data
      author = AuthorAdvanced.create(first_name: "Mixed", last_name: "Author")
      supplier = Supplier.create(name: "Mixed Supplier", address: "789 Mixed Ave")

      book = BookAdvanced.create(
        title: "Mixed Book",
        author_id: author.id,
        supplier_id: supplier.id,
        out_of_print: false
      )

      customer = Customer.create(name: "Mixed Customer", email: "mixed@example.com")

      review = Review.create(
        rating: 3,
        comment: "Okay book",
        customer_id: customer.id,
        book_id: book.id
      )

      # Test mixed array content
      query = AuthorAdvanced.joins(books: [:supplier, {reviews: :customer}])

      sql = query.to_sql

      sql.should contain("INNER JOIN books_advanced ON")
      sql.should contain("INNER JOIN suppliers ON")
      sql.should contain("INNER JOIN reviews ON")
      sql.should contain("INNER JOIN customers ON")

      results = query.to_a
      results.size.should eq(1)
      results.first.last_name.should eq("Author")
    end
  end
end

def create_test_tables
  connection = Takarik::Data.connection

  # Drop tables if they exist (in reverse dependency order)
  tables_to_drop = [
    "order_items", "reviews", "orders", "books_advanced",
    "customers", "authors_advanced", "suppliers",
  ]

  tables_to_drop.each do |table|
    begin
      connection.exec("DROP TABLE IF EXISTS #{table}")
    rescue
      # Table might not exist
    end
  end

  # Create tables
  connection.exec <<-SQL
      CREATE TABLE customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(255),
        email VARCHAR(255),
        created_at DATETIME,
        updated_at DATETIME
      )
    SQL

  connection.exec <<-SQL
      CREATE TABLE authors_advanced (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        created_at DATETIME,
        updated_at DATETIME
      )
    SQL

  connection.exec <<-SQL
      CREATE TABLE suppliers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name VARCHAR(255),
        address VARCHAR(255),
        created_at DATETIME,
        updated_at DATETIME
      )
    SQL

  connection.exec <<-SQL
      CREATE TABLE books_advanced (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title VARCHAR(255),
        out_of_print BOOLEAN DEFAULT 0,
        author_id INTEGER,
        supplier_id INTEGER,
        created_at DATETIME,
        updated_at DATETIME
      )
    SQL

  connection.exec <<-SQL
      CREATE TABLE orders (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        total REAL,
        customer_id INTEGER,
        created_at DATETIME,
        updated_at DATETIME
      )
    SQL

  connection.exec <<-SQL
      CREATE TABLE order_items (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        quantity INTEGER,
        price REAL,
        order_id INTEGER,
        book_id INTEGER,
        created_at DATETIME,
        updated_at DATETIME
      )
    SQL

  connection.exec <<-SQL
      CREATE TABLE reviews (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rating INTEGER,
        comment TEXT,
        customer_id INTEGER,
        book_id INTEGER,
        created_at DATETIME,
        updated_at DATETIME
      )
    SQL
end

def clean_test_data
  connection = Takarik::Data.connection

  tables_to_clean = [
    "order_items", "reviews", "orders", "books_advanced",
    "customers", "authors_advanced", "suppliers",
  ]

  tables_to_clean.each do |table|
    begin
      connection.exec("DELETE FROM #{table}")
    rescue
      # Table might not exist yet
    end
  end
end
