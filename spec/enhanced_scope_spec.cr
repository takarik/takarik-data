require "./spec_helper"

# Test models for enhanced scopes
class BookScopes < Takarik::Data::BaseModel
  table_name "books_scopes"
  column :title, String
  column :price, Float64
  column :out_of_print, Bool
  column :published_year, Int32
  column :author_id, Int32
  column :category, String

  timestamps

  # 1. Simple scope without arguments
  scope :out_of_print do
    where(out_of_print: true)
  end

  # 2. Scope with single argument
  scope :costs_more_than do |amount|
    where("price > ?", amount)
  end

  # 3. Scope with multiple arguments
  scope :by_author_and_category do |author_id, category|
    where(author_id: author_id, category: category)
  end

  # 4. Scope with conditionals
  scope :published_before do |year|
    where("published_year < ?", year) if year && year > 0
  end

  # 5. Complex conditional scope
  scope :by_author do |author_id|
    if author_id && author_id > 0
      where(author_id: author_id)
    end
  end

  # 6. Scope that chains other scopes
  scope :out_of_print_and_expensive do
    out_of_print.where("price > ?", 100.0)
  end

  # 7. Scope with complex conditional logic
  scope :filter_by_price_range do |min_price, max_price|
    if min_price && max_price && min_price < max_price
      where("price BETWEEN ? AND ?", min_price, max_price)
    elsif min_price && min_price > 0
      where("price >= ?", min_price)
    elsif max_price && max_price > 0
      where("price <= ?", max_price)
    end
  end

  # Compare with equivalent class method
  def self.published_before_method(year)
    where("published_year < ?", year) if year && year > 0
  end
end

describe "Enhanced Scopes" do
  before_each do
    # Set up books table
    Takarik::Data::BaseModel.connection.exec "DROP TABLE IF EXISTS books_scopes"
    Takarik::Data::BaseModel.connection.exec <<-SQL
      CREATE TABLE books_scopes (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        price REAL,
        out_of_print INTEGER DEFAULT 0,
        published_year INTEGER,
        author_id INTEGER,
        category TEXT,
        created_at TEXT,
        updated_at TEXT
      )
    SQL

    # Create test data
    BookScopes.create(title: "Ruby Guide", price: 50.0, out_of_print: 0, published_year: 2020, author_id: 1, category: "Programming")
    BookScopes.create(title: "Advanced Ruby", price: 150.0, out_of_print: 1, published_year: 2015, author_id: 1, category: "Programming")
    BookScopes.create(title: "Crystal Programming", price: 75.0, out_of_print: 0, published_year: 2021, author_id: 2, category: "Programming")
    BookScopes.create(title: "Old Programming", price: 200.0, out_of_print: 1, published_year: 2010, author_id: 2, category: "Programming")
    BookScopes.create(title: "Modern Web", price: 25.0, out_of_print: 0, published_year: 2022, author_id: 3, category: "Web")
    BookScopes.create(title: "Database Design", price: 120.0, out_of_print: 0, published_year: 2019, author_id: 3, category: "Database")
  end

  describe "simple scopes without arguments" do
    it "works correctly" do
      out_of_print_books = BookScopes.out_of_print.to_a
      out_of_print_books.size.should eq(2)
      out_of_print_books.all? { |b| b.out_of_print == true }.should be_true
    end

    it "generates correct SQL" do
      sql = BookScopes.out_of_print.to_sql
      sql.should contain("WHERE out_of_print = ?")
    end

    it "returns QueryBuilder for chaining" do
      query = BookScopes.out_of_print
      query.should be_a(Takarik::Data::QueryBuilder(BookScopes))
    end
  end

  describe "scopes with single argument" do
    it "works with valid arguments" do
      expensive_books = BookScopes.costs_more_than(100.0).to_a
      expensive_books.size.should eq(3)
      expensive_books.all? { |b| b.price.try(&.>(100.0)) }.should be_true
    end

    it "works with edge case arguments" do
      very_expensive = BookScopes.costs_more_than(180.0).to_a
      very_expensive.size.should eq(1)
      very_expensive.first.try(&.title).should eq("Old Programming")
    end

    it "generates correct SQL with parameters" do
      sql = BookScopes.costs_more_than(100.0).to_sql
      sql.should contain("WHERE price > ?")
    end
  end

  describe "scopes with multiple arguments" do
    it "works with multiple parameters" do
      author1_programming = BookScopes.by_author_and_category(1, "Programming").to_a
      author1_programming.size.should eq(2)
      author1_programming.all? { |b| b.author_id == 1 }.should be_true
      author1_programming.all? { |b| b.category == "Programming" }.should be_true
    end

    it "returns empty when no matches" do
      no_matches = BookScopes.by_author_and_category(999, "NonExistent").to_a
      no_matches.size.should eq(0)
    end
  end

  describe "scopes with conditionals" do
    it "applies filter when condition is met" do
      old_books = BookScopes.published_before(2020).to_a
      old_books.size.should eq(3)
      old_books.all? { |b| b.published_year.try(&.<(2020)) }.should be_true
    end

    it "returns all records when condition is not met (nil argument)" do
      all_books = BookScopes.published_before(nil).to_a
      all_books.size.should eq(6) # All books
    end

    it "returns all records when condition is not met (invalid argument)" do
      all_books = BookScopes.published_before(-1).to_a
      all_books.size.should eq(6) # All books
    end

    it "generates conditional SQL correctly" do
      valid_sql = BookScopes.published_before(2020).to_sql
      valid_sql.should contain("WHERE published_year < ?")

      invalid_sql = BookScopes.published_before(nil).to_sql
      invalid_sql.should eq("SELECT * FROM books_scopes")
    end

    it "always returns QueryBuilder for chaining" do
      valid_query = BookScopes.published_before(2020)
      invalid_query = BookScopes.published_before(nil)

      valid_query.should be_a(Takarik::Data::QueryBuilder(BookScopes))
      invalid_query.should be_a(Takarik::Data::QueryBuilder(BookScopes))
    end
  end

  describe "complex conditional scopes" do
    it "works with complex author filtering" do
      author2_books = BookScopes.by_author(2).to_a
      author2_books.size.should eq(2)
      author2_books.all? { |b| b.author_id == 2 }.should be_true
    end

    it "returns all when author condition not met" do
      no_author_books = BookScopes.by_author(nil).to_a
      no_author_books.size.should eq(6)
    end

    it "handles complex price range filtering" do
      # Both min and max provided (50.0 to 150.0)
      # Ruby Guide: 50.0, Advanced Ruby: 150.0, Crystal Programming: 75.0, Database Design: 120.0
      mid_range = BookScopes.filter_by_price_range(50.0, 150.0).to_a
      mid_range.size.should eq(4) # Updated expectation
      mid_range.all? { |b| b.price.try(&.>=(50.0)) && b.price.try(&.<=(150.0)) }.should be_true

      # Only min provided (>= 100.0)
      # Advanced Ruby: 150.0, Old Programming: 200.0, Database Design: 120.0
      min_only = BookScopes.filter_by_price_range(100.0, nil).to_a
      min_only.size.should eq(3)
      min_only.all? { |b| b.price.try(&.>=(100.0)) }.should be_true

      # Only max provided (<= 100.0)
      # Ruby Guide: 50.0, Crystal Programming: 75.0, Modern Web: 25.0
      max_only = BookScopes.filter_by_price_range(nil, 100.0).to_a
      max_only.size.should eq(3)
      max_only.all? { |b| b.price.try(&.<=(100.0)) }.should be_true

      # Invalid range
      invalid_range = BookScopes.filter_by_price_range(nil, nil).to_a
      invalid_range.size.should eq(6) # All books
    end
  end

  describe "scope chaining within scopes" do
    it "allows scopes to call other scopes" do
      expensive_out_of_print = BookScopes.out_of_print_and_expensive.to_a
      expensive_out_of_print.size.should eq(2)
      expensive_out_of_print.all? { |b| b.out_of_print == true }.should be_true
      expensive_out_of_print.all? { |b| b.price.try(&.>(100.0)) }.should be_true
    end

    it "generates correct SQL for chained scopes" do
      sql = BookScopes.out_of_print_and_expensive.to_sql
      sql.should contain("WHERE (out_of_print = ?) AND (price > ?)")
    end
  end

  describe "QueryBuilder chaining after scopes" do
    it "allows QueryBuilder chaining after simple scopes" do
      chained = BookScopes.costs_more_than(50.0).where(category: "Programming").to_a
      chained.size.should eq(3)
      chained.all? { |b| b.price.try(&.>(50.0)) }.should be_true
      chained.all? { |b| b.category == "Programming" }.should be_true
    end

    it "allows QueryBuilder chaining after conditional scopes (valid)" do
      # Note: Cannot chain scope after scope due to QueryBuilder method_missing limitation
      # But can chain QueryBuilder methods after scopes
      # published_before(2020): Advanced Ruby (2015), Old Programming (2010), Database Design (2019)
      # price > 100.0: Advanced Ruby (150.0), Old Programming (200.0), Database Design (120.0)
      chained = BookScopes.published_before(2020).where("price > ?", 100.0).to_a
      chained.size.should eq(3) # Updated expectation
      chained.all? { |b| b.published_year.try(&.<(2020)) }.should be_true
      chained.all? { |b| b.price.try(&.>(100.0)) }.should be_true
    end

    it "allows QueryBuilder chaining after conditional scopes (invalid - returns all)" do
      chained = BookScopes.published_before(nil).where("price > ?", 100.0).to_a
      chained.size.should eq(3) # All books that cost more than 100
      chained.all? { |b| b.price.try(&.>(100.0)) }.should be_true
    end

    it "generates correct SQL for chained queries" do
      sql = BookScopes.costs_more_than(50.0).where(category: "Programming").to_sql
      sql.should contain("WHERE (price > ?) AND (category = ?)")
    end
  end

  describe "comparison with class methods" do
    it "scopes always return QueryBuilder unlike class methods" do
      scope_result = BookScopes.published_before(nil)
      method_result = BookScopes.published_before_method(nil)

      scope_result.should be_a(Takarik::Data::QueryBuilder(BookScopes))
      method_result.should be_nil
    end

    it "allows chaining after scopes but not after nil-returning methods" do
      # This should work - scope returns QueryBuilder
      scope_chain = BookScopes.published_before(nil).where(category: "Programming").to_a
      scope_chain.size.should eq(4) # All programming books

      # This would fail with NoMethodError if we tried to chain after nil
      method_result = BookScopes.published_before_method(nil)
      method_result.should be_nil
    end
  end

  describe "SQL generation verification" do
    it "generates clean SQL for various scope combinations" do
      # Simple scope
      simple_sql = BookScopes.out_of_print.to_sql
      simple_sql.should eq("SELECT * FROM books_scopes WHERE out_of_print = ?")

      # Scope with argument
      arg_sql = BookScopes.costs_more_than(100.0).to_sql
      arg_sql.should eq("SELECT * FROM books_scopes WHERE price > ?")

      # Valid conditional scope
      conditional_sql = BookScopes.published_before(2020).to_sql
      conditional_sql.should eq("SELECT * FROM books_scopes WHERE published_year < ?")

      # Invalid conditional scope
      invalid_sql = BookScopes.published_before(nil).to_sql
      invalid_sql.should eq("SELECT * FROM books_scopes")
    end
  end
end
