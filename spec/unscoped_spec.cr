require "./spec_helper"

describe "Unscoped Functionality" do
  before_each do
    # Set up test table
    Takarik::Data::BaseModel.connection.exec "DROP TABLE IF EXISTS unscoped_books"
    Takarik::Data::BaseModel.connection.exec <<-SQL
      CREATE TABLE unscoped_books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT,
        out_of_print INTEGER DEFAULT 0,
        year_published INTEGER,
        price REAL,
        category TEXT,
        created_at TEXT,
        updated_at TEXT
      )
    SQL

    # Create test data
    UnscopedBook.create(title: "Active Book 1", out_of_print: false, year_published: 2020, price: 25.99, category: "Fiction")
    UnscopedBook.create(title: "Active Book 2", out_of_print: false, year_published: 2022, price: 30.00, category: "Fiction")
    UnscopedBook.create(title: "Inactive Book 1", out_of_print: true, year_published: 1980, price: 150.00, category: "Classic")
    UnscopedBook.create(title: "Inactive Book 2", out_of_print: true, year_published: 1960, price: 200.00, category: "Classic")
  end

  describe "basic unscoped functionality" do
    it "returns a fresh query builder without any conditions" do
      # Basic unscoped should return clean query
      result = UnscopedBook.unscoped
      sql = result.to_sql

      sql.should eq("SELECT * FROM unscoped_books")
      result.params.should be_empty
    end

    it "removes existing where conditions when called on QueryBuilder" do
      # Test the Rails example: Book.where(out_of_print: true).unscoped.all
      base_query = UnscopedBook.where(out_of_print: true)
      unscoped_query = base_query.unscoped

      base_sql = base_query.to_sql
      unscoped_sql = unscoped_query.to_sql

      base_sql.should contain("WHERE")
      base_sql.should contain("out_of_print = ?")

      unscoped_sql.should eq("SELECT * FROM unscoped_books")
      unscoped_query.params.should be_empty
    end

    it "allows chaining after unscoped" do
      # Test chaining after unscoped
      result = UnscopedBook.where(out_of_print: true).unscoped.where(category: "Fiction").order(:title)

      sql = result.to_sql
      params = result.params

      sql.should contain("WHERE category = ?")
      sql.should contain("ORDER BY title ASC")
      sql.should_not contain("out_of_print")

      params.should eq(["Fiction"])
    end
  end

  describe "unscoped with default scope" do
    it "bypasses default scope completely" do
      # Test with model that has default scope
      all_books = UnscopedBookWithDefault.unscoped.to_a
      scoped_books = UnscopedBookWithDefault.all.to_a

      # Unscoped should return all books (including old ones)
      all_books.size.should eq(4)

      # Scoped should only return recent books (published >= 1970)
      scoped_books.size.should eq(3) # 2020, 2022, 1980 books
      scoped_books.all? { |book| book.year_published.not_nil! >= 1970 }.should be_true
    end

    it "generates clean SQL without default scope conditions" do
      unscoped_sql = UnscopedBookWithDefault.unscoped.to_sql
      scoped_sql = UnscopedBookWithDefault.all.to_sql

      unscoped_sql.should eq("SELECT * FROM unscoped_books")
      scoped_sql.should contain("WHERE")
      scoped_sql.should contain("year_published >= ?")
    end

    it "allows chaining after bypassing default scope" do
      # Test chaining after unscoped with default scope model
      result = UnscopedBookWithDefault.unscoped.where(category: "Classic").order(:price)
      books = result.to_a

      # Should find both classic books (including the old one from 1960)
      books.size.should eq(2)
      books.all? { |book| book.category == "Classic" }.should be_true

      # Should include the 1960 book that would be filtered by default scope
      old_book = books.find { |book| book.year_published == 1960 }
      old_book.should_not be_nil
    end
  end

  describe "unscoped with block syntax" do
    it "executes block in unscoped context" do
      # Test Rails example: Book.unscoped { Book.out_of_print }
      result = UnscopedBookWithDefault.unscoped { UnscopedBookWithDefault.where(out_of_print: true) }
      books = result.to_a

      # Should find all out_of_print books, including old ones (bypassing default scope)
      books.size.should eq(2)
      books.all? { |book| book.out_of_print == true }.should be_true

      # Should include the 1960 book that would be filtered by default scope
      old_book = books.find { |book| book.year_published == 1960 }
      old_book.should_not be_nil
    end

    it "generates correct SQL in block context" do
      # Test SQL generation in block context
      result = UnscopedBookWithDefault.unscoped { UnscopedBookWithDefault.where(category: "Fiction").order(:title) }
      sql = result.to_sql

      # Should not contain default scope conditions
      sql.should contain("WHERE category = ?")
      sql.should contain("ORDER BY title ASC")
      sql.should_not contain("year_published >= ?")
    end

    it "allows complex queries in block context" do
      # Test complex query in unscoped block
      result = UnscopedBookWithDefault.unscoped do
        UnscopedBookWithDefault.where(category: "Classic")
          .where("price > ?", 100.0)
          .order(:year_published)
      end

      books = result.to_a
      books.size.should eq(2) # Both classic books have price > 100
      books.all? { |book| book.category == "Classic" }.should be_true
      books.all? { |book| book.price.not_nil! > 100.0 }.should be_true

      # Should be ordered by year_published
      books.first.year_published.should eq(1960)
      books.last.year_published.should eq(1980)
    end
  end

  describe "unscoped removes all query parts" do
    it "removes complex query conditions" do
      # Build a complex query
      complex_query = UnscopedBook
        .where(category: "Fiction")
        .where("price > ?", 20.0)
        .order(:title)
        .limit(5)
        .offset(2)
        .group(:category)
        .having("COUNT(*) > ?", 1)

      # Unscope should remove everything
      unscoped_query = complex_query.unscoped

      sql = unscoped_query.to_sql
      sql.should eq("SELECT * FROM unscoped_books")
      unscoped_query.params.should be_empty
    end

    it "works with joins and includes" do
      # This would be more complex with actual associations, but test the principle
      query_with_conditions = UnscopedBook.where(title: "Test").order(:id)
      unscoped_query = query_with_conditions.unscoped

      sql = unscoped_query.to_sql
      sql.should eq("SELECT * FROM unscoped_books")
    end
  end

  describe "comparison with regular scoping" do
    it "shows difference between scoped and unscoped queries" do
      # Regular scoped query
      scoped = UnscopedBookWithDefault.where(category: "Classic")
      scoped_books = scoped.to_a

      # Unscoped query with same condition
      unscoped = UnscopedBookWithDefault.unscoped.where(category: "Classic")
      unscoped_books = unscoped.to_a

      # Scoped should have fewer results (default scope filters out old books)
      scoped_books.size.should eq(1)   # Only 1980 book
      unscoped_books.size.should eq(2) # Both 1960 and 1980 books

      # Verify SQL differences
      scoped_sql = scoped.to_sql
      unscoped_sql = unscoped.to_sql

      scoped_sql.should contain("year_published >= ?")
      unscoped_sql.should_not contain("year_published >= ?")
    end
  end
end

class UnscopedBook < Takarik::Data::BaseModel
  table_name "unscoped_books"
  column :title, String
  column :out_of_print, Bool
  column :year_published, Int32
  column :price, Float64
  column :category, String

  timestamps

  scope :in_print do
    where(out_of_print: false)
  end

  scope :out_of_print do
    where(out_of_print: true)
  end
end

class UnscopedBookWithDefault < Takarik::Data::BaseModel
  table_name "unscoped_books"
  column :title, String
  column :out_of_print, Bool
  column :year_published, Int32
  column :price, Float64
  column :category, String

  timestamps

  # Default scope to only show books published after 1970
  default_scope do
    where("year_published >= ?", 1970)
  end

  scope :in_print do
    where(out_of_print: false)
  end

  scope :out_of_print do
    where(out_of_print: true)
  end
end
