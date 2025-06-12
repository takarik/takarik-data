require "./spec_helper"

describe "Merge Functionality" do
  before_each do
    # Set up test table
    Takarik::Data::BaseModel.connection.exec "DROP TABLE IF EXISTS merge_books"
    Takarik::Data::BaseModel.connection.exec <<-SQL
      CREATE TABLE merge_books (
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
    MergeBook.create(title: "In Print Book", out_of_print: false, year_published: 2020, price: 25.99, category: "Fiction")
    MergeBook.create(title: "Out of Print Book", out_of_print: true, year_published: 1980, price: 150.00, category: "Classic")
    MergeBook.create(title: "Recent Book", out_of_print: false, year_published: 2022, price: 30.00, category: "Fiction")
    MergeBook.create(title: "Old Book", out_of_print: true, year_published: 1960, price: 200.00, category: "Classic")
  end

  describe "basic merge functionality" do
    it "merges scope conditions, with the merged scope winning" do
      # Test the classic Rails example: Book.in_print.merge(Book.out_of_print)
      result = MergeBook.in_print.merge(MergeBook.out_of_print)

      # Should only return out_of_print books (merged condition wins)
      books = result.to_a
      books.size.should eq(2)
      books.all? { |book| book.out_of_print == true }.should be_true

      # Verify SQL shows only the merged condition
      sql = result.to_sql
      sql.should contain("out_of_print = ?")
      sql.should_not contain("AND")  # Should not have both conditions
    end

    it "merges where conditions with scope conditions" do
      # Test merging a where clause with a scope
      result = MergeBook.where(out_of_print: false).merge(MergeBook.out_of_print)

      books = result.to_a
      books.size.should eq(2)
      books.all? { |book| book.out_of_print == true }.should be_true
    end

    it "merges complex conditions" do
      # Test merging multiple conditions
      base_query = MergeBook.where(category: "Fiction").where("price > ?", 20.0)
      merge_query = MergeBook.where(category: "Classic").where("year_published < ?", 1970)

      result = base_query.merge(merge_query)
      books = result.to_a

      # Should only have the merged conditions (Classic books from before 1970)
      books.size.should eq(1)
      books.first.category.should eq("Classic")
      books.first.year_published.should eq(1960)
    end
  end

  describe "merge with other query methods" do
    it "merges order clauses" do
      base_query = MergeBook.order(:title)
      merge_query = MergeBook.order(:price)

      result = base_query.merge(merge_query)
      sql = result.to_sql

      # Should have the merged order (price), not the original (title)
      sql.should contain("ORDER BY price")
      sql.should_not contain("ORDER BY title")
    end

    it "merges limit and offset" do
      base_query = MergeBook.limit(10).offset(5)
      merge_query = MergeBook.limit(3).offset(1)

      result = base_query.merge(merge_query)
      sql = result.to_sql

      # Should have the merged limit and offset
      sql.should contain("LIMIT 3")
      sql.should contain("OFFSET 1")
    end

    it "merges select clauses" do
      base_query = MergeBook.select(:title)
      merge_query = MergeBook.select(:price, :category)

      result = base_query.merge(merge_query)
      sql = result.to_sql

      # Should have the merged select clause
      sql.should contain("SELECT price, category")
      sql.should_not contain("SELECT title")
    end
  end

  describe "merge preserves chainability" do
    it "allows chaining after merge" do
      result = MergeBook.in_print.merge(MergeBook.out_of_print).where("price > ?", 100.0).order(:title)

      books = result.to_a
      books.size.should eq(2)  # Out of print books with price > 100
      books.all? { |book| book.out_of_print == true }.should be_true
      books.all? { |book| book.price.not_nil! > 100.0 }.should be_true

      # Should be ordered by title
      books.first.title.should eq("Old Book")
      books.last.title.should eq("Out of Print Book")
    end

    it "works with multiple merges" do
      query1 = MergeBook.where(category: "Fiction")
      query2 = MergeBook.where(category: "Classic")
      query3 = MergeBook.where("price > ?", 100.0)

      result = query1.merge(query2).merge(query3)
      books = result.to_a

      # Should only have the final merged condition (price > 100)
      books.size.should eq(2)
      books.all? { |book| book.price.not_nil! > 100.0 }.should be_true
    end
  end

  describe "merge with default scope" do
    it "works correctly when model has default scope" do
      # Test with a model that has a default scope
      result = MergeBookWithDefault.in_print.merge(MergeBookWithDefault.out_of_print)

      books = result.to_a
      # Should respect the merged condition (out_of_print) but still apply default scope
      books.all? { |book| book.out_of_print == true }.should be_true
      books.all? { |book| book.year_published.not_nil! >= 1970 }.should be_true  # Default scope condition
    end
  end

  describe "SQL generation" do
    it "generates correct SQL for merged conditions" do
      result = MergeBook.where(category: "Fiction").where("price > ?", 20.0).merge(MergeBook.where(out_of_print: true))

      sql = result.to_sql
      params = result.params

      # Should only have the merged condition
      sql.should contain("WHERE")
      sql.should contain("out_of_print = ?")
      sql.should_not contain("category")
      sql.should_not contain("price")

      params.should eq([true])
    end

    it "handles empty merge correctly" do
      base_query = MergeBook.where(category: "Fiction")
      empty_query = MergeBook.all

      result = base_query.merge(empty_query)
      sql = result.to_sql

      # Should have no WHERE clause since merged query has no conditions
      sql.should_not contain("WHERE")
    end
  end
end

class MergeBook < Takarik::Data::BaseModel
  table_name "merge_books"
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

  scope :recent do
    where("year_published >= ?", 1974) # 50 years ago from 2024
  end

  scope :old do
    where("year_published < ?", 1974)
  end
end

class MergeBookWithDefault < Takarik::Data::BaseModel
  table_name "merge_books"
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
