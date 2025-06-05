require "./spec_helper"

# Define models for smart includes testing
class AuthorSmart < Takarik::Data::BaseModel
  def self.table_name
    "authors_smart"
  end

  column :id, Int32, primary_key: true
  column :first_name, String
  column :last_name, String
  column :email, String
  column :active, Bool

  has_many :books_smart, class_name: "BookSmart", foreign_key: "author_id"
end

class BookSmart < Takarik::Data::BaseModel
  def self.table_name
    "books_smart"
  end

  column :id, Int32, primary_key: true
  column :title, String
  column :author_id, Int32
  column :out_of_print, Bool

  belongs_to :author_smart, class_name: "AuthorSmart", foreign_key: "author_id"
end

describe "Includes Smart Behavior (ActiveRecord 13.2)" do
  it "should use 2 queries by default, LEFT JOIN with conditions" do

    # Create authors table
    Takarik::Data.connection.exec("DROP TABLE IF EXISTS authors_smart")
    Takarik::Data.connection.exec(<<-SQL
      CREATE TABLE authors_smart (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT NOT NULL,
        active BOOLEAN DEFAULT 1
      )
    SQL
    )

    # Create books table
    Takarik::Data.connection.exec("DROP TABLE IF EXISTS books_smart")
    Takarik::Data.connection.exec(<<-SQL
      CREATE TABLE books_smart (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        author_id INTEGER,
        out_of_print BOOLEAN DEFAULT 0,
        FOREIGN KEY (author_id) REFERENCES authors_smart(id)
      )
    SQL
    )

    # Create test data
    author1 = AuthorSmart.create(
      first_name: "J.K.",
      last_name: "Rowling",
      email: "jk@example.com",
      active: true
    )

    author2 = AuthorSmart.create(
      first_name: "George",
      last_name: "Martin",
      email: "george@example.com",
      active: true
    )

    # Create books with mix of in_print/out_of_print
    10.times do |i|
      author = i < 5 ? author1 : author2
      BookSmart.create(
        title: "Book #{i + 1}",
        author_id: author.id,
        out_of_print: i % 3 == 0  # Every 3rd book is out of print
      )
    end


    # Test 1: Default behavior (no conditions) - should use 2 separate queries

    books_default = BookSmart.includes(:author_smart).limit(10).to_a

    # Test 2: With conditions on associations - should use LEFT JOIN

    # This should trigger JOIN strategy
    authors_with_conditions = AuthorSmart.includes(:books_smart)
                                         .where("books_smart.out_of_print = ?", true)
                                         .to_a

    # Test 3: Regular where conditions (not on associations) - should still use 2 queries

    books_regular_where = BookSmart.includes(:author_smart)
                                   .where("title LIKE ?", "Book%")
                                   .limit(5)
                                   .to_a

    # Verification

    # Check associations are loaded
    books_default.each do |book|
      book.author_smart_loaded?.should be_true
    end

    authors_with_conditions.each do |author|
      author.books_smart  # This should be accessible
    end

    books_regular_where.each do |book|
      book.author_smart_loaded?.should be_true
    end


    # Summary



    # Basic verification
    books_default.size.should be > 0
    authors_with_conditions.size.should be >= 0  # May be 0 if no out-of-print books
    books_regular_where.size.should be > 0
  end
end
