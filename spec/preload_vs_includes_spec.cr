require "./spec_helper"

# Define models for comparison testing
class AuthorComparison < Takarik::Data::BaseModel
  def self.table_name
    "authors_comparison"
  end

  column :id, Int32, primary_key: true
  column :first_name, String
  column :last_name, String
  column :email, String

  has_many :books_comparison, class_name: "BookComparison", foreign_key: "author_id"
end

class BookComparison < Takarik::Data::BaseModel
  def self.table_name
    "books_comparison"
  end

  column :id, Int32, primary_key: true
  column :title, String
  column :author_id, Int32

  belongs_to :author_comparison, class_name: "AuthorComparison", foreign_key: "author_id"
end

describe "Preload vs Includes vs N+1 Comparison" do
  it "should demonstrate all three approaches according to ActiveRecord specification" do
    Takarik::Data.establish_connection("sqlite3:./test_comparison.db")

    # Create authors table
    Takarik::Data.connection.exec("DROP TABLE IF EXISTS authors_comparison")
    Takarik::Data.connection.exec(<<-SQL
      CREATE TABLE authors_comparison (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT NOT NULL
      )
    SQL
    )

    # Create books table
    Takarik::Data.connection.exec("DROP TABLE IF EXISTS books_comparison")
    Takarik::Data.connection.exec(<<-SQL
      CREATE TABLE books_comparison (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        author_id INTEGER,
        FOREIGN KEY (author_id) REFERENCES authors_comparison(id)
      )
    SQL
    )

    # Create test data
    author1 = AuthorComparison.create(
      first_name: "J.K.",
      last_name: "Rowling",
      email: "jk@example.com"
    )

    author2 = AuthorComparison.create(
      first_name: "George",
      last_name: "Martin",
      email: "george@example.com"
    )

    # Create 10 books
    10.times do |i|
      author = i < 5 ? author1 : author2
      BookComparison.create(
        title: "Book #{i + 1}",
        author_id: author.id
      )
    end



    # 1. N+1 Problem demonstration

    books_n1 = BookComparison.limit(10).to_a
    books_n1.each_with_index do |book, index|
    end

    # 2. includes approach (LEFT JOIN)

    books_inc = BookComparison.includes(:author_comparison).limit(10).to_a

    # 3. preload approach (separate queries)

    books_pre = BookComparison.preload(:author_comparison).limit(10).to_a

    # Verify functionality
    books_n1.size.should eq(books_inc.size)
    books_inc.size.should eq(books_pre.size)

    books_pre.each do |book|
      book.author_comparison.loaded?.should be_true
    end

    books_inc.each do |book|
      book.author_comparison.loaded?.should be_true
    end


    # Performance comparison




  end
end
