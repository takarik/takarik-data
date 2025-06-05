require "./spec_helper"

# Define models for comprehensive comparison
class AuthorAll < Takarik::Data::BaseModel
  def self.table_name
    "authors_all"
  end

  column :id, Int32, primary_key: true
  column :first_name, String
  column :last_name, String
  column :email, String

  has_many :books_all, class_name: "BookAll", foreign_key: "author_id"
end

class BookAll < Takarik::Data::BaseModel
  def self.table_name
    "books_all"
  end

  column :id, Int32, primary_key: true
  column :title, String
  column :author_id, Int32

  belongs_to :author_all, class_name: "AuthorAll", foreign_key: "author_id"
end

describe "Complete ActiveRecord Eager Loading Comparison" do
  it "should demonstrate all four approaches: N+1, includes, preload, eager_load" do
    Takarik::Data.establish_connection("sqlite3:./test_all_methods.db")

    # Create authors table
    Takarik::Data.connection.exec("DROP TABLE IF EXISTS authors_all")
    Takarik::Data.connection.exec(<<-SQL
      CREATE TABLE authors_all (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT NOT NULL
      )
    SQL
    )

    # Create books table
    Takarik::Data.connection.exec("DROP TABLE IF EXISTS books_all")
    Takarik::Data.connection.exec(<<-SQL
      CREATE TABLE books_all (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        author_id INTEGER,
        FOREIGN KEY (author_id) REFERENCES authors_all(id)
      )
    SQL
    )

    # Create test data
    author1 = AuthorAll.create(
      first_name: "J.K.",
      last_name: "Rowling",
      email: "jk@example.com"
    )

    author2 = AuthorAll.create(
      first_name: "George",
      last_name: "Martin",
      email: "george@example.com"
    )

    # Create 10 books
    10.times do |i|
      author = i < 5 ? author1 : author2
      BookAll.create(
        title: "Book #{i + 1}",
        author_id: author.get_attribute("id")
      )
    end



    # 1. N+1 Problem demonstration

    books_n1 = BookAll.limit(10).to_a
    books_n1.each_with_index do |book, index|
    end

    # 2. includes approach

    books_inc = BookAll.includes(:author_all).limit(10).to_a

    # 3. preload approach

    books_pre = BookAll.preload(:author_all).limit(10).to_a

    # 4. eager_load approach

    books_eager = BookAll.eager_load(:author_all).limit(10).to_a

    # Verification

    # Check that all approaches return same number of records
    books_n1.size.should eq(books_inc.size)
    books_inc.size.should eq(books_pre.size)
    books_pre.size.should eq(books_eager.size)

    # Check that associations are loaded for eager loading methods
    books_inc.all? { |book| book.author_all.loaded? }.should be_true
    books_pre.all? { |book| book.author_all.loaded? }.should be_true
    books_eager.all? { |book| book.author_all.loaded? }.should be_true


    # Performance summary




  end
end
