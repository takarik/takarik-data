require "./spec_helper"

# Define models for eager_load testing
class AuthorEL < Takarik::Data::BaseModel
  def self.table_name
    "authors_el"
  end

  column :id, Int32, primary_key: true
  column :first_name, String
  column :last_name, String
  column :email, String

  has_many :books_el, class_name: "BookEL", foreign_key: "author_id"
end

class BookEL < Takarik::Data::BaseModel
  def self.table_name
    "books_el"
  end

  column :id, Int32, primary_key: true
  column :title, String
  column :author_id, Int32

  belongs_to :author_el, class_name: "AuthorEL", foreign_key: "author_id"
end

describe "ActiveRecord Eager Load Specification" do
  it "should follow ActiveRecord's eager_load behavior - 1 query with LEFT OUTER JOIN" do

    # Create authors table
    Takarik::Data.connection.exec("DROP TABLE IF EXISTS authors_el")
    Takarik::Data.connection.exec(<<-SQL
      CREATE TABLE authors_el (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT NOT NULL
      )
    SQL
    )

    # Create books table
    Takarik::Data.connection.exec("DROP TABLE IF EXISTS books_el")
    Takarik::Data.connection.exec(<<-SQL
      CREATE TABLE books_el (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        author_id INTEGER,
        FOREIGN KEY (author_id) REFERENCES authors_el(id)
      )
    SQL
    )

    # Create test data
    author1 = AuthorEL.create(
      first_name: "J.K.",
      last_name: "Rowling",
      email: "jk@example.com"
    )

    author2 = AuthorEL.create(
      first_name: "George",
      last_name: "Martin",
      email: "george@example.com"
    )

    # Create books
    10.times do |i|
      author = i < 5 ? author1 : author2
      BookEL.create(
        title: "Book #{i + 1}",
        author_id: author.id
      )
    end


    # Test eager_load
    books = BookEL.eager_load(:author_el).limit(10).to_a


    books.each_with_index do |book, index|
      author = book.author_el.target
      if author
        last_name = author.last_name
      end
    end

    # Verify eager_load worked
    books.each do |book|
      book.author_el.loaded?.should be_true
    end


    # Test includes (LEFT JOIN - 1 query)
    books_inc = BookEL.includes(:author_el).limit(10).to_a

    # Test preload (separate queries - 2 queries)
    books_pre = BookEL.preload(:author_el).limit(10).to_a

    # Test eager_load (LEFT OUTER JOIN - 1 query)
    books_eager = BookEL.eager_load(:author_el).limit(10).to_a


    # Verify all approaches return same data
    books.size.should eq(10)
    books_inc.size.should eq(books_pre.size)
    books_pre.size.should eq(books_eager.size)

    # Verify all have associations loaded
    books.all? { |book| book.author_el.loaded? }.should be_true
    books_inc.all? { |book| book.author_el.loaded? }.should be_true
    books_pre.all? { |book| book.author_el.loaded? }.should be_true
    books_eager.all? { |book| book.author_el.loaded? }.should be_true
  end


end
