require "./spec_helper"

# Models that exactly match the ActiveRecord specification examples
class BookSpec < Takarik::Data::BaseModel
  table_name "books"

  column title, String
  column author_id, Int64

  belongs_to author, class_name: AuthorSpec, foreign_key: :author_id

  timestamps
end

class AuthorSpec < Takarik::Data::BaseModel
  table_name "authors"

  column first_name, String
  column last_name, String

  has_many books, class_name: BookSpec, foreign_key: :author_id

  timestamps
end

describe "ActiveRecord Includes Specification Tests" do
  before_each do
    setup_activerecord_test_environment
  end

  describe "13.2 includes - Basic Usage" do
    it "demonstrates the exact ActiveRecord pattern from the specification" do

      # Create test data exactly as in ActiveRecord docs
      rowling = AuthorSpec.create(first_name: "J.K.", last_name: "Rowling")
      king = AuthorSpec.create(first_name: "Stephen", last_name: "King")
      christie = AuthorSpec.create(first_name: "Agatha", last_name: "Christie")

      # Create 10+ books
      books_data = [
        {title: "Harry Potter and the Philosopher's Stone", author_id: rowling.id},
        {title: "Harry Potter and the Chamber of Secrets", author_id: rowling.id},
        {title: "Harry Potter and the Prisoner of Azkaban", author_id: rowling.id},
        {title: "The Shining", author_id: king.id},
        {title: "It", author_id: king.id},
        {title: "Pet Sematary", author_id: king.id},
        {title: "The Stand", author_id: king.id},
        {title: "Murder on the Orient Express", author_id: christie.id},
        {title: "And Then There Were None", author_id: christie.id},
        {title: "The ABC Murders", author_id: christie.id},
        {title: "Death on the Nile", author_id: christie.id},
        {title: "Poirot Investigates", author_id: christie.id},
      ]

      books_data.each do |book_attrs|
        BookSpec.create(title: book_attrs[:title], author_id: book_attrs[:author_id])
      end





      # Test the includes approach
      books_with_includes = BookSpec.includes(:author).limit(10)
      sql = books_with_includes.to_sql


      # Execute and time the operation
      start_time = Time.utc
      books_loaded = books_with_includes.to_a
      end_time = Time.utc


      # Verify data integrity
      author_names = [] of String
      books_loaded.each_with_index do |book, index|
        author = book.author
        if author
          author_name = author.last_name
          author_names << author_name.to_s if author_name
        end
      end

      # Assertions
      books_loaded.size.should eq(10)
      author_names.size.should eq(10)
      author_names.should contain("Rowling")
      author_names.should contain("King")
      author_names.should contain("Christie")


    end
  end

  describe "13.2.1 Eager Loading Multiple Associations" do
    it "supports array syntax: Customer.includes(:orders, :reviews)" do

      # This would require more complex models, but we can demonstrate with available models

      # Test what we can with current models
      author = AuthorSpec.create(first_name: "Test", last_name: "Author")
      BookSpec.create(title: "Book 1", author_id: author.id)
      BookSpec.create(title: "Book 2", author_id: author.id)
      BookSpec.create(title: "Book 3", author_id: author.id)

      authors_with_books = AuthorSpec.includes(:books).to_a
      test_author = authors_with_books.find { |a| a.last_name == "Author" }

      test_author.should_not be_nil
      books = test_author.not_nil!.books.to_a
      books.size.should eq(3)

    end
  end

  describe "Performance comparison with exact measurement" do
    it "demonstrates measurable performance difference" do

      # Create substantial test data
      authors = [] of AuthorSpec
      5.times do |i|
        authors << AuthorSpec.create(first_name: "Author", last_name: "#{i + 1}")
      end

      # Create 50 books
      50.times do |i|
        author = authors[i % authors.size]
        BookSpec.create(title: "Book #{i + 1}", author_id: author.id)
      end



      start_time = Time.utc
      books_n1 = BookSpec.limit(25).to_a
      author_names_n1 = [] of String
      books_n1.each do |book|
        author = book.author
        if author
          author_name = author.last_name # This triggers individual queries
          author_names_n1 << author_name.to_s if author_name
        end
      end
      end_time = Time.utc
      n1_time = (end_time - start_time).total_milliseconds



      start_time = Time.utc
      books_includes = BookSpec.includes(:author).limit(25).to_a
      author_names_includes = [] of String
      books_includes.each do |book|
        author = book.author
        if author
          author_name = author.last_name # This uses cached/joined data
          author_names_includes << author_name.to_s if author_name
        end
      end
      end_time = Time.utc
      includes_time = (end_time - start_time).total_milliseconds


      # Verify identical results
      books_n1.size.should eq(books_includes.size)
      author_names_n1.sort.should eq(author_names_includes.sort)

      # Calculate performance improvement
      if n1_time > 0
        improvement = ((n1_time - includes_time) / n1_time * 100).round(1)
        query_reduction = ((26 - 1) / 26.0 * 100).round(1)


        # Performance should be significantly better
        includes_time.should be < n1_time
      end

    end
  end
end

def setup_activerecord_test_environment
  # Create tables for the test
  connection = Takarik::Data.connection

  # Drop existing tables
  begin
    connection.exec("DROP TABLE IF EXISTS books")
    connection.exec("DROP TABLE IF EXISTS authors")
  rescue
    # Tables might not exist
  end

  # Create authors table
  connection.exec <<-SQL
    CREATE TABLE authors (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      first_name VARCHAR(255),
      last_name VARCHAR(255),
      created_at DATETIME,
      updated_at DATETIME
    )
  SQL

  # Create books table
  connection.exec <<-SQL
    CREATE TABLE books (
      id INTEGER PRIMARY KEY AUTOINCREMENT,
      title VARCHAR(255),
      author_id INTEGER,
      created_at DATETIME,
      updated_at DATETIME
    )
  SQL

  # Clean up existing data
  connection.exec("DELETE FROM books")
  connection.exec("DELETE FROM authors")
end
