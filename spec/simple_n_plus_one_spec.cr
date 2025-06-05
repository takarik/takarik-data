require "./spec_helper"

# Simple models for the exact example from the issue
class Book < Takarik::Data::BaseModel
  table_name "books"

  column title, String
  column author_id, Int64

  belongs_to author, class_name: Author, foreign_key: :author_id

  timestamps
end

class Author < Takarik::Data::BaseModel
  table_name "authors"

  column first_name, String
  column last_name, String

  has_many books, class_name: Book, foreign_key: :author_id

  timestamps
end

describe "N+1 Queries Problem - Exact User Example" do
  before_each do
    # Create the tables
    begin
      Takarik::Data.connection.exec "DROP TABLE IF EXISTS books"
      Takarik::Data.connection.exec "DROP TABLE IF EXISTS authors"
    rescue
      # Tables might not exist yet
    end

    Takarik::Data.connection.exec <<-SQL
      CREATE TABLE authors (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name VARCHAR(255),
        last_name VARCHAR(255),
        created_at DATETIME,
        updated_at DATETIME
      )
    SQL

    Takarik::Data.connection.exec <<-SQL
      CREATE TABLE books (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title VARCHAR(255),
        author_id INTEGER,
        created_at DATETIME,
        updated_at DATETIME
      )
    SQL
  end

  it "demonstrates N+1 problem with the exact code from the issue" do
    # Create authors
    authors = [
      Author.create(first_name: "J.K.", last_name: "Rowling"),
      Author.create(first_name: "Stephen", last_name: "King"),
      Author.create(first_name: "Agatha", last_name: "Christie"),
      Author.create(first_name: "Isaac", last_name: "Asimov"),
      Author.create(first_name: "George", last_name: "Orwell"),
    ]

    # Create 10 books
    books_data = [
      "Harry Potter 1", "Harry Potter 2", "The Shining", "It", "Pet Sematary",
      "Murder Mystery 1", "Murder Mystery 2", "Foundation", "I Robot", "1984",
    ]

    books_data.each_with_index do |title, index|
      author = authors[index % authors.size]
      Book.create(title: title, author_id: author.id)
    end

    # Execute the problematic code
    books = Book.limit(10).to_a

    # Track the authors' last names
    last_names = [] of String
    books.each_with_index do |book, index|
      author = book.author
      if author
        author_last_name = author.last_name
        last_names << author_last_name.to_s if author_last_name
      end
    end

    # Verify we got the expected data
    books.size.should eq(10)
    last_names.size.should eq(10)
  end

  it "shows the solution using includes() for eager loading" do
    # Create the same test data
    authors = [
      Author.create(first_name: "J.K.", last_name: "Rowling"),
      Author.create(first_name: "Stephen", last_name: "King"),
      Author.create(first_name: "Agatha", last_name: "Christie"),
    ]

    # Create 10 books
    10.times do |i|
      author = authors[i % authors.size]
      Book.create(title: "Book #{i + 1}", author_id: author.id)
    end

    # Execute the improved code
    books = Book.limit(10).includes(:author).to_a

    # Track the authors' last names
    last_names = [] of String
    books.each_with_index do |book, index|
      author = book.author
      if author
        author_last_name = author.last_name
        last_names << author_last_name.to_s if author_last_name
      end
    end

    # Verify we got the same data
    books.size.should eq(10)
    last_names.size.should eq(10)
  end

  it "demonstrates performance impact at scale" do
    # Create one author for simplicity
    author = Author.create(first_name: "Prolific", last_name: "Writer")

    [10, 50, 100, 500].each do |count|
      # Clean up
      Takarik::Data.connection.exec("DELETE FROM books")

      # Create books
      count.times do |i|
        Book.create(title: "Book #{i + 1}", author_id: author.id)
      end

      # Verify both approaches work
      books_n1 = Book.limit(count).to_a
      books_optimized = Book.limit(count).includes(:author).to_a

      books_n1.size.should eq(count)
      books_optimized.size.should eq(count)
    end
  end
end
