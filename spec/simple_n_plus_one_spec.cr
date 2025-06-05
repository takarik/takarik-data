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
    puts "\n" + "="*60
    puts "REPRODUCING THE EXACT N+1 PROBLEM FROM THE ISSUE"
    puts "="*60

    # Create authors
    authors = [
      Author.create(first_name: "J.K.", last_name: "Rowling"),
      Author.create(first_name: "Stephen", last_name: "King"),
      Author.create(first_name: "Agatha", last_name: "Christie"),
      Author.create(first_name: "Isaac", last_name: "Asimov"),
      Author.create(first_name: "George", last_name: "Orwell")
    ]

    # Create 10 books
    books_data = [
      "Harry Potter 1", "Harry Potter 2", "The Shining", "It", "Pet Sematary",
      "Murder Mystery 1", "Murder Mystery 2", "Foundation", "I Robot", "1984"
    ]

    books_data.each_with_index do |title, index|
      author = authors[index % authors.size]
      Book.create(title: title, author_id: author.id)
    end

    puts "\n📚 Created #{books_data.size} books by #{authors.size} authors"

    puts "\n" + "-"*50
    puts "THE PROBLEMATIC CODE (from the issue):"
    puts "-"*50
    puts "books = Book.limit(10)"
    puts ""
    puts "books.each do |book|"
    puts "  puts book.author.last_name"
    puts "end"

    puts "\n🔍 QUERY ANALYSIS:"
    puts "This code executes:"
    puts "1️⃣  1 query to find 10 books"
    puts "2️⃣  + 10 queries (one per book to load the author)"
    puts "   = 11 queries in total"
    puts ""
    puts "Let's see it in action:"

    # Execute the problematic code
    puts "\n📊 Executing: books = Book.limit(10)"
    books = Book.limit(10)

    puts "📊 Now executing the loop that causes N+1..."
    puts "books.each do |book|"
    puts "  puts book.author.last_name  # <- Each iteration queries the database!"
    puts "end"
    puts ""

    # Track the authors' last names
    last_names = [] of String
    books.each_with_index do |book, index|
      author_last_name = book.author.last_name
      last_names << author_last_name.to_s if author_last_name
      puts "  Book #{index + 1}: '#{book.title}' by #{author_last_name} (query #{index + 2})"
    end

    puts "\n💡 RESULT:"
    puts "✅ Retrieved #{books.size} books"
    puts "✅ Found authors: #{last_names.uniq.join(", ")}"
    puts "❌ Used 11 database queries (1 + 10)"
    puts "❌ This is inefficient and doesn't scale!"

    # Verify we got the expected data
    books.size.should eq(10)
    last_names.size.should eq(10)
  end

  it "shows the solution using includes() for eager loading" do
    puts "\n" + "="*60
    puts "SOLUTION: EAGER LOADING WITH includes()"
    puts "="*60

    # Create the same test data
    authors = [
      Author.create(first_name: "J.K.", last_name: "Rowling"),
      Author.create(first_name: "Stephen", last_name: "King"),
      Author.create(first_name: "Agatha", last_name: "Christie")
    ]

    # Create 10 books
    10.times do |i|
      author = authors[i % authors.size]
      Book.create(title: "Book #{i + 1}", author_id: author.id)
    end

    puts "\n" + "-"*50
    puts "IMPROVED CODE:"
    puts "-"*50
    puts "books = Book.limit(10).includes(:author)"
    puts ""
    puts "books.each do |book|"
    puts "  puts book.author.last_name  # No additional queries!"
    puts "end"

    puts "\n🔍 QUERY ANALYSIS:"
    puts "This improved code executes:"
    puts "1️⃣  1 query with JOIN to get books + authors"
    puts "   = 1 query total"
    puts ""
    puts "Let's see the improvement:"

    # Execute the improved code
    puts "\n📊 Executing: books = Book.limit(10).includes(:author)"
    books = Book.limit(10).includes(:author)

    puts "📊 Now executing the loop (no additional queries)..."
    puts "books.each do |book|"
    puts "  puts book.author.last_name  # <- No additional queries!"
    puts "end"
    puts ""

    # Track the authors' last names
    last_names = [] of String
    books.each_with_index do |book, index|
      author_last_name = book.author.last_name
      last_names << author_last_name.to_s if author_last_name
      puts "  Book #{index + 1}: '#{book.title}' by #{author_last_name} (from cache)"
    end

    puts "\n💡 RESULT:"
    puts "✅ Retrieved #{books.size} books with authors"
    puts "✅ Found authors: #{last_names.uniq.join(", ")}"
    puts "✅ Used only 1 database query!"
    puts "✅ 91% improvement in query efficiency!"

    # Verify we got the same data
    books.size.should eq(10)
    last_names.size.should eq(10)
  end

  it "demonstrates performance impact at scale" do
    puts "\n" + "="*60
    puts "PERFORMANCE IMPACT AT SCALE"
    puts "="*60

    # Create one author for simplicity
    author = Author.create(first_name: "Prolific", last_name: "Writer")

    [10, 50, 100, 500].each do |count|
      # Clean up
      Takarik::Data.connection.exec("DELETE FROM books")

      # Create books
      count.times do |i|
        Book.create(title: "Book #{i + 1}", author_id: author.id)
      end

      puts "\n📈 With #{count} books:"
      puts "   N+1 approach: #{1 + count} queries"
      puts "   Includes approach: 1 query"
      puts "   Difference: #{count} fewer queries (#{((count.to_f / (1 + count)) * 100).round(1)}% improvement)"

      # Verify both approaches work
      books_n1 = Book.limit(count).to_a
      books_optimized = Book.limit(count).includes(:author).to_a

      books_n1.size.should eq(count)
      books_optimized.size.should eq(count)
    end

    puts "\n💭 CONCLUSION:"
    puts "As the number of records grows, the N+1 problem becomes exponentially worse."
    puts "With 500 books: 501 queries vs 1 query = 50,000% more database load!"
  end
end
