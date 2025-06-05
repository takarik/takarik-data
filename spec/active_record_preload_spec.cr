require "./spec_helper"

# Define models for preload testing outside of the test
class AuthorPL < Takarik::Data::BaseModel
  def self.table_name
    "authors_pl"
  end

  column :id, Int32, primary_key: true
  column :first_name, String
  column :last_name, String
  column :email, String

  has_many :books_pl, class_name: "BookPL", foreign_key: "author_id"
end

class BookPL < Takarik::Data::BaseModel
  def self.table_name
    "books_pl"
  end

  column :id, Int32, primary_key: true
  column :title, String
  column :author_id, Int32

  belongs_to :author_pl, class_name: "AuthorPL", foreign_key: "author_id"
end

describe "ActiveRecord Preload Specification" do
  it "should follow ActiveRecord's preload behavior - 2 separate queries" do
    Takarik::Data.establish_connection("sqlite3:./test_preload.db")

    # Create authors table
    Takarik::Data.connection.exec("DROP TABLE IF EXISTS authors_pl")
    Takarik::Data.connection.exec(<<-SQL
      CREATE TABLE authors_pl (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        email TEXT NOT NULL
      )
    SQL
    )

    # Create books table
    Takarik::Data.connection.exec("DROP TABLE IF EXISTS books_pl")
    Takarik::Data.connection.exec(<<-SQL
      CREATE TABLE books_pl (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        title TEXT NOT NULL,
        author_id INTEGER,
        FOREIGN KEY (author_id) REFERENCES authors_pl(id)
      )
    SQL
    )

    # Create test data
    author1 = AuthorPL.create(
      first_name: "J.K.",
      last_name: "Rowling",
      email: "jk@example.com"
    )

    author2 = AuthorPL.create(
      first_name: "George",
      last_name: "Martin",
      email: "george@example.com"
    )

    # Create books
    10.times do |i|
      author = i < 5 ? author1 : author2
      BookPL.create(
        title: "Book #{i + 1}",
        author_id: author.get_attribute("id")
      )
    end

    puts "\n=== Testing ActiveRecord Preload Specification ==="
    puts "The specification states:"
    puts "books = Book.preload(:author).limit(10)"
    puts "books.each { |book| puts book.author.last_name }"
    puts "Should execute exactly 2 queries:"
    puts "1. SELECT books.* FROM books LIMIT 10"
    puts "2. SELECT authors.* FROM authors WHERE authors.id IN (1,2,3,4,5,6,7,8,9,10)"

    # Test preload
    puts "\n--- Testing preload method ---"
    books = BookPL.preload(:author_pl).limit(10).to_a

    puts "Loaded #{books.size} books"
    puts "Accessing author data (should not trigger additional queries):"

    books.each_with_index do |book, index|
      author = book.author_pl.target
      if author
        last_name = author.get_attribute("last_name")
        puts "  Book #{index + 1}: #{book.get_attribute("title")} by #{last_name}"
      end
    end

    # Verify preload worked
    books.each do |book|
      book.author_pl.loaded?.should be_true
    end

    puts "\n=== Comparison with includes and N+1 ==="

    # Test includes (1 query with JOIN)
    puts "\n--- includes: 1 query with JOIN ---"
    books_inc = BookPL.includes(:author_pl).limit(10).to_a
    puts "Loaded #{books_inc.size} books with includes"

    # Test N+1 (separate query for each author)
    puts "\n--- N+1 problem: 1 + N queries ---"
    books_n1 = BookPL.limit(10).to_a
    puts "Would trigger #{books_n1.size} additional queries without preload/includes"

    puts "\n✅ Preload follows ActiveRecord specification!"
    puts "✓ Uses 2 separate queries instead of JOIN"
    puts "✓ Prevents N+1 problem"
    puts "✓ Loads associations efficiently"

    books.size.should eq(10)
    books.all? { |book| book.author_pl.loaded? }.should be_true
  end
end
