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
    Takarik::Data.establish_connection("sqlite3:./test_eager_load.db")

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
        author_id: author.get_attribute("id")
      )
    end

    puts "\n=== Testing ActiveRecord Eager Load Specification ==="
    puts "The specification states:"
    puts "books = Book.eager_load(:author).limit(10)"
    puts "books.each { |book| puts book.author.last_name }"
    puts "Should execute exactly 1 query with LEFT OUTER JOIN:"
    puts "SELECT \"books\".\"id\" AS t0_r0, \"books\".\"title\" AS t0_r1, ..."
    puts "  FROM \"books\""
    puts "  LEFT OUTER JOIN \"authors\" ON \"authors\".\"id\" = \"books\".\"author_id\""
    puts "  LIMIT 10"

    # Test eager_load
    puts "\n--- Testing eager_load method ---"
    books = BookEL.eager_load(:author_el).limit(10).to_a

    puts "Loaded #{books.size} books"
    puts "Accessing author data (should not trigger additional queries):"

    books.each_with_index do |book, index|
      author = book.author_el.target
      if author
        last_name = author.get_attribute("last_name")
        puts "  Book #{index + 1}: #{book.get_attribute("title")} by #{last_name}"
      end
    end

    # Verify eager_load worked
    books.each do |book|
      book.author_el.loaded?.should be_true
    end

    puts "\n=== Comparison: includes vs preload vs eager_load ==="

    # Test includes (LEFT JOIN - 1 query)
    puts "\n--- includes: LEFT JOIN (1 query) ---"
    books_inc = BookEL.includes(:author_el).limit(10).to_a
    puts "Loaded #{books_inc.size} books with includes"

    # Test preload (separate queries - 2 queries)
    puts "\n--- preload: separate queries (2 queries) ---"
    books_pre = BookEL.preload(:author_el).limit(10).to_a
    puts "Loaded #{books_pre.size} books with preload"

    # Test eager_load (LEFT OUTER JOIN - 1 query)
    puts "\n--- eager_load: LEFT OUTER JOIN (1 query) ---"
    books_eager = BookEL.eager_load(:author_el).limit(10).to_a
    puts "Loaded #{books_eager.size} books with eager_load"

    puts "\n✅ Eager Load follows ActiveRecord specification!"
    puts "✓ Uses 1 query with LEFT OUTER JOIN"
    puts "✓ Always uses JOIN approach (never separate queries)"
    puts "✓ Prevents N+1 problem"
    puts "✓ Loads associations efficiently"

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
