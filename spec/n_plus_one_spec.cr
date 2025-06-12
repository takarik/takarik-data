require "./spec_helper"

describe "N+1 Query Problem Demonstration" do
  before_each do
    # Clean up existing data
    Takarik::Data.connection.exec("DELETE FROM book_strings")
    Takarik::Data.connection.exec("DELETE FROM author_strings")
  end

  describe "Classic N+1 Problem Example" do
    it "demonstrates the problematic code pattern that causes N+1 queries" do
      # Create test data - authors and books
      author1 = AuthorString.create(name: "J.K. Rowling")
      author2 = AuthorString.create(name: "Stephen King")
      author3 = AuthorString.create(name: "Agatha Christie")

      # Create 10 books distributed among authors
      books_data = [
        {title: "Harry Potter 1", author_id: author1.id},
        {title: "Harry Potter 2", author_id: author1.id},
        {title: "Harry Potter 3", author_id: author1.id},
        {title: "The Shining", author_id: author2.id},
        {title: "It", author_id: author2.id},
        {title: "Pet Sematary", author_id: author2.id},
        {title: "Murder on Orient Express", author_id: author3.id},
        {title: "And Then There Were None", author_id: author3.id},
        {title: "The ABC Murders", author_id: author3.id},
        {title: "Death on the Nile", author_id: author3.id},
      ]

      books_data.each do |book_data|
        BookString.create(title: book_data[:title], author_id: book_data[:author_id])
      end

      # THE PROBLEMATIC CODE THAT CAUSES N+1 QUERIES:
      # This is exactly the pattern mentioned in the issue

      # 1. This query gets 10 books (1 query)
      books = BookString.limit(10).to_a

      # 2. Each iteration will cause a separate query to get the author (N queries)
      author_names = [] of String
      books.each_with_index do |book, index|
        author = book.author
        if author
          author_name = author.name
          author_names << author_name.to_s if author_name
        end
      end

      # Verify we got all the data correctly
      author_names.size.should eq(10)
      author_names.should contain("J.K. Rowling")
      author_names.should contain("Stephen King")
      author_names.should contain("Agatha Christie")
    end

    it "demonstrates the solution using includes() to prevent N+1" do
      # Create the same test data
      author1 = AuthorString.create(name: "J.K. Rowling")
      author2 = AuthorString.create(name: "Stephen King")
      author3 = AuthorString.create(name: "Agatha Christie")

      10.times do |i|
        case i % 3
        when 0
          BookString.create(title: "Book #{i + 1}", author_id: author1.id)
        when 1
          BookString.create(title: "Book #{i + 1}", author_id: author2.id)
        else
          BookString.create(title: "Book #{i + 1}", author_id: author3.id)
        end
      end

      # Using includes() to eager load the association
      books = BookString.limit(10).includes(:author).to_a

      # Now accessing the authors doesn't trigger additional queries
      author_names = [] of String
      books.each_with_index do |book, index|
        # Check if the association is loaded
        author = book.author
        if author
          author_name = author.name
          author_names << author_name.to_s if author_name
        end
      end

      # Verify we got the same data
      author_names.size.should eq(10)
      author_names.should contain("J.K. Rowling")
      author_names.should contain("Stephen King")
      author_names.should contain("Agatha Christie")
    end

    it "shows how the problem scales with more records" do
      # Create one author and many books
      author = AuthorString.create(name: "Prolific Author")

      # Create different numbers of books to show scaling
      [5, 10, 20, 50].each do |book_count|
        # Clean up previous books
        Takarik::Data.connection.exec("DELETE FROM book_strings")

        # Create books
        book_count.times do |i|
          BookString.create(title: "Book #{i + 1}", author_id: author.id)
        end

        # Verify both approaches work
        books_n_plus_one = BookString.limit(book_count).to_a
        books_n_plus_one.size.should eq(book_count)

        books_with_includes = BookString.limit(book_count).includes(:author).to_a
        books_with_includes.size.should eq(book_count)
      end
    end
  end

  describe "Has Many N+1 Problem" do
    it "demonstrates N+1 with authors accessing their books" do
      # Create authors
      authors_data = [
        {name: "Author One", book_count: 2},
        {name: "Author Two", book_count: 3},
        {name: "Author Three", book_count: 1},
      ]

      authors_data.each do |author_data|
        author = AuthorString.create(name: author_data[:name])
        author_data[:book_count].times do |i|
          BookString.create(title: "#{author_data[:name]} Book #{i + 1}", author_id: author.id)
        end
      end

      # Simulate the problematic code
      authors = AuthorString.all.to_a

      total_books = 0
      authors.each_with_index do |author, index|
        books = author.books.to_a
        total_books += books.size
      end

      # Verify the data
      total_books.should eq(6) # 2 + 3 + 1 = 6 books total
      authors.size.should eq(3)
    end
  end

  describe "Real World Scenario" do
    it "simulates a book listing page with author information" do
      # Create realistic test data
      authors = [
        "J.K. Rowling", "Stephen King", "Agatha Christie",
        "Isaac Asimov", "George Orwell",
      ].map { |name| AuthorString.create(name: name) }

      books_data = [
        {title: "Harry Potter and the Philosopher's Stone", author: authors[0]},
        {title: "Harry Potter and the Chamber of Secrets", author: authors[0]},
        {title: "The Shining", author: authors[1]},
        {title: "It", author: authors[1]},
        {title: "Murder on the Orient Express", author: authors[2]},
        {title: "And Then There Were None", author: authors[2]},
        {title: "Foundation", author: authors[3]},
        {title: "I, Robot", author: authors[3]},
        {title: "1984", author: authors[4]},
        {title: "Animal Farm", author: authors[4]},
      ]

      books_data.each do |book_data|
        BookString.create(title: book_data[:title], author_id: book_data[:author].id)
      end

      # Test both approaches work correctly
      books_inefficient = BookString.limit(10).to_a
      books_efficient = BookString.limit(10).includes(:author).to_a

      books_inefficient.size.should eq(10)
      books_efficient.size.should eq(10)

      # Both should produce the same output
      inefficient_output = books_inefficient.map { |book|
        author = book.author
        author_name = author ? author.name : "Unknown"
        "#{book.title} by #{author_name}"
      }
      efficient_output = books_efficient.map { |book|
        author = book.author
        author_name = author ? author.name : "Unknown"
        "#{book.title} by #{author_name}"
      }

      inefficient_output.should eq(efficient_output)
    end
  end
end
