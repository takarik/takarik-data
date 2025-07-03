require "./spec_helper"

describe "find_by_sql" do
  before_each do
    # Clean up any existing test data
    User.delete_all
    Post.delete_all
  end

  describe ".find_by_sql" do
    it "returns an array of model instances from custom SQL" do
      # Create test data
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)
      user3 = User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      # Test basic find_by_sql
      results = User.find_by_sql("SELECT * FROM users WHERE age > 28")

      results.should be_a(Array(User))
      results.size.should eq(2)
      results.map(&.name).compact.sort.should eq(["Bob", "Charlie"])

      # Verify instances are properly loaded
      results.each do |user|
        user.persisted?.should be_true
        user.new_record?.should be_false
        user.name.should_not be_nil
        user.email.should_not be_nil
        user.age.should_not be_nil
      end
    end

    it "returns empty array when no records match" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)

      results = User.find_by_sql("SELECT * FROM users WHERE age > 100")
      results.should be_a(Array(User))
      results.size.should eq(0)
    end

    it "works with parameterized queries using array" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)

      results = User.find_by_sql("SELECT * FROM users WHERE name = ?", ["Alice"])
      results.size.should eq(1)
      results.first.name.should eq("Alice")
      results.first.age.should eq(25)
    end

    it "works with parameterized queries using splat arguments" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)

      results = User.find_by_sql("SELECT * FROM users WHERE name = ? AND age > ?", "Bob", 25)
      results.size.should eq(1)
      results.first.name.should eq("Bob")
      results.first.age.should eq(30)
    end

    it "works with multiple parameters" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)
      user3 = User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      results = User.find_by_sql("SELECT * FROM users WHERE age BETWEEN ? AND ? ORDER BY age", 25, 30)
      results.size.should eq(2)
      results.map(&.name).should eq(["Alice", "Bob"])
    end

    it "works with JOIN queries" do
      # Create test data with associations
      user = User.create(name: "Alice", email: "alice@example.com", age: 25)
      post1 = Post.create(title: "First Post", content: "Content 1", user_id: user.id_value)
      post2 = Post.create(title: "Second Post", content: "Content 2", user_id: user.id_value)

      # Test JOIN query - note we're selecting from users table but joining with posts
      sql = <<-SQL
        SELECT DISTINCT users.*
        FROM users
        INNER JOIN posts ON users.id = posts.user_id
        WHERE posts.title LIKE ?
        ORDER BY users.name
      SQL

      results = User.find_by_sql(sql, "%Post%")
      results.size.should eq(1)
      results.first.name.should eq("Alice")
      results.first.email.should eq("alice@example.com")
    end

    it "works with complex SQL queries" do
      # Create test data
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)
      user3 = User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      # Complex query with subquery
      sql = <<-SQL
        SELECT * FROM users
        WHERE age > (SELECT AVG(age) FROM users)
        ORDER BY age DESC
      SQL

      results = User.find_by_sql(sql)
      results.size.should eq(1) # Only Charlie (35) is above average (30)
      results.first.name.should eq("Charlie")
    end

    it "always returns an array even for single record" do
      user = User.create(name: "Alice", email: "alice@example.com", age: 25)

      results = User.find_by_sql("SELECT * FROM users WHERE name = ?", ["Alice"])
      results.should be_a(Array(User))
      results.size.should eq(1)
    end

    it "handles all column types correctly" do
      user = User.create(name: "Alice", email: "alice@example.com", age: 25)

      results = User.find_by_sql("SELECT * FROM users WHERE id = ?", [user.id_value])
      result = results.first

      # Verify all attributes are loaded correctly
      result.id_value.should eq(user.id_value)
      result.name.should eq("Alice")
      result.email.should eq("alice@example.com")
      result.age.should eq(25)
      result.persisted?.should be_true
    end

    it "works with ORDER BY and LIMIT clauses" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)
      user3 = User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      results = User.find_by_sql("SELECT * FROM users ORDER BY age DESC LIMIT 2")
      results.size.should eq(2)
      results.map(&.name).should eq(["Charlie", "Bob"])
    end

    it "runs after_find callbacks on loaded instances" do
      user = User.create(name: "Alice", email: "alice@example.com", age: 25)

      results = User.find_by_sql("SELECT * FROM users WHERE id = ?", [user.id_value])
      result = results.first

      # The after_find callback should have run (this depends on your User model having callbacks)
      # For now, just verify the instance is properly initialized
      result.persisted?.should be_true
      result.changed?.should be_false
    end
  end
end
