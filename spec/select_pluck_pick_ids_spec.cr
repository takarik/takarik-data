require "./spec_helper"

describe "select_all, pluck, pick, and ids methods" do
  before_each do
    # Clean up any existing test data
    User.all.delete_all
    Post.all.delete_all
  end

  describe ".select_all" do
    it "returns array of hashes from custom SQL" do
      # Create test data
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)

      # Test basic select_all
      results = User.select_all("SELECT name, age FROM users WHERE age > 25")

      results.should be_a(Array(Hash(String, DB::Any)))
      results.size.should eq(1)
      results.first["name"].should eq("Bob")
      results.first["age"].should eq(30)
    end

    it "returns empty array when no records match" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)

      results = User.select_all("SELECT * FROM users WHERE age > 100")
      results.should be_a(Array(Hash(String, DB::Any)))
      results.size.should eq(0)
    end

    it "works with parameterized queries using array" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)

      results = User.select_all("SELECT name, email FROM users WHERE name = ?", ["Alice"])
      results.size.should eq(1)
      results.first["name"].should eq("Alice")
      results.first["email"].should eq("alice@example.com")
    end

    it "works with parameterized queries using splat arguments" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)

      results = User.select_all("SELECT name FROM users WHERE age BETWEEN ? AND ?", 25, 30)
      results.size.should eq(2)
      results.map { |r| r["name"].as(String) }.sort.should eq(["Alice", "Bob"])
    end

    it "works with JOIN queries" do
      # Create test data with associations
      user = User.create(name: "Alice", email: "alice@example.com", age: 25)
      post = Post.create(title: "Test Post", content: "Content", user_id: user.id_value)

      sql = <<-SQL
        SELECT users.name, posts.title
        FROM users
        INNER JOIN posts ON users.id = posts.user_id
        WHERE posts.title = ?
      SQL

      results = User.select_all(sql, "Test Post")
      results.size.should eq(1)
      results.first["name"].should eq("Alice")
      results.first["title"].should eq("Test Post")
    end

    it "returns raw data without model instantiation" do
      user = User.create(name: "Alice", email: "alice@example.com", age: 25)

      results = User.select_all("SELECT name, age FROM users WHERE id = ?", [user.id_value])
      result = results.first

      # Should be raw hash data, not model instances
      result.should be_a(Hash(String, DB::Any))
      result["name"].should eq("Alice")
      result["age"].should eq(25)
    end
  end

  describe ".pluck" do
    it "plucks single column values" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)
      user3 = User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      names = User.where("age > 25").order("age").pluck("name")
      names.should be_a(Array(DB::Any))
      names.should eq(["Bob", "Charlie"])
    end

    it "plucks multiple column values" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)

      data = User.order("age").pluck("name", "age")
      data.should be_a(Array(Array(DB::Any)))
      data.should eq([["Alice", 25], ["Bob", 30]])
    end

    it "works with distinct values" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 25)
      User.create(name: "Charlie", email: "charlie@example.com", age: 30)

      ages = User.distinct.pluck("age").map(&.as(Int64)).sort
      ages.should eq([25_i64, 30_i64])
    end

    it "works with where conditions" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      names = User.where("age >= 30").order("age").pluck("name")
      names.should eq(["Bob", "Charlie"])
    end

    it "returns empty array when no records match" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)

      names = User.where("age > 100").pluck("name")
      names.should be_a(Array(DB::Any))
      names.size.should eq(0)
    end

    it "works with joins" do
      user = User.create(name: "Alice", email: "alice@example.com", age: 25)
      post1 = Post.create(title: "First Post", content: "Content 1", user_id: user.id_value)
      post2 = Post.create(title: "Second Post", content: "Content 2", user_id: user.id_value)

      titles = User.joins(:posts).where("users.name = ?", "Alice").pluck("posts.title").map(&.as(String)).sort
      titles.should eq(["First Post", "Second Post"])
    end

    it "triggers immediate query and cannot be chained" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)

      # pluck should return an Array, not a QueryBuilder
      result = User.pluck("name")
      result.should be_a(Array(DB::Any))

      # This would fail if we tried to chain: result.limit(1)
      # because Array doesn't have a limit method
    end
  end

  describe ".pick" do
    it "picks single column value from first record" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)

      name = User.order("age").pick("name")
      name.should eq("Alice")
    end

    it "picks multiple column values from first record" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)

      data = User.order("age").pick("name", "age")
      data.should eq(["Alice", 25])
    end

    it "returns nil when no records match" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)

      result = User.where("age > 100").pick("name")
      result.should be_nil
    end

    it "works with where conditions" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)
      user3 = User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      name = User.where("age >= 30").order("age").pick("name")
      name.should eq("Bob")
    end

    it "is equivalent to limit(1).pluck().first" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)

      # These should be equivalent
      pick_result = User.order("age").pick("name")
      pluck_result = User.order("age").limit(1).pluck("name").first?

      pick_result.should eq(pluck_result)
      pick_result.should eq("Alice")
    end

    it "works with multiple columns equivalent to limit(1).pluck().first" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)

      # These should be equivalent
      pick_result = User.order("age").pick("name", "age")
      pluck_result = User.order("age").limit(1).pluck("name", "age").first?

      pick_result.should eq(pluck_result)
      pick_result.should eq(["Alice", 25])
    end
  end

  describe ".ids" do
    it "plucks all primary key values" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)
      user3 = User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      ids = User.order("age").ids
      ids.should be_a(Array(DB::Any))
      ids.should eq([user1.id_value, user2.id_value, user3.id_value])
    end

    it "works with where conditions" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)
      user3 = User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      ids = User.where("age >= 30").order("age").ids
      ids.should eq([user2.id_value, user3.id_value])
    end

    it "returns empty array when no records match" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)

      ids = User.where("age > 100").ids
      ids.should be_a(Array(DB::Any))
      ids.size.should eq(0)
    end

    it "is equivalent to pluck(primary_key)" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)

      # These should be equivalent
      ids_result = User.order("age").ids
      pluck_result = User.order("age").pluck(User.primary_key)

      ids_result.should eq(pluck_result)
    end

    it "works with distinct" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)

      # Should work the same as regular ids since primary keys are unique
      ids = User.distinct.order("age").ids
      ids.should eq([user1.id_value, user2.id_value])
    end
  end

  describe "method chaining and performance" do
    it "pluck triggers immediate query and cannot be chained further" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)

      # This should work - building query then plucking
      result = User.where("age > 20").order("name").pluck("name")
      result.should be_a(Array(DB::Any))

      # But pluck result cannot be chained further (it's an Array, not QueryBuilder)
      result.responds_to?(:limit).should be_false
    end

    it "pick triggers immediate query and cannot be chained further" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)

      # This should work - building query then picking
      result = User.where("age > 20").pick("name")
      result.should eq("Alice")

      # pick returns a single value, not a chainable object
      result.should be_a(String)
    end

    it "ids triggers immediate query and cannot be chained further" do
      user = User.create(name: "Alice", email: "alice@example.com", age: 25)

      # This should work - building query then getting ids
      result = User.where("age > 20").ids
      result.should be_a(Array(DB::Any))
      result.should eq([user.id_value])

      # But ids result cannot be chained further
      result.responds_to?(:limit).should be_false
    end

    it "select_all triggers immediate query and returns raw data" do
      user = User.create(name: "Alice", email: "alice@example.com", age: 25)

      result = User.select_all("SELECT name FROM users WHERE age > ?", 20)
      result.should be_a(Array(Hash(String, DB::Any)))
      result.first["name"].should eq("Alice")

      # select_all returns array of hashes, not model instances
      result.first.should_not be_a(User)
    end
  end
end
