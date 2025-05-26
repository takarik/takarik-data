require "./spec_helper"

describe Takarik::Data::QueryBuilder do
  # Ensure User class has the connection
  before_all do
    User.establish_connection("sqlite3://./test.db")
    Post.establish_connection("sqlite3://./test.db")
  end

  before_each do
    User.create(name: "Alice", email: "alice@example.com", age: 25, active: true)
    User.create(name: "Bob", email: "bob@example.com", age: 30, active: true)
    User.create(name: "Charlie", email: "charlie@example.com", age: 35, active: false)
    User.create(name: "Diana", email: "diana@example.com", age: 28, active: true)
  end

  describe "basic queries" do
    it "builds simple where queries" do
      query = User.where(active: true)
      query.to_sql.should contain("WHERE active = ?")

      results = query.to_a
      results.size.should eq(3)
      results.all?(&.active).should be_true
    end

    it "builds where queries with multiple conditions" do
      query = User.where(active: true, age: 30)
      query.to_sql.should contain("WHERE active = ? AND age = ?")

      results = query.to_a
      results.size.should eq(1)
      results.first.name.should eq("Bob")
    end

    it "builds where queries with string conditions" do
      query = User.where("age > ?", 28)
      query.to_sql.should contain("WHERE age > ?")

      results = query.to_a
      results.size.should eq(2)
      results.map(&.name).should contain("Bob")
      results.map(&.name).should contain("Charlie")
    end

    it "builds where_not queries" do
      query = User.where_not(active: false)
      query.to_sql.should contain("WHERE active != ?")
    end

    it "builds IN queries" do
      query = User.where("age", [25, 30])
      query.to_sql.should contain("WHERE age IN (?, ?)")
    end

    it "builds NOT IN queries" do
      query = User.where_not("age", [25])
      query.to_sql.should contain("WHERE age NOT IN (?)")
    end

    it "supports different array types for IN queries" do
      # Integer arrays
      query = User.where("age", [25, 30, 35])
      query.to_sql.should contain("WHERE age IN (?, ?, ?)")

      # String arrays
      query = User.where("name", ["Alice", "Bob"])
      query.to_sql.should contain("WHERE name IN (?, ?)")

      # Boolean arrays
      query = User.where("active", [true, false])
      query.to_sql.should contain("WHERE active IN (?, ?)")

      # Float arrays
      query = User.where("score", [85.5, 92.3])
      query.to_sql.should contain("WHERE score IN (?, ?)")

      # NOT IN with different types
      query = User.where_not("name", ["Charlie"])
      query.to_sql.should contain("WHERE name NOT IN (?)")
    end

    it "builds LIKE queries" do
      query = User.where("name LIKE", "A%")
      query.to_sql.should contain("WHERE name LIKE ?")
    end

    it "builds BETWEEN queries" do
      query = User.where("age", 26..32)
      query.to_sql.should contain("WHERE age BETWEEN ? AND ?")
    end

    it "supports different Range types" do
      # Integer ranges
      query = User.where("age", 18..65)
      query.to_sql.should contain("WHERE age BETWEEN ? AND ?")

      # Exclusive ranges
      query = User.where("age", 18...65)
      query.to_sql.should contain("WHERE age >= ? AND age < ?")

      # Float ranges
      query = User.where("score", 80.0..100.0)
      query.to_sql.should contain("WHERE score BETWEEN ? AND ?")

      # String ranges (alphabetical)
      query = User.where("name", "A".."M")
      query.to_sql.should contain("WHERE name BETWEEN ? AND ?")

      # Time ranges
      start_time = Time.utc(2023, 1, 1)
      end_time = Time.utc(2023, 12, 31)
      query = User.where("created_at", start_time..end_time)
      query.to_sql.should contain("WHERE created_at BETWEEN ? AND ?")
    end

    it "builds IS NULL queries" do
      # Test with nil value
      query = User.where("email", nil)
      query.to_sql.should contain("WHERE email IS NULL")
    end

    it "builds IS NOT NULL queries" do
      query = User.where_not(email: nil)
      query.to_sql.should contain("WHERE email IS NOT NULL")
    end

    it "builds comparison queries" do
      query = User.where("age >", 28)
      query.to_sql.should contain("WHERE age > ?")

      query = User.where("age >=", 28)
      query.to_sql.should contain("WHERE age >= ?")

      query = User.where("age <", 30)
      query.to_sql.should contain("WHERE age < ?")

      query = User.where("age <=", 30)
      query.to_sql.should contain("WHERE age <= ?")
    end

    it "supports different parameter types for raw SQL" do
      # Integer parameters
      query = User.where("age > ?", 25)
      query.to_sql.should contain("WHERE age > ?")

      # String parameters
      query = User.where("name LIKE ?", "A%")
      query.to_sql.should contain("WHERE name LIKE ?")

      # Boolean parameters
      query = User.where("active = ?", true)
      query.to_sql.should contain("WHERE active = ?")

      # Float parameters
      query = User.where("score >= ?", 85.5)
      query.to_sql.should contain("WHERE score >= ?")

      # Multiple parameters
      query = User.where("age BETWEEN ? AND ?", 25, 35)
      query.to_sql.should contain("WHERE age BETWEEN ? AND ?")

      # Mixed parameter types
      query = User.where("name = ? AND active = ?", "Alice", true)
      query.to_sql.should contain("WHERE name = ? AND active = ?")
    end

    it "supports variadic parameters for unlimited parameter count" do
      # Multiple integer parameters
      query = User.where("age IN (?, ?, ?)", 25, 30, 35)
      query.to_sql.should contain("WHERE age IN (?, ?, ?)")

      # Multiple string parameters
      query = User.where("name IN (?, ?, ?, ?)", "Alice", "Bob", "Charlie", "Diana")
      query.to_sql.should contain("WHERE name IN (?, ?, ?, ?)")

      # Complex conditions with many parameters
      query = User.where("(age = ? OR age = ?) AND (name = ? OR name = ? OR name = ?)", 25, 30, "Alice", "Bob", "Charlie")
      query.to_sql.should contain("WHERE (age = ? OR age = ?) AND (name = ? OR name = ? OR name = ?)")

      # Boolean parameters
      query = User.where("active IN (?, ?)", true, false)
      query.to_sql.should contain("WHERE active IN (?, ?)")

      # Float parameters
      query = User.where("score IN (?, ?, ?)", 85.5, 90.0, 95.5)
      query.to_sql.should contain("WHERE score IN (?, ?, ?)")

      # Many parameters (testing scalability)
      query = User.where("id IN (?, ?, ?, ?, ?, ?, ?, ?)", 1, 2, 3, 4, 5, 6, 7, 8)
      query.to_sql.should contain("WHERE id IN (?, ?, ?, ?, ?, ?, ?, ?)")
    end

    it "executes enhanced where queries with data" do
      # Test IN queries
      query = User.where("age", [25, 30])
      query.size.should eq(2)
      query.map(&.name).should contain("Alice")
      query.map(&.name).should contain("Bob")

      # Test comparison queries
      query = User.where("age >", 28)
      query.size.should eq(2)

      # Test LIKE queries
      query = User.where("name LIKE", "A%")
      query.size.should eq(1)
      query.first.try(&.name).should eq("Alice")

      # Test BETWEEN queries
      query = User.where("age", 26..32)
      query.size.should eq(2)
      query.map(&.name).should contain("Bob")
      query.map(&.name).should contain("Diana")

      # Test different range types with data
      query = User.where("age", 25..30)
      query.size.should eq(3)  # Alice (25), Bob (30), Diana (28)

      # Test exclusive ranges
      query = User.where("age", 26...30)  # Exclusive end
      query.size.should eq(1)  # Only Diana (28)
      query.first.try(&.name).should eq("Diana")
    end

    it "executes variadic parameter queries with data" do
      # Test multiple integer parameters
      query = User.where("age IN (?, ?)", 25, 30)
      query.size.should eq(2)
      query.map(&.name).should contain("Alice")
      query.map(&.name).should contain("Bob")

      # Test multiple string parameters
      query = User.where("name IN (?, ?)", "Alice", "Charlie")
      query.size.should eq(2)
      query.map(&.name).should contain("Alice")
      query.map(&.name).should contain("Charlie")

      # Test complex conditions with multiple parameters
      query = User.where("(age = ? OR age = ?) AND active = ?", 25, 30, true)
      query.size.should eq(2)  # Alice and Bob (both active)
      query.map(&.name).should contain("Alice")
      query.map(&.name).should contain("Bob")

      # Test many parameters
      query = User.where("age IN (?, ?, ?, ?)", 25, 28, 30, 35)
      query.size.should eq(4)  # All users
      names = query.map(&.name).compact.sort
      names.should eq(["Alice", "Bob", "Charlie", "Diana"])
    end
  end

  describe "ordering" do
    it "orders by single column ascending" do
      query = User.order("age", "ASC")
      query.to_sql.should contain("ORDER BY age ASC")

      results = query.to_a
      results.first.try(&.name).should eq("Alice")  # age 25
      results.last.try(&.name).should eq("Charlie")  # age 35
    end

    it "orders by single column descending" do
      query = User.order("age", "DESC")
      results = query.to_a
      results.first.try(&.name).should eq("Charlie")  # age 35
      results.last.try(&.name).should eq("Alice")  # age 25
    end

    it "orders by multiple columns" do
      query = User.order(active: "DESC", age: "ASC")
      query.to_sql.should contain("ORDER BY active DESC, age ASC")
    end
  end

  describe "limiting and pagination" do
    it "limits results" do
      query = User.limit(2)
      query.to_sql.should contain("LIMIT 2")

      query.size.should eq(2)
    end

    it "offsets results" do
      query = User.order("age", "ASC").offset(1).limit(2)
      query.to_sql.should contain("OFFSET 1")

      query.size.should eq(2)
      query.first.try(&.age).should eq(28)  # Diana, second youngest
    end

    it "paginates results" do
      query = User.order("age", "ASC").page(2, 2)
      query.size.should eq(2)
      query.first.try(&.age).should eq(30)  # Bob, third in age order
    end
  end

  describe "selecting columns" do
    it "selects specific columns" do
      query = User.select("name", "email")
      query.to_sql.should contain("SELECT name, email")
    end

    it "selects columns as array" do
      query = User.select(["name", "age"])
      query.to_sql.should contain("SELECT name, age")
    end
  end

  describe "grouping and having" do
    it "groups by columns" do
      query = User.group("active")
      query.to_sql.should contain("GROUP BY active")
    end

    it "groups by multiple columns" do
      query = User.group("active", "age")
      query.to_sql.should contain("GROUP BY active, age")
    end

    it "adds having clause" do
      query = User.group("active").having("COUNT(*) > ?", 1)
      query.to_sql.should contain("HAVING COUNT(*) > ?")
    end
  end

  describe "joins" do
    before_each do
      user = User.first!
      Post.create(title: "Test Post", content: "Content", user_id: user.id, published: true)
    end

    it "performs inner joins" do
      query = User.inner_join("posts", "posts.user_id = users.id")
      query.to_sql.should contain("INNER JOIN posts ON posts.user_id = users.id")
    end

    it "performs left joins" do
      query = User.left_join("posts", "posts.user_id = users.id")
      query.to_sql.should contain("LEFT JOIN posts ON posts.user_id = users.id")
    end

    it "performs right joins" do
      query = User.right_join("posts", "posts.user_id = users.id")
      query.to_sql.should contain("RIGHT JOIN posts ON posts.user_id = users.id")
    end
  end

  describe "execution methods" do
    it "returns first record" do
      user = User.order("age", "ASC").first
      user.should_not be_nil
      user.try(&.name).should eq("Alice")
    end

    it "returns first record with first!" do
      user = User.order("age", "ASC").first!
      user.name.should eq("Alice")
    end

    it "raises exception when no record found with first!" do
      expect_raises(Exception, "No records found") do
        User.where(name: "NonExistent").first!
      end
    end

    it "returns last record" do
      user = User.order("age", "ASC").last
      user.should_not be_nil
      user.try(&.name).should eq("Charlie")
    end

    it "counts records" do
      count = User.where(active: true).count
      count.should eq(3)
    end

    it "checks if records exist" do
      User.where(active: true).exists?.should be_true
      User.where(name: "NonExistent").exists?.should be_false
    end

    it "checks if query is empty" do
      User.where(active: true).empty?.should be_false
      User.where(name: "NonExistent").empty?.should be_true
    end

    it "checks if any records match" do
      User.where(active: true).any?.should be_true
      User.where(name: "NonExistent").any?.should be_false
    end
  end

  describe "pluck methods" do
    it "plucks single column" do
      names = User.order("age", "ASC").pluck("name")
      names.size.should eq(4)
      names.first.should eq("Alice")
    end

    it "plucks multiple columns" do
      data = User.order("age", "ASC").pluck("name", "age")
      data.size.should eq(4)
      data.first.should eq(["Alice", 25])
    end
  end

  describe "aggregation methods" do
    it "calculates sum" do
      total_age = User.sum("age")
      total_age.should eq(118)  # 25 + 30 + 35 + 28
    end

    it "calculates average" do
      avg_age = User.average("age")
      avg_age.should eq(29.5)  # 118 / 4
    end

    it "finds minimum" do
      min_age = User.minimum("age")
      min_age.should eq(25)
    end

    it "finds maximum" do
      max_age = User.maximum("age")
      max_age.should eq(35)
    end
  end

  describe "update and delete operations" do
    it "updates all matching records" do
      affected = User.where(active: true).update_all(age: 99)
      affected.should eq(3)

      User.where(age: 99).count.should eq(3)
    end

    it "deletes all matching records" do
      affected = User.where(active: false).delete_all
      affected.should eq(1)

      User.count.should eq(3)
    end

    it "destroys all matching records" do
      count = User.where(active: false).destroy_all
      count.should eq(1)

      User.count.should eq(3)
    end
  end

  describe "method chaining" do
    it "chains multiple conditions" do
      query = User
        .where(active: true)
        .where("age >=", 25)
        .order("age", "ASC")
        .limit(2)

      query.size.should eq(2)
      results = query.to_a
      results.first.try(&.name).should eq("Alice")
      results.last.try(&.name).should eq("Diana")
    end

    it "chains with enumerable methods" do
      names = User
        .where(active: true)
        .order("age", "ASC")
        .map(&.name)

      names.should eq(["Alice", "Diana", "Bob"])
    end

    it "uses each for iteration" do
      count = 0
      User.where(active: true).each do |user|
        count += 1
        user.active.should be_true
      end
      count.should eq(3)
    end

    it "uses select for filtering" do
      young_users = User.all.select { |u| (u.age || 0) < 30 }
      young_users.size.should eq(2)
    end

    it "uses find for searching" do
      user = User.all.find { |u| u.name == "Bob" }
      user.should_not be_nil
      user.try(&.name).should eq("Bob")
    end

    it "uses complex chaining" do
      # Test complex chaining
      complex_query = User
        .where(active: true)
        .where("age >=", 25)
        .order("name")
        .limit(5)

      complex_query.to_sql.should contain("WHERE active = ? AND age >= ?")
      complex_query.to_sql.should contain("ORDER BY name ASC")
      complex_query.to_sql.should contain("LIMIT 5")
    end

    it "chains multiple where conditions" do
      query = User
        .where(active: true)
        .where("age >=", 25)
        .where("name LIKE", "A%")

      query.to_sql.should contain("WHERE active = ? AND age >= ? AND name LIKE ?")
    end
  end

  describe "complex queries" do
    it "builds complex query with all features" do
      query = User
        .select("users.name", "users.age")
        .where(active: true)
        .where("age >=", 25)
        .order("age", "DESC")
        .limit(10)
        .offset(0)

      sql = query.to_sql
      sql.should contain("SELECT users.name, users.age")
      sql.should contain("FROM \"users\"")
      sql.should contain("WHERE active = ? AND age >= ?")
      sql.should contain("ORDER BY age DESC")
      sql.should contain("LIMIT 10")
      sql.should contain("OFFSET 0")

      query.size.should eq(3)
      query.first.try(&.age).should eq(30)  # Bob, highest age among active users
    end
  end
end
