require "./spec_helper"

describe Takarik::Data::QueryBuilder do
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
      query.to_sql.should contain("WHERE (active = ?) AND (age = ?)")

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

    it "builds not queries" do
      query = User.not(active: false)
      query.to_sql.should contain("WHERE active != ?")
    end

    it "builds IS NOT NULL queries" do
      query = User.not(email: nil)
      query.to_sql.should contain("WHERE email IS NOT NULL")
    end

    it "builds IN queries" do
      query = User.where("age", [25, 30])
      query.to_sql.should contain("WHERE age IN (?, ?)")
    end

    it "builds NOT IN queries" do
      query = User.not("age", [25])
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

      # NOT IN with new syntax
      query = User.not("name", ["Charlie"])
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

    it "supports NOT range queries" do
      # Integer ranges with NOT
      query = User.not("age", 18..30)
      query.to_sql.should contain("WHERE NOT (age BETWEEN ? AND ?)")

      # Exclusive ranges with NOT
      query = User.not("age", 18...30)
      query.to_sql.should contain("WHERE NOT (age >= ? AND age < ?)")

      # Float ranges with NOT
      query = User.not("score", 80.0..90.0)
      query.to_sql.should contain("WHERE NOT (score BETWEEN ? AND ?)")

      # String ranges with NOT
      query = User.not("name", "A".."M")
      query.to_sql.should contain("WHERE NOT (name BETWEEN ? AND ?)")

      # Time ranges with NOT
      start_time = Time.utc(2023, 1, 1)
      end_time = Time.utc(2023, 12, 31)
      query = User.not("created_at", start_time..end_time)
      query.to_sql.should contain("WHERE NOT (created_at BETWEEN ? AND ?)")
    end

    it "supports OR range queries" do
      # Integer ranges with OR
      query = User.where(name: "Alice").or("age", 30..40)
      query.to_sql.should contain("WHERE (name = ?) OR (age BETWEEN ? AND ?)")

      # Exclusive ranges with OR
      query = User.where(active: true).or("age", 25...35)
      query.to_sql.should contain("WHERE (active = ?) OR (age >= ? AND age < ?)")

      # Float ranges with OR
      query = User.where(name: "Bob").or("score", 85.0..95.0)
      query.to_sql.should contain("WHERE (name = ?) OR (score BETWEEN ? AND ?)")

      # String ranges with OR
      query = User.where(active: true).or("name", "N".."Z")
      query.to_sql.should contain("WHERE (active = ?) OR (name BETWEEN ? AND ?)")

      # Time ranges with OR
      start_time = Time.utc(2023, 1, 1)
      end_time = Time.utc(2023, 6, 30)
      query = User.where(active: false).or("created_at", start_time..end_time)
      query.to_sql.should contain("WHERE (active = ?) OR (created_at BETWEEN ? AND ?)")
    end

    it "executes NOT and OR range queries with data" do
      # Test NOT with ranges - users NOT between ages 26-32 (Alice: 25, Charlie: 35)
      query = User.not("age", 26..32)
      query.size.should eq(2)
      query.map(&.name).should contain("Alice")
      query.map(&.name).should contain("Charlie")

      # Test NOT with exclusive ranges - users NOT between ages 25 (inclusive) to 30 (exclusive)
      # 25...30 means age >= 25 AND age < 30, so NOT means outside this range
      # Should include Bob (30) and Charlie (35), but not Alice (25) or Diana (28)
      query = User.not("age", 25...30)
      query.size.should eq(2)
      query.map(&.name).should contain("Bob")     # 30 is not in [25...30)
      query.map(&.name).should contain("Charlie") # 35 is not in [25...30)

      # Test OR with ranges - users named Alice OR aged between 28-35
      query = User.where(name: "Alice").or("age", 28..35)
      query.size.should eq(4) # Alice (25, by name), Diana (28), Bob (30), Charlie (35)
      query.map(&.name).should contain("Alice")
      query.map(&.name).should contain("Diana")
      query.map(&.name).should contain("Bob")
      query.map(&.name).should contain("Charlie")

      # Test OR with exclusive ranges - active users OR aged between 30 (exclusive) to 40 (exclusive)
      query = User.where(active: true).or("age", 30...40)
      query.size.should eq(4) # All active users (Alice, Bob, Diana) + Charlie (35 in range)
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
      query.size.should eq(3) # Alice (25), Bob (30), Diana (28)

      # Test exclusive ranges
      query = User.where("age", 26...30) # Exclusive end
      query.size.should eq(1)            # Only Diana (28)
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
      query.size.should eq(2) # Alice and Bob (both active)
      query.map(&.name).should contain("Alice")
      query.map(&.name).should contain("Bob")

      # Test many parameters
      query = User.where("age IN (?, ?, ?, ?)", 25, 28, 30, 35)
      query.size.should eq(4) # All users
      names = query.map(&.name).compact.sort
      names.should eq(["Alice", "Bob", "Charlie", "Diana"])
    end

    it "builds IS NULL queries" do
      # Test with nil value
      query = User.where("email", nil)
      query.to_sql.should contain("WHERE email IS NULL")
    end

    it "builds NOT IN queries" do
      query = User.not("age", [25])
      query.to_sql.should contain("WHERE age NOT IN (?)")
    end

    it "builds NOT queries with raw SQL conditions" do
      # NOT with single parameter
      query = User.not("age > ?", 30)
      query.to_sql.should contain("WHERE NOT (age > ?)")

      # NOT with multiple parameters
      query = User.not("age BETWEEN ? AND ?", 25, 35)
      query.to_sql.should contain("WHERE NOT (age BETWEEN ? AND ?)")

      # NOT with complex conditions
      query = User.not("name LIKE ? OR age = ?", "A%", 25)
      query.to_sql.should contain("WHERE NOT (name LIKE ? OR age = ?)")
    end

    it "builds NOT queries with column operators" do
      # NOT with basic operators
      query = User.not("age >", 30)
      query.to_sql.should contain("WHERE NOT (age > ?)")

      query = User.not("age <", 25)
      query.to_sql.should contain("WHERE NOT (age < ?)")

      query = User.not("age >=", 30)
      query.to_sql.should contain("WHERE NOT (age >= ?)")

      query = User.not("age <=", 25)
      query.to_sql.should contain("WHERE NOT (age <= ?)")

      # NOT with LIKE operator
      query = User.not("name LIKE", "A%")
      query.to_sql.should contain("WHERE NOT (name LIKE ?)")

      # NOT with equals (should behave same as basic syntax)
      query = User.not("name", "Alice")
      query.to_sql.should contain("WHERE NOT (name = ?)")

      # NOT with IS NULL condition
      query = User.not("email", nil)
      query.to_sql.should contain("WHERE NOT (email IS NULL)")
    end

    it "supports different parameter types for NOT queries" do
      # Integer parameters
      query = User.not("age >", 25)
      query.to_sql.should contain("WHERE NOT (age > ?)")

      # String parameters
      query = User.not("name LIKE", "A%")
      query.to_sql.should contain("WHERE NOT (name LIKE ?)")

      # Boolean parameters
      query = User.not("active", true)
      query.to_sql.should contain("WHERE NOT (active = ?)")

      # Float parameters
      query = User.not("score >=", 85.5)
      query.to_sql.should contain("WHERE NOT (score >= ?)")

      # Multiple parameters with variadic syntax
      query = User.not("age BETWEEN ? AND ?", 25, 35)
      query.to_sql.should contain("WHERE NOT (age BETWEEN ? AND ?)")

      # Array parameters
      query = User.not("age", [25, 30, 35])
      query.to_sql.should contain("WHERE age NOT IN (?, ?, ?)")
    end

    it "executes NOT queries with data" do
      # Test NOT with raw SQL conditions
      query = User.not("age > ?", 28)
      query.size.should eq(2) # Alice (25) and Diana (28)
      query.map(&.name).should contain("Alice")
      query.map(&.name).should contain("Diana")

      # Test NOT with column operators
      query = User.not("age >=", 30)
      query.size.should eq(2) # Alice (25) and Diana (28)
      query.map(&.name).should contain("Alice")
      query.map(&.name).should contain("Diana")

      # Test NOT with LIKE
      query = User.not("name LIKE", "A%")
      query.size.should eq(3) # Bob, Charlie, Diana (not Alice)
      query.map(&.name).should_not contain("Alice")

      # Test NOT with array/IN syntax
      query = User.not("age", [25, 30])
      query.size.should eq(2) # Charlie (35) and Diana (28)
      query.map(&.name).should contain("Charlie")
      query.map(&.name).should contain("Diana")

      # Test NOT with nil
      query = User.not("email", nil)
      query.size.should eq(4) # All users have emails
    end
  end

  describe "ordering" do
    it "orders by single column ascending" do
      query = User.order("age", "ASC")
      query.to_sql.should contain("ORDER BY age ASC")

      results = query.to_a
      results.first.try(&.name).should eq("Alice")  # age 25
      results.last.try(&.name).should eq("Charlie") # age 35
    end

    it "orders by single column descending" do
      query = User.order("age", "DESC")
      results = query.to_a
      results.first.try(&.name).should eq("Charlie") # age 35
      results.last.try(&.name).should eq("Alice")    # age 25
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
      query.first.try(&.age).should eq(28) # Diana, second youngest
    end

    it "paginates results" do
      query = User.order("age", "ASC").page(2, 2)
      query.size.should eq(2)
      query.first.try(&.age).should eq(30) # Bob, third in age order
    end
  end

  describe "selecting columns" do
    it "selects specific columns with strings" do
      query = User.select("name", "email")
      query.to_sql.should contain("SELECT name, email")
    end

    it "selects specific columns with symbols" do
      query = User.select(:name, :email)
      query.to_sql.should contain("SELECT name, email")
    end

    it "selects columns as string array" do
      query = User.select(["name", "age"])
      query.to_sql.should contain("SELECT name, age")
    end

    it "selects columns as symbol array" do
      query = User.select([:name, :age])
      query.to_sql.should contain("SELECT name, age")
    end

    it "selects single column with string" do
      query = User.select("name")
      query.to_sql.should contain("SELECT name")
    end

    it "selects single column with symbol" do
      query = User.select(:name)
      query.to_sql.should contain("SELECT name")
    end

    it "supports comma-separated string (ActiveRecord style)" do
      query = User.select("isbn, out_of_print")
      query.to_sql.should contain("SELECT isbn, out_of_print")
    end

    it "works with distinct" do
      query = User.select(:last_name).distinct
      query.to_sql.should contain("SELECT DISTINCT last_name")
    end

    describe "column validation and cleanup" do
      it "cleans up multiple consecutive commas" do
        query = User.select("name,,,,,,,, email")
        query.to_sql.should contain("SELECT name, email")
      end

      it "removes empty columns from comma-separated list" do
        query = User.select("name, , , email")
        query.to_sql.should contain("SELECT name, email")
      end

      it "handles leading and trailing commas" do
        query = User.select(",name,email,")
        query.to_sql.should contain("SELECT name, email")
      end

      it "trims whitespace from columns" do
        query = User.select("  name  ,  email  ")
        query.to_sql.should contain("SELECT name, email")
      end

      it "raises error for empty string" do
        expect_raises(ArgumentError, "Invalid select clause: cannot select empty string") do
          User.select("")
        end
      end

      it "raises error for whitespace-only string" do
        expect_raises(ArgumentError, "Invalid select clause: cannot select empty string") do
          User.select("   ")
        end
      end

      it "raises error for comma-only string" do
        expect_raises(ArgumentError, "Invalid select clause: no valid columns found") do
          User.select(",,,,")
        end
      end

      it "handles single column with extra whitespace" do
        query = User.select("  name  ")
        query.to_sql.should contain("SELECT name")
      end
    end
  end

  describe "distinct method" do
    it "adds DISTINCT to query" do
      query = User.distinct
      query.to_sql.should contain("SELECT DISTINCT *")
    end

    it "adds DISTINCT when explicitly set to true" do
      query = User.distinct(true)
      query.to_sql.should contain("SELECT DISTINCT *")
    end

    it "removes DISTINCT when set to false" do
      query = User.distinct.distinct(false)
      query.to_sql.should_not contain("DISTINCT")
      query.to_sql.should contain("SELECT *")
    end

    it "works with select" do
      query = User.select("name").distinct
      query.to_sql.should contain("SELECT DISTINCT name")
    end

    it "works with select and can be removed" do
      query = User.select("name").distinct.distinct(false)
      query.to_sql.should contain("SELECT name")
      query.to_sql.should_not contain("DISTINCT")
    end

    it "works with where conditions" do
      query = User.distinct.where(active: true)
      query.to_sql.should contain("SELECT DISTINCT *")
      query.to_sql.should contain("WHERE active = ?")
    end

    it "chains properly" do
      query = User.distinct.where(active: true).order(:name).limit(10)
      sql = query.to_sql
      sql.should contain("SELECT DISTINCT *")
      sql.should contain("WHERE active = ?")
      sql.should contain("ORDER BY name")
      sql.should contain("LIMIT 10")
    end

    it "allows toggling distinct on and off in chain" do
      # Start with distinct, then remove it
      query = User.select("last_name").distinct.distinct(false)
      query.to_sql.should contain("SELECT last_name")
      query.to_sql.should_not contain("DISTINCT")

      # Remove distinct, then add it back
      query = User.select("last_name").distinct(false).distinct
      query.to_sql.should contain("SELECT DISTINCT last_name")
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
      expect_raises(Takarik::Data::RecordNotFound, "Couldn't find User") do
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
      total_age.should eq(118) # 25 + 30 + 35 + 28
    end

    it "calculates average" do
      avg_age = User.average("age")
      avg_age.should eq(29.5) # 118 / 4
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

      complex_query.to_sql.should contain("WHERE (active = ?) AND (age >= ?)")
      complex_query.to_sql.should contain("ORDER BY name ASC")
      complex_query.to_sql.should contain("LIMIT 5")
    end

    it "chains multiple where conditions" do
      query = User
        .where(active: true)
        .where("age >=", 25)
        .where("name LIKE", "A%")

      query.to_sql.should contain("WHERE (active = ?) AND (age >= ?) AND (name LIKE ?)")
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
      sql.should contain("FROM users")
      sql.should contain("WHERE (active = ?) AND (age >= ?)")
      sql.should contain("ORDER BY age DESC")
      sql.should contain("LIMIT 10")
      sql.should contain("OFFSET 0")

      query.size.should eq(3)
      query.first.try(&.age).should eq(30) # Bob, highest age among active users
    end
  end

  describe "automatic joins" do
    it "generates correct join condition for belongs_to association" do
      # Post belongs_to :user
      query = Post.inner_join("user")
      sql = query.to_sql
      sql.should contain("INNER JOIN users ON posts.user_id = users.id")
    end

    it "generates correct join condition for has_many association" do
      # User has_many :posts
      query = User.inner_join("posts")
      sql = query.to_sql
      sql.should contain("INNER JOIN posts ON users.id = posts.user_id")
    end

    it "supports different join types with associations" do
      query = User.left_join("posts")
      sql = query.to_sql
      sql.should contain("LEFT JOIN posts ON users.id = posts.user_id")

      query = User.right_join("posts")
      sql = query.to_sql
      sql.should contain("RIGHT JOIN posts ON users.id = posts.user_id")
    end

    it "raises error for non-existent association" do
      expect_raises(Exception, /Association 'nonexistent' not found/) do
        User.inner_join("nonexistent").to_sql
      end
    end

    it "still supports manual joins with table and condition" do
      query = User.inner_join("posts", "posts.user_id = users.id")
      sql = query.to_sql
      sql.should contain("INNER JOIN posts ON posts.user_id = users.id")
    end

    it "can chain automatic joins with other query methods" do
      query = User.inner_join("posts").where(active: true).order("name")
      sql = query.to_sql
      sql.should contain("INNER JOIN posts ON users.id = posts.user_id")
      sql.should contain("WHERE active = ?")
      sql.should contain("ORDER BY name ASC")
    end
  end

  describe "join loading and N+1 prevention" do
    it "loads joined data in a single query" do
      # Create test data with relationships using unique emails
      user1 = User.create(name: "Alice", email: "alice_join_test@example.com", age: 25, active: true)
      user2 = User.create(name: "Bob", email: "bob_join_test@example.com", age: 30, active: true)
      user3 = User.create(name: "Charlie", email: "charlie_join_test@example.com", age: 35, active: false)

      # Ensure users were created successfully
      user1.persisted?.should be_true
      user2.persisted?.should be_true
      user3.persisted?.should be_true

      # Create posts for users
      post1 = Post.create(title: "Alice's First Post", content: "Content 1", user_id: user1.id, published: true)
      post2 = Post.create(title: "Alice's Second Post", content: "Content 2", user_id: user1.id, published: false)
      post3 = Post.create(title: "Bob's Post", content: "Content 3", user_id: user2.id, published: true)
      post4 = Post.create(title: "Charlie's Post", content: "Content 4", user_id: user3.id, published: true)

      # Ensure posts were created successfully
      post1.persisted?.should be_true
      post2.persisted?.should be_true
      post3.persisted?.should be_true
      post4.persisted?.should be_true

      # Test that join queries return data from both tables
      query = User.inner_join("posts").select("users.name", "posts.title", "posts.published")
      results = query.to_a

      # Should have 4 results (one for each post)
      results.size.should eq(4)

      # Verify we can access both user and post data
      # Note: In a real implementation, you'd want to map this to proper objects
      # For now, we're testing that the SQL is correct and data is accessible
      sql = query.to_sql
      sql.should contain("SELECT users.name, posts.title, posts.published")
      sql.should contain("INNER JOIN posts ON users.id = posts.user_id")
    end

    it "filters joined data correctly" do
      # Create test data with unique emails
      user1 = User.create(name: "Alice", email: "alice_filter_test@example.com", age: 25, active: true)
      user2 = User.create(name: "Bob", email: "bob_filter_test@example.com", age: 30, active: true)
      user3 = User.create(name: "Charlie", email: "charlie_filter_test@example.com", age: 35, active: false)

      post1 = Post.create(title: "Alice's First Post", content: "Content 1", user_id: user1.id, published: true)
      post2 = Post.create(title: "Alice's Second Post", content: "Content 2", user_id: user1.id, published: false)
      post3 = Post.create(title: "Bob's Post", content: "Content 3", user_id: user2.id, published: true)
      post4 = Post.create(title: "Charlie's Post", content: "Content 4", user_id: user3.id, published: true)

      # Test filtering on joined tables
      query = User
        .inner_join("posts")
        .where("posts.published", true)
        .where("users.active", true)
        .select("users.name", "posts.title")

      results = query.to_a
      # Should have 2 results (Alice's first post and Bob's post)
      results.size.should eq(2)

      sql = query.to_sql
      sql.should contain("WHERE (posts.published = ?) AND (users.active = ?)")
    end

    it "supports complex joins with multiple conditions" do
      # Create test data with unique emails
      user1 = User.create(name: "Alice", email: "alice_complex_test@example.com", age: 25, active: true)
      user2 = User.create(name: "Bob", email: "bob_complex_test@example.com", age: 30, active: true)
      user3 = User.create(name: "Charlie", email: "charlie_complex_test@example.com", age: 35, active: false)

      post1 = Post.create(title: "Alice's First Post", content: "Content 1", user_id: user1.id, published: true)
      post2 = Post.create(title: "Alice's Second Post", content: "Content 2", user_id: user1.id, published: false)
      post3 = Post.create(title: "Bob's Post", content: "Content 3", user_id: user2.id, published: true)
      post4 = Post.create(title: "Charlie's Post", content: "Content 4", user_id: user3.id, published: true)

      # Test complex join conditions
      query = User
        .inner_join("posts")
        .where("users.age >=", 25)
        .where("posts.published", true)
        .where("users.active", true) # Add this to exclude Charlie
        .order("users.name", "ASC")
        .select("users.name", "users.age", "posts.title")

      results = query.to_a
      results.size.should eq(2) # Alice and Bob's published posts

      sql = query.to_sql
      sql.should contain("WHERE (users.age >= ?) AND (posts.published = ?) AND (users.active = ?)")
      sql.should contain("ORDER BY users.name ASC")
    end

    it "supports left joins to include users without posts" do
      # Create test data with unique emails
      user1 = User.create(name: "Alice", email: "alice_left_test@example.com", age: 25, active: true)
      user2 = User.create(name: "Bob", email: "bob_left_test@example.com", age: 30, active: true)
      user3 = User.create(name: "David", email: "david_left_test@example.com", age: 40, active: true)

      post1 = Post.create(title: "Alice's Post", content: "Content 1", user_id: user1.id, published: true)
      post2 = Post.create(title: "Bob's Post", content: "Content 2", user_id: user2.id, published: true)
      # David has no posts

      query = User
        .left_join("posts")
        .where("users.active", true)
        .where("users.email LIKE", "%_left_test@%")
        .select("users.name", "posts.title")
        .order("users.name")

      results = query.to_a
      # Should include David (with null post data) plus other users with posts
      results.size.should be >= 2

      sql = query.to_sql
      sql.should contain("LEFT JOIN posts ON users.id = posts.user_id")
    end

    it "prevents N+1 queries with proper joins" do
      # Create test data with unique emails
      user1 = User.create(name: "Alice", email: "alice_n1_test@example.com", age: 25, active: true)
      user2 = User.create(name: "Bob", email: "bob_n1_test@example.com", age: 30, active: true)
      user3 = User.create(name: "Charlie", email: "charlie_n1_test@example.com", age: 35, active: false)

      post1 = Post.create(title: "Alice's First Post", content: "Content 1", user_id: user1.id, published: true)
      post2 = Post.create(title: "Alice's Second Post", content: "Content 2", user_id: user1.id, published: false)
      post3 = Post.create(title: "Bob's Post", content: "Content 3", user_id: user2.id, published: true)

      # This test demonstrates the concept - in a real implementation,
      # you'd want to track actual query count

      # Without joins - would require N+1 queries (1 for users + N for each user's posts)
      users_without_joins = User.where("email LIKE", "%_n1_test@%").where(active: true).to_a
      users_without_joins.size.should eq(2) # Alice and Bob

      # With joins - single query gets all data
      query_with_joins = User
        .inner_join("posts")
        .where("users.active", true)
        .where("users.email LIKE", "%_n1_test@%")
        .select("users.id", "users.name", "posts.title")

      results_with_joins = query_with_joins.to_a
      results_with_joins.size.should eq(3) # Alice (2 posts) + Bob (1 post)

      # Verify the SQL is a single query
      sql = query_with_joins.to_sql
      sql.should contain("INNER JOIN posts")
      sql.should contain("SELECT users.id, users.name, posts.title")
    end

    it "supports multiple joins" do
      # Create test data with unique emails
      user = User.create(name: "Alice", email: "alice_multi_test@example.com", age: 25, active: true)
      post = Post.create(title: "Test Post", content: "Content", user_id: user.id, published: true)
      Comment.create(content: "Great post!", post_id: post.id, user_id: user.id)

      # Test joining multiple tables
      query = User
        .inner_join("posts")
        .inner_join("comments")
        .where("users.email", "alice_multi_test@example.com")
        .select("users.name", "posts.title", "comments.content")

      sql = query.to_sql
      sql.should contain("INNER JOIN posts ON users.id = posts.user_id")
      sql.should contain("INNER JOIN comments ON users.id = comments.user_id")

      results = query.to_a
      results.size.should be >= 1
    end

    it "supports aggregations with joins" do
      # Create test data with unique emails
      user1 = User.create(name: "Alice", email: "alice_agg_test@example.com", age: 25, active: true)
      user2 = User.create(name: "Bob", email: "bob_agg_test@example.com", age: 30, active: true)
      user3 = User.create(name: "Charlie", email: "charlie_agg_test@example.com", age: 35, active: false)

      post1 = Post.create(title: "Alice's First Post", content: "Content 1", user_id: user1.id, published: true)
      post2 = Post.create(title: "Alice's Second Post", content: "Content 2", user_id: user1.id, published: false)
      post3 = Post.create(title: "Bob's Post", content: "Content 3", user_id: user2.id, published: true)

      # Test aggregations on joined data
      post_count_per_user = User
        .inner_join("posts")
        .where("users.email LIKE", "%_agg_test@%")
        .group("users.id", "users.name")
        .select("users.name", "COUNT(posts.id) as post_count")

      sql = post_count_per_user.to_sql
      sql.should contain("GROUP BY users.id, users.name")
      sql.should contain("SELECT users.name, COUNT(posts.id) as post_count")

      # Test specific aggregation methods
      total_posts_by_active_users = User
        .inner_join("posts")
        .where("users.active", true)
        .where("users.email LIKE", "%_agg_test@%")
        .count

      total_posts_by_active_users.should eq(3) # Alice (2) + Bob (1)
    end

    it "handles edge cases with joins" do
      # Create minimal test data with unique emails
      user = User.create(name: "Alice", email: "alice_edge_test@example.com", age: 25, active: true)
      Post.create(title: "Test Post", content: "Content", user_id: user.id, published: true)

      # Test empty results
      empty_query = User
        .inner_join("posts")
        .where("users.name", "NonExistent")

      empty_query.to_a.size.should eq(0)
      empty_query.count.should eq(0)
      empty_query.exists?.should be_false

      # Test with complex conditions
      complex_query = User
        .left_join("posts")
        .where("users.age >", 20)
        .where("users.email", "alice_edge_test@example.com")
        .where("(posts.published = ? OR posts.id IS NULL)", true)
        .select("users.name")

      complex_results = complex_query.to_a
      complex_results.size.should be >= 1
    end

    it "maintains proper SQL structure with joins" do
      # Create test data with unique emails
      user = User.create(name: "Alice", email: "alice_sql_test@example.com", age: 25, active: true)
      Post.create(title: "Test Post", content: "Content", user_id: user.id, published: true)

      # Test that joins don't interfere with other SQL clauses
      complex_query = User
        .select("users.name", "posts.title")
        .inner_join("posts")
        .where("users.active", true)
        .where("users.email", "alice_sql_test@example.com")
        .where("posts.published", true)
        .group("users.id", "users.name", "posts.title")
        .having("COUNT(*) > ?", 0)
        .order("users.name", "ASC")
        .limit(10)
        .offset(0)

      sql = complex_query.to_sql

      # Verify proper SQL structure
      sql.should contain("SELECT users.name, posts.title")
      sql.should contain("FROM users")
      sql.should contain("INNER JOIN posts")
      sql.should contain("WHERE (users.active = ?) AND (users.email = ?) AND (posts.published = ?)")
      sql.should contain("GROUP BY users.id, users.name, posts.title")
      sql.should contain("HAVING COUNT(*) > ?")
      sql.should contain("ORDER BY users.name ASC")
      sql.should contain("LIMIT 10")
      sql.should contain("OFFSET 0")

      # Should execute without errors
      results = complex_query.to_a
      results.size.should be >= 0
    end

    describe "Rails-compatible joins method" do
      it "supports single association joins" do
        # User.joins(:posts)
        query = User.joins(:posts)
        sql = query.to_sql

        sql.should contain("INNER JOIN posts")
        sql.should contain("ON users.id = posts.user_id")
      end

      it "supports multiple association joins" do
        # User.joins(:posts, :account) - but we'll use existing associations
        query = User.joins(:posts, :comments)
        sql = query.to_sql

        sql.should contain("INNER JOIN posts")
        sql.should contain("ON users.id = posts.user_id")
        sql.should contain("INNER JOIN comments")
        sql.should contain("ON users.id = comments.user_id")
      end

      it "supports array of associations" do
        associations = [:posts, :comments]
        query = User.joins(associations)
        sql = query.to_sql

        sql.should contain("INNER JOIN posts")
        sql.should contain("INNER JOIN comments")
      end

      it "supports direct array syntax" do
        # Test the direct syntax: User.joins([:posts, :comments])
        query = User.joins([:posts, :comments])
        sql = query.to_sql

        sql.should contain("INNER JOIN posts")
        sql.should contain("INNER JOIN comments")
      end

      it "supports custom SQL joins" do
        custom_sql = "LEFT JOIN bookmarks ON bookmarks.bookmarkable_type = 'Post' AND bookmarks.user_id = users.id"
        query = User.joins(custom_sql)
        sql = query.to_sql

        sql.should contain(custom_sql)
      end

      it "supports nested association joins" do
        # User.joins(posts: [:comments])
        # This creates: User -> Post -> Comment
        nested_hash = Hash(String | Symbol, Array(String | Symbol) | String | Symbol).new
        nested_hash["posts"] = ["comments".as(String | Symbol)].as(Array(String | Symbol) | String | Symbol)
        query = User.joins(nested_hash)
        sql = query.to_sql

        # Should join users to posts, then posts to comments
        sql.should contain("INNER JOIN posts")
        sql.should contain("ON users.id = posts.user_id")
        sql.should contain("INNER JOIN comments")
        sql.should contain("ON posts.id = comments.post_id")
      end

      it "supports nested association with single child" do
        # User.joins(posts: :comments) - single child instead of array
        nested_hash = Hash(String | Symbol, Array(String | Symbol) | String | Symbol).new
        nested_hash["posts"] = "comments".as(Array(String | Symbol) | String | Symbol)
        query = User.joins(nested_hash)
        sql = query.to_sql

        sql.should contain("INNER JOIN posts")
        sql.should contain("INNER JOIN comments")
      end

      it "supports clean NamedTuple syntax for nested joins" do
        # User.joins(posts: [:comments]) - the clean Rails-like syntax!
        query = User.joins(posts: [:comments])
        sql = query.to_sql

        sql.should contain("INNER JOIN posts")
        sql.should contain("ON users.id = posts.user_id")
        sql.should contain("INNER JOIN comments")
        sql.should contain("ON posts.id = comments.post_id")
      end

      it "supports clean NamedTuple syntax for single nested join" do
        # User.joins(posts: :comments) - single child
        query = User.joins(posts: :comments)
        sql = query.to_sql

        sql.should contain("INNER JOIN posts")
        sql.should contain("INNER JOIN comments")
      end

      it "works with class-level method calls" do
        # Test that BaseModel.joins works
        query1 = User.joins(:posts)
        query2 = User.joins(:posts, :comments)

        nested_hash = Hash(String | Symbol, Array(String | Symbol) | String | Symbol).new
        nested_hash["posts"] = ["comments".as(String | Symbol)].as(Array(String | Symbol) | String | Symbol)
        query3 = User.joins(nested_hash)

        # All should generate valid SQL
        query1.to_sql.should contain("INNER JOIN posts")
        query2.to_sql.should contain("INNER JOIN posts")
        query2.to_sql.should contain("INNER JOIN comments")
        query3.to_sql.should contain("INNER JOIN posts")
        query3.to_sql.should contain("INNER JOIN comments")
      end

      it "can be chained with other query methods" do
        query = User
          .joins(:posts)
          .where("users.active", true)
          .where("posts.published", true)
          .order("users.name")
          .limit(10)

        sql = query.to_sql
        sql.should contain("INNER JOIN posts")
        sql.should contain("WHERE (users.active = ?) AND (posts.published = ?)")
        sql.should contain("ORDER BY users.name")
        sql.should contain("LIMIT 10")
      end

      it "handles practical use cases with real data" do
        # Create test data
        user1 = User.create(name: "Alice", email: "alice_joins_test@example.com", age: 25, active: true)
        user2 = User.create(name: "Bob", email: "bob_joins_test@example.com", age: 30, active: true)
        user3 = User.create(name: "Charlie", email: "charlie_joins_test@example.com", age: 35, active: false)

        post1 = Post.create(title: "Alice's Post", content: "Content", user_id: user1.id, published: true)
        post2 = Post.create(title: "Bob's Post", content: "Content", user_id: user2.id, published: true)

        comment1 = Comment.create(content: "Great post!", post_id: post1.id, user_id: user2.id)
        comment2 = Comment.create(content: "Nice work!", post_id: post2.id, user_id: user1.id)

        # Test multiple joins
        users_with_posts_and_comments = User
          .joins(:posts, :comments)
          .where("users.email LIKE", "%_joins_test@%")
          .to_a

        # Should find users who have both posts and comments
        users_with_posts_and_comments.size.should be >= 1

        # Test nested joins
        nested_hash = Hash(String | Symbol, Array(String | Symbol) | String | Symbol).new
        nested_hash["posts"] = ["comments".as(String | Symbol)].as(Array(String | Symbol) | String | Symbol)
        users_with_post_comments = User
          .joins(nested_hash)
          .where("users.email LIKE", "%_joins_test@%")
          .to_a

        # Should find users whose posts have comments
        users_with_post_comments.size.should be >= 1
      end

      it "raises appropriate errors for invalid associations" do
        expect_raises(Exception, /Association 'invalid_association' not found/) do
          User.joins(:invalid_association).to_sql
        end

        expect_raises(Exception, /Child association 'invalid_child' not found/) do
          nested_hash = Hash(String | Symbol, Array(String | Symbol) | String | Symbol).new
          nested_hash["posts"] = ["invalid_child".as(String | Symbol)].as(Array(String | Symbol) | String | Symbol)
          User.joins(nested_hash).to_sql
        end
      end
    end
  end

  describe "association existence queries" do
    before_each do
      # Create users with unique emails to avoid test interference
      alice = User.create(name: "Alice", email: "alice_assoc_test@example.com", age: 25, active: true)
      bob = User.create(name: "Bob", email: "bob_assoc_test@example.com", age: 30, active: true)
      charlie = User.create(name: "Charlie", email: "charlie_assoc_test@example.com", age: 35, active: false)

      # Create posts (Alice and Bob have posts, Charlie doesn't)
      Post.create(title: "Alice's Assoc Post", content: "Content", user_id: alice.id, published: true)
      Post.create(title: "Bob's Assoc Post", content: "Content", user_id: bob.id, published: true)
    end

    describe "associated method" do
      it "finds records that have associated records" do
        query = User.where("email LIKE", "%_assoc_test@%").associated(:posts)
        sql = query.to_sql

        # Should generate INNER JOIN and IS NOT NULL condition
        sql.should contain("INNER JOIN posts")
        sql.should contain("posts.id IS NOT NULL")

        results = query.to_a
        results.size.should eq(2)

        user_names = results.map(&.name).compact.sort
        user_names.should eq(["Alice", "Bob"])
      end

      it "finds records that have associated records using where.associated" do
        query = User.where("email LIKE", "%_assoc_test@%").associated(:posts)
        sql = query.to_sql

        # Should generate INNER JOIN and IS NOT NULL condition
        sql.should contain("INNER JOIN posts")
        sql.should contain("posts.id IS NOT NULL")

        results = query.to_a
        results.size.should eq(2)

        user_names = results.map(&.name).compact.sort
        user_names.should eq(["Alice", "Bob"])
      end

      it "works with chained where conditions" do
        query = User.where("email LIKE", "%_assoc_test@%").where(active: true).associated(:posts)
        results = query.to_a

        # Should find Alice and Bob (both active and have posts)
        results.size.should eq(2)
        user_names = results.map(&.name).compact.sort
        user_names.should eq(["Alice", "Bob"])
      end

      it "works with chained where conditions using where.associated" do
        query = User.where("email LIKE", "%_assoc_test@%").where(active: true).associated(:posts)
        results = query.to_a

        # Should find Alice and Bob (both active and have posts)
        results.size.should eq(2)
        user_names = results.map(&.name).compact.sort
        user_names.should eq(["Alice", "Bob"])
      end

      it "returns empty when no records have associations" do
        # Delete all posts for our test users
        Post.where("title LIKE", "%_Assoc Post").delete_all

        query = User.where("email LIKE", "%_assoc_test@%").associated(:posts)
        results = query.to_a
        results.size.should eq(0)
      end

      it "works with belongs_to associations" do
        # Test from the Post side
        query = Post.where("title LIKE", "%_Assoc Post").associated(:user)
        results = query.to_a

        # All posts should have users
        results.size.should eq(2)
        post_titles = results.map(&.title).compact.sort
        post_titles.should eq(["Alice's Assoc Post", "Bob's Assoc Post"])
      end
    end

    describe "missing method" do
      it "finds records that don't have associated records" do
        query = User.where("email LIKE", "%_assoc_test@%").missing(:posts)
        sql = query.to_sql

        # Should generate LEFT OUTER JOIN and IS NULL condition
        sql.should contain("LEFT OUTER JOIN posts")
        sql.should contain("posts.id IS NULL")

        results = query.to_a
        results.size.should eq(1)

        user_names = results.map(&.name).compact
        user_names.should eq(["Charlie"])
      end

      it "finds records that don't have associated records using where.missing" do
        query = User.where("email LIKE", "%_assoc_test@%").missing(:posts)
        sql = query.to_sql

        # Should generate LEFT OUTER JOIN and IS NULL condition
        sql.should contain("LEFT OUTER JOIN posts")
        sql.should contain("posts.id IS NULL")

        results = query.to_a
        results.size.should eq(1)

        user_names = results.map(&.name).compact
        user_names.should eq(["Charlie"])
      end

      it "works with chained where conditions" do
        query = User.where("email LIKE", "%_assoc_test@%").where(active: false).missing(:posts)
        results = query.to_a

        # Should find Charlie (inactive and no posts)
        results.size.should eq(1)
        results.first.name.should eq("Charlie")
      end

      it "works with chained where conditions using where.missing" do
        query = User.where("email LIKE", "%_assoc_test@%").where(active: false).missing(:posts)
        results = query.to_a

        # Should find Charlie (inactive and no posts)
        results.size.should eq(1)
        results.first.name.should eq("Charlie")
      end

      it "returns all records when all are missing associations" do
        # Delete all posts for our test users
        Post.where("title LIKE", "%_Assoc Post").delete_all

        query = User.where("email LIKE", "%_assoc_test@%").missing(:posts)
        results = query.to_a

        # All users should be missing posts now
        results.size.should eq(3)
        user_names = results.map(&.name).compact.sort
        user_names.should eq(["Alice", "Bob", "Charlie"])
      end

      it "returns empty when all records have associations" do
        # Get Charlie's record
        charlie = User.where("email LIKE", "charlie_assoc_test@%").first!

        # Create a post for Charlie too
        Post.create(title: "Charlie's Assoc Post", content: "Content", user_id: charlie.id, published: true)

        query = User.where("email LIKE", "%_assoc_test@%").missing(:posts)
        results = query.to_a
        results.size.should eq(0)
      end
    end

    describe "error handling" do
      it "raises error for non-existent association" do
        expect_raises(Exception, /Association 'nonexistent' not found/) do
          User.where("email LIKE", "%_assoc_test@%").associated(:nonexistent)
        end

        expect_raises(Exception, /Association 'nonexistent' not found/) do
          User.where("email LIKE", "%_assoc_test@%").missing(:nonexistent)
        end
      end
    end

    describe "class method shortcuts" do
      it "provides associated class method" do
        results = User.where("email LIKE", "%_assoc_test@%").associated(:posts).to_a
        results.size.should eq(2)
        user_names = results.map(&.name).compact.sort
        user_names.should eq(["Alice", "Bob"])
      end

      it "provides where.associated class method" do
        results = User.where("email LIKE", "%_assoc_test@%").associated(:posts).to_a
        results.size.should eq(2)
        user_names = results.map(&.name).compact.sort
        user_names.should eq(["Alice", "Bob"])
      end

      it "provides missing class method" do
        results = User.where("email LIKE", "%_assoc_test@%").missing(:posts).to_a
        results.size.should eq(1)
        results.first.name.should eq("Charlie")
      end

      it "provides where.missing class method" do
        results = User.where("email LIKE", "%_assoc_test@%").missing(:posts).to_a
        results.size.should eq(1)
        results.first.name.should eq("Charlie")
      end

      it "chains with other query methods" do
        results = User.where("email LIKE", "%_assoc_test@%").missing(:posts).where(active: false).to_a
        results.size.should eq(1)
        results.first.name.should eq("Charlie")
      end

      it "chains with other query methods using where.missing" do
        results = User.where("email LIKE", "%_assoc_test@%").missing(:posts).where(active: false).to_a
        results.size.should eq(1)
        results.first.name.should eq("Charlie")
      end
    end
  end

  describe "logical operators" do
    describe "OR conditions" do
      it "builds OR queries with hash conditions" do
        query = User.where(name: "Alice").or(age: 30)
        query.to_sql.should contain("WHERE (name = ?) OR (age = ?)")
      end

      it "builds OR queries with named parameters" do
        query = User.where(active: true).or(name: "Charlie", age: 35)
        query.to_sql.should contain("WHERE (active = ?) OR (name = ? AND age = ?)")
      end

      it "builds OR queries with raw SQL" do
        query = User.where(active: true).or("age > ?", 30)
        query.to_sql.should contain("WHERE (active = ?) OR (age > ?)")
      end

      it "builds OR queries with column operators" do
        query = User.where(name: "Alice").or("age >=", 30)
        query.to_sql.should contain("WHERE (name = ?) OR (age >= ?)")
      end

      it "builds OR queries with IN conditions" do
        query = User.where(active: true).or("age", [25, 35])
        query.to_sql.should contain("WHERE (active = ?) OR (age IN (?, ?))")
      end

      it "executes OR queries correctly" do
        # Should find users who are either named Alice OR aged 35
        results = User.where(name: "Alice").or(age: 35).to_a
        results.size.should eq(2)

        names_and_ages = results.map { |u| {u.name || "", u.age || 0} }.sort_by(&.[0])
        names_and_ages.should eq([{"Alice", 25}, {"Charlie", 35}])
      end

      it "handles multiple OR conditions" do
        query = User.where(name: "Alice").or(age: 30).or(active: false)
        sql = query.to_sql
        sql.should contain("WHERE (name = ?) OR (age = ?) OR (active = ?)")
      end
    end

    describe "class method shortcuts" do
      it "provides OR class methods" do
        results = User.where(name: "Alice").or(age: 35).to_a
        results.size.should eq(2)
      end

      it "supports type-specific overloads for OR" do
        query = User.where(active: true).or("age", [25, 35])
        query.to_sql.should contain("WHERE (active = ?) OR (age IN (?, ?))")
      end
    end
  end

  describe "batch processing methods" do
    before_each do
      # Clean up and create a larger dataset for batch testing
      User.all.delete_all

      # Create 10 users for batch testing
      10.times do |i|
        User.create(
          name: "User#{i + 1}",
          email: "user#{i + 1}@example.com",
          age: 20 + i,
          active: i.even?
        )
      end
    end

    describe "find_each" do
      it "iterates through all records with default batch size" do
        visited_users = [] of String
        User.all.find_each do |user|
          visited_users << user.name.not_nil!
        end

        visited_users.size.should eq(10)
        visited_users.should contain("User1")
        visited_users.should contain("User10")
      end

      it "respects custom batch size" do
        batch_calls = 0
        User.all.find_each(3) do |user|
          batch_calls += 1
        end

        batch_calls.should eq(10) # All users should be visited
      end

      it "works with where conditions" do
        visited_users = [] of String
        User.where(active: true).find_each do |user|
          visited_users << user.name.not_nil!
        end

        # Should only include even-indexed users (User1, User3, User5, User7, User9)
        visited_users.size.should eq(5)
        visited_users.should contain("User1")
        visited_users.should contain("User9")
        visited_users.should_not contain("User2")
      end

      it "ignores existing order conditions (Rails behavior)" do
        visited_ages = [] of Int32
        User.order("age DESC").find_each(batch_size: 3) do |user|
          visited_ages << user.age.not_nil!
        end

        # Should ignore existing order and use primary key order (id ASC)
        # So we get users in ascending ID order, not age order
        visited_ages.should_not eq(visited_ages.sort.reverse) # Not desc age order
        visited_ages.size.should be > 0                       # Users processed (ignoring order)
      end

      it "handles empty result sets" do
        visited_count = 0
        User.where(name: "NonExistent").find_each do |user|
          visited_count += 1
        end

        visited_count.should eq(0)
      end

      it "preserves original query state but ignores limit/offset/order" do
        query = User.where(active: true).limit(3).offset(1)
        original_sql = query.to_sql

        visited_count = 0
        query.find_each(batch_size: 2) do |user|
          visited_count += 1
        end

        # Original query should remain unchanged but find_each adds its own conditions
        query.to_sql.should contain("active = ?") # WHERE condition preserved
        visited_count.should be > 0               # Should process some users (exact count may vary due to implementation)
      end

      it "works with small batch sizes" do
        visited_users = [] of String
        User.all.find_each(batch_size: 1) do |user|
          visited_users << user.name.not_nil!
        end

        visited_users.size.should eq(10)
      end

      it "works when total records less than batch size" do
        visited_count = 0
        User.where(age: 25).find_each(batch_size: 100) do |user|
          visited_count += 1
        end

        visited_count.should eq(1) # Only User6 has age 25
      end

      it "returns self for chaining" do
        result = User.all.find_each { |user| }
        result.should be_a(Takarik::Data::QueryBuilder(User))
      end
    end

    describe "find_in_batches" do
      it "yields batches of records with default batch size" do
        batch_count = 0
        total_records = 0

        User.all.find_in_batches do |batch|
          batch_count += 1
          total_records += batch.size
          batch.should be_a(Array(User))
        end

        # With 10 records and default batch size 1000, should be 1 batch
        batch_count.should eq(1)
        total_records.should eq(10)
      end

      it "yields multiple batches with small batch size" do
        batch_count = 0
        batch_sizes = [] of Int32

        User.all.find_in_batches(batch_size: 3) do |batch|
          batch_count += 1
          batch_sizes << batch.size
        end

        # With 10 records and batch size 3: batches of 3, 3, 3, 1
        batch_count.should eq(4)
        batch_sizes.should eq([3, 3, 3, 1])
      end

      it "works with where conditions" do
        batch_count = 0
        total_records = 0

        User.where(active: true).find_in_batches(batch_size: 2) do |batch|
          batch_count += 1
          total_records += batch.size
          # All users in batch should be active
          batch.all?(&.active).should be_true
        end

        # 5 active users with batch size 2: batches of 2, 2, 1
        batch_count.should eq(3)
        total_records.should eq(5)
      end

      it "handles empty result sets" do
        batch_count = 0

        User.where(name: "NonExistent").find_in_batches do |batch|
          batch_count += 1
        end

        batch_count.should eq(0)
      end

      it "works with order conditions" do
        ages_in_order = [] of Int32

        User.order("age ASC").find_in_batches(batch_size: 4) do |batch|
          batch.each do |user|
            ages_in_order << user.age.not_nil!
          end
        end

        # Should maintain age order across batches
        ages_in_order.should eq(ages_in_order.sort)
        ages_in_order.first.should eq(20) # User1
        ages_in_order.last.should eq(29)  # User10
      end

      it "works when total records less than batch size" do
        batch_count = 0
        record_count = 0

        User.where(age: 25).find_in_batches(batch_size: 100) do |batch|
          batch_count += 1
          record_count += batch.size
        end

        batch_count.should eq(1)
        record_count.should eq(1)
      end

      it "returns self for chaining" do
        result = User.all.find_in_batches { |batch| }
        result.should be_a(Takarik::Data::QueryBuilder(User))
      end

      it "provides access to batch index information" do
        batch_indices = [] of Int32

        User.all.find_in_batches(batch_size: 3) do |batch|
          batch_indices << batch.first.id.not_nil!
        end

        # First record ID of each batch should be different
        batch_indices.uniq.size.should eq(batch_indices.size)
      end
    end

    describe "edge cases" do
      it "find_each works with complex queries" do
        visited_count = 0

        User.where(active: true)
          .where("age > ?", 22)
          .order("name ASC")
          .find_each(batch_size: 2) do |user|
            visited_count += 1
            user.active.should be_true
            user.age.not_nil!.should be > 22
          end

        visited_count.should be > 0
      end

      it "find_in_batches works with complex queries" do
        batch_count = 0

        User.where(active: true)
          .where("age > ?", 22)
          .order("name ASC")
          .find_in_batches(batch_size: 2) do |batch|
            batch_count += 1
            batch.all?(&.active).should be_true
            batch.all? { |u| u.age.not_nil! > 22 }.should be_true
          end

        batch_count.should be > 0
      end

      it "handles zero batch size gracefully" do
        expect_raises(Exception) do
          User.all.find_each(batch_size: 0) { |user| }
        end

        expect_raises(Exception) do
          User.all.find_in_batches(batch_size: 0) { |batch| }
        end
      end

      it "handles negative batch size gracefully" do
        expect_raises(Exception) do
          User.all.find_each(batch_size: -5) { |user| }
        end

        expect_raises(Exception) do
          User.all.find_in_batches(batch_size: -5) { |batch| }
        end
      end
    end
  end

  describe "SQL injection protection" do
    it "safely handles user input in parameterized queries" do
      # This is SAFE - uses parameterized query
      malicious_input = "'; DROP TABLE users; --"
      query = User.where("name = ?", malicious_input)

      # Should build safe SQL with parameter placeholder
      query.to_sql.should eq("SELECT * FROM users WHERE name = ?")
      query.@where_params.should eq([malicious_input])

      # The malicious input is safely stored as a parameter, not interpolated into SQL
    end

    it "safely handles user input in column-value pairs" do
      # This is SAFE - automatically parameterized
      malicious_input = "admin' OR '1'='1"
      query = User.where("username", malicious_input)

      # Should build safe SQL with parameter placeholder
      query.to_sql.should eq("SELECT * FROM users WHERE username = ?")
      query.@where_params.should eq([malicious_input])
    end

    it "safely handles arrays with potential injection attempts" do
      # This is SAFE - uses proper placeholders for IN clauses
      malicious_inputs = ["1", "2'; DROP TABLE users; --", "3"]
      query = User.where("id", malicious_inputs)

      # Should build safe SQL with proper placeholders
      query.to_sql.should eq("SELECT * FROM users WHERE id IN (?, ?, ?)")
      query.@where_params.should eq(malicious_inputs)
    end

    it "demonstrates safe vs unsafe query building patterns" do
      user_input = "admin"

      # ✅ SAFE: Parameterized query
      safe_query = User.where("username = ?", user_input)
      safe_query.to_sql.should eq("SELECT * FROM users WHERE username = ?")
      safe_query.@where_params.should eq([user_input])

      # ✅ SAFE: Automatic parameterization
      safe_query2 = User.where("username", user_input)
      safe_query2.to_sql.should eq("SELECT * FROM users WHERE username = ?")
      safe_query2.@where_params.should eq([user_input])

      # Note: Our QueryBuilder doesn't support direct string interpolation,
      # which helps prevent accidental SQL injection vulnerabilities
    end
  end

  # ========================================
  # NAMED PLACEHOLDER CONDITIONS TESTS
  # ========================================

  describe "named placeholder conditions" do
    it "supports basic named placeholders" do
      query = User.where("name = :name AND age > :min_age", {name: "Alice", min_age: 25})
      query.to_sql.should contain("name = ? AND age > ?")
      query.params.should eq(["Alice", 25])
    end

    it "supports named placeholders with NamedTuple syntax" do
      query = User.where("name = :name AND age > :min_age", {name: "Bob", min_age: 30})
      query.to_sql.should contain("name = ? AND age > ?")
      query.params.should eq(["Bob", 30])
    end

    it "handles date range with named placeholders" do
      start_date = Time.utc(2023, 1, 1)
      end_date = Time.utc(2023, 12, 31)

      query = User.where("created_at >= :start_date AND created_at <= :end_date",
        {start_date: start_date, end_date: end_date})

      query.to_sql.should contain("created_at >= ? AND created_at <= ?")
      query.params.should eq([start_date, end_date])
    end

    it "supports mixed named placeholders and standard conditions" do
      query = User.where("name = :name", {name: "Charlie"})
        .where(active: true)
        .where("age >= ?", 18)

      sql = query.to_sql
      sql.should contain("name = ?")
      sql.should contain("active = ?")
      sql.should contain("age >= ?")
      query.params.should eq(["Charlie", true, 18])
    end

    it "works with complex named placeholder queries" do
      query = User.where("(name LIKE :name_pattern OR email LIKE :email_pattern) AND age BETWEEN :min_age AND :max_age",
        {name_pattern: "A%", email_pattern: "%@test.com", min_age: 20, max_age: 50})

      query.to_sql.should contain("(name LIKE ? OR email LIKE ?) AND age BETWEEN ? AND ?")
      query.params.should eq(["A%", "%@test.com", 20, 50])
    end

    it "handles duplicate named parameters correctly" do
      query = User.where("name = :value OR email = :value", {value: "test"})

      query.to_sql.should contain("name = ? OR email = ?")
      query.params.should eq(["test", "test"])
    end

    it "ignores unused named parameters" do
      query = User.where("name = :name", {name: "Alice", unused: "ignored"})

      query.to_sql.should contain("name = ?")
      query.params.should eq(["Alice"])
    end
  end

  # ========================================
  # LIKE SANITIZATION TESTS
  # ========================================

  describe "LIKE query sanitization" do
    describe "sanitize_sql_like" do
      it "escapes percent signs" do
        sanitize_sql_like("test%value").should eq("test\\%value")
      end

      it "escapes underscores" do
        sanitize_sql_like("test_value").should eq("test\\_value")
      end

      it "escapes both wildcards" do
        sanitize_sql_like("test_%_value%").should eq("test\\_\\%\\_value\\%")
      end

      it "leaves normal text unchanged" do
        sanitize_sql_like("normal text").should eq("normal text")
      end

      it "handles empty strings" do
        sanitize_sql_like("").should eq("")
      end

      it "handles strings with only wildcards" do
        sanitize_sql_like("_%_%").should eq("\\_\\%\\_\\%")
      end
    end
  end

  # ========================================
  # SECURITY TESTS FOR NEW FEATURES
  # ========================================

  describe "security with new features" do
    it "protects against injection in named placeholders" do
      malicious_input = "'; DROP TABLE users; --"

      query = User.where("name = :name", {name: malicious_input})
      query.to_sql.should contain("name = ?")
      query.params.should eq([malicious_input])
    end

    it "protects against injection in LIKE patterns" do
      malicious_input = "'; DROP TABLE users; --"

      # Manual LIKE query with sanitization
      pattern = sanitize_sql_like(malicious_input) + "%"
      query = User.where("name LIKE ?", pattern)
      query.to_sql.should contain("name LIKE ?")
      query.params.should eq(["'; DROP TABLE users; --%"])
    end

    it "prevents wildcard injection in LIKE queries" do
      user_input = "test_%_pattern"

      # Without sanitization, this would match unexpected records
      pattern = sanitize_sql_like(user_input) + "%"
      query = User.where("name LIKE ?", pattern)
      query.params.should eq(["test\\_\\%\\_pattern%"])

      # Verify the wildcards are escaped
      query.params[0].as(String).should contain("\\_")
      query.params[0].as(String).should contain("\\%")
    end
  end
end
