require "./spec_helper"

describe "calculation methods" do
  before_each do
    # Clean up any existing test data
    User.delete_all
    Post.delete_all
  end

  describe ".count" do
    it "returns 0 when no records exist" do
      User.count.should eq(0)
    end

    it "counts all records" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.count.should eq(3)
    end

    it "counts records with conditions" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.where("age > 28").count.should eq(2)
      User.where("age > 40").count.should eq(0)
      User.where(name: "Alice").count.should eq(1)
    end

    it "counts records with joins" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)
      user3 = User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      Post.create(title: "Alice's Post", content: "Content", user_id: user1.id_value)
      Post.create(title: "Bob's Post", content: "Content", user_id: user2.id_value)

      User.joins(:posts).count.should eq(2) # Only users with posts
    end

    it "counts records with limit" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.limit(2).count.should eq(2)
      User.limit(5).count.should eq(3)
    end

    it "counts records with group by" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 25) # Same age
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      result = User.group("age").count
      result.should be_a(Hash(String, Int64))
      hash_result = result.as(Hash(String, Int64))
      hash_result["25"].should eq(2)
      hash_result["35"].should eq(1)
    end
  end

  describe ".count(column)" do
    it "counts non-null values in a specific column" do
      # Create users with some having nil values in certain fields
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      # All users have names and emails
      User.count("name").should eq(3)
      User.count("email").should eq(3)
      User.count("age").should eq(3)
    end

    it "works with symbols" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)

      User.count(:name).should eq(2)
      User.count(:email).should eq(2)
    end

    it "counts with conditions" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.where("age > 28").count("name").should eq(2)
      User.where("age > 40").count("name").should eq(0)
    end

    it "counts with group by" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 25) # Same age
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      result = User.group("age").count("name")
      result.should be_a(Hash(String, Int64))
      hash_result = result.as(Hash(String, Int64))
      hash_result["25"].should eq(2)
      hash_result["35"].should eq(1)
    end
  end

  describe ".sum" do
    it "calculates sum of a numeric column" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.sum("age").should eq(90)
    end

    it "works with symbols" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)

      User.sum(:age).should eq(55)
    end

    it "calculates sum with conditions" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.where("age > 28").sum("age").should eq(65) # 30 + 35
      User.where("age > 40").sum("age").should eq(0)  # No records
    end

    it "calculates sum with joins" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)
      user3 = User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      Post.create(title: "Alice's Post", content: "Content", user_id: user1.id_value)
      Post.create(title: "Bob's Post", content: "Content", user_id: user2.id_value)

      User.joins(:posts).sum("age").should eq(55) # 25 + 30 (only users with posts)
    end

    it "returns 0 for empty result set" do
      User.sum("age").should eq(0)
      User.where("age > 100").sum("age").should eq(0)
    end
  end

  describe ".average" do
    it "calculates average of a numeric column" do
      User.create(name: "Alice", email: "alice@example.com", age: 20)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 40)

      result = User.average("age")
      result.should eq(30.0)
    end

    it "works with symbols" do
      User.create(name: "Alice", email: "alice@example.com", age: 20)
      User.create(name: "Bob", email: "bob@example.com", age: 40)

      result = User.average(:age)
      result.should eq(30.0)
    end

    it "calculates average with conditions" do
      User.create(name: "Alice", email: "alice@example.com", age: 20)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 40)

      result = User.where("age >= 30").average("age")
      result.should eq(35.0) # (30 + 40) / 2
    end

    it "calculates average with joins" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 20)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 40)
      user3 = User.create(name: "Charlie", email: "charlie@example.com", age: 60)

      Post.create(title: "Alice's Post", content: "Content", user_id: user1.id_value)
      Post.create(title: "Bob's Post", content: "Content", user_id: user2.id_value)

      result = User.joins(:posts).average("age")
      result.should eq(30.0) # (20 + 40) / 2 (only users with posts)
    end

    it "returns nil for empty result set" do
      User.average("age").should be_nil
      User.where("age > 100").average("age").should be_nil
    end
  end

  describe ".minimum" do
    it "finds minimum value in a column" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.minimum("age").should eq(25)
    end

    it "works with symbols" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)

      User.minimum(:age).should eq(25)
    end

    it "finds minimum with conditions" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.where("age > 28").minimum("age").should eq(30)
    end

    it "finds minimum with joins" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)
      user3 = User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      Post.create(title: "Alice's Post", content: "Content", user_id: user1.id_value)
      Post.create(title: "Bob's Post", content: "Content", user_id: user2.id_value)

      User.joins(:posts).minimum("age").should eq(25) # Alice has the minimum age among users with posts
    end

    it "works with string columns" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.minimum("name").should eq("Alice") # Alphabetically first
    end

    it "returns nil for empty result set" do
      User.minimum("age").should be_nil
      User.where("age > 100").minimum("age").should be_nil
    end
  end

  describe ".maximum" do
    it "finds maximum value in a column" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.maximum("age").should eq(35)
    end

    it "works with symbols" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)

      User.maximum(:age).should eq(30)
    end

    it "finds maximum with conditions" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.where("age < 33").maximum("age").should eq(30)
    end

    it "finds maximum with joins" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)
      user3 = User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      Post.create(title: "Alice's Post", content: "Content", user_id: user1.id_value)
      Post.create(title: "Bob's Post", content: "Content", user_id: user2.id_value)

      User.joins(:posts).maximum("age").should eq(30) # Bob has the maximum age among users with posts
    end

    it "works with string columns" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.maximum("name").should eq("Charlie") # Alphabetically last
    end

    it "returns nil for empty result set" do
      User.maximum("age").should be_nil
      User.where("age > 100").maximum("age").should be_nil
    end
  end

  describe "calculation methods with complex queries" do
    it "works with distinct" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 25) # Same age
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      # Note: DISTINCT with aggregations might behave differently depending on the database
      User.select("age").distinct.count.should eq(2) # Two distinct ages: 25, 35
    end

    it "works with order clauses" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      # Order shouldn't affect aggregations
      User.order("age").count.should eq(3)
      User.order("age DESC").sum("age").should eq(90)
    end

    it "works with offset and limit" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      # With limit, count should respect the limit
      User.limit(2).count.should eq(2)
      User.offset(1).limit(1).count.should eq(1)
    end

    it "works with complex where conditions" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)
      User.create(name: "David", email: "david@example.com", age: 40)

      User.where("age BETWEEN ? AND ?", 28, 37).count.should eq(2)             # Bob and Charlie
      User.where("age BETWEEN ? AND ?", 28, 37).sum("age").should eq(65)       # 30 + 35
      User.where("age BETWEEN ? AND ?", 28, 37).average("age").should eq(32.5) # (30 + 35) / 2
    end
  end

  describe "edge cases" do
    it "handles none relations" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)

      User.none.count.should eq(0)
      User.none.sum("age").should eq(0)
      User.none.average("age").should be_nil
      User.none.minimum("age").should be_nil
      User.none.maximum("age").should be_nil
    end

    it "handles empty tables" do
      User.count.should eq(0)
      User.sum("age").should eq(0)
      User.average("age").should be_nil
      User.minimum("age").should be_nil
      User.maximum("age").should be_nil
    end

    it "handles single record" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)

      User.count.should eq(1)
      User.sum("age").should eq(25)
      User.average("age").should eq(25.0)
      User.minimum("age").should eq(25)
      User.maximum("age").should eq(25)
    end
  end

  describe "performance and SQL generation" do
    it "generates efficient SQL for calculations" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)

      # These should generate optimized SQL queries
      # COUNT(*), SUM(age), AVG(age), MIN(age), MAX(age)
      User.count.should eq(2)
      User.sum("age").should eq(55)
      User.average("age").should eq(27.5)
      User.minimum("age").should eq(25)
      User.maximum("age").should eq(30)
    end

    it "works with method chaining" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      # Method chaining should work properly
      User.where("age > 25").order("age").limit(2).count.should eq(2)
      User.where("age > 25").sum("age").should eq(65) # 30 + 35
    end
  end
end
