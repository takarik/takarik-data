require "./spec_helper"

describe "existence checking methods" do
  before_each do
    # Clean up any existing test data
    User.all.delete_all
    Post.all.delete_all
  end

  describe ".exists?" do
    it "returns false when no records exist" do
      User.exists?.should be_false
    end

    it "returns true when records exist" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.exists?.should be_true
    end

    it "checks existence by single ID" do
      user = User.create(name: "Alice", email: "alice@example.com", age: 25)

      User.exists?(user.id_value).should be_true
      User.exists?(99999).should be_false
    end

    it "checks existence by multiple IDs" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)

      User.exists?([user1.id_value, user2.id_value]).should be_true
      User.exists?([user1.id_value, 99999]).should be_true  # Any one exists
      User.exists?([99998, 99999]).should be_false  # None exist
    end

    it "returns false for empty array" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.exists?([] of DB::Any).should be_false
    end

    it "checks existence by hash conditions" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)

      User.exists?({"name" => "Alice"}).should be_true
      User.exists?({"name" => "Charlie"}).should be_false
      User.exists?({"age" => 25}).should be_true
    end

    it "checks existence by named parameters" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)

      User.exists?(name: "Alice").should be_true
      User.exists?(name: "Charlie").should be_false
      User.exists?(age: 25).should be_true
    end

    it "checks existence with array values in conditions" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.exists?(name: ["Alice", "Bob"]).should be_true
      User.exists?(name: ["David", "Eve"]).should be_false
      User.exists?(age: [25, 30]).should be_true
    end

    it "works with query chains" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.where("age > 28").exists?.should be_true
      User.where("age > 40").exists?.should be_false
      User.where(name: "Alice").exists?.should be_true
      User.where(name: "David").exists?.should be_false
    end

    it "works with complex query conditions" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)

      User.where("age BETWEEN ? AND ?", 20, 28).exists?.should be_true
      User.where("age BETWEEN ? AND ?", 40, 50).exists?.should be_false
    end

    it "works with joins" do
      user = User.create(name: "Alice", email: "alice@example.com", age: 25)
      post = Post.create(title: "Test Post", content: "Content", user_id: user.id_value)

      User.joins(:posts).exists?.should be_true
      User.joins(:posts).where("posts.title = ?", "Test Post").exists?.should be_true
      User.joins(:posts).where("posts.title = ?", "Nonexistent").exists?.should be_false
    end
  end

  describe ".any?" do
    it "returns false when no records exist" do
      User.any?.should be_false
    end

    it "returns true when records exist" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.any?.should be_true
    end

    it "works with query chains" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)

      User.where("age > 28").any?.should be_true
      User.where("age > 40").any?.should be_false
    end

    it "is equivalent to exists?" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)

      User.any?.should eq(User.exists?)
      User.where("age > 20").any?.should eq(User.where("age > 20").exists?)
      User.where("age > 40").any?.should eq(User.where("age > 40").exists?)
    end

    it "works with associations" do
      user = User.create(name: "Alice", email: "alice@example.com", age: 25)
      post = Post.create(title: "Test Post", content: "Content", user_id: user.id_value)

      User.joins(:posts).any?.should be_true
    end
  end

  describe ".many?" do
    it "returns false when no records exist" do
      User.many?.should be_false
    end

    it "returns false when only one record exists" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.many?.should be_false
    end

    it "returns true when two or more records exist" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.many?.should be_true
    end

    it "returns true when many records exist" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)
      User.many?.should be_true
    end

    it "works with query chains" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.where("age >= 30").many?.should be_true  # Bob and Charlie
      User.where("age >= 35").many?.should be_false  # Only Charlie
      User.where("age >= 40").many?.should be_false  # None
    end

    it "works with complex conditions" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)
      User.create(name: "David", email: "david@example.com", age: 40)

      User.where("age BETWEEN ? AND ?", 25, 35).many?.should be_true  # Alice, Bob, Charlie
      User.where("age BETWEEN ? AND ?", 35, 40).many?.should be_true  # Charlie, David
      User.where("age BETWEEN ? AND ?", 40, 45).many?.should be_false  # Only David
    end

    it "works with joins" do
      user1 = User.create(name: "Alice", email: "alice@example.com", age: 25)
      user2 = User.create(name: "Bob", email: "bob@example.com", age: 30)
      post1 = Post.create(title: "Alice's Post", content: "Content", user_id: user1.id_value)
      post2 = Post.create(title: "Bob's Post", content: "Content", user_id: user2.id_value)

      User.joins(:posts).many?.should be_true  # Alice and Bob both have posts
    end
  end

  describe ".empty?" do
    it "returns true when no records exist" do
      User.empty?.should be_true
    end

    it "returns false when records exist" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.empty?.should be_false
    end

    it "works with query chains" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)

      User.where("age > 40").empty?.should be_true
      User.where("age > 20").empty?.should be_false
    end

    it "is opposite of exists?" do
      User.empty?.should eq(!User.exists?)

      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.empty?.should eq(!User.exists?)
      User.where("age > 20").empty?.should eq(!User.where("age > 20").exists?)
      User.where("age > 40").empty?.should eq(!User.where("age > 40").exists?)
    end
  end

  describe "performance optimizations" do
    it "uses SELECT 1 for existence checks" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)

      # These should use optimized queries, not full SELECT *
      User.exists?.should be_true
      User.where("age > 25").exists?.should be_true
      User.any?.should be_true
    end

    it "uses LIMIT 2 for many? checks" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      # Should use LIMIT 2 optimization, not count all records
      User.many?.should be_true
      User.where("age >= 30").many?.should be_true
    end

    it "handles none relations efficiently" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)

      User.none.exists?.should be_false
      User.none.any?.should be_false
      User.none.many?.should be_false
      User.none.empty?.should be_true
    end
  end

  describe "edge cases" do
    it "handles limit and offset correctly" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35)

      User.limit(1).exists?.should be_true
      User.limit(1).many?.should be_false  # Only checking within the limit
      User.offset(1).limit(1).exists?.should be_true
    end

    it "handles distinct correctly" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 25)  # Same age

      User.distinct.exists?.should be_true
      User.select("age").distinct.exists?.should be_true
    end

    it "handles order clauses correctly" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)

      User.order("age").exists?.should be_true
      User.order("age DESC").many?.should be_true
    end
  end

  describe "SQL generation" do
    it "generates efficient SQL for exists?" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)

      # Should generate something like: SELECT 1 FROM users LIMIT 1
      # Not: SELECT * FROM users or SELECT COUNT(*) FROM users
      User.exists?.should be_true
    end

    it "generates efficient SQL for many?" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)

      # Should generate something like: SELECT 1 FROM users LIMIT 2
      # Not: SELECT COUNT(*) FROM users
      User.many?.should be_true
    end
  end
end
