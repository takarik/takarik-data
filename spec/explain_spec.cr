require "./spec_helper"

describe "QueryBuilder EXPLAIN functionality" do
  before_each do
    # Clean up any existing data
    User.all.delete_all
    Post.all.delete_all
    Comment.all.delete_all
  end

  describe "#explain" do
    it "should return explain output for simple queries" do
      user = User.create(name: "John", email: "john@example.com", age: 25, active: true)
      result = User.where(id: user.id).explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should contain("users")
      result.should_not be_empty
    end

    it "should return explain output for queries with joins" do
      result = User.joins(:posts).explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should contain("users")
      result.should contain("posts")
      result.should_not be_empty
    end

    it "should return explain output for queries with where conditions" do
      result = User.where(active: true).explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should contain("users")
      result.should_not be_empty
    end

    it "should return explain output for complex queries" do
      result = User.where(active: true).joins(:posts).where("posts.published = ?", true).explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should contain("users")
      result.should contain("posts")
      result.should_not be_empty
    end

    it "should handle queries with includes by showing multiple explain outputs" do
      user = User.create(name: "John", email: "john@example.com", age: 25, active: true)
      result = User.where(id: user.id).includes(:posts).explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should contain("users")
      result.should contain("posts")
      result.should_not be_empty

      # Should contain multiple EXPLAIN statements for includes
      explain_count = result.scan(/EXPLAIN/).size
      explain_count.should be >= 2
    end

    it "should work with aggregation queries" do
      result = User.where(active: true).group(:active).explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should contain("users")
      result.should_not be_empty
    end

    it "should work with order and limit clauses" do
      result = User.where(active: true).order(:name).limit(5).explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should contain("users")
      result.should_not be_empty
    end

    it "should work with distinct queries" do
      result = User.select(:name).distinct.explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should contain("users")
      result.should_not be_empty
    end

    it "should handle empty result sets" do
      result = User.where(id: -1).explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should contain("users")
      result.should_not be_empty
    end

    it "should work with subqueries and complex conditions" do
      user1 = User.create(name: "John", email: "john@example.com", age: 25, active: true)
      user2 = User.create(name: "Jane", email: "jane@example.com", age: 30, active: true)
      result = User.where("id IN (?)", [user1.id, user2.id]).explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should contain("users")
      result.should_not be_empty
    end
  end

  describe "#explain with database-specific options" do
    it "should accept SQLite-specific options" do
      user = User.create(name: "John", email: "john@example.com", age: 25, active: true)
      # SQLite supports EXPLAIN QUERY PLAN
      result = User.where(id: user.id).explain(:query_plan)

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should_not be_empty
    end

    it "should handle multiple options gracefully" do
      user = User.create(name: "John", email: "john@example.com", age: 25, active: true)
      # Test with options that may not be supported by SQLite
      result = User.where(id: user.id).explain(:analyze, :verbose)

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should_not be_empty
    end

    it "should work without any options" do
      user = User.create(name: "John", email: "john@example.com", age: 25, active: true)
      result = User.where(id: user.id).explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should_not be_empty
    end
  end

  describe "explain with associations" do
    it "should explain belongs_to associations" do
      result = Post.includes(:user).explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should contain("posts")
      result.should contain("users")
      result.should_not be_empty
    end

    it "should explain has_many associations" do
      result = User.includes(:posts).explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should contain("users")
      result.should contain("posts")
      result.should_not be_empty
    end

    it "should explain nested associations" do
      result = User.joins(:posts).explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should contain("users")
      result.should contain("posts")
      result.should_not be_empty
    end
  end

  describe "explain output format" do
    it "should include the SQL query being explained" do
      result = User.where(active: true).explain

      result.should contain("SELECT")
      result.should contain("FROM users")
      result.should contain("WHERE")
    end

    it "should be properly formatted and readable" do
      user = User.create(name: "John", email: "john@example.com", age: 25, active: true)
      result = User.where(id: user.id).explain

      # Should contain line breaks for readability
      result.should contain("\n")
      result.should_not be_empty

      # Should not contain obvious formatting errors
      result.should_not contain("EXPLAINSELECT")
      result.should_not contain("  EXPLAIN")
    end

    it "should handle parameter substitution correctly" do
      result = User.where("name = ?", "John").explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should_not be_empty
    end
  end

  describe "error handling" do
    it "should handle invalid queries gracefully" do
      # This should still work even if the query would fail during execution
      result = User.where("invalid_column = ?", "value").explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
    end

    it "should work with none relations" do
      result = User.none.explain

      result.should be_a(String)
      result.should contain("EXPLAIN")
      result.should_not be_empty
    end
  end
end
