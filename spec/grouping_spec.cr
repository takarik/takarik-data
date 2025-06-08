require "./spec_helper"

# Define test model for ActiveRecord compatibility examples
class TestOrder < Takarik::Data::BaseModel
  table_name "test_orders"
  column status, String
  column created_at, Time
end

describe "GROUP BY functionality" do
  before_each do
    # Clear existing data
    User.all.delete_all

    # Create test data with different statuses
    User.create(name: "Alice", email: "alice@example.com", age: 25, active: true)
    User.create(name: "Bob", email: "bob@example.com", age: 30, active: true)
    User.create(name: "Charlie", email: "charlie@example.com", age: 25, active: false)
    User.create(name: "Diana", email: "diana@example.com", age: 30, active: true)
    User.create(name: "Eve", email: "eve@example.com", age: 35, active: false)
  end

  describe "basic GROUP BY" do
    it "groups by string column" do
      query = User.group("active")
      query.to_sql.should contain("GROUP BY active")
    end

    it "groups by symbol column" do
      query = User.group(:active)
      query.to_sql.should contain("GROUP BY active")
    end

    it "groups by multiple string columns" do
      query = User.group("active", "age")
      query.to_sql.should contain("GROUP BY active, age")
    end

    it "groups by multiple symbol columns" do
      query = User.group(:active, :age)
      query.to_sql.should contain("GROUP BY active, age")
    end

    it "groups by array of strings" do
      query = User.group(["active", "age"])
      query.to_sql.should contain("GROUP BY active, age")
    end

    it "groups by array of symbols" do
      query = User.group([:active, :age])
      query.to_sql.should contain("GROUP BY active, age")
    end
  end

  describe "grouped count" do
    it "returns hash for single column group" do
      result = User.group(:active).count
      result.should be_a(Hash(String, Int64))

      # Should have counts for true and false
      result.should eq({"1" => 3_i64, "0" => 2_i64})
    end

        it "returns hash for multiple column group" do
      result = User.group(:active, :age).count
      result.should be_a(Hash(String, Int64))

      # Should have combined keys
      if result.is_a?(Hash(String, Int64))
        result.keys.should contain("1, 25")
        result.keys.should contain("1, 30")
        result.keys.should contain("0, 25")
        result.keys.should contain("0, 35")
      end
    end

    it "works with where conditions" do
      result = User.where(active: true).group(:age).count
      result.should be_a(Hash(String, Int64))

      # Only active users
      result.should eq({"25" => 1_i64, "30" => 2_i64})
    end

    it "handles empty results" do
      result = User.where(name: "NonExistent").group(:active).count
      result.should be_a(Hash(String, Int64))
      if result.is_a?(Hash(String, Int64))
        result.should be_empty
      end
    end
  end

  describe "regular count still works" do
    it "returns Int64 for non-grouped count" do
      result = User.count
      result.should be_a(Int64)
      result.should eq(5)
    end

    it "returns Int64 for count with where" do
      result = User.where(active: true).count
      result.should be_a(Int64)
      result.should eq(3)
    end
  end

  describe "exists? and empty? with groups" do
    it "exists? works with grouped results" do
      User.group(:active).exists?.should be_true
      User.where(name: "NonExistent").group(:active).exists?.should be_false
    end

    it "empty? works with grouped results" do
      User.group(:active).empty?.should be_false
      User.where(name: "NonExistent").group(:active).empty?.should be_true
    end

    it "any? works with grouped results" do
      User.group(:active).any?.should be_true
      User.where(name: "NonExistent").group(:active).any?.should be_false
    end
  end

  describe "chaining with other methods" do
    it "works with select and group" do
      query = User.select("age").group("age")
      query.to_sql.should contain("SELECT age")
      query.to_sql.should contain("GROUP BY age")
    end

    it "works with where and group" do
      query = User.where(active: true).group("age")
      query.to_sql.should contain("WHERE active = ?")
      query.to_sql.should contain("GROUP BY age")
    end

    it "works with order and group" do
      query = User.group("age").order("age")
      query.to_sql.should contain("GROUP BY age")
      query.to_sql.should contain("ORDER BY age ASC")
    end

    it "works with having and group" do
      query = User.group("age").having("COUNT(*) > ?", 1)
      query.to_sql.should contain("GROUP BY age")
      query.to_sql.should contain("HAVING COUNT(*) > ?")
    end
  end

    describe "ActiveRecord compatibility examples" do
    it "matches ActiveRecord example: Order.select('created_at').group('created_at')" do
      query = User.select("created_at").group("created_at")
      query.to_sql.should eq("SELECT created_at FROM users GROUP BY created_at")
    end
  end
end
