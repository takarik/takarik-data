require "./spec_helper"

# Create table for composite primary key testing
Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS test_orders (
    shop_id INTEGER,
    order_id INTEGER,
    order_number VARCHAR(255),
    total REAL,
    created_at DATETIME,
    updated_at DATETIME,
    PRIMARY KEY (shop_id, order_id)
  )
SQL

# Model with composite primary key
class TestOrder < Takarik::Data::BaseModel
  table_name "test_orders"
  primary_key [:shop_id, :order_id]

  column order_number, String
  column total, Float64
  timestamps
end

describe "Find Methods" do
  before_each do
    # Clean up test data
    Takarik::Data.connection.exec("DELETE FROM users")
    Takarik::Data.connection.exec("DELETE FROM test_orders")
  end

  describe "Single ID find" do
    it "finds a record by single ID" do
      user = User.new
      user.name = "John Doe"
      user.email = "john@example.com"
      user.age = 30
      user.save

      found_user = User.find(user.id)
      found_user.should_not be_nil
      found_user.not_nil!.name.should eq("John Doe")
    end

    it "returns nil when record not found" do
      user = User.find(999)
      user.should be_nil
    end
  end

  describe "Multiple ID find" do
    it "finds multiple records by array of IDs" do
      user1 = User.new
      user1.name = "User One"
      user1.email = "user1@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "User Two"
      user2.email = "user2@example.com"
      user2.age = 30
      user2.save

      found_users = User.find([user1.id, user2.id])
      found_users.size.should eq(2)
      found_ids = found_users.map(&.id)
      found_ids.includes?(user1.id).should be_true
      found_ids.includes?(user2.id).should be_true
    end

    it "finds multiple records by splat arguments" do
      user1 = User.new
      user1.name = "User Alpha"
      user1.email = "alpha@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "User Beta"
      user2.email = "beta@example.com"
      user2.age = 30
      user2.save

      found_users = User.find(user1.id, user2.id)
      found_users.size.should eq(2)
      found_ids = found_users.map(&.id)
      found_ids.includes?(user1.id).should be_true
      found_ids.includes?(user2.id).should be_true
    end

    it "returns empty array for empty ID array" do
      users = User.find([] of Int32)
      users.should be_empty
    end
  end

  describe "Composite primary key find" do
    it "finds a record by composite primary key" do
      order = TestOrder.new
      order.shop_id = 1
      order.order_id = 100
      order.order_number = "ORD-001"
      order.total = 99.99
      order.save

      found_order = TestOrder.find([1, 100])
      found_order.should_not be_nil
      found_order.not_nil!.shop_id.should eq(1)
      found_order.not_nil!.order_id.should eq(100)
    end

    it "returns nil when composite key not found" do
      order = TestOrder.find([999, 999])
      order.should be_nil
    end
  end

  describe "find! methods (with exceptions)" do
    it "raises RecordNotFound for single missing ID" do
      expect_raises(Takarik::Data::RecordNotFound) do
        User.find!(999)
      end
    end

    it "raises RecordNotFound for missing IDs in array" do
      user = User.new
      user.name = "Test User"
      user.email = "test@example.com"
      user.age = 25
      user.save

      expect_raises(Takarik::Data::RecordNotFound) do
        User.find!([user.id, 999])
      end
    end

    it "returns successful results when all records found" do
      user1 = User.new
      user1.name = "User One"
      user1.email = "user1@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "User Two"
      user2.email = "user2@example.com"
      user2.age = 30
      user2.save

      # Should not raise exception
      found_users = User.find!([user1.id, user2.id])
      found_users.size.should eq(2)
    end
  end

  describe "RecordNotFound exception" do
    it "can be caught and handled" do
      exception_caught = false
      begin
        User.find!(999)
      rescue Takarik::Data::RecordNotFound
        exception_caught = true
      end
      exception_caught.should be_true
    end

    it "has meaningful error messages" do
      begin
        User.find!(999)
      rescue ex : Takarik::Data::RecordNotFound
        ex.message.not_nil!.includes?("Couldn't find User").should be_true
        ex.message.not_nil!.includes?("'id'=999").should be_true
      end
    end
  end

  describe "Edge cases" do
    it "handles empty arrays correctly" do
      users = User.find([] of Int32)
      users.should be_empty

      # find! with empty array should return empty array, not raise
      users = User.find!([] of Int32)
      users.should be_empty
    end
  end
end
