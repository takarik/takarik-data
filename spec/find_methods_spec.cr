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

  describe "take methods" do
    it "takes a single record without ordering" do
      user1 = User.new
      user1.name = "First User"
      user1.email = "first@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "Second User"
      user2.email = "second@example.com"
      user2.age = 30
      user2.save

      taken_user = User.take
      taken_user.should_not be_nil
      # Should be one of the created users (order not guaranteed)
      [user1.id, user2.id].includes?(taken_user.not_nil!.id).should be_true
    end

    it "returns nil when no records exist for take" do
      user = User.take
      user.should be_nil
    end

    it "takes multiple records without ordering" do
      user1 = User.new
      user1.name = "User One"
      user1.email = "one@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "User Two"
      user2.email = "two@example.com"
      user2.age = 30
      user2.save

      user3 = User.new
      user3.name = "User Three"
      user3.email = "three@example.com"
      user3.age = 35
      user3.save

      taken_users = User.take(2)
      taken_users.size.should eq(2)
      # Should return 2 of the 3 users (order not guaranteed)
      taken_users.each do |user|
        [user1.id, user2.id, user3.id].includes?(user.id).should be_true
      end
    end

    it "returns empty array when taking from empty table" do
      users = User.take(3)
      users.should be_empty
    end

    it "takes fewer records than requested when table has fewer records" do
      user = User.new
      user.name = "Only User"
      user.email = "only@example.com"
      user.age = 25
      user.save

      users = User.take(3) # Request 3, but only 1 exists
      users.size.should eq(1)
      users.first.id.should eq(user.id)
    end
  end

  describe "take! methods (with exceptions)" do
    it "raises RecordNotFound when no records exist for take!" do
      expect_raises(Takarik::Data::RecordNotFound) do
        User.take!
      end
    end

    it "raises RecordNotFound when no records exist for take!(n)" do
      expect_raises(Takarik::Data::RecordNotFound) do
        User.take!(2)
      end
    end

    it "returns successful result when records exist for take!" do
      user = User.new
      user.name = "Test User"
      user.email = "test@example.com"
      user.age = 25
      user.save

      taken_user = User.take!
      taken_user.should_not be_nil
      taken_user.id.should eq(user.id)
    end

    it "returns successful results when records exist for take!(n)" do
      user1 = User.new
      user1.name = "User One"
      user1.email = "one@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "User Two"
      user2.email = "two@example.com"
      user2.age = 30
      user2.save

      taken_users = User.take!(2)
      taken_users.size.should eq(2)
    end

    it "succeeds when taking fewer records than requested with take!(n)" do
      user = User.new
      user.name = "Only User"
      user.email = "only@example.com"
      user.age = 25
      user.save

      # Should succeed even though we request 3 but only 1 exists
      users = User.take!(3)
      users.size.should eq(1)
      users.first.id.should eq(user.id)
    end
  end

  describe "first methods" do
    it "finds the first record ordered by primary key" do
      user1 = User.new
      user1.name = "Second User"
      user1.email = "second@example.com"
      user1.age = 30
      user1.save

      user2 = User.new
      user2.name = "First User"
      user2.email = "first@example.com"
      user2.age = 25
      user2.save

      # Should return the user with lowest ID (first inserted)
      first_user = User.first
      first_user.should_not be_nil
      first_user.not_nil!.id.should eq(user1.id)
      first_user.not_nil!.name.should eq("Second User")
    end

    it "returns nil when no records exist for first" do
      user = User.first
      user.should be_nil
    end

    it "finds multiple records ordered by primary key" do
      user1 = User.new
      user1.name = "User One"
      user1.email = "one@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "User Two"
      user2.email = "two@example.com"
      user2.age = 30
      user2.save

      user3 = User.new
      user3.name = "User Three"
      user3.email = "three@example.com"
      user3.age = 35
      user3.save

      first_users = User.first(2)
      first_users.size.should eq(2)
      # Should return the first 2 users in ID order
      first_users[0].id.should eq(user1.id)
      first_users[1].id.should eq(user2.id)
    end

    it "returns empty array when getting first(n) from empty table" do
      users = User.first(3)
      users.should be_empty
    end

    it "gets fewer records than requested when table has fewer records" do
      user = User.new
      user.name = "Only User"
      user.email = "only@example.com"
      user.age = 25
      user.save

      users = User.first(3) # Request 3, but only 1 exists
      users.size.should eq(1)
      users.first.id.should eq(user.id)
    end

    it "respects existing order from query chain" do
      user1 = User.new
      user1.name = "Alpha"
      user1.email = "alpha@example.com"
      user1.age = 30
      user1.save

      user2 = User.new
      user2.name = "Beta"
      user2.email = "beta@example.com"
      user2.age = 25
      user2.save

      # When ordering by name, Beta should come first
      first_user = User.order(:name).first
      first_user.should_not be_nil
      first_user.not_nil!.name.should eq("Alpha") # Alphabetically first
    end

    it "works with composite primary keys" do
      order1 = TestOrder.new
      order1.shop_id = 2
      order1.order_id = 100
      order1.order_number = "ORD-001"
      order1.total = 99.99
      order1.save

      order2 = TestOrder.new
      order2.shop_id = 1
      order2.order_id = 200
      order2.order_number = "ORD-002"
      order2.total = 149.99
      order2.save

      # Should order by shop_id first, then order_id
      first_order = TestOrder.first
      first_order.should_not be_nil
      first_order.not_nil!.shop_id.should eq(1) # Lower shop_id comes first
      first_order.not_nil!.order_id.should eq(200)
    end
  end

  describe "first! methods (with exceptions)" do
    it "raises RecordNotFound when no records exist for first!" do
      expect_raises(Takarik::Data::RecordNotFound) do
        User.first!
      end
    end

    it "raises RecordNotFound when no records exist for first!(n)" do
      expect_raises(Takarik::Data::RecordNotFound) do
        User.first!(2)
      end
    end

    it "returns successful result when records exist for first!" do
      user = User.new
      user.name = "Test User"
      user.email = "test@example.com"
      user.age = 25
      user.save

      first_user = User.first!
      first_user.should_not be_nil
      first_user.id.should eq(user.id)
    end

    it "returns successful results when records exist for first!(n)" do
      user1 = User.new
      user1.name = "User One"
      user1.email = "one@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "User Two"
      user2.email = "two@example.com"
      user2.age = 30
      user2.save

      first_users = User.first!(2)
      first_users.size.should eq(2)
    end

    it "succeeds when getting fewer records than requested with first!(n)" do
      user = User.new
      user.name = "Only User"
      user.email = "only@example.com"
      user.age = 25
      user.save

      # Should succeed even though we request 3 but only 1 exists
      users = User.first!(3)
      users.size.should eq(1)
      users.first.id.should eq(user.id)
    end
  end

    describe "last methods" do
    it "finds the last record ordered by primary key" do
      user1 = User.new
      user1.name = "First User"
      user1.email = "first@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "Second User"
      user2.email = "second@example.com"
      user2.age = 30
      user2.save

      # Should return the user with highest ID (last inserted)
      last_user = User.last
      last_user.should_not be_nil
      last_user.not_nil!.id.should eq(user2.id)
      last_user.not_nil!.name.should eq("Second User")
    end

    it "returns nil when no records exist for last" do
      user = User.last
      user.should be_nil
    end

    it "finds multiple records ordered by primary key (highest first)" do
      user1 = User.new
      user1.name = "User One"
      user1.email = "one@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "User Two"
      user2.email = "two@example.com"
      user2.age = 30
      user2.save

      user3 = User.new
      user3.name = "User Three"
      user3.email = "three@example.com"
      user3.age = 35
      user3.save

      last_users = User.last(2)
      last_users.size.should eq(2)
      # Should return the last 2 users in reverse ID order (highest first)
      last_users[0].id.should eq(user3.id)
      last_users[1].id.should eq(user2.id)
    end

    it "returns empty array when getting last(n) from empty table" do
      users = User.last(3)
      users.should be_empty
    end

    it "gets fewer records than requested when table has fewer records" do
      user = User.new
      user.name = "Only User"
      user.email = "only@example.com"
      user.age = 25
      user.save

      users = User.last(3) # Request 3, but only 1 exists
      users.size.should eq(1)
      users.first.id.should eq(user.id)
    end

    it "respects existing order from query chain and reverses it" do
      user1 = User.new
      user1.name = "Alpha"
      user1.email = "alpha@example.com"
      user1.age = 30
      user1.save

      user2 = User.new
      user2.name = "Zeta"
      user2.email = "zeta@example.com"
      user2.age = 25
      user2.save

      # When ordering by name ASC, last should reverse to DESC and get Zeta
      last_user = User.order(:name).last
      last_user.should_not be_nil
      last_user.not_nil!.name.should eq("Zeta") # Alphabetically last
    end

    it "works with composite primary keys" do
      order1 = TestOrder.new
      order1.shop_id = 1
      order1.order_id = 100
      order1.order_number = "ORD-001"
      order1.total = 99.99
      order1.save

      order2 = TestOrder.new
      order2.shop_id = 2
      order2.order_id = 200
      order2.order_number = "ORD-002"
      order2.total = 149.99
      order2.save

      # Should order by shop_id DESC first, then order_id DESC
      last_order = TestOrder.last
      last_order.should_not be_nil
      last_order.not_nil!.shop_id.should eq(2) # Higher shop_id comes first
      last_order.not_nil!.order_id.should eq(200)
    end
  end

  describe "last! methods (with exceptions)" do
    it "raises RecordNotFound when no records exist for last!" do
      expect_raises(Takarik::Data::RecordNotFound) do
        User.last!
      end
    end

    it "raises RecordNotFound when no records exist for last!(n)" do
      expect_raises(Takarik::Data::RecordNotFound) do
        User.last!(2)
      end
    end

    it "returns successful result when records exist for last!" do
      user = User.new
      user.name = "Test User"
      user.email = "test@example.com"
      user.age = 25
      user.save

      last_user = User.last!
      last_user.should_not be_nil
      last_user.id.should eq(user.id)
    end

    it "returns successful results when records exist for last!(n)" do
      user1 = User.new
      user1.name = "User One"
      user1.email = "one@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "User Two"
      user2.email = "two@example.com"
      user2.age = 30
      user2.save

      last_users = User.last!(2)
      last_users.size.should eq(2)
    end

    it "succeeds when getting fewer records than requested with last!(n)" do
      user = User.new
      user.name = "Only User"
      user.email = "only@example.com"
      user.age = 25
      user.save

      # Should succeed even though we request 3 but only 1 exists
      users = User.last!(3)
      users.size.should eq(1)
      users.first.id.should eq(user.id)
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
