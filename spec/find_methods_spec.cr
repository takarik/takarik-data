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

# Create table that matches Rails example: [:store_id, :id] as primary key
Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS test_customers (
    store_id INTEGER,
    id INTEGER,
    first_name VARCHAR(255),
    last_name VARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME,
    PRIMARY KEY (store_id, id)
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

# Model that exactly matches Rails example: [:store_id, :id] as primary key
class TestCustomer < Takarik::Data::BaseModel
  table_name "test_customers"
  primary_key [:store_id, :id]

  column first_name, String
  column last_name, String
  timestamps
end

describe "Find Methods" do
  before_each do
    # Clean up test data
    Takarik::Data.connection.exec("DELETE FROM users")
    Takarik::Data.connection.exec("DELETE FROM test_orders")
    Takarik::Data.connection.exec("DELETE FROM test_customers")
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

  describe "find_by methods" do
    it "finds a record by single condition" do
      user = User.new
      user.name = "Lifo"
      user.email = "lifo@example.com"
      user.age = 30
      user.save

      found_user = User.find_by(name: "Lifo")
      found_user.should_not be_nil
      found_user.not_nil!.id.should eq(user.id)
      found_user.not_nil!.name.should eq("Lifo")
    end

    it "finds a record by multiple conditions" do
      user1 = User.new
      user1.name = "John"
      user1.email = "john@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "John"
      user2.email = "john.doe@example.com"
      user2.age = 30
      user2.save

      # Should find user1 based on both name and email
      found_user = User.find_by(name: "John", email: "john@example.com")
      found_user.should_not be_nil
      found_user.not_nil!.id.should eq(user1.id)
      found_user.not_nil!.email.should eq("john@example.com")
    end

    it "returns nil when no record matches conditions" do
      user = User.find_by(name: "NonExistent")
      user.should be_nil
    end

    it "returns first matching record without implicit ordering" do
      user1 = User.new
      user1.name = "Same Name"
      user1.email = "first@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "Same Name"
      user2.email = "second@example.com"
      user2.age = 30
      user2.save

      # Should return one of the users (implementation dependent without ORDER BY)
      found_user = User.find_by(name: "Same Name")
      found_user.should_not be_nil
      found_user.not_nil!.name.should eq("Same Name")
      # Could be either user1 or user2, so just check it's one of them
      [user1.id, user2.id].should contain(found_user.not_nil!.id)
    end

    it "works with Hash(String, DB::Any) syntax" do
      user = User.new
      user.name = "Hash User"
      user.email = "hash@example.com"
      user.age = 35
      user.save

      conditions = Hash(String, DB::Any).new
      conditions["name"] = "Hash User".as(DB::Any)
      conditions["age"] = 35.as(DB::Any)

      found_user = User.find_by(conditions)
      found_user.should_not be_nil
      found_user.not_nil!.id.should eq(user.id)
    end

    it "works with named arguments syntax" do
      user = User.new
      user.name = "Named Args User"
      user.email = "named@example.com"
      user.age = 40
      user.save

      found_user = User.find_by(name: "Named Args User", age: 40)
      found_user.should_not be_nil
      found_user.not_nil!.id.should eq(user.id)
    end
  end

  describe "find_by! methods (with exceptions)" do
    it "finds a record and returns it successfully" do
      user = User.new
      user.name = "Exception Test"
      user.email = "exception@example.com"
      user.age = 45
      user.save

      found_user = User.find_by!(name: "Exception Test")
      found_user.should_not be_nil
      found_user.id.should eq(user.id)
    end

    it "raises RecordNotFound when no record matches" do
      expect_raises(Takarik::Data::RecordNotFound) do
        User.find_by!(name: "Does Not Exist")
      end
    end

    it "raises RecordNotFound with meaningful message" do
      begin
        User.find_by!(name: "Missing User")
        fail "Expected RecordNotFound to be raised"
      rescue ex : Takarik::Data::RecordNotFound
        ex.message.not_nil!.should contain("User")
      end
    end

    it "works with Hash(String, DB::Any) syntax and raises on miss" do
      conditions = Hash(String, DB::Any).new
      conditions["name"] = "Missing Hash User".as(DB::Any)

      expect_raises(Takarik::Data::RecordNotFound) do
        User.find_by!(conditions)
      end
    end

    it "works with named arguments syntax and raises on miss" do
      expect_raises(Takarik::Data::RecordNotFound) do
        User.find_by!(name: "Missing Named User", age: 999)
      end
    end
  end

  describe "QueryBuilder exception consistency" do
    it "throws RecordNotFound for first! on empty QueryBuilder result" do
      expect_raises(Takarik::Data::RecordNotFound) do
        User.where(name: "NonExistent").first!
      end
    end

    it "throws RecordNotFound for last! on empty QueryBuilder result" do
      expect_raises(Takarik::Data::RecordNotFound) do
        User.where(name: "NonExistent").last!
      end
    end

    it "throws RecordNotFound for take! on empty QueryBuilder result" do
      expect_raises(Takarik::Data::RecordNotFound) do
        User.where(name: "NonExistent").take!
      end
    end

    it "QueryBuilder exceptions have proper model names" do
      begin
        User.where(name: "Missing").first!
        fail "Expected RecordNotFound to be raised"
      rescue ex : Takarik::Data::RecordNotFound
        ex.message.not_nil!.should contain("User")
      end
    end

    it "QueryBuilder last! works correctly" do
      user = User.new
      user.name = "Last Test"
      user.email = "last@example.com"
      user.age = 25
      user.save

      found_user = User.where(name: "Last Test").last!
      found_user.should_not be_nil
      found_user.id.should eq(user.id)
    end

    it "QueryBuilder first! works correctly" do
      user = User.new
      user.name = "First Test"
      user.email = "first@example.com"
      user.age = 30
      user.save

      found_user = User.where(name: "First Test").first!
      found_user.should_not be_nil
      found_user.id.should eq(user.id)
    end
  end

  describe "QueryBuilder multiple record methods" do
    it "first(n) returns multiple records with query ordering" do
      user1 = User.new
      user1.name = "Alpha"
      user1.email = "alpha@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "Beta"
      user2.email = "beta@example.com"
      user2.age = 30
      user2.save

      user3 = User.new
      user3.name = "Gamma"
      user3.email = "gamma@example.com"
      user3.age = 35
      user3.save

      # Get first 2 users ordered by name
      users = User.order(:name).first(2)
      users.size.should eq(2)
      users[0].name.should eq("Alpha")
      users[1].name.should eq("Beta")
    end

    it "first!(n) returns multiple records or raises RecordNotFound" do
      user = User.new
      user.name = "Only User"
      user.email = "only@example.com"
      user.age = 25
      user.save

      # Should succeed with one result when requesting 2
      users = User.where(name: "Only User").first!(2)
      users.size.should eq(1)
      users[0].name.should eq("Only User")

      # Should raise when no results
      expect_raises(Takarik::Data::RecordNotFound) do
        User.where(name: "NonExistent").first!(2)
      end
    end

    it "take(n) returns multiple records without ordering" do
      user1 = User.new
      user1.name = "Take User 1"
      user1.email = "take1@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "Take User 2"
      user2.email = "take2@example.com"
      user2.age = 30
      user2.save

      # Should return up to 2 records without implicit ordering
      users = User.where("name LIKE", "Take User%").take(2)
      users.size.should eq(2)
      # Order is not guaranteed, so just check we got both users
      names = users.map(&.name)
      names.should contain("Take User 1")
      names.should contain("Take User 2")
    end

    it "take!(n) returns multiple records or raises RecordNotFound" do
      user = User.new
      user.name = "Take Only User"
      user.email = "takeonly@example.com"
      user.age = 25
      user.save

      # Should succeed with one result when requesting 3
      users = User.where(name: "Take Only User").take!(3)
      users.size.should eq(1)
      users[0].name.should eq("Take Only User")

      # Should raise when no results
      expect_raises(Takarik::Data::RecordNotFound) do
        User.where(name: "NonExistent").take!(3)
      end
    end

    it "last(n) returns multiple records in reverse order" do
      user1 = User.new
      user1.name = "Last Alpha"
      user1.email = "lastalpha@example.com"
      user1.age = 25
      user1.save

      user2 = User.new
      user2.name = "Last Beta"
      user2.email = "lastbeta@example.com"
      user2.age = 30
      user2.save

      user3 = User.new
      user3.name = "Last Gamma"
      user3.email = "lastgamma@example.com"
      user3.age = 35
      user3.save

      # Get last 2 users ordered by name (should be reverse alphabetical)
      users = User.where("name LIKE", "Last%").order(:name).last(2)
      users.size.should eq(2)
      # Should return in reverse order: Gamma, Beta (alphabetically last first)
      users[0].name.should eq("Last Gamma")
      users[1].name.should eq("Last Beta")
    end

    it "last!(n) returns multiple records or raises RecordNotFound" do
      user = User.new
      user.name = "Last Only User"
      user.email = "lastonly@example.com"
      user.age = 25
      user.save

      # Should succeed with one result when requesting 2
      users = User.where(name: "Last Only User").last!(2)
      users.size.should eq(1)
      users[0].name.should eq("Last Only User")

      # Should raise when no results
      expect_raises(Takarik::Data::RecordNotFound) do
        User.where(name: "NonExistent").last!(2)
      end
    end

    it "returns empty arrays for multiple record methods when appropriate" do
      # These should return empty arrays, not raise exceptions
      User.where(name: "NonExistent").first(3).should be_empty
      User.where(name: "NonExistent").take(3).should be_empty
      User.where(name: "NonExistent").last(3).should be_empty
    end
  end

  describe "ID column vs Primary Key distinction" do
    it "find() uses primary key logic for composite keys" do
      # Create records with different composite keys
      order1 = TestOrder.new
      order1.shop_id = 5
      order1.order_id = 10
      order1.order_number = "ORD-001"
      order1.total = 99.99
      order1.save

      order2 = TestOrder.new
      order2.shop_id = 3
      order2.order_id = 5
      order2.order_number = "ORD-002"
      order2.total = 149.99
      order2.save

      # find() uses primary key logic - searches by composite key [shop_id, order_id]
      found_order = TestOrder.find([5, 10])
      found_order.should_not be_nil
      found_order.not_nil!.shop_id.should eq(5)
      found_order.not_nil!.order_id.should eq(10)
      found_order.not_nil!.order_number.should eq("ORD-001")
    end

    it "find_by(id:) treats :id as literal column name" do
      # Create records with different composite keys
      order1 = TestOrder.new
      order1.shop_id = 5
      order1.order_id = 10
      order1.order_number = "ORD-001"
      order1.total = 99.99
      order1.save

      order2 = TestOrder.new
      order2.shop_id = 3
      order2.order_id = 5
      order2.order_number = "ORD-002"
      order2.total = 149.99
      order2.save

      # find_by(id:) treats :id as literal column name - searches only by order_id column
      found_order = TestOrder.find_by(order_id: 10)
      found_order.should_not be_nil
      found_order.not_nil!.shop_id.should eq(5)
      found_order.not_nil!.order_id.should eq(10)
      found_order.not_nil!.order_number.should eq("ORD-001")

      # This would find the order with order_id = 5, regardless of shop_id
      found_order2 = TestOrder.find_by(order_id: 5)
      found_order2.should_not be_nil
      found_order2.not_nil!.shop_id.should eq(3)
      found_order2.not_nil!.order_id.should eq(5)
      found_order2.not_nil!.order_number.should eq("ORD-002")
    end

    it "id_value method returns the :id column value specifically" do
      # Create a record with composite key [:store_id, :id] - exact Rails example
      customer = TestCustomer.new
      customer.store_id = 5
      customer.id = 10
      customer.first_name = "Joe"
      customer.last_name = "Doe"
      customer.save

      # id_value should return the id value (the :id column)
      customer.id_value.should eq(10)

      # This demonstrates the difference from primary key access
      # For composite keys, primary key would be [5, 10]
      # But id_value specifically returns just the :id column value: 10
    end

    it "demonstrates the Rails example scenario exactly" do
      # Rails example: customers with [:store_id, :id] as primary key
      # Create customer Joe with id=10, store_id=5
      customer_joe = TestCustomer.new
      customer_joe.store_id = 5
      customer_joe.id = 10
      customer_joe.first_name = "Joe"
      customer_joe.last_name = "Doe"
      customer_joe.save

      # Create customer Bob with id=5, store_id=3
      customer_bob = TestCustomer.new
      customer_bob.store_id = 3
      customer_bob.id = 5
      customer_bob.first_name = "Bob"
      customer_bob.last_name = "Smith"
      customer_bob.save

      # Get the last customer (should be Joe)
      last_customer = TestCustomer.last
      last_customer.not_nil!.first_name.should eq("Joe")

      # ❌ PROBLEMATIC: If someone naively uses find_by(id:) with wrong value
      # They might intend to find the customer with composite key [5, 10]
      # But find_by(id: 5) only searches the :id column, finding Bob instead!
      wrong_result = TestCustomer.find_by(id: 5)
      wrong_result.not_nil!.first_name.should eq("Bob") # Wrong customer!
      wrong_result.not_nil!.store_id.should eq(3)

      # ✅ CORRECT APPROACH 1: Use find() with composite primary key
      correct_with_find = TestCustomer.find([5, 10]) # [store_id, id]
      correct_with_find.not_nil!.first_name.should eq("Joe")

      # ✅ CORRECT APPROACH 2: Use id_value to get the right :id column value
      # id_value gives us the :id column value (10) to use with find_by
      correct_with_id_value = TestCustomer.find_by(id: last_customer.not_nil!.id_value)
      correct_with_id_value.not_nil!.first_name.should eq("Joe")
      correct_with_id_value.not_nil!.id.should eq(10)

      # Demonstrate the value of id_value: it gives us the :id column specifically
      last_customer.not_nil!.id_value.should eq(10) # The :id column value

      # This shows why id_value is needed: to safely use the :id column with find_by
      # Without id_value, developers might accidentally pass wrong values to find_by(id:)
    end

    it "demonstrates the exact Rails documentation warning" do
      # Recreate the exact Rails example from the documentation:
      # "Take caution when using find_by(id:) on models where :id is not the primary key"

      # Customer with id: 10, store_id: 5, first_name: "Joe"
      customer_joe = TestCustomer.new
      customer_joe.store_id = 5
      customer_joe.id = 10
      customer_joe.first_name = "Joe"
      customer_joe.save

      # Customer with id: 5, store_id: 3, first_name: "Bob"
      customer_bob = TestCustomer.new
      customer_bob.store_id = 3
      customer_bob.id = 5
      customer_bob.first_name = "Bob"
      customer_bob.save

      # Rails comment: "Here, we might intend to search for a single record
      # with the composite primary key [5, 10], but Active Record will search
      # for a record with an :id column of either 5 or 10, and may return the wrong record."

      customer = TestCustomer.last # Gets Joe (id: 10, store_id: 5)
      customer.not_nil!.first_name.should eq("Joe")

      # ❌ PROBLEMATIC: Customer.find_by(id: customer.id)
      # This finds a record with :id column = 10, which is correct in this case
      # But it's searching ONLY by :id column, ignoring store_id
      result1 = TestCustomer.find_by(id: customer.not_nil!.id)
      result1.not_nil!.first_name.should eq("Joe") # Happens to be correct

      # But if we had meant to search for composite key [5, 10] and mistakenly
      # passed just the id part (10), we'd get the wrong expectation
      # The real problem comes when developers think find_by(id:) works like find()

      # ✅ SOLUTION: Use id_value method for clarity
      # "The id_value method can be used to fetch the value of the :id column
      # for a record, for use in finder methods such as find_by and where"
      result2 = TestCustomer.find_by(id: customer.not_nil!.id_value)
      result2.not_nil!.first_name.should eq("Joe")
      result2.not_nil!.id.should eq(10)

      # The key insight: id_value makes it explicit that you're getting the :id column
      customer.not_nil!.id_value.should eq(10)
    end

    it "where() also treats :id as literal column name like find_by" do
      order1 = TestOrder.new
      order1.shop_id = 5
      order1.order_id = 10
      order1.order_number = "ORD-001"
      order1.total = 99.99
      order1.save

      order2 = TestOrder.new
      order2.shop_id = 3
      order2.order_id = 10
      order2.order_number = "ORD-002"
      order2.total = 149.99
      order2.save

      # where() searches by literal column name, not primary key logic
      orders = TestOrder.where(order_id: 10).to_a
      orders.size.should eq(2) # Both orders have order_id = 10

      # This is different from find([5, 10]) which would find only one specific record
      specific_order = TestOrder.find([5, 10])
      specific_order.not_nil!.order_number.should eq("ORD-001")
    end

    it "id_value works correctly with regular single primary key models" do
      user = User.new
      user.name = "John Doe"
      user.email = "john@example.com"
      user.age = 30
      user.save

      # For single primary key models, id_value and primary key are the same
      user.id_value.should eq(user.id)
      user.id_value.should eq(user.id_value)
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
