require "./spec_helper"

describe "Enhanced find_each Method (Latest Rails)" do
  before_each do
    # Clean up test data
    Takarik::Data.connection.exec("DELETE FROM users")
    # Skip test_customers cleanup since we're not using TestCustomer in this spec
  end

  describe "Parameter order and defaults" do
    it "uses correct parameter order: start, finish, batch_size" do
      # Create test data
      5.times do |i|
        User.create(name: "User #{i + 1}", email: "user#{i + 1}@example.com", age: 25)
      end

      count = 0
      # Test new parameter order
      User.find_each(start: nil, finish: nil, batch_size: 2) do |user|
        count += 1
      end

      count.should eq(5)
    end

    it "supports all new parameters with defaults" do
      User.create(name: "Test User", email: "test@example.com", age: 25)

      count = 0
      User.find_each(
        start: nil,
        finish: nil,
        batch_size: 1000,
        error_on_ignore: nil,
        cursor: nil,
        order: :asc
      ) do |user|
        count += 1
      end

      count.should eq(1)
    end
  end

  describe "Cursor parameter" do
    it "allows custom cursor column" do
      # Create users with specific ages
      user1 = User.create(name: "User 1", email: "user1@example.com", age: 20)
      user2 = User.create(name: "User 2", email: "user2@example.com", age: 25)
      user3 = User.create(name: "User 3", email: "user3@example.com", age: 30)

      ages = [] of Int32
      User.find_each(cursor: "age", batch_size: 2) do |user|
        if age = user.age
          ages << age
        end
      end

      ages.sort.should eq([20, 25, 30])
    end

    it "defaults to primary key when cursor is nil" do
      User.create(name: "User 1", email: "user1@example.com", age: 25)
      User.create(name: "User 2", email: "user2@example.com", age: 25)

      count = 0
      User.find_each(cursor: nil) do |user|
        count += 1
      end

      count.should eq(2)
    end
  end

  describe "Enhanced order parameter" do
    it "accepts single Symbol order" do
      User.create(name: "User 1", email: "user1@example.com", age: 25)
      User.create(name: "User 2", email: "user2@example.com", age: 25)

      count = 0
      User.find_each(order: :desc) do |user|
        count += 1
      end

      count.should eq(2)
    end

    it "accepts Array(Symbol) order for multiple columns" do
      # Test with single column (array with one element)
      User.create(name: "User 1", email: "user1@example.com", age: 25)

      count = 0
      User.find_each(cursor: "id", order: [:asc]) do |user|
        count += 1
      end

      count.should eq(1)
    end

    it "validates order array size matches cursor columns size" do
      User.create(name: "User 1", email: "user1@example.com", age: 25)

      expect_raises(ArgumentError, "Order array size (2) must match cursor columns size (1)") do
        User.find_each(cursor: "id", order: [:asc, :desc]) { |user| }
      end
    end

    it "validates order directions" do
      User.create(name: "User 1", email: "user1@example.com", age: 25)

      expect_raises(ArgumentError, "Order must be :asc or :desc") do
        User.find_each(order: :invalid) { |user| }
      end
    end
  end

  describe "Enumerator support" do
    it "returns Enumerator when no block given" do
      User.create(name: "User 1", email: "user1@example.com", age: 25)
      User.create(name: "User 2", email: "user2@example.com", age: 30)

      # Should return an Enumerator
      enumerator = User.find_each(batch_size: 1)
      enumerator.should be_a(Iterator(User))

      # Should be able to iterate
      users = enumerator.to_a
      users.size.should eq(2)
    end

    it "supports chaining with other Enumerator methods" do
      User.create(name: "Alice", email: "alice@example.com", age: 25)
      User.create(name: "Bob", email: "bob@example.com", age: 30)

      # Chain with with_index (simulated)
      names_with_index = [] of {String, Int32}
      User.find_each.each_with_index do |user, index|
        if name = user.name
          names_with_index << {name, index}
        end
      end

      names_with_index.size.should eq(2)
      names_with_index[0][1].should eq(0) # First index
      names_with_index[1][1].should eq(1) # Second index
    end
  end

  describe "Worker distribution examples" do
    it "supports worker 1 processing until finish ID" do
      # Create 10 users
      users = [] of User
      10.times do |i|
        user = User.create(name: "Worker User #{i + 1}", email: "worker#{i + 1}@example.com", age: 25)
        users << user
      end

      # Worker 1: process until user 5
      worker1_count = 0
      User.find_each(finish: users[4].id) do |user|
        worker1_count += 1
      end

      worker1_count.should eq(5) # Users 1-5
    end

    it "supports worker 2 processing from start ID onwards" do
      # Create 10 users
      users = [] of User
      10.times do |i|
        user = User.create(name: "Worker User #{i + 1}", email: "worker#{i + 1}@example.com", age: 25)
        users << user
      end

      # Worker 2: process from user 6 onwards
      worker2_count = 0
      User.find_each(start: users[5].id) do |user|
        worker2_count += 1
      end

      worker2_count.should eq(5) # Users 6-10
    end
  end

  describe "Advanced features" do
    it "supports custom cursor column with different data types" do
      # Test with age as cursor (integer column)
      User.create(name: "User 1", email: "user1@example.com", age: 20)
      User.create(name: "User 2", email: "user2@example.com", age: 30)

      count = 0
      User.find_each(cursor: "age") do |user|
        count += 1
      end

      count.should eq(2)
    end

    it "handles mixed order directions" do
      # For now, test with single column but mixed scenarios
      User.create(name: "User 1", email: "user1@example.com", age: 25)

      count = 0
      User.find_each(cursor: "id", order: [:desc]) do |user|
        count += 1
      end

      count.should eq(1)
    end
  end

  describe "Error handling" do
    it "validates cursor parameter types" do
      User.create(name: "User 1", email: "user1@example.com", age: 25)

      # Should work with String
      count = 0
      User.find_each(cursor: "id") { |user| count += 1 }
      count.should eq(1)

      # Should work with Array(String)
      count = 0
      User.find_each(cursor: ["id"]) { |user| count += 1 }
      count.should eq(1)
    end

    it "provides helpful error messages" do
      expect_raises(Exception, "Batch size must be positive") do
        User.find_each(batch_size: 0) { |user| }
      end
    end
  end

  describe "Backward compatibility" do
    it "maintains compatibility with simple usage" do
      User.create(name: "User 1", email: "user1@example.com", age: 25)

      count = 0
      User.find_each do |user|
        count += 1
      end

      count.should eq(1)
    end

    it "works with where chains" do
      User.create(name: "Active User", email: "active@example.com", age: 25, active: true)
      User.create(name: "Inactive User", email: "inactive@example.com", age: 30, active: false)

      count = 0
      User.where(active: true).find_each do |user|
        count += 1
      end

      count.should eq(1) # Only active user
    end
  end
end
