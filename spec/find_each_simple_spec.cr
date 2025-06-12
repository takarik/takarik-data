require "./spec_helper"

describe "find_each Method - Basic Tests" do
  before_each do
    # Clean up test data
    Takarik::Data.connection.exec("DELETE FROM users")
  end

  it "processes records in batches" do
    # Create test data
    5.times do |i|
      user = User.new
      user.name = "User #{i + 1}"
      user.email = "user#{i + 1}@example.com"
      user.age = 25
      user.save
    end

    # Track yielded users
    yielded_count = 0

    User.find_each(batch_size: 2) do |user|
      yielded_count += 1
    end

    # Should yield all users
    yielded_count.should eq(5)
  end

  it "works with where conditions" do
    # Create users with different ages
    User.create(name: "Young", email: "young@example.com", age: 20)
    User.create(name: "Old", email: "old@example.com", age: 50)

    yielded_count = 0
    User.where("age <", 30).find_each do |user|
      yielded_count += 1
    end

    yielded_count.should eq(1)  # Only the young user
  end

  it "handles empty datasets" do
    yielded_count = 0
    User.find_each do |user|
      yielded_count += 1
    end

    yielded_count.should eq(0)
  end

  it "raises error for invalid batch_size" do
    expect_raises(Exception, "Batch size must be positive") do
      User.find_each(batch_size: 0) { |user| }
    end
  end

  it "respects start parameter" do
    users = [] of User
    3.times do |i|
      user = User.create(name: "Test #{i + 1}", email: "test#{i + 1}@example.com", age: 25)
      users << user
    end

    # Start from the second user
    yielded_count = 0
    User.find_each(start: users[1].id) do |user|
      yielded_count += 1
    end

    yielded_count.should eq(2)  # Users 2 and 3
  end

  it "respects finish parameter" do
    users = [] of User
    3.times do |i|
      user = User.create(name: "Test #{i + 1}", email: "test#{i + 1}@example.com", age: 25)
      users << user
    end

    # End at the second user
    yielded_count = 0
    User.find_each(finish: users[1].id) do |user|
      yielded_count += 1
    end

    yielded_count.should eq(2)  # Users 1 and 2
  end

  it "works with descending order" do
    users = [] of User
    3.times do |i|
      user = User.create(name: "Test #{i + 1}", email: "test#{i + 1}@example.com", age: 25)
      users << user
    end

    yielded_ids = [] of Int32
    User.find_each(order: :desc) do |user|
      if id = user.id
        yielded_ids << id
      end
    end

    # Should be in descending order
    yielded_ids.should eq(yielded_ids.sort.reverse)
  end

  it "ignores existing order by default" do
    User.create(name: "Alpha", email: "alpha@example.com", age: 25)
    User.create(name: "Beta", email: "beta@example.com", age: 25)

    yielded_count = 0
    # Should not raise error
    User.order(:name).find_each do |user|
      yielded_count += 1
    end

    yielded_count.should eq(2)
  end

  it "raises error when order present and error_on_ignore is true" do
          expect_raises(ArgumentError, "Scoped order is ignored, use :cursor with :order to configure custom order.") do
      User.order(:name).find_each(error_on_ignore: true) { |user| }
    end
  end
end
