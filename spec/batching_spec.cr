require "./spec_helper"

describe "Complete Batching Methods (Rails API)" do
  before_each do
    # Clean up test data
    Takarik::Data.connection.exec("DELETE FROM users")

    # Create test users
    10.times do |i|
      User.create(
        name: "BatchUser #{i + 1}",
        email: "batch#{i + 1}@example.com",
        age: 20 + i,
        active: i.even?
      )
    end
  end

  describe "find_in_batches (Enhanced)" do
    it "supports new parameter signature" do
      batch_sizes = [] of Int32

      User.find_in_batches(start: nil, finish: nil, batch_size: 3) do |batch|
        batch_sizes << batch.size
      end

      batch_sizes.should eq([3, 3, 3, 1])
    end

    it "supports custom cursor and order" do
      ages = [] of Int32

      User.find_in_batches(cursor: "age", order: :desc, batch_size: 3) do |batch|
        batch.each { |u| ages << u.age.not_nil! }
      end

      # Should be in descending age order
      ages.should eq(ages.sort.reverse)
    end

    it "returns Enumerator when no block given" do
      enumerator = User.find_in_batches(batch_size: 2)
      batches = enumerator.to_a

      batches.size.should eq(5) # 10 users / 2 = 5 batches
      batches.each { |batch| batch.should be_a(Array(User)) }
    end

    it "works with query chaining" do
      batch_count = 0

      User.where(active: true).find_in_batches(batch_size: 2) do |batch|
        batch_count += 1
        batch.all?(&.active).should be_true
      end

      batch_count.should eq(3) # 5 active users / 2 = 3 batches
    end
  end

  describe "in_batches (Rails-style)" do
    it "yields QueryBuilder objects with block" do
      batch_count = 0
      total_records = 0

      User.in_batches(of: 3) do |relation|
        batch_count += 1
        relation.should be_a(Takarik::Data::QueryBuilder(User))

        # Each relation should have records
        records = relation.to_a
        total_records += records.size
        records.size.should be <= 3
      end

      batch_count.should eq(4) # 10 users / 3 = 4 batches (3,3,3,1)
      total_records.should eq(10)
    end

    it "returns BatchEnumerator without block" do
      enumerator = User.in_batches(of: 4)
      enumerator.should be_a(Takarik::Data::BatchEnumerator(User))

      # Test that we can iterate over it
      batch_count = 0
      enumerator.each do |relation|
        batch_count += 1
        relation.should be_a(Takarik::Data::QueryBuilder(User))
      end

      batch_count.should eq(3) # 10 users / 4 = 3 batches (4,4,2)
    end

    it "works with query chaining" do
      batch_count = 0

      User.where(active: true).in_batches(of: 2) do |relation|
        batch_count += 1
        # All records in this batch should be active
        records = relation.to_a
        records.all?(&.active).should be_true
      end

      batch_count.should eq(3) # 5 active users / 2 = 3 batches
    end

    it "supports load parameter for preloading records" do
      batch_count = 0

      User.in_batches(of: 3, load: true) do |relation|
        batch_count += 1
        # With load: true, the relation should contain the actual records
        records = relation.to_a
        records.size.should be <= 3
      end

      batch_count.should eq(4)
    end

    it "handles empty result sets" do
      User.where("age > ?", 100).in_batches(of: 5) do |relation|
        # This block should never be called
        fail "Should not yield any batches for empty result set"
      end

      # Should not raise any errors
      true.should be_true
    end

    it "respects batch size parameter" do
      batch_sizes = [] of Int32

      User.in_batches(of: 2) do |relation|
        batch_sizes << relation.to_a.size
      end

      batch_sizes.should eq([2, 2, 2, 2, 2]) # 10 users / 2 = 5 batches of 2
    end
  end

  describe "Integration with existing methods" do
    it "both batch methods work together" do
      find_each_count = 0
      find_in_batches_count = 0

      User.find_each(batch_size: 4) { |user| find_each_count += 1 }
      User.find_in_batches(batch_size: 4) { |batch| find_in_batches_count += batch.size }

      find_each_count.should eq(10)
      find_in_batches_count.should eq(10)
    end

    it "maintains query builder chain compatibility" do
      # Complex query chain should work with all batch methods
      active_users = User.where(active: true).where("age > ?", 22)

      # Should all process the same filtered set
      find_each_results = [] of String
      active_users.find_each { |u| find_each_results << u.name.not_nil! }

      find_in_batches_results = [] of String
      active_users.find_in_batches(batch_size: 2) do |batch|
        batch.each { |u| find_in_batches_results << u.name.not_nil! }
      end

      # All should process the same users
      find_each_results.sort.should eq(find_in_batches_results.sort)
    end
  end
end
