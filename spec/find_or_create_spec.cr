require "./spec_helper"

describe "Find or Create functionality" do
  before_each do
    # Clear existing data
    User.all.each(&.destroy)
  end

  describe "find_or_create_by" do
    it "finds existing record" do
      # Create a user first
      existing_user = User.create(name: "Andy", email: "andy@example.com", age: 25, active: true)

      # Try to find or create the same user
      found_user = User.find_or_create_by(name: "Andy")

      found_user.should eq(existing_user)
      found_user.persisted?.should be_true
      User.count.should eq(1)
    end

    it "creates new record when not found" do
      # Try to find or create a user that doesn't exist
      new_user = User.find_or_create_by(name: "Andy", email: "andy@example.com")

      new_user.persisted?.should be_true
      new_user.name.should eq("Andy")
      new_user.email.should eq("andy@example.com")
      User.count.should eq(1)
    end

    it "works with multiple conditions" do
      # Create a user with different last name
      User.create(name: "Andy", email: "andy1@example.com", age: 25, active: true)

      # Try to find or create with different email
      new_user = User.find_or_create_by(name: "Andy", email: "andy2@example.com")

      new_user.persisted?.should be_true
      new_user.name.should eq("Andy")
      new_user.email.should eq("andy2@example.com")
      User.count.should eq(2)
    end

    it "works with block to set additional attributes" do
      new_user = User.find_or_create_by(name: "Andy", email: "andy@example.com") do |user|
        user.age = 30
        user.active = false
      end

      new_user.persisted?.should be_true
      new_user.name.should eq("Andy")
      new_user.email.should eq("andy@example.com")
      new_user.age.should eq(30)
      new_user.active.should eq(false)
    end

    it "ignores block when record is found" do
      # Create a user first
      existing_user = User.create(name: "Andy", email: "andy@example.com", age: 25, active: true)

      # Try to find with block - block should be ignored
      found_user = User.find_or_create_by(name: "Andy") do |user|
        user.email = "different@example.com"
        user.age = 99
      end

      found_user.should eq(existing_user)
      found_user.email.should eq("andy@example.com") # Original email, not from block
      found_user.age.should eq(25)                   # Original age, not from block
    end
  end

  describe "find_or_create_by!" do
    it "finds existing record" do
      existing_user = User.create(name: "Andy", email: "andy@example.com", age: 25, active: true)

      found_user = User.find_or_create_by!(name: "Andy")

      found_user.should eq(existing_user)
      found_user.persisted?.should be_true
    end

    it "creates new record when not found" do
      new_user = User.find_or_create_by!(name: "Andy", email: "andy@example.com")

      new_user.persisted?.should be_true
      new_user.name.should eq("Andy")
      new_user.email.should eq("andy@example.com")
    end

    it "raises exception on validation failure" do
      # Test that validation failure raises exception
      expect_raises(Takarik::Data::Validations::ValidationError) do
        User.find_or_create_by!(name: "A") # Name too short (minimum 2 chars)
      end
    end
  end

  describe "find_or_initialize_by" do
    it "finds existing record" do
      existing_user = User.create(name: "Andy", email: "andy@example.com", age: 25, active: true)

      found_user = User.find_or_initialize_by(name: "Andy")

      found_user.should eq(existing_user)
      found_user.persisted?.should be_true
    end

    it "initializes new record when not found" do
      new_user = User.find_or_initialize_by(name: "Andy", email: "andy@example.com")

      new_user.persisted?.should be_false
      new_user.new_record?.should be_true
      new_user.name.should eq("Andy")
      new_user.email.should eq("andy@example.com")
      User.count.should eq(0) # Not saved yet
    end

    it "can save initialized record later" do
      new_user = User.find_or_initialize_by(name: "Andy", email: "andy@example.com")
      new_user.age = 25

      result = new_user.save
      result.should be_true
      new_user.persisted?.should be_true
      User.count.should eq(1)
    end

    it "works with block to set additional attributes" do
      new_user = User.find_or_initialize_by(name: "Andy", email: "andy@example.com") do |user|
        user.age = 30
      end

      new_user.persisted?.should be_false
      new_user.name.should eq("Andy")
      new_user.email.should eq("andy@example.com")
      new_user.age.should eq(30)
    end
  end

  describe "create_with" do
    it "sets default attributes for find_or_create_by" do
      new_user = User.create_with(active: false, age: 25, email: "andy@example.com").find_or_create_by(name: "Andy")

      new_user.persisted?.should be_true
      new_user.name.should eq("Andy")
      new_user.email.should eq("andy@example.com")
      new_user.active.should eq(false)
      new_user.age.should eq(25)
    end

    it "find_or_create_by conditions override create_with attributes" do
      new_user = User.create_with(active: false, age: 25, email: "default@example.com").find_or_create_by(name: "Andy", active: true, email: "andy@example.com")

      new_user.persisted?.should be_true
      new_user.name.should eq("Andy")
      new_user.email.should eq("andy@example.com") # from find_or_create_by, not create_with
      new_user.active.should eq(true)              # from find_or_create_by, not create_with
      new_user.age.should eq(25)                   # from create_with
    end

    it "doesn't affect finding existing records" do
      existing_user = User.create(name: "Andy", email: "andy@example.com", age: 30, active: true)

      found_user = User.create_with(active: false, age: 25).find_or_create_by(name: "Andy")

      found_user.should eq(existing_user)
      found_user.active.should eq(true) # Original value, not from create_with
      found_user.age.should eq(30)      # Original value, not from create_with
    end

    it "works with find_or_initialize_by" do
      new_user = User.create_with(active: false, age: 25, email: "andy@example.com").find_or_initialize_by(name: "Andy")

      new_user.persisted?.should be_false
      new_user.name.should eq("Andy")
      new_user.email.should eq("andy@example.com")
      new_user.active.should eq(false)
      new_user.age.should eq(25)
    end

    it "can be chained with other query methods" do
      new_user = User.where(active: true).create_with(age: 25, email: "andy@example.com").find_or_create_by(name: "Andy")

      new_user.persisted?.should be_true
      new_user.name.should eq("Andy")
      new_user.email.should eq("andy@example.com")
      new_user.age.should eq(25)
    end
  end

  describe "Rails compatibility examples" do
    it "matches Rails find_or_create_by behavior" do
      # Example from Rails docs
      customer = User.find_or_create_by(name: "Andy", email: "andy@example.com")
      customer.persisted?.should be_true
      customer.name.should eq("Andy")

      # Second call should find the existing record
      same_customer = User.find_or_create_by(name: "Andy", email: "andy@example.com")
      same_customer.should eq(customer)
    end

    it "matches Rails create_with behavior" do
      # Example from Rails docs
      customer = User.create_with(active: false, email: "andy@example.com").find_or_create_by(name: "Andy")
      customer.name.should eq("Andy")
      customer.email.should eq("andy@example.com")
      customer.active.should eq(false)
    end

    it "matches Rails block behavior" do
      # Example from Rails docs
      customer = User.find_or_create_by(name: "Andy", email: "andy@example.com") do |c|
        c.active = false
      end
      customer.name.should eq("Andy")
      customer.email.should eq("andy@example.com")
      customer.active.should eq(false)
    end

    it "matches Rails find_or_initialize_by behavior" do
      # Example from Rails docs
      nina = User.find_or_initialize_by(name: "Nina", email: "nina@example.com")
      nina.persisted?.should be_false
      nina.new_record?.should be_true
      nina.name.should eq("Nina")
      nina.email.should eq("nina@example.com")

      # Save when ready
      nina.save.should be_true
      nina.persisted?.should be_true
    end
  end
end
