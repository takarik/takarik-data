require "./spec_helper"

# Test models for generic validates testing
class GenericValidationUser < Takarik::Data::BaseModel
  table_name "users"

  primary_key :id, Int32
  column :name, String
  column :email, String
  column :age, Int32
  column :active, Bool
  column :created_at, Time
  column :updated_at, Time

  # Using new generic validates syntax
  validates :name, presence: true, length: {minimum: 2, maximum: 50}
  validates :email, presence: true, uniqueness: true, format: {with: /\A[\w+\-.]+@[a-z\d\-]+(\.[a-z\d\-]+)*\.[a-z]+\z/i}
  validates :age, presence: true, numericality: {greater_than: 0, less_than: 150}

  timestamps
end

class LengthTestModel < Takarik::Data::BaseModel
  table_name "users"

  primary_key :id, Int32
  column :name, String
  column :description, String

  validates :name, length: {is: 5}
  validates :description, length: {in: 10..100}

  timestamps
end

class NumberTestModel < Takarik::Data::BaseModel
  table_name "users"

  primary_key :id, Int32
  column :score, Int32
  column :rating, Float64
  column :count, Int32

  validates :score, numericality: {only_integer: true, greater_than_or_equal_to: 0}
  validates :rating, numericality: {greater_than: 0.0, less_than_or_equal_to: 5.0}
  validates :count, numericality: {even: true}

  timestamps
end

class MixedValidationModel < Takarik::Data::BaseModel
  table_name "users"

  primary_key :id, Int32
  column :name, String
  column :email, String
  column :age, Int32

  # Mix legacy and new styles
  validates_presence_of :name  # Legacy
  validates :email, presence: true, format: {with: /@/}  # New
  validates_length_of :name, minimum: 2  # Legacy

  timestamps
end

describe "Generic Validates Macro" do
  before_each do
    # Clean up test data
    User.all.delete_all
  end

  describe "presence validation" do
    it "validates presence using generic syntax" do
      user = GenericValidationUser.new
      user.email = "test@example.com"
      user.age = 25

      user.valid?.should be_false
      user.errors["name"].should contain("can't be blank")

      user.name = "John"
      user.valid?.should be_true
    end
  end

  describe "length validation" do
    it "validates minimum and maximum length" do
      user = GenericValidationUser.new
      user.name = "A"  # Too short
      user.email = "test@example.com"
      user.age = 25

      user.valid?.should be_false
      user.errors["name"].should contain("is too short (minimum is 2 characters)")

      user.name = "A" * 51  # Too long
      user.valid?.should be_false
      user.errors["name"].should contain("is too long (maximum is 50 characters)")

      user.name = "John"  # Just right
      user.valid?.should be_true
    end

    it "validates exact length" do
      model = LengthTestModel.new
      model.name = "1234"  # Too short

      model.valid?.should be_false
      model.errors["name"].should contain("is the wrong length (should be 5 characters)")

      model.name = "12345"  # Exact
      model.valid?.should be_true
    end

    it "validates length range" do
      model = LengthTestModel.new
      model.description = "123456789"  # Too short (9 chars)

      model.valid?.should be_false
      model.errors["description"].should contain("is the wrong length (should be within 10..100)")

      model.description = "1234567890"  # Just right (10 chars)
      model.valid?.should be_true
    end
  end

  describe "format validation" do
    it "validates format using regex" do
      user = GenericValidationUser.new
      user.name = "John"
      user.email = "invalid-email"
      user.age = 25

      user.valid?.should be_false
      user.errors["email"].should contain("is invalid")

      user.email = "john@example.com"
      user.valid?.should be_true
    end
  end

  describe "uniqueness validation" do
    it "validates uniqueness" do
      # Create first user
      user1 = GenericValidationUser.create(name: "John", email: "john@example.com", age: 30)
      user1.persisted?.should be_true

      # Try to create second user with same email
      user2 = GenericValidationUser.new
      user2.name = "Jane"
      user2.email = "john@example.com"  # Same email
      user2.age = 25

      user2.valid?.should be_false
      user2.errors["email"].should contain("has already been taken")

      # Different email should work
      user2.email = "jane@example.com"
      user2.valid?.should be_true
    end

    it "excludes current record when updating" do
      user = GenericValidationUser.create(name: "John", email: "john@example.com", age: 30)

      # Should be able to update same record without uniqueness error
      user.name = "John Updated"
      user.valid?.should be_true
    end
  end

  describe "numericality validation" do
    it "validates greater_than and less_than" do
      user = GenericValidationUser.new
      user.name = "John"
      user.email = "john@example.com"
      user.age = -5  # Invalid

      user.valid?.should be_false
      user.errors["age"].should contain("must be greater than 0")

      user.age = 200  # Invalid
      user.valid?.should be_false
      user.errors["age"].should contain("must be less than 150")

      user.age = 25  # Valid
      user.valid?.should be_true
    end

    it "validates only_integer" do
      model = NumberTestModel.new
      model.score = 100  # Valid integer

      model.valid?.should be_true

      # Test string that's not a valid integer
      model.set_attribute("score", "100.5")
      model.valid?.should be_false
      model.errors["score"].should contain("must be an integer")
    end

    it "validates even numbers" do
      model = NumberTestModel.new
      model.count = 3  # Odd number

      model.valid?.should be_false
      model.errors["count"].should contain("must be even")

      model.count = 4  # Even number
      model.valid?.should be_true
    end

    it "validates greater_than_or_equal_to and less_than_or_equal_to" do
      model = NumberTestModel.new
      model.score = -1  # Less than 0

      model.valid?.should be_false
      model.errors["score"].should contain("must be greater than or equal to 0")

      model.score = 0  # Exactly 0 - should be valid
      model.valid?.should be_true

      model.rating = 5.1  # Greater than 5.0
      model.valid?.should be_false
      model.errors["rating"].should contain("must be less than or equal to 5.0")

      model.rating = 5.0  # Exactly 5.0 - should be valid
      model.valid?.should be_true
    end
  end

  describe "multiple validations on single field" do
    it "applies all validations for a field" do
      user = GenericValidationUser.new
      user.name = "A"  # Fails both presence and length
      user.email = "invalid"  # Fails format
      user.age = -5  # Fails numericality

      user.valid?.should be_false

      # Should have length error for name
      user.errors["name"].should contain("is too short (minimum is 2 characters)")

      # Should have format error for email
      user.errors["email"].should contain("is invalid")

      # Should have numericality error for age
      user.errors["age"].should contain("must be greater than 0")
    end
  end

  describe "backward compatibility" do
    it "still works with legacy validation macros" do
      # The existing User model uses legacy macros
      user = User.new
      user.valid?.should be_false
      user.errors["name"].should contain("can't be blank")
      user.errors["email"].should contain("can't be blank")

      user.name = "John"
      user.email = "john@example.com"
      user.age = 25
      user.valid?.should be_true
    end

    it "can mix legacy and new validation styles" do
      model = MixedValidationModel.new
      model.valid?.should be_false
      model.errors.has_key?("name").should be_true
      model.errors.has_key?("email").should be_true
    end
  end

  describe "integration with association objects" do
    it "works with association object support" do
      user = GenericValidationUser.create(
        name: "John",
        email: "john@example.com",
        age: 30
      )

      user.persisted?.should be_true
      user.name.should eq("John")
      user.email.should eq("john@example.com")
      user.age.should eq(30)
    end
  end
end
