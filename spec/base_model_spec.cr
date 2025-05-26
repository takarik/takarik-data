require "./spec_helper"

describe Takarik::Data::BaseModel do
  # Ensure User class has the connection
  before_all do
    User.establish_connection("sqlite3://./test.db")
    Post.establish_connection("sqlite3://./test.db")
  end

  describe "basic CRUD operations" do
    it "creates a new record" do
      user = User.new
      user.name = "John Doe"
      user.email = "john@example.com"
      user.age = 30

      user.save.should be_true
      user.persisted?.should be_true
      user.new_record?.should be_false
      user.id.should_not be_nil
    end

    it "creates a record using class method" do
      user = User.create(name: "Jane Doe", email: "jane@example.com", age: 25)

      user.persisted?.should be_true
      user.name.should eq("Jane Doe")
      user.email.should eq("jane@example.com")
      user.age.should eq(25)
    end

    it "finds a record by id" do
      user = User.create(name: "Bob Smith", email: "bob@example.com", age: 35)

      found_user = User.find(user.id)
      found_user.should_not be_nil
      found_user.try(&.name).should eq("Bob Smith")
    end

    it "finds a record by id with find!" do
      user = User.create(name: "Alice Brown", email: "alice@example.com", age: 28)

      found_user = User.find!(user.id)
      found_user.name.should eq("Alice Brown")
    end

    it "raises exception when record not found with find!" do
      expect_raises(Exception, "Record not found") do
        User.find!(999)
      end
    end

    it "updates a record" do
      user = User.create(name: "Charlie Wilson", email: "charlie@example.com", age: 40)

      user.name = "Charles Wilson"
      user.age = 41
      user.save.should be_true

      updated_user = User.find!(user.id)
      updated_user.name.should eq("Charles Wilson")
      updated_user.age.should eq(41)
    end

    it "updates a record using update method" do
      user = User.create(name: "David Lee", email: "david@example.com", age: 33)

      user.update(name: "Dave Lee", age: 34).should be_true

      updated_user = User.find!(user.id)
      updated_user.name.should eq("Dave Lee")
      updated_user.age.should eq(34)
    end

    it "destroys a record" do
      user = User.create(name: "Eve Taylor", email: "eve@example.com", age: 29)
      user_id = user.id

      user.destroy.should be_true
      user.persisted?.should be_false

      User.find(user_id).should be_nil
    end

    it "tracks changed attributes" do
      user = User.create(name: "Frank Miller", email: "frank@example.com", age: 45)

      user.changed?.should be_false
      user.changed_attributes.should be_empty

      user.name = "Franklin Miller"
      user.age = 46

      user.changed?.should be_true
      user.changed_attributes.should contain("name")
      user.changed_attributes.should contain("age")
    end

    it "reloads a record" do
      user = User.create(name: "Grace Davis", email: "grace@example.com", age: 31)

      # Simulate external update
      User.connection.exec("UPDATE users SET name = 'Gracie Davis' WHERE id = ?", user.id)

      user.reload
      user.name.should eq("Gracie Davis")
    end
  end

  describe "query methods" do
    before_each do
      User.create(name: "Alice", email: "alice@example.com", age: 25, active: true)
      User.create(name: "Bob", email: "bob@example.com", age: 30, active: true)
      User.create(name: "Charlie", email: "charlie@example.com", age: 35, active: false)
    end

    it "finds all records" do
      users = User.all
      users.size.should eq(3)
    end

    it "finds records with where conditions" do
      active_users = User.where(active: true)
      active_users.size.should eq(2)
      active_users.all?(&.active).should be_true
    end

    it "finds first record" do
      user = User.first
      user.should_not be_nil
      user.try(&.name).should eq("Alice")
    end

    it "finds last record" do
      user = User.last
      user.should_not be_nil
      user.try(&.name).should eq("Charlie")
    end

    it "counts records" do
      User.count.should eq(3)
    end

    it "uses query builder for complex queries" do
      users = User.where("age > ?", 25).order("age", "ASC").limit(2)
      users.size.should eq(2)
      users.first.try(&.name).should eq("Bob")
      users.last.try(&.name).should eq("Charlie")
    end
  end

  describe "validations" do
    it "validates presence of required fields" do
      user = User.new
      user.valid?.should be_false
      user.errors["name"].should contain("can't be blank")
      user.errors["email"].should contain("can't be blank")
    end

    it "validates length constraints" do
      user = User.new
      user.name = "A"
      user.email = "test@example.com"

      user.valid?.should be_false
      user.errors["name"].should contain("is too short (minimum is 2 characters)")
    end

    it "validates format constraints" do
      user = User.new
      user.name = "John Doe"
      user.email = "invalid-email"

      user.valid?.should be_false
      user.errors["email"].should contain("is invalid")
    end

    it "validates uniqueness constraints" do
      User.create(name: "John", email: "john@example.com", age: 30)

      user = User.new
      user.name = "Jane"
      user.email = "john@example.com"
      user.age = 25

      user.valid?.should be_false
      user.errors["email"].should contain("has already been taken")
    end

    it "validates numericality constraints" do
      user = User.new
      user.name = "John"
      user.email = "john@example.com"
      user.age = -5

      user.valid?.should be_false
      user.errors["age"].should contain("must be greater than 0")
    end

    it "prevents saving invalid records" do
      user = User.new
      user.name = "A"  # Too short
      user.email = "invalid"  # Invalid format

      user.save.should be_false
      user.persisted?.should be_false
    end

    it "raises exception when saving invalid records with save!" do
      user = User.new
      user.name = "A"  # Too short
      user.email = "invalid"  # Invalid format

      expect_raises(Takarik::Data::Validations::ValidationError) do
        user.save!
      end
    end
  end

  describe "associations" do
    it "creates belongs_to associations" do
      user = User.create(name: "Author", email: "author@example.com", age: 30)
      post = Post.new
      post.title = "Test Post"
      post.content = "This is a test post"
      post.user = user
      post.save

      post.user.should eq(user)
      post.user_id.should eq(user.id)
    end

    it "creates has_many associations" do
      user = User.create(name: "Author", email: "author@example.com", age: 30)

      post1 = user.create_posts(title: "First Post", content: "Content 1", published: true)
      post2 = user.create_posts(title: "Second Post", content: "Content 2", published: false)

      posts = user.posts
      posts.size.should eq(2)
      posts.includes?(post1).should be_true
      posts.includes?(post2).should be_true
    end

    it "builds associated records" do
      user = User.create(name: "Author", email: "author@example.com", age: 30)

      post = user.build_posts(title: "Built Post", content: "Built content")
      post.user.should eq(user)
      post.user_id.should eq(user.id)
      post.persisted?.should be_false
    end
  end

  describe "scopes" do
    before_each do
      User.create(name: "Alice", email: "alice@example.com", age: 17, active: true)
      User.create(name: "Bob", email: "bob@example.com", age: 25, active: true)
      User.create(name: "Charlie", email: "charlie@example.com", age: 30, active: false)
    end

    it "uses active scope" do
      active_users = User.active
      active_users.size.should eq(2)
      active_users.all?(&.active).should be_true
    end

    it "uses adults scope" do
      adult_users = User.adults
      adult_users.size.should eq(2)
      adult_users.all? { |u| (u.age || 0) >= 18 }.should be_true
    end

    it "chains scopes" do
      active_adults = User.where(active: true).where_gte("age", 18)
      active_adults.size.should eq(1)
      active_adults.first.try(&.name).should eq("Bob")
    end
  end

  describe "callbacks" do
    it "executes before_save callbacks" do
      user = User.new
      user.name = "John"
      user.email = "JOHN@EXAMPLE.COM"
      user.age = 30

      user.save
      user.email.should eq("john@example.com")  # Should be lowercased by callback
    end
  end

  describe "timestamps" do
    it "sets created_at and updated_at on create" do
      user = User.create(name: "John", email: "john@example.com", age: 30)

      user.created_at.should_not be_nil
      user.updated_at.should_not be_nil
      # Allow for small time differences (within 1 second)
      time_diff = (user.updated_at.not_nil! - user.created_at.not_nil!).total_seconds.abs
      time_diff.should be < 1.0
    end

    it "updates updated_at on save" do
      user = User.create(name: "John", email: "john@example.com", age: 30)
      original_updated_at = user.updated_at

      sleep(1.millisecond)  # Ensure time difference
      user.name = "Johnny"
      user.save

      user.updated_at.should_not eq(original_updated_at)
    end
  end

  describe "equality" do
    it "considers records with same id as equal" do
      user1 = User.create(name: "John", email: "john@example.com", age: 30)
      user2 = User.find!(user1.id)

      user1.should eq(user2)
    end

    it "considers new records as not equal" do
      user1 = User.new
      user2 = User.new

      user1.should_not eq(user2)
    end
  end

  describe "serialization" do
    it "converts to hash" do
      user = User.create(name: "John", email: "john@example.com", age: 30)
      hash = user.to_h

      hash["name"].should eq("John")
      hash["email"].should eq("john@example.com")
      hash["age"].should eq(30)
    end
  end
end
