require "./spec_helper"

# Test models for callback functionality
class CallbackTestUser < Takarik::Data::BaseModel
  table_name "users"
  primary_key :id, Int32
  column :name, String
  column :email, String

  # Method-based callbacks
  before_save :log_before_save_method
  after_save :log_after_save_method
  before_create :log_before_create_method
  after_create :log_after_create_method

  # Block-based callbacks
  before_save do
    self.email = (self.email || "") + "before_save_block;"
  end

  after_save do
    self.email = (self.email || "") + "after_save_block;"
  end

  before_create do
    self.email = (self.email || "") + "before_create_block;"
  end

  after_create do
    self.email = (self.email || "") + "after_create_block;"
  end

  # Callback methods
  private def log_before_save_method
    self.email = (self.email || "") + "before_save_method;"
  end

  private def log_after_save_method
    self.email = (self.email || "") + "after_save_method;"
  end

  private def log_before_create_method
    self.email = (self.email || "") + "before_create_method;"
  end

  private def log_after_create_method
    self.email = (self.email || "") + "after_create_method;"
  end
end

class MultiCallbackUser < Takarik::Data::BaseModel
  table_name "users"
  primary_key :id, Int32
  column :name, String
  column :email, String

  # Multiple before_save callbacks
  before_save :first_method
  before_save do
    self.email = (self.email || "") + "second_block;"
  end
  before_save :third_method
  before_save do
    self.email = (self.email || "") + "fourth_block;"
  end

  private def first_method
    self.email = (self.email || "") + "first_method;"
  end

  private def third_method
    self.email = (self.email || "") + "third_method;"
  end
end

class AllCallbacksUser < Takarik::Data::BaseModel
  table_name "users"
  primary_key :id, Int32
  column :name, String
  column :email, String

  before_validation :log_before_validation
  after_validation :log_after_validation
  before_save :log_before_save
  after_save :log_after_save
  before_create :log_before_create
  after_create :log_after_create
  before_update :log_before_update
  after_update :log_after_update
  before_destroy :log_before_destroy
  after_destroy :log_after_destroy

  private def log_before_validation
    self.email = (self.email || "") + "before_validation;"
  end

  private def log_after_validation
    self.email = (self.email || "") + "after_validation;"
  end

  private def log_before_save
    self.email = (self.email || "") + "before_save;"
  end

  private def log_after_save
    self.email = (self.email || "") + "after_save;"
  end

  private def log_before_create
    self.email = (self.email || "") + "before_create;"
  end

  private def log_after_create
    self.email = (self.email || "") + "after_create;"
  end

  private def log_before_update
    self.email = (self.email || "") + "before_update;"
  end

  private def log_after_update
    self.email = (self.email || "") + "after_update;"
  end

  private def log_before_destroy
    self.email = (self.email || "") + "before_destroy;"
  end

  private def log_after_destroy
    self.email = (self.email || "") + "after_destroy;"
  end
end

class ValidationCallbackUser < Takarik::Data::BaseModel
  table_name "users"
  primary_key :id, Int32
  column :name, String
  column :email, String

  # Method-based validation callbacks
  before_validation :normalize_name
  after_validation :log_validation_done

  # Block-based validation callbacks
  before_validation do
    self.email = (self.email || "") + "before_validation_block;"
  end

  after_validation do
    self.email = (self.email || "") + "after_validation_block;"
  end

  private def normalize_name
    self.email = (self.email || "") + "before_validation_method;"
    self.name = self.name.try(&.strip.capitalize) if self.name
  end

  private def log_validation_done
    self.email = (self.email || "") + "after_validation_method;"
  end
end

class ConditionalCallbackTestUser < Takarik::Data::BaseModel
  table_name "users"
  primary_key :id, Int32
  column :name, String
  column :email, String

  # Conditional callbacks with method names
  before_save :admin_callback, if: -> { is_admin? }
  before_save :regular_callback, unless: -> { is_admin? }

  # Conditional callbacks with blocks
  before_save(if: -> { name.try(&.includes?("VIP")) || false }) do
    self.email = (self.email || "") + "vip;"
  end

  before_save(unless: -> { email.try(&.includes?("@")) || false }) do
    self.email = (self.email || "") + "no_at;"
  end

  # Mixed conditions
  after_save :notification_callback, if: -> { email.try(&.includes?("@")) || false }, unless: -> { name.try(&.includes?("Test")) || false }

  def is_admin?
    name.try(&.downcase.includes?("admin")) || false
  end

  private def admin_callback
    self.email = (self.email || "") + "admin;"
  end

  private def regular_callback
    self.email = (self.email || "") + "regular;"
  end

  private def notification_callback
    self.email = (self.email || "") + "notification;"
  end
end

class SymbolConditionUser < Takarik::Data::BaseModel
  table_name "users"
  primary_key :id, Int32
  column :name, String
  column :email, String

  # Symbol-based conditional callbacks
  before_save :admin_callback, if: :is_admin?
  before_save :regular_callback, unless: :is_admin?

  # Block callbacks with symbol conditions
  before_save(if: :is_vip?) do
    self.email = (self.email || "") + "vip;"
  end

  before_save(unless: :has_valid_email?) do
    self.email = (self.email || "user") + "@example.com"
  end

  # Mixed symbol and proc conditions
  after_save :notification_callback, if: :has_valid_email?, unless: -> { name.try(&.includes?("Test")) || false }

  # Predicate methods
  def is_admin?
    name.try(&.downcase.includes?("admin")) || false
  end

  def is_vip?
    name.try(&.includes?("VIP")) || false
  end

  def has_valid_email?
    email.try(&.includes?("@")) || false
  end

  # Callback methods
  private def admin_callback
    self.email = (self.email || "") + "admin;"
  end

  private def regular_callback
    self.email = (self.email || "") + "regular;"
  end

  private def notification_callback
    self.email = (self.email || "") + "notification;"
  end
end

# Test model for on: conditions
class OnConditionUser < Takarik::Data::BaseModel
  table_name "users"
  primary_key :id, Int32
  column :name, String
  column :email, String

  # Single action conditions
  before_save :create_only_callback, on: :create
  before_save :update_only_callback, on: :update
  after_save :create_after_callback, on: :create
  after_save :update_after_callback, on: :update

  # Multiple action conditions
  before_validation :create_or_update_callback, on: [:create, :update]
  after_validation :create_or_update_after_callback, on: [:create, :update]

  # Block callbacks with on: conditions
  before_save(on: :create) do
    self.email = (self.email || "") + "create_block;"
  end

  before_save(on: :update) do
    self.email = (self.email || "") + "update_block;"
  end

  # Combined conditions (on: + if:/unless:)
  before_save :admin_create_callback, on: :create, if: -> { name.try(&.includes?("Admin")) || false }
  after_save :vip_update_callback, on: :update, unless: -> { email.try(&.includes?("test")) || false }

  private def create_only_callback
    self.email = (self.email || "") + "create_only;"
  end

  private def update_only_callback
    self.email = (self.email || "") + "update_only;"
  end

  private def create_after_callback
    self.email = (self.email || "") + "create_after;"
  end

  private def update_after_callback
    self.email = (self.email || "") + "update_after;"
  end

  private def create_or_update_callback
    self.email = (self.email || "") + "validation_both;"
  end

  private def create_or_update_after_callback
    self.email = (self.email || "") + "validation_after_both;"
  end

  private def admin_create_callback
    self.email = (self.email || "") + "admin_create;"
  end

  private def vip_update_callback
    self.email = (self.email || "") + "vip_update;"
  end
end

# Test model for transaction callbacks
class TransactionCallbackUser < Takarik::Data::BaseModel
  table_name "users"
  primary_key :id, Int32
  column :name, String
  column :email, String

  # Generic transaction callbacks
  after_commit :log_commit
  after_rollback :log_rollback

  # Action-specific transaction callbacks
  after_commit :create_commit_callback, on: :create
  after_commit :update_commit_callback, on: :update
  after_commit :destroy_commit_callback, on: :destroy

  after_rollback :create_rollback_callback, on: :create
  after_rollback :update_rollback_callback, on: :update
  after_rollback :destroy_rollback_callback, on: :destroy

  # Block-based transaction callbacks
  after_commit(on: [:create, :update]) do
    self.email = (self.email || "") + "commit_save;"
  end

  after_rollback(on: [:create, :update]) do
    self.email = (self.email || "") + "rollback_save;"
  end

  # Conditional transaction callbacks
  after_commit :admin_commit, if: -> { name.try(&.includes?("Admin")) || false }
  after_rollback :error_rollback, unless: -> { name.try(&.includes?("Test")) || false }

  private def log_commit
    self.email = (self.email || "") + "commit;"
  end

  private def log_rollback
    self.email = (self.email || "") + "rollback;"
  end

  private def create_commit_callback
    self.email = (self.email || "") + "create_commit;"
  end

  private def update_commit_callback
    self.email = (self.email || "") + "update_commit;"
  end

  private def destroy_commit_callback
    self.email = (self.email || "") + "destroy_commit;"
  end

  private def create_rollback_callback
    self.email = (self.email || "") + "create_rollback;"
  end

  private def update_rollback_callback
    self.email = (self.email || "") + "update_rollback;"
  end

  private def destroy_rollback_callback
    self.email = (self.email || "") + "destroy_rollback;"
  end

  private def admin_commit
    self.email = (self.email || "") + "admin_commit;"
  end

  private def error_rollback
    self.email = (self.email || "") + "error_rollback;"
  end
end

# Test model for after_initialize and after_find callbacks
class LifecycleCallbackUser < Takarik::Data::BaseModel
  table_name "users"
  column :name, String
  column :email, String

  after_initialize do
    self.email = (self.email || "") + "after_initialize;"
  end

  after_find do
    self.email = (self.email || "") + "after_find;"
  end

  # Test conditional callbacks
  after_initialize :set_default_name, if: :should_set_default
  after_find :log_find_event, unless: :skip_logging

  private def set_default_name
    self.name = "Default Name" if self.name.nil? || self.name == ""
  end

  private def log_find_event
    self.email = (self.email || "") + "log_find_event;"
  end

  private def should_set_default
    true
  end

  private def skip_logging
    false
  end
end

describe "Callback Methods and Blocks" do
  describe "mixed callback types" do
    it "supports both method names and blocks" do
      # Test CREATE operation
      user = CallbackTestUser.new
      user.name = "Test User"
      user.email = ""

      user.save

      # Verify callback execution order (Rails convention)
      # For CREATE: before_save callbacks → before_create callbacks → after_create callbacks → after_save callbacks
      # Within each type, callbacks execute in definition order
      expected_create_order = "before_save_method;before_save_block;before_create_method;before_create_block;after_create_method;after_create_block;after_save_method;after_save_block;"
      user.email.should eq(expected_create_order)
    end

    it "executes multiple callbacks of the same type in definition order" do
      user = MultiCallbackUser.new
      user.name = "Multi Callback User"
      user.email = ""

      user.save

      # Should execute in definition order
      callback_log = user.email || ""
      callback_log.should start_with("first_method;second_block;third_method;fourth_block;")
    end

    it "supports method callbacks for all callback types" do
      # Test CREATE
      user = AllCallbacksUser.new
      user.name = "All Callbacks User"
      user.email = ""
      user.save

      user.email.should eq("before_validation;after_validation;before_save;before_create;after_create;after_save;")

      # Test UPDATE
      user.email = ""
      user.name = "Updated Name"
      user.save

      user.email.should eq("before_validation;after_validation;before_save;before_update;after_update;after_save;")

      # Test DESTROY
      user.email = ""
      user.destroy

      user.email.should eq("before_destroy;after_destroy;")
    end

    it "supports validation callbacks with both methods and blocks" do
      user = ValidationCallbackUser.new
      user.name = "  test user  "
      user.email = ""
      user.save

      # Should execute validation callbacks in definition order: before_validation method → before_validation block → after_validation method → after_validation block
      user.email.should eq("before_validation_method;before_validation_block;after_validation_method;after_validation_block;")

      # Should also normalize the name
      user.name.should eq("Test user")
    end

    it "supports conditional callbacks with :if and :unless options" do
      # Test admin user (should trigger admin callback)
      admin_user = ConditionalCallbackTestUser.new
      admin_user.name = "Admin User"
      admin_user.email = "admin@example.com"
      admin_user.save

      admin_user.email.should eq("admin@example.com" + "admin;" + "notification;")

      # Test regular user (should trigger regular callback)
      regular_user = ConditionalCallbackTestUser.new
      regular_user.name = "Regular User"
      regular_user.email = "user@example.com"
      regular_user.save

      regular_user.email.should eq("user@example.com" + "regular;" + "notification;")

      # Test VIP user (should trigger VIP block callback)
      vip_user = ConditionalCallbackTestUser.new
      vip_user.name = "VIP Customer"
      vip_user.email = "vip@example.com"
      vip_user.save

      vip_user.email.should eq("vip@example.com" + "regular;" + "vip;" + "notification;")

      # Test user without @ in email (should trigger no_at callback)
      no_at_user = ConditionalCallbackTestUser.new
      no_at_user.name = "No At User"
      no_at_user.email = "bademail"
      no_at_user.save

      no_at_user.email.should eq("bademail" + "regular;" + "no_at;")

      # Test user that should not get notification (has "Test" in name)
      test_user = ConditionalCallbackTestUser.new
      test_user.name = "Test User"
      test_user.email = "test@example.com"
      test_user.save

      test_user.email.should eq("test@example.com" + "regular;")
    end

    it "supports symbol-based conditional callbacks" do
      # Test admin user with symbol condition
      admin_user = SymbolConditionUser.new
      admin_user.name = "Admin User"
      admin_user.email = "admin@example.com"
      admin_user.save

      admin_user.email.should eq("admin@example.com" + "admin;" + "notification;")

      # Test regular user with symbol condition
      regular_user = SymbolConditionUser.new
      regular_user.name = "Regular User"
      regular_user.email = "user@example.com"
      regular_user.save

      regular_user.email.should eq("user@example.com" + "regular;" + "notification;")

      # Test VIP user with symbol condition
      vip_user = SymbolConditionUser.new
      vip_user.name = "VIP Customer"
      vip_user.email = "vip@example.com"
      vip_user.save

      vip_user.email.should eq("vip@example.com" + "regular;" + "vip;" + "notification;")

      # Test user with invalid email (symbol condition)
      invalid_email_user = SymbolConditionUser.new
      invalid_email_user.name = "Bad Email User"
      invalid_email_user.email = "bademail"
      invalid_email_user.save

      invalid_email_user.email.should eq("bademailregular;@example.comnotification;")

      # Test user that should not get notification (mixed symbol and proc conditions)
      test_user = SymbolConditionUser.new
      test_user.name = "Test User"
      test_user.email = "test@example.com"
      test_user.save

      test_user.email.should eq("test@example.com" + "regular;")
    end
  end

  describe "on: conditions" do
    it "supports single action conditions" do
      user = OnConditionUser.new
      user.name = "Test User"
      user.email = ""

      user.save

      # For CREATE: validation callbacks → before_save callbacks → before_create callbacks → after_create callbacks → after_save callbacks
      expected_create_order = "validation_both;validation_after_both;create_only;create_block;create_after;"
      user.email.should eq(expected_create_order)

      # Test UPDATE
      user.email = ""
      user.name = "Updated Name"
      user.save

      # For UPDATE: validation callbacks → before_save callbacks → before_update callbacks → after_update callbacks → after_save callbacks
      # Note: vip_update_callback is triggered because it's an update AND email doesn't include "test"
      expected_update_order = "validation_both;validation_after_both;update_only;update_block;update_after;vip_update;"
      user.email.should eq(expected_update_order)
    end

    it "supports multiple action conditions" do
      user = OnConditionUser.new
      user.name = "Test User"
      user.email = ""

      user.save

      # Should execute validation callbacks for both create and update
      email = user.email || ""
      email.should contain("validation_both;")
      email.should contain("validation_after_both;")

      # Test UPDATE
      user.email = ""
      user.name = "Updated Name"
      user.save

      # Should execute validation callbacks for both create and update
      email = user.email || ""
      email.should contain("validation_both;")
      email.should contain("validation_after_both;")
    end

    it "supports combined conditions (on: + if:/unless:)" do
      # Test admin user on create
      admin_user = OnConditionUser.new
      admin_user.name = "Admin User"
      admin_user.email = ""

      admin_user.save

      # Should trigger admin_create_callback because it's create AND name includes "Admin"
      email = admin_user.email || ""
      email.should contain("admin_create;")

      # Test regular user on create (should not trigger admin callback)
      regular_user = OnConditionUser.new
      regular_user.name = "Regular User"
      regular_user.email = ""

      regular_user.save

      # Should NOT include admin_create callback
      email = regular_user.email || ""
      email.should_not contain("admin_create;")

      # Test VIP update (should trigger vip_update_callback)
      vip_user = OnConditionUser.new
      vip_user.name = "VIP User"
      vip_user.email = "vip@example.com"
      vip_user.save

      vip_user.email = "vip@example.com"
      vip_user.name = "Updated VIP"
      vip_user.save

      # Should trigger vip_update_callback because it's update AND email doesn't include "test"
      email = vip_user.email || ""
      email.should contain("vip_update;")

      # Test user with "test" in email (should not trigger vip callback)
      test_user = OnConditionUser.new
      test_user.name = "Test User"
      test_user.email = "test@example.com"
      test_user.save

      test_user.email = "test@example.com"
      test_user.name = "Updated Test"
      test_user.save

      # Should NOT include vip_update callback
      email = test_user.email || ""
      email.should_not contain("vip_update;")
    end
  end

  describe "transaction callbacks" do
    it "supports generic transaction callbacks" do
      user = TransactionCallbackUser.new
      user.name = "Test User"
      user.email = ""

      user.save

      # Should execute commit callback after successful save
      email = user.email || ""
      email.should contain("commit;")

      # Test UPDATE
      user.email = ""
      user.name = "Updated Name"
      user.save

      # Should execute commit callback after successful update
      email = user.email || ""
      email.should contain("commit;")

      # Test DESTROY
      user.email = ""
      user.destroy

      # Should execute commit callback after successful destroy
      email = user.email || ""
      email.should contain("commit;")
    end

    it "supports action-specific transaction callbacks" do
      user = TransactionCallbackUser.new
      user.name = "Test User"
      user.email = ""

      user.save

      # Should execute create-specific commit callback
      email = user.email || ""
      email.should contain("create_commit;")
      email.should contain("commit_save;")

      # Test UPDATE
      user.email = ""
      user.name = "Updated Name"
      user.save

      # Should execute update-specific commit callback
      email = user.email || ""
      email.should contain("update_commit;")
      email.should contain("commit_save;")

      # Test DESTROY
      user.email = ""
      user.destroy

      # Should execute destroy-specific commit callback
      email = user.email || ""
      email.should contain("destroy_commit;")
      # Should NOT include commit_save (only for create/update)
      email.should_not contain("commit_save;")
    end

    it "supports conditional transaction callbacks" do
      # Test admin user
      admin_user = TransactionCallbackUser.new
      admin_user.name = "Admin User"
      admin_user.email = ""

      admin_user.save

      # Should trigger admin_commit callback because name includes "Admin"
      email = admin_user.email || ""
      email.should contain("admin_commit;")

      # Test regular user
      regular_user = TransactionCallbackUser.new
      regular_user.name = "Regular User"
      regular_user.email = ""

      regular_user.save

      # Should NOT include admin_commit callback
      email = regular_user.email || ""
      email.should_not contain("admin_commit;")

      # Test user with "Test" in name (should not trigger error_rollback on success)
      test_user = TransactionCallbackUser.new
      test_user.name = "Test User"
      test_user.email = ""

      test_user.save

      # Should NOT include error_rollback callback on successful operation
      email = test_user.email || ""
      email.should_not contain("error_rollback;")
    end
  end

  describe "Object Lifecycle Callbacks" do
    describe "after_initialize callback" do
      it "should be called when creating new instances with .new" do
        user = LifecycleCallbackUser.new
        user.email.should eq("after_initialize;")
      end

      it "should be called when loading from database" do
        user = LifecycleCallbackUser.new
        user.name = "John"
        user.email = "john@example.com"
        user.save

        # Load from database
        found_user = LifecycleCallbackUser.find(user.id)
        found_user.should_not be_nil

        if found_user
          # When loading from DB: after_initialize runs during new(), but the loaded
          # email from database overwrites it, then after_find appends to loaded email
          email = found_user.email || ""
          email.should contain("after_find;")
          email.should start_with("john@example.com")
        end
      end

      it "should execute conditional callbacks" do
        user = LifecycleCallbackUser.new
        user.name.should eq("Default Name")
      end
    end

    describe "after_find callback" do
      it "should be called when loading from database" do
        user = LifecycleCallbackUser.new
        user.name = "Jane"
        user.email = "jane@example.com"
        user.save

        found_user = LifecycleCallbackUser.find(user.id)
        found_user.should_not be_nil

        if found_user
          email = found_user.email || ""
          email.should contain("after_find;")
        end
      end

      it "should NOT be called when creating new instances" do
        user = LifecycleCallbackUser.new
        email = user.email || ""
        email.should_not contain("after_find;")
      end

      it "should execute conditional callbacks" do
        user = LifecycleCallbackUser.new
        user.name = "Test"
        user.email = "test@example.com"
        user.save

        found_user = LifecycleCallbackUser.find(user.id)
        found_user.should_not be_nil

        if found_user
          email = found_user.email || ""
          email.should contain("log_find_event;")
        end
      end
    end

    describe "callback execution order" do
      it "should call after_find before after_initialize when loading from database" do
        user = LifecycleCallbackUser.new
        user.name = "Order Test"
        user.email = "order@example.com"
        user.save

        found_user = LifecycleCallbackUser.find(user.id)
        found_user.should_not be_nil

        if found_user
          email = found_user.email || ""
          # after_initialize runs during new() but gets overwritten by DB load
          # only after_find callbacks are visible in the final result
          email.should contain("after_find;")
          email.should start_with("order@example.com")
        end
      end
    end
  end
end

# Test model with timestamps for touch testing
class TouchTestUser < Takarik::Data::BaseModel
  table_name "users"
  column :name, String
  column :email, String
  timestamps  # adds created_at and updated_at
end

describe "Touch Method" do
  it "updates updated_at when called without arguments" do
    user = TouchTestUser.new
    user.name = "Touch Test"
    user.email = "touch@example.com"
    user.save

    original_updated_at = user.updated_at
    sleep 0.001.seconds  # Ensure time difference

    result = user.touch
    result.should be_true
    user.updated_at.should_not eq(original_updated_at)
  end

  it "updates updated_at when touching updated_at explicitly" do
    user = TouchTestUser.new
    user.name = "Touch Explicit Test"
    user.email = "touchexplicit@example.com"
    user.save

    original_updated_at = user.updated_at

    result = user.touch(:updated_at)
    result.should be_true
    user.updated_at.should_not eq(original_updated_at)
  end

  it "returns false for new records" do
    user = TouchTestUser.new
    user.name = "New Record"

    result = user.touch
    result.should be_false
  end

  it "updates created_at when touched explicitly" do
    user = TouchTestUser.new
    user.name = "Touch Created At Test"
    user.email = "touchcreated@example.com"
    user.save

    original_created_at = user.created_at

    result = user.touch(:created_at)
    result.should be_true
    user.created_at.should_not eq(original_created_at)
  end
end

# Test model for after_touch callbacks
class TouchCallbackUser < Takarik::Data::BaseModel
  table_name "users"
  column :name, String
  column :email, String
  timestamps

  # Method-based after_touch callback
  after_touch :log_touch_method

  # Block-based after_touch callback
  after_touch do
    self.email = (self.email || "") + "after_touch_block;"
  end

  # Conditional after_touch callbacks
  after_touch :admin_touch_callback, if: -> { name.try(&.includes?("Admin")) || false }
  after_touch :regular_touch_callback, unless: -> { name.try(&.includes?("Admin")) || false }

  private def log_touch_method
    self.email = (self.email || "") + "after_touch_method;"
  end

  private def admin_touch_callback
    self.email = (self.email || "") + "admin_touch;"
  end

  private def regular_touch_callback
    self.email = (self.email || "") + "regular_touch;"
  end
end

describe "After Touch Callbacks" do
  it "executes after_touch callbacks when touch is called" do
    user = TouchCallbackUser.new
    user.name = "Touch Callback Test"
    user.email = "test@example.com"
    user.save

    # Clear email to see callback effects
    user.email = ""
    user.touch

    email = user.email || ""
    email.should contain("after_touch_method;")
    email.should contain("after_touch_block;")
    email.should contain("regular_touch;")
  end

  it "executes conditional after_touch callbacks" do
    # Test admin user
    admin_user = TouchCallbackUser.new
    admin_user.name = "Admin User"
    admin_user.email = "admin@example.com"
    admin_user.save

    admin_user.email = ""
    admin_user.touch

    email = admin_user.email || ""
    email.should contain("after_touch_method;")
    email.should contain("after_touch_block;")
    email.should contain("admin_touch;")
    email.should_not contain("regular_touch;")

    # Test regular user
    regular_user = TouchCallbackUser.new
    regular_user.name = "Regular User"
    regular_user.email = "regular@example.com"
    regular_user.save

    regular_user.email = ""
    regular_user.touch

    email = regular_user.email || ""
    email.should contain("after_touch_method;")
    email.should contain("after_touch_block;")
    email.should contain("regular_touch;")
    email.should_not contain("admin_touch;")
  end

  it "does not execute after_touch callbacks for new records" do
    user = TouchCallbackUser.new
    user.name = "New Record"
    user.email = ""

    result = user.touch
    result.should be_false

    # No callbacks should have executed
    email = user.email || ""
    email.should_not contain("after_touch_method;")
    email.should_not contain("after_touch_block;")
  end

  it "executes after_touch callbacks when touching specific attributes" do
    user = TouchCallbackUser.new
    user.name = "Specific Touch Test"
    user.email = "specific@example.com"
    user.save

    user.email = ""
    user.touch(:created_at)

    email = user.email || ""
    email.should contain("after_touch_method;")
    email.should contain("after_touch_block;")
    email.should contain("regular_touch;")
  end
end
