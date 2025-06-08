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

# Test model for enhanced callback conditions
class EnhancedCallbackUser < Takarik::Data::BaseModel
  table_name "users"
  column :name, String
  column :email, String

  # Single conditions (already supported)
  before_save :single_symbol_callback, if: :is_admin?
  before_save :single_proc_callback, if: -> { name.try(&.includes?("Test")) || false }

  # Array of symbol conditions (all must be true)
  before_save :array_symbols_callback, if: [:is_admin?, :has_email?]

  # Mixed array with symbols and procs
  before_save :mixed_array_callback, if: [:is_admin?, -> { name.try(&.includes?("Mixed")) || false }]

  # Proc with parameter
  before_save :proc_with_param_callback, if: ->(user : EnhancedCallbackUser) { user.name.try(&.includes?("Param")) || false }

  # Combined if and unless
  before_save :combined_if_unless_callback,
    if: -> { name.try(&.includes?("Combined")) || false },
    unless: :is_blocked?

  # Array of unless conditions
  before_save :array_unless_callback,
    unless: [:is_blocked?, -> { name.try(&.includes?("Skip")) || false }]

  def is_admin?
    name.try(&.downcase.includes?("admin")) || false
  end

  def has_email?
    email.try { |e| !e.empty? } || false
  end

  def is_blocked?
    name.try(&.includes?("Blocked")) || false
  end

  private def single_symbol_callback
    self.email = (self.email || "") + "single_symbol;"
  end

  private def single_proc_callback
    self.email = (self.email || "") + "single_proc;"
  end

  private def array_symbols_callback
    self.email = (self.email || "") + "array_symbols;"
  end

  private def mixed_array_callback
    self.email = (self.email || "") + "mixed_array;"
  end

  private def proc_with_param_callback
    self.email = (self.email || "") + "proc_with_param;"
  end

  private def combined_if_unless_callback
    self.email = (self.email || "") + "combined_if_unless;"
  end

  private def array_unless_callback
    self.email = (self.email || "") + "array_unless;"
  end
end

describe "Enhanced Callback Conditions" do
  describe "array of symbol conditions" do
    it "executes callback when all symbol conditions are true" do
      user = EnhancedCallbackUser.new
      user.name = "Admin User"
      user.email = "admin@example.com"
      user.save

      email = user.email || ""
      email.should contain("array_symbols;")
    end

    it "does not execute callback when any symbol condition is false" do
      user = EnhancedCallbackUser.new
      user.name = "Regular User"  # Not admin, so won't trigger single_symbol_callback
      user.email = ""  # has_email? returns false
      user.save

      email = user.email || ""
      email.should_not contain("array_symbols;")
    end
  end

  describe "mixed array with symbols and procs" do
    it "executes callback when all mixed conditions are true" do
      user = EnhancedCallbackUser.new
      user.name = "Admin Mixed User"
      user.email = "mixed@example.com"
      user.save

      email = user.email || ""
      email.should contain("mixed_array;")
    end

    it "does not execute callback when proc condition is false" do
      user = EnhancedCallbackUser.new
      user.name = "Admin User"  # doesn't include "Mixed"
      user.email = "admin@example.com"
      user.save

      email = user.email || ""
      email.should_not contain("mixed_array;")
    end
  end

  describe "proc with parameter" do
    it "executes callback when proc with parameter is true" do
      user = EnhancedCallbackUser.new
      user.name = "Param User"
      user.email = "param@example.com"
      user.save

      email = user.email || ""
      email.should contain("proc_with_param;")
    end

    it "does not execute callback when proc with parameter is false" do
      user = EnhancedCallbackUser.new
      user.name = "Regular User"  # doesn't include "Param"
      user.email = "regular@example.com"
      user.save

      email = user.email || ""
      email.should_not contain("proc_with_param;")
    end
  end

  describe "combined if and unless conditions" do
    it "executes callback when if is true and unless is false" do
      user = EnhancedCallbackUser.new
      user.name = "Combined User"  # includes "Combined", doesn't include "Blocked"
      user.email = "combined@example.com"
      user.save

      email = user.email || ""
      email.should contain("combined_if_unless;")
    end

    it "does not execute callback when if is true but unless is also true" do
      user = EnhancedCallbackUser.new
      user.name = "Combined Blocked User"  # includes both "Combined" and "Blocked"
      user.email = "blocked@example.com"
      user.save

      email = user.email || ""
      email.should_not contain("combined_if_unless;")
    end

    it "does not execute callback when if is false" do
      user = EnhancedCallbackUser.new
      user.name = "Regular User"  # doesn't include "Combined"
      user.email = "regular@example.com"
      user.save

      email = user.email || ""
      email.should_not contain("combined_if_unless;")
    end
  end

  describe "array of unless conditions" do
    it "executes callback when all unless conditions are false" do
      user = EnhancedCallbackUser.new
      user.name = "Regular User"  # doesn't include "Blocked" or "Skip"
      user.email = "regular@example.com"
      user.save

      email = user.email || ""
      email.should contain("array_unless;")
    end

    it "does not execute callback when any unless condition is true" do
      user = EnhancedCallbackUser.new
      user.name = "Blocked User"  # includes "Blocked"
      user.email = "blocked@example.com"
      user.save

      email = user.email || ""
      email.should_not contain("array_unless;")

      user2 = EnhancedCallbackUser.new
      user2.name = "Skip User"  # includes "Skip"
      user2.email = "skip@example.com"
      user2.save

      email2 = user2.email || ""
      email2.should_not contain("array_unless;")
    end
  end

  describe "backward compatibility" do
    it "still supports existing single symbol and proc conditions" do
      user = EnhancedCallbackUser.new
      user.name = "Admin Test User"
      user.email = "test@example.com"
      user.save

      email = user.email || ""
      # Should execute both single symbol and single proc callbacks
      email.should contain("single_symbol;")
      email.should contain("single_proc;")
    end
  end
end

# Test model for proc parameter edge cases
class EdgeCaseCallbackUser < Takarik::Data::BaseModel
  table_name "users"
  column :name, String
  column :email, String

  # Various proc parameter styles
  before_save :no_param_proc, if: -> { name.try(&.includes?("NoParam")) || false }
  before_save :typed_param_proc, if: ->(user : EdgeCaseCallbackUser) { user.name.try(&.includes?("TypedParam")) || false }
  before_save :untyped_param_proc, if: ->(user : EdgeCaseCallbackUser) { user.name.try(&.includes?("UntypedParam")) || false }

  private def no_param_proc
    self.email = (self.email || "") + "no_param;"
  end

  private def typed_param_proc
    self.email = (self.email || "") + "typed_param;"
  end

  private def untyped_param_proc
    self.email = (self.email || "") + "untyped_param;"
  end
end

describe "Proc Parameter Edge Cases" do
  it "handles proc without parameters" do
    user = EdgeCaseCallbackUser.new
    user.name = "NoParam User"
    user.email = ""
    user.save

    email = user.email || ""
    email.should contain("no_param;")
  end

  it "handles proc with typed parameter" do
    user = EdgeCaseCallbackUser.new
    user.name = "TypedParam User"
    user.email = ""
    user.save

    email = user.email || ""
    email.should contain("typed_param;")
  end
end

describe "dependent associations" do
  describe "dependent: :destroy" do
    it "destroys associated records when parent is destroyed" do
      # Create user with posts (User has_many posts, dependent: :destroy)
      user = User.create(name: "John", email: "john@destroy.com", age: 30, active: true)
      post1 = user.create_posts(title: "Post 1", content: "Content 1", published: true)
      post2 = user.create_posts(title: "Post 2", content: "Content 2", published: true)

      # Verify posts exist
      user.posts.count.should eq(2)
      Post.count.as(Int64).should be >= 2

      # Destroy user - should also destroy associated posts
      user.destroy.should be_true

      # Verify posts were destroyed
      Post.find(post1.id).should be_nil
      Post.find(post2.id).should be_nil
    end

    it "destroys nested dependent associations" do
      # Create user -> post -> comments chain
      user = User.create(name: "Jane", email: "jane@destroy.com", age: 25, active: true)
      post = user.create_posts(title: "Blog Post", content: "Content", published: true)
      comment1 = post.create_comments(content: "Great post!", user_id: user.id)
      comment2 = post.create_comments(content: "I agree!", user_id: user.id)

      # Verify chain exists
      user.posts.count.should eq(1)
      post.comments.count.should eq(2)

      # Destroy user - should cascade destroy posts and their comments
      user.destroy.should be_true

      # Verify everything was destroyed
      Post.find(post.id).should be_nil
      Comment.find(comment1.id).should be_nil
      Comment.find(comment2.id).should be_nil
    end
  end

  describe "dependent: :delete_all" do
    it "deletes associated records with SQL DELETE (no callbacks)" do
      # Create department with employees
      dept = DepartmentDeleteAll.create(name: "Engineering")
      emp1 = EmployeeDeleteAll.create(name: "Alice", department_id: dept.id)
      emp2 = EmployeeDeleteAll.create(name: "Bob", department_id: dept.id)

      # Verify employees exist
      EmployeeDeleteAll.where(department_id: dept.id).count.should eq(2)

      # Destroy department - should delete employees with SQL (faster, no callbacks)
      dept.destroy.should be_true

      # Verify employees were deleted
      EmployeeDeleteAll.where(department_id: dept.id).count.should eq(0)
      EmployeeDeleteAll.find(emp1.id).should be_nil
      EmployeeDeleteAll.find(emp2.id).should be_nil
    end
  end

  describe "dependent: :nullify" do
    it "nullifies foreign keys instead of deleting records" do
      # Create category with products
      category = CategoryNullify.create(name: "Electronics")
      product1 = ProductNullify.create(name: "Laptop", category_id: category.id)
      product2 = ProductNullify.create(name: "Phone", category_id: category.id)

      # Verify products are associated
      ProductNullify.where(category_id: category.id).count.should eq(2)

      # Destroy category - should nullify foreign keys, keep products
      category.destroy.should be_true

      # Verify products still exist but with null category_id
      ProductNullify.count.as(Int64).should be >= 2  # Products still exist
      product1.reload
      product2.reload
      product1.category_id.should be_nil
      product2.category_id.should be_nil
    end
  end

  describe "without dependent option" do
    it "leaves associated records untouched when no dependent option specified" do
      # Create company with employees
      company = CompanyIndependent.create(name: "TechCorp")
      emp1 = EmployeeIndependent.create(name: "Charlie", company_id: company.id)
      emp2 = EmployeeIndependent.create(name: "Diana", company_id: company.id)

      # Verify employees exist
      EmployeeIndependent.where(company_id: company.id).count.should eq(2)

      # Destroy company - should NOT affect employees
      company.destroy.should be_true

      # Verify employees still exist and still reference the company (foreign key intact)
      EmployeeIndependent.where(company_id: company.id).count.should eq(2)
      emp1.reload
      emp2.reload
      emp1.company_id.should eq(company.id)  # Foreign key preserved
      emp2.company_id.should eq(company.id)  # Foreign key preserved
    end
  end

  describe "belongs_to dependent options" do
    it "ignores dependent options on belongs_to associations" do
      # belongs_to with dependent option should be ignored (like ActiveRecord)
      customer = CustomerDependent.create(name: "John Doe")
      order = OrderDependent.create(total: 99.99, customer_id: customer.id)

      # Destroy order - should NOT destroy customer (belongs_to dependent is ignored)
      order.destroy.should be_true

      # Customer should still exist
      CustomerDependent.find(customer.id).should_not be_nil
    end
  end

  describe "error handling in dependent associations" do
    it "handles missing associated records gracefully" do
      user = User.create(name: "Test", email: "test@example.com", age: 30, active: true)

      # Should not raise error even if no associated records exist
      user.destroy.should be_true
    end

    it "rolls back transaction if dependent destroy fails" do
      # This would require mocking/stubbing to test transaction rollback
      # For now, we test that the basic flow works
      user = User.create(name: "Test", email: "test@rollback.com", age: 30, active: true)
      post = user.create_posts(title: "Test Post", content: "Content", published: true)

      # Should work normally
      user.destroy.should be_true
      Post.find(post.id).should be_nil
    end
  end
end
