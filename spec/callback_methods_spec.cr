require "./spec_helper"

# Test models for callback functionality
class CallbackTestUser < Takarik::Data::BaseModel
  table_name "users"
  primary_key id, Int32
  column name, String
  column email, String

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
  primary_key id, Int32
  column name, String
  column email, String

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
  primary_key id, Int32
  column name, String
  column email, String

  before_save :log_before_save
  after_save :log_after_save
  before_create :log_before_create
  after_create :log_after_create
  before_update :log_before_update
  after_update :log_after_update
  before_destroy :log_before_destroy
  after_destroy :log_after_destroy

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

      user.email.should eq("before_save;before_create;after_create;after_save;")

      # Test UPDATE
      user.email = ""
      user.name = "Updated Name"
      user.save

      user.email.should eq("before_save;before_update;after_update;after_save;")

      # Test DESTROY
      user.email = ""
      user.destroy

      user.email.should eq("before_destroy;after_destroy;")
    end
  end
end
