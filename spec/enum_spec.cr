require "./spec_helper"

describe "Enum Support" do
  before_each do
    # Set up test table
    Takarik::Data::BaseModel.connection.exec "DROP TABLE IF EXISTS orders_enum"
    Takarik::Data::BaseModel.connection.exec <<-SQL
      CREATE TABLE orders_enum (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        customer_name TEXT,
        status INTEGER DEFAULT 0,
        priority INTEGER DEFAULT 0,
        created_at TEXT,
        updated_at TEXT
      )
    SQL
  end

  describe "enum declaration" do
    it "creates enum mappings" do
      OrderEnum.status_mappings.should eq({
        "shipped" => 0,
        "being_packaged" => 1,
        "complete" => 2,
        "cancelled" => 3
      })

      OrderEnum.priority_mappings.should eq({
        "low" => 0,
        "medium" => 1,
        "high" => 2,
        "urgent" => 3
      })
    end

    it "provides enum values" do
      OrderEnum.status_values.should eq(["shipped", "being_packaged", "complete", "cancelled"])
      OrderEnum.priority_values.should eq(["low", "medium", "high", "urgent"])
    end
  end

  describe "enum scopes" do
    before_each do
      OrderEnum.create(customer_name: "Alice", status: 0, priority: 2)   # shipped, high
      OrderEnum.create(customer_name: "Bob", status: 1, priority: 1)     # being_packaged, medium
      OrderEnum.create(customer_name: "Charlie", status: 2, priority: 0) # complete, low
      OrderEnum.create(customer_name: "Dave", status: 3, priority: 3)    # cancelled, urgent
    end

    it "creates positive scopes for each enum value" do
      # Status scopes
      OrderEnum.shipped.count.should eq(1)
      OrderEnum.being_packaged.count.should eq(1)
      OrderEnum.complete.count.should eq(1)
      OrderEnum.cancelled.count.should eq(1)

      # Priority scopes
      OrderEnum.low.count.should eq(1)
      OrderEnum.medium.count.should eq(1)
      OrderEnum.high.count.should eq(1)
      OrderEnum.urgent.count.should eq(1)
    end

    it "creates negative scopes for each enum value" do
      # Status negative scopes
      OrderEnum.not_shipped.count.should eq(3)
      OrderEnum.not_being_packaged.count.should eq(3)
      OrderEnum.not_complete.count.should eq(3)
      OrderEnum.not_cancelled.count.should eq(3)

      # Priority negative scopes
      OrderEnum.not_low.count.should eq(3)
      OrderEnum.not_medium.count.should eq(3)
      OrderEnum.not_high.count.should eq(3)
      OrderEnum.not_urgent.count.should eq(3)
    end

    it "allows chaining enum scopes with other conditions" do
      result = OrderEnum.shipped.where("customer_name LIKE ?", "A%").to_a
      result.size.should eq(1)
      result.first.customer_name.should eq("Alice")

      # Chain multiple enum scopes
      result = OrderEnum.shipped.high.to_a
      result.size.should eq(1)
      result.first.customer_name.should eq("Alice")
    end

    it "generates correct SQL for enum scopes" do
      OrderEnum.shipped.to_sql.should contain("status = ?")
      OrderEnum.not_cancelled.to_sql.should contain("status != ?")
    end
  end

  describe "instance query methods" do
    it "creates query methods for each enum value" do
      order = OrderEnum.create(customer_name: "Test", status: 0, priority: 2)

      # Status query methods
      order.shipped?.should be_true
      order.being_packaged?.should be_false
      order.complete?.should be_false
      order.cancelled?.should be_false

      # Priority query methods
      order.low?.should be_false
      order.medium?.should be_false
      order.high?.should be_true
      order.urgent?.should be_false
    end

    it "updates query methods when enum value changes" do
      order = OrderEnum.create(customer_name: "Test", status: 0, priority: 0)

      order.shipped?.should be_true
      order.complete?.should be_false

      order.status = 2
      order.shipped?.should be_false
      order.complete?.should be_true
    end
  end

  describe "instance setter methods" do
    it "creates setter methods that update and save" do
      order = OrderEnum.create(customer_name: "Test", status: 0, priority: 0)

      # Test status setter
      order.complete!.should be_true
      order.status.should eq(2)
      order.complete?.should be_true

      # Verify it was saved to database
      reloaded = OrderEnum.find(order.id.not_nil!)
      reloaded.not_nil!.status.should eq(2)
      reloaded.not_nil!.complete?.should be_true

      # Test priority setter
      order.urgent!.should be_true
      order.priority.should eq(3)
      order.urgent?.should be_true
    end

    it "returns the correct boolean value after setting" do
      order = OrderEnum.create(customer_name: "Test", status: 1)

      order.shipped!.should be_true
      order.being_packaged!.should be_true
      order.complete!.should be_true
      order.cancelled!.should be_true
    end
  end

  describe "enum name methods" do
    it "returns string representation of enum value" do
      order = OrderEnum.create(customer_name: "Test", status: 1, priority: 2)

      order.status_name.should eq("being_packaged")
      order.priority_name.should eq("high")
    end

    it "returns nil for invalid enum values" do
      order = OrderEnum.create(customer_name: "Test", status: 999)
      order.status_name.should be_nil
    end

    it "allows setting enum by string name" do
      order = OrderEnum.create(customer_name: "Test", status: 0)

      order.status_name = "complete"
      order.status.should eq(2)
      order.complete?.should be_true

      order.priority_name = "urgent"
      order.priority.should eq(3)
      order.urgent?.should be_true
    end

    it "raises error for invalid string names" do
      order = OrderEnum.create(customer_name: "Test")

      expect_raises(Exception, /Invalid status value: invalid/) do
        order.status_name = "invalid"
      end

      expect_raises(Exception, /Invalid priority value: bad/) do
        order.priority_name = "bad"
      end
    end
  end

  describe "enum with different scenarios" do
    it "works with records having nil enum values" do
      order = OrderEnum.create(customer_name: "Test", status: 0)  # explicitly set to first enum value

      order.shipped?.should be_true
      order.status_name.should eq("shipped")
    end

    it "works with multiple enum attributes on same model" do
      order = OrderEnum.create(customer_name: "Test", status: 2, priority: 1)

      # Both enums work independently
      order.complete?.should be_true
      order.medium?.should be_true

      order.shipped?.should be_false
      order.high?.should be_false

      # Can set both independently
      order.cancelled!
      order.urgent!

      order.cancelled?.should be_true
      order.urgent?.should be_true
    end
  end
end

class OrderEnum < Takarik::Data::BaseModel
  table_name "orders_enum"
  column :customer_name, String

  timestamps

  # Define enum with multiple values
  enumerate :status, [:shipped, :being_packaged, :complete, :cancelled]

  # Define another enum to test multiple enums on same model
  enumerate :priority, [:low, :medium, :high, :urgent]
end
