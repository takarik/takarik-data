require "./spec_helper"

describe "Dynamic Finders (Single Column)" do
  before_each do
    # Set up test table
    Takarik::Data::BaseModel.connection.exec "DROP TABLE IF EXISTS simple_customers"
    Takarik::Data::BaseModel.connection.exec <<-SQL
      CREATE TABLE simple_customers (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        first_name TEXT,
        last_name TEXT,
        email TEXT,
        age INTEGER,
        active INTEGER DEFAULT 1,
        created_at TEXT,
        updated_at TEXT
      )
    SQL

    # Create test data
    SimpleCustomer.create(first_name: "Ryan", last_name: "Smith", email: "ryan@example.com", age: 30, active: true)
    SimpleCustomer.create(first_name: "John", last_name: "Doe", email: "john@example.com", age: 25, active: true)
    SimpleCustomer.create(first_name: "Jane", last_name: "Smith", email: "jane@example.com", age: 28, active: false)
  end

  describe "single column dynamic finders" do
    it "finds by first_name" do
      customer = SimpleCustomer.find_by_first_name("Ryan")
      customer.should_not be_nil
      customer.not_nil!.first_name.should eq("Ryan")
      customer.not_nil!.last_name.should eq("Smith")
    end

    it "finds by email" do
      customer = SimpleCustomer.find_by_email("john@example.com")
      customer.should_not be_nil
      customer.not_nil!.first_name.should eq("John")
      customer.not_nil!.email.should eq("john@example.com")
    end

    it "finds by age" do
      customer = SimpleCustomer.find_by_age(25)
      customer.should_not be_nil
      customer.not_nil!.first_name.should eq("John")
      customer.not_nil!.age.should eq(25)
    end

    it "finds by boolean field (active)" do
      customer = SimpleCustomer.find_by_active(false)
      customer.should_not be_nil
      customer.not_nil!.first_name.should eq("Jane")
      customer.not_nil!.active.should eq(false)
    end

    it "returns nil when not found" do
      customer = SimpleCustomer.find_by_first_name("NonExistent")
      customer.should be_nil
    end
  end

  describe "single column dynamic finders with bang (!)" do
    it "finds by first_name!" do
      customer = SimpleCustomer.find_by_first_name!("Ryan")
      customer.first_name.should eq("Ryan")
      customer.last_name.should eq("Smith")
    end

    it "finds by email!" do
      customer = SimpleCustomer.find_by_email!("john@example.com")
      customer.first_name.should eq("John")
      customer.email.should eq("john@example.com")
    end

    it "raises RecordNotFound when not found" do
      expect_raises(Takarik::Data::RecordNotFound, "Couldn't find SimpleCustomer") do
        SimpleCustomer.find_by_first_name!("NonExistent")
      end
    end

    it "raises RecordNotFound for non-existent email" do
      expect_raises(Takarik::Data::RecordNotFound) do
        SimpleCustomer.find_by_email!("nonexistent@example.com")
      end
    end
  end

  describe "comparison with regular find_by" do
    it "generates equivalent results to find_by" do
      customer1 = SimpleCustomer.find_by_first_name("Ryan")
      customer2 = SimpleCustomer.find_by(first_name: "Ryan")

      customer1.should_not be_nil
      customer2.should_not be_nil
      customer1.not_nil!.id.should eq(customer2.not_nil!.id)
    end

    it "generates equivalent results for bang methods" do
      customer1 = SimpleCustomer.find_by_email!("john@example.com")
      customer2 = SimpleCustomer.find_by!(email: "john@example.com")

      customer1.id.should eq(customer2.id)
      customer1.email.should eq(customer2.email)
    end
  end

  describe "multi-column usage (recommended approach)" do
    it "should use find_by with hash for multiple columns" do
      # Multi-column dynamic finders are not implemented for simplicity
      # Users should use the existing find_by method with hash syntax
      customer = SimpleCustomer.find_by(first_name: "Ryan", last_name: "Smith")
      customer.should_not be_nil
      customer.not_nil!.first_name.should eq("Ryan")
      customer.not_nil!.last_name.should eq("Smith")
      customer.not_nil!.email.should eq("ryan@example.com")
    end

    it "should use find_by! with hash for multiple columns with exception" do
      customer = SimpleCustomer.find_by!(first_name: "Ryan", last_name: "Smith")
      customer.first_name.should eq("Ryan")
      customer.last_name.should eq("Smith")
      customer.email.should eq("ryan@example.com")
    end

    it "should raise RecordNotFound for non-matching combination" do
      expect_raises(Takarik::Data::RecordNotFound) do
        SimpleCustomer.find_by!(first_name: "Ryan", last_name: "NonExistent")
      end
    end
  end
end

class SimpleCustomer < Takarik::Data::BaseModel
  table_name "simple_customers"
  column :first_name, String
  column :last_name, String
  column :email, String
  column :age, Int32
  column :active, Bool

  timestamps
end
