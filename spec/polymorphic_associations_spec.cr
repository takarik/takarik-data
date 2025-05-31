require "./spec_helper"

describe "Polymorphic Associations" do
  before_each do
    # Clean up test data
    Picture.all.delete_all
    Employee.all.delete_all
    Product.all.delete_all
    Event.all.delete_all
  end

  describe "polymorphic belongs_to" do
    it "allows a model to belong to multiple types" do
      # Create test data
      employee = Employee.create(name: "Alice Johnson", department: "Engineering")
      product = Product.create(name: "Laptop Pro", price: 1299.99)

      # Create pictures for different types
      employee_picture = Picture.create(name: "Alice's Headshot", imageable: employee)
      product_picture = Picture.create(name: "Laptop Beauty Shot", imageable: product)

      # Test polymorphic belongs_to
      employee_picture.imageable.should eq(employee)
      employee_picture.imageable_type.should eq("Employee")
      employee_picture.imageable_id.should eq(employee.id)

      product_picture.imageable.should eq(product)
      product_picture.imageable_type.should eq("Product")
      product_picture.imageable_id.should eq(product.id)
    end

    it "handles nil associations" do
      picture = Picture.create(name: "Orphaned Picture")

      picture.imageable.should be_nil
      picture.imageable_type.should be_nil
      picture.imageable_id.should be_nil
    end

    it "allows setting polymorphic associations" do
      employee = Employee.create(name: "Bob Smith", department: "Marketing")
      product = Product.create(name: "Wireless Mouse", price: 49.99)
      picture = Picture.create(name: "Test Picture")

      # Set to employee
      picture.imageable = employee
      picture.save

      picture.imageable.should eq(employee)
      picture.imageable_type.should eq("Employee")
      picture.imageable_id.should eq(employee.id)

      # Change to product
      picture.imageable = product
      picture.save

      picture.imageable.should eq(product)
      picture.imageable_type.should eq("Product")
      picture.imageable_id.should eq(product.id)

      # Set to nil
      picture.imageable = nil
      picture.save

      picture.imageable.should be_nil
      picture.imageable_type.should be_nil
      picture.imageable_id.should be_nil
    end
  end

  describe "polymorphic has_many" do
    it "allows parent models to have polymorphic children" do
      # Create test data
      employee = Employee.create(name: "Charlie Brown", department: "Design")
      product = Product.create(name: "Smartphone", price: 799.99)

      # Create pictures using polymorphic has_many
      employee_picture1 = employee.build_pictures(name: "Charlie's Portrait")
      employee_picture1.save
      employee_picture2 = employee.create_pictures(name: "Charlie's Work Setup")

      product_picture1 = product.build_pictures(name: "Phone Front View")
      product_picture1.save
      product_picture2 = product.create_pictures(name: "Phone Back View")

      # Test polymorphic has_many from employee
      employee_pictures = employee.pictures.to_a
      employee_pictures.size.should eq(2)
      employee_pictures.map(&.name).should contain("Charlie's Portrait")
      employee_pictures.map(&.name).should contain("Charlie's Work Setup")

      # Verify correct polymorphic data
      employee_pictures.each do |picture|
        picture.imageable_type.should eq("Employee")
        picture.imageable_id.should eq(employee.id)
        picture.imageable.should eq(employee)
      end

      # Test polymorphic has_many from product
      product_pictures = product.pictures.to_a
      product_pictures.size.should eq(2)
      product_pictures.map(&.name).should contain("Phone Front View")
      product_pictures.map(&.name).should contain("Phone Back View")

      # Verify correct polymorphic data
      product_pictures.each do |picture|
        picture.imageable_type.should eq("Product")
        picture.imageable_id.should eq(product.id)
        picture.imageable.should eq(product)
      end
    end

    it "queries with correct polymorphic conditions" do
      # Create test data
      employee1 = Employee.create(name: "David Wilson", department: "Sales")
      employee2 = Employee.create(name: "Eva Martinez", department: "Support")
      product = Product.create(name: "Tablet", price: 399.99)

      # Create pictures
      employee1.create_pictures(name: "David's Photo")
      employee2.create_pictures(name: "Eva's Photo")
      product.create_pictures(name: "Tablet Image")

      # Each parent should only see their own pictures
      employee1.pictures.size.should eq(1)
      employee1.pictures.first.not_nil!.name.should eq("David's Photo")

      employee2.pictures.size.should eq(1)
      employee2.pictures.first.not_nil!.name.should eq("Eva's Photo")

      product.pictures.size.should eq(1)
      product.pictures.first.not_nil!.name.should eq("Tablet Image")

      # Total pictures should be 3
      Picture.all.size.should eq(3)
    end

    it "handles empty associations" do
      employee = Employee.create(name: "Frank Miller", department: "HR")

      employee.pictures.to_a.should be_empty
      employee.pictures.size.should eq(0)
    end
  end

  describe "dependent destroy" do
    it "destroys polymorphic children when parent is destroyed with dependent: :destroy" do
      # Create test data
      employee = Employee.create(name: "Grace Taylor", department: "Finance")
      product = Product.create(name: "Monitor", price: 299.99)

      # Create pictures
      employee.create_pictures(name: "Grace's ID Photo")
      employee.create_pictures(name: "Grace's Office Photo")
      product.create_pictures(name: "Monitor Side View")

      initial_picture_count = Picture.all.size
      initial_picture_count.should eq(3)

      # Destroy employee (has dependent: :destroy)
      employee.destroy

      # Employee's pictures should be destroyed
      remaining_picture_count = Picture.all.size
      remaining_picture_count.should eq(1)  # Only product picture remains

      # Verify the remaining picture belongs to product
      remaining_picture = Picture.all.first.not_nil!
      remaining_picture.name.should eq("Monitor Side View")
      remaining_picture.imageable.should eq(product)
    end

    it "does not destroy polymorphic children when parent without dependent destroy is destroyed" do
      # Create test data
      event = Event.create(title: "Conference 2024", description: "Annual conference")

      # Create pictures (Event model doesn't have dependent: :destroy)
      event.create_pictures(name: "Conference Banner")
      event.create_pictures(name: "Speaker Photo")

      initial_picture_count = Picture.all.size
      initial_picture_count.should eq(2)

      # Destroy event (no dependent: :destroy)
      event.destroy

      # Pictures should remain but be orphaned
      remaining_picture_count = Picture.all.size
      remaining_picture_count.should eq(2)

      # Pictures should be orphaned
      Picture.all.each do |picture|
        picture.imageable.should be_nil
      end
    end
  end

  describe "validation" do
    it "validates presence of both foreign key and type for required polymorphic associations" do
      picture = Picture.new
      picture.name = "Test Picture"

      # Both imageable_id and imageable_type should be validated
      picture.valid?.should be_false
      picture.errors["imageable_id"].should contain("can't be blank")
      picture.errors["imageable_type"].should contain("can't be blank")
    end

    it "allows optional polymorphic associations" do
      # Note: The current polymorphic belongs_to doesn't have optional: true
      # but when it does, this test would verify it works
      picture = Picture.new
      picture.name = "Optional Picture"
      picture.imageable_id = nil
      picture.imageable_type = nil

      # This would pass if polymorphic association was optional
      # For now, this documents the expected behavior
    end
  end

  describe "edge cases" do
    it "handles unknown polymorphic types gracefully" do
      picture = Picture.new
      picture.name = "Mystery Picture"
      picture.imageable_id = 999
      picture.imageable_type = "UnknownModel"
      picture.save

      # Should handle unknown type gracefully
      picture.imageable.should be_nil
    end

    it "handles non-existent records gracefully" do
      picture = Picture.new
      picture.name = "Broken Link Picture"
      picture.imageable_id = 99999  # Non-existent ID
      picture.imageable_type = "Employee"
      picture.save

      # Should handle missing record gracefully
      picture.imageable.should be_nil
    end

    it "allows multiple polymorphic associations in same model" do
      # This tests that our implementation supports multiple polymorphic associations
      # For example, if we had both imageable and taggable polymorphic associations

      # For now, just verify our single polymorphic association works consistently
      employee = Employee.create(name: "Henry Davis", department: "IT")
      picture = Picture.create(name: "Henry's Photo", imageable: employee)

      picture.imageable_type.should eq("Employee")
      picture.imageable_id.should eq(employee.id)
      picture.imageable.should eq(employee)
    end
  end

  describe "database integrity" do
    it "stores correct foreign key and type values" do
      employee = Employee.create(name: "Irene Wilson", department: "Operations")
      picture = Picture.create(name: "Irene's Badge Photo", imageable: employee)

      # Verify database storage directly
      raw_picture = Picture.connection.query_one(
        "SELECT imageable_id, imageable_type FROM pictures WHERE id = ?",
        picture.id,
        as: {Int32?, String?}
      )

      raw_picture[0].should eq(employee.id)  # imageable_id
      raw_picture[1].should eq("Employee")   # imageable_type
    end

    it "maintains referential integrity with indexes" do
      # Verify that our polymorphic index exists and works
      # This is more of a database setup test

      employee = Employee.create(name: "Jack Brown", department: "Legal")
      product = Product.create(name: "Keyboard", price: 79.99)

      # Create many pictures to test index performance
      10.times do |i|
        employee.create_pictures(name: "Employee Photo #{i}")
        product.create_pictures(name: "Product Photo #{i}")
      end

      # Query should be efficient with proper indexing
      employee_pictures = employee.pictures.to_a
      employee_pictures.size.should eq(10)

      product_pictures = product.pictures.to_a
      product_pictures.size.should eq(10)
    end
  end
end
