require "./spec_helper"

# Test models for has_many :through (with intermediate model)
class Assembly < Takarik::Data::BaseModel
  table_name "assemblies"

  primary_key :id, Int32
  column :name, String
  column :created_at, Time
  column :updated_at, Time

  has_many :manifests, dependent: :destroy
  has_many :parts, through: :manifests

  timestamps
end

class Manifest < Takarik::Data::BaseModel
  table_name "manifests"

  primary_key :id, Int32
  column :assembly_id, Int32
  column :part_id, Int32
  column :quantity, Int32
  column :created_at, Time
  column :updated_at, Time

  belongs_to :assembly
  belongs_to :part

  timestamps
end

class Part < Takarik::Data::BaseModel
  table_name "parts"

  primary_key :id, Int32
  column :name, String
  column :part_number, String
  column :created_at, Time
  column :updated_at, Time

  has_many :manifests, dependent: :destroy
  has_many :assemblies, through: :manifests

  timestamps
end

# Test models for has_and_belongs_to_many (direct many-to-many)
class Course < Takarik::Data::BaseModel
  table_name "courses"

  primary_key :id, Int32
  column :name, String
  column :description, String
  column :created_at, Time
  column :updated_at, Time

  has_and_belongs_to_many :students

  timestamps
end

class Student < Takarik::Data::BaseModel
  table_name "students"

  primary_key :id, Int32
  column :name, String
  column :email, String
  column :created_at, Time
  column :updated_at, Time

  has_and_belongs_to_many :courses

  timestamps
end

describe "Many-to-Many Associations" do
  before_each do
    # Clean up test data
    Assembly.delete_all
    Part.delete_all
    Manifest.delete_all
    Course.delete_all
    Student.delete_all

    # Clean join table
    begin
      Course.connection.exec("DELETE FROM courses_students")
    rescue
      # Table might not exist yet
    end
  end

  describe "has_many :through" do
    it "creates associations through intermediate model" do
      # Create test data
      assembly = Assembly.create(name: "Car Engine")
      part1 = Part.create(name: "Piston", part_number: "P001")
      part2 = Part.create(name: "Valve", part_number: "V001")

      # Create intermediate associations
      manifest1 = Manifest.create(assembly: assembly, part: part1, quantity: 4)
      manifest2 = Manifest.create(assembly: assembly, part: part2, quantity: 8)

      # Test has_many :through from assembly side
      assembly_parts = assembly.parts.to_a
      assembly_parts.size.should eq(2)
      assembly_parts.map(&.name).should contain("Piston")
      assembly_parts.map(&.name).should contain("Valve")

      # Test has_many :through from part side
      part1_assemblies = part1.assemblies.to_a
      part1_assemblies.size.should eq(1)
      part1_assemblies.first.name.should eq("Car Engine")
    end

    it "allows additional attributes on join model" do
      assembly = Assembly.create(name: "Transmission")
      part = Part.create(name: "Gear", part_number: "G001")

      # The intermediate model can have its own attributes
      manifest = Manifest.create(assembly: assembly, part: part, quantity: 12)

      manifest.quantity.should eq(12)
      manifest.assembly.should eq(assembly)
      manifest.part.should eq(part)
    end
  end

  describe "has_and_belongs_to_many" do
    it "creates direct many-to-many associations" do
      # Create test data
      course1 = Course.create(name: "Mathematics", description: "Advanced Math")
      course2 = Course.create(name: "Physics", description: "Basic Physics")
      student1 = Student.create(name: "Alice", email: "alice@example.com")
      student2 = Student.create(name: "Bob", email: "bob@example.com")

      # Add associations
      course1.add_student(student1)
      course1.add_student(student2)
      course2.add_student(student1)

      # Test associations from course side
      math_students = course1.students
      math_students.size.should eq(2)
      math_students.map(&.name).should contain("Alice")
      math_students.map(&.name).should contain("Bob")

      physics_students = course2.students
      physics_students.size.should eq(1)
      physics_students.first.name.should eq("Alice")

      # Test associations from student side
      alice_courses = student1.courses
      alice_courses.size.should eq(2)
      alice_courses.map(&.name).should contain("Mathematics")
      alice_courses.map(&.name).should contain("Physics")

      bob_courses = student2.courses
      bob_courses.size.should eq(1)
      bob_courses.first.name.should eq("Mathematics")
    end

    it "prevents duplicate associations" do
      course = Course.create(name: "Chemistry", description: "Basic Chemistry")
      student = Student.create(name: "Charlie", email: "charlie@example.com")

      # Add same association twice
      course.add_student(student)
      course.add_student(student)

      # Should only have one association
      course.students.size.should eq(1)
    end

    it "allows removing associations" do
      course = Course.create(name: "Biology", description: "Basic Biology")
      student1 = Student.create(name: "David", email: "david@example.com")
      student2 = Student.create(name: "Eve", email: "eve@example.com")

      # Add associations
      course.add_student(student1)
      course.add_student(student2)
      course.students.size.should eq(2)

      # Remove one association
      course.remove_student(student1)
      course.students.size.should eq(1)
      course.students.first.name.should eq("Eve")
    end

    it "allows clearing all associations" do
      course = Course.create(name: "History", description: "World History")
      student1 = Student.create(name: "Frank", email: "frank@example.com")
      student2 = Student.create(name: "Grace", email: "grace@example.com")

      # Add associations
      course.add_student(student1)
      course.add_student(student2)
      course.students.size.should eq(2)

      # Clear all associations
      course.clear_students
      course.students.size.should eq(0)
    end
  end

  describe "join table naming" do
    it "generates correct join table names" do
      # The join table should be named alphabetically: courses_students
      course = Course.create(name: "Test Course", description: "Test")
      student = Student.create(name: "Test Student", email: "test@example.com")

      course.add_student(student)

      # Verify the join table was used correctly
      count = Course.connection.scalar("SELECT COUNT(*) FROM courses_students").as(Int64)
      count.should eq(1)
    end
  end
end
