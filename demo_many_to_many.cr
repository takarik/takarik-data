require "sqlite3"
require "./src/takarik-data"

# Establish database connection
Takarik::Data.establish_connection("sqlite3://./test.db")

# Demo models for has_many :through (with intermediate model)
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

# Demo models for has_and_belongs_to_many (direct many-to-many)
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

puts "🚀 Takarik::Data Many-to-Many Associations Demo"
puts "=" * 50

# Clean up existing data
Assembly.all.delete_all
Part.all.delete_all
Manifest.all.delete_all
Course.all.delete_all
Student.all.delete_all
Course.connection.exec("DELETE FROM courses_students")

puts "\n📦 HAS_MANY :THROUGH Demo (Assembly -> Manifests -> Parts)"
puts "-" * 50

# Create assemblies and parts
engine = Assembly.create(name: "V8 Engine")
transmission = Assembly.create(name: "Manual Transmission")

piston = Part.create(name: "Piston", part_number: "P001")
valve = Part.create(name: "Valve", part_number: "V001")
gear = Part.create(name: "Gear", part_number: "G001")
clutch = Part.create(name: "Clutch", part_number: "C001")

puts "✅ Created assemblies: #{Assembly.all.map(&.name).join(", ")}"
puts "✅ Created parts: #{Part.all.map(&.name).join(", ")}"

# Create intermediate associations with additional attributes
Manifest.create(assembly: engine, part: piston, quantity: 8)
Manifest.create(assembly: engine, part: valve, quantity: 16)
Manifest.create(assembly: transmission, part: gear, quantity: 5)
Manifest.create(assembly: transmission, part: clutch, quantity: 1)

puts "\n🔗 Association Results:"
puts "Engine parts: #{engine.parts.map(&.name).join(", ")} (#{engine.parts.size} types)"
puts "Transmission parts: #{transmission.parts.map(&.name).join(", ")} (#{transmission.parts.size} types)"
puts "Piston used in: #{piston.assemblies.map(&.name).join(", ")}"
puts "Gear used in: #{gear.assemblies.map(&.name).join(", ")}"

# Show intermediate model attributes
engine_manifests = engine.manifests.to_a
puts "\n📋 Manifest Details (with quantities):"
engine_manifests.each do |manifest|
  if part = manifest.part
    puts "  - #{part.name}: #{manifest.quantity} units"
  end
end

puts "\n🎓 HAS_AND_BELONGS_TO_MANY Demo (Course <-> Students)"
puts "-" * 50

# Create courses and students
math = Course.create(name: "Advanced Mathematics", description: "Calculus and Linear Algebra")
physics = Course.create(name: "Quantum Physics", description: "Introduction to Quantum Mechanics")
chemistry = Course.create(name: "Organic Chemistry", description: "Carbon-based Chemistry")

alice = Student.create(name: "Alice Johnson", email: "alice@university.edu")
bob = Student.create(name: "Bob Smith", email: "bob@university.edu")
charlie = Student.create(name: "Charlie Brown", email: "charlie@university.edu")

puts "✅ Created courses: #{Course.all.map(&.name).join(", ")}"
puts "✅ Created students: #{Student.all.map(&.name).join(", ")}"

# Add associations (students can take multiple courses)
math.add_student(alice)
math.add_student(bob)
math.add_student(charlie)

physics.add_student(alice)
physics.add_student(bob)

chemistry.add_student(alice)

puts "\n🔗 Enrollment Results:"
puts "Math students: #{math.students.map(&.name).join(", ")} (#{math.students.size} enrolled)"
puts "Physics students: #{physics.students.map(&.name).join(", ")} (#{physics.students.size} enrolled)"
puts "Chemistry students: #{chemistry.students.map(&.name).join(", ")} (#{chemistry.students.size} enrolled)"

puts "\n👨‍🎓 Student Schedules:"
[alice, bob, charlie].each do |student|
  courses = student.courses.map(&.name).join(", ")
  puts "  #{student.name}: #{courses} (#{student.courses.size} courses)"
end

puts "\n🔄 Association Management Demo:"
puts "-" * 30

# Test duplicate prevention
puts "Adding Alice to Math again (should prevent duplicate)..."
math.add_student(alice)
puts "Math students after duplicate attempt: #{math.students.size} (should still be 3)"

# Test removal
puts "\nRemoving Bob from Physics..."
physics.remove_student(bob)
puts "Physics students after removal: #{physics.students.map(&.name).join(", ")}"

# Test clearing all associations
puts "\nClearing all Chemistry enrollments..."
chemistry.clear_students
puts "Chemistry students after clearing: #{chemistry.students.size} (should be 0)"

puts "\n📊 Final Statistics:"
puts "-" * 20
puts "Total assemblies: #{Assembly.count}"
puts "Total parts: #{Part.count}"
puts "Total manifests: #{Manifest.count}"
puts "Total courses: #{Course.count}"
puts "Total students: #{Student.count}"
puts "Total enrollments: #{Course.connection.scalar("SELECT COUNT(*) FROM courses_students")}"

puts "\n✨ Many-to-Many Associations Demo Complete!"
puts "\nKey Features Demonstrated:"
puts "  ✅ has_many :through with intermediate model"
puts "  ✅ Additional attributes on join model (quantity)"
puts "  ✅ has_and_belongs_to_many direct associations"
puts "  ✅ Bidirectional associations"
puts "  ✅ Duplicate prevention"
puts "  ✅ Association removal and clearing"
puts "  ✅ Automatic join table naming"
