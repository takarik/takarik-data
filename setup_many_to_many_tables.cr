require "sqlite3"
require "./src/takarik-data"

# Establish database connection
Takarik::Data.establish_connection("sqlite3://./test.db")

# Database setup for many-to-many association tests
connection = Takarik::Data::BaseModel.connection

puts "Setting up many-to-many test tables..."

# Drop existing tables if they exist
connection.exec("DROP TABLE IF EXISTS courses_students")
connection.exec("DROP TABLE IF EXISTS manifests")
connection.exec("DROP TABLE IF EXISTS assemblies")
connection.exec("DROP TABLE IF EXISTS parts")
connection.exec("DROP TABLE IF EXISTS courses")
connection.exec("DROP TABLE IF EXISTS students")

# Create tables for has_many :through example
connection.exec(<<-SQL
  CREATE TABLE assemblies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )
SQL
)

connection.exec(<<-SQL
  CREATE TABLE parts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    part_number TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )
SQL
)

connection.exec(<<-SQL
  CREATE TABLE manifests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    assembly_id INTEGER NOT NULL,
    part_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL DEFAULT 1,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (assembly_id) REFERENCES assemblies(id),
    FOREIGN KEY (part_id) REFERENCES parts(id)
  )
SQL
)

# Create tables for has_and_belongs_to_many example
connection.exec(<<-SQL
  CREATE TABLE courses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )
SQL
)

connection.exec(<<-SQL
  CREATE TABLE students (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL,
    created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
    updated_at DATETIME DEFAULT CURRENT_TIMESTAMP
  )
SQL
)

# Create join table for has_and_belongs_to_many (no primary key, just foreign keys)
connection.exec(<<-SQL
  CREATE TABLE courses_students (
    course_id INTEGER NOT NULL,
    student_id INTEGER NOT NULL,
    PRIMARY KEY (course_id, student_id),
    FOREIGN KEY (course_id) REFERENCES courses(id),
    FOREIGN KEY (student_id) REFERENCES students(id)
  )
SQL
)

puts "Many-to-many test tables created successfully!"
puts ""
puts "Tables created:"
puts "- assemblies (for has_many :through)"
puts "- parts (for has_many :through)"
puts "- manifests (intermediate table for has_many :through)"
puts "- courses (for has_and_belongs_to_many)"
puts "- students (for has_and_belongs_to_many)"
puts "- courses_students (join table for has_and_belongs_to_many)"
