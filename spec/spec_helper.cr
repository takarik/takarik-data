require "spec"
require "../src/takarik-data"
require "sqlite3"

# Set up test database
Takarik::Data.establish_connection("sqlite3://./test.db")

# Create test tables
Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255),
    email VARCHAR(255),
    age INTEGER,
    active BOOLEAN DEFAULT 1,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(255),
    content TEXT,
    user_id INTEGER,
    published BOOLEAN DEFAULT 0,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content TEXT,
    post_id INTEGER,
    user_id INTEGER,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

# Create tables for dependent association testing
Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS department_delete_alls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS employee_delete_alls (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255),
    department_id INTEGER,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS category_nullifies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS product_nullifies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255),
    category_id INTEGER,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS company_independents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS employee_independents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255),
    company_id INTEGER,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS customer_dependents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS order_dependents (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    total REAL,
    customer_id INTEGER,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS author_strings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS book_strings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(255),
    author_id INTEGER,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS publisher_strings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS magazine_strings (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(255),
    publisher_id INTEGER,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS library_symbols (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS book_symbols (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(255),
    library_id INTEGER,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS projects (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS tasks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title VARCHAR(255),
    project_id INTEGER,
    assignee_id INTEGER,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS users_optional (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name VARCHAR(255),
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

# Create tables for many-to-many associations testing
# Tables for has_many :through tests
Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS assemblies (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS parts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    part_number TEXT NOT NULL UNIQUE,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS manifests (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    assembly_id INTEGER NOT NULL,
    part_id INTEGER NOT NULL,
    quantity INTEGER NOT NULL,
    created_at DATETIME,
    updated_at DATETIME,
    FOREIGN KEY (assembly_id) REFERENCES assemblies (id),
    FOREIGN KEY (part_id) REFERENCES parts (id)
  )
SQL

# Tables for has_and_belongs_to_many tests
Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS courses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    description TEXT,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS students (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT NOT NULL UNIQUE,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS courses_students (
    course_id INTEGER NOT NULL,
    student_id INTEGER NOT NULL,
    PRIMARY KEY (course_id, student_id),
    FOREIGN KEY (course_id) REFERENCES courses (id),
    FOREIGN KEY (student_id) REFERENCES students (id)
  )
SQL

# Tables for polymorphic associations testing
Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS pictures (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    imageable_id INTEGER,
    imageable_type TEXT,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS employees (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    department TEXT,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS products (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    price DECIMAL(10,2),
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

Takarik::Data.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS events (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    description TEXT,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

# Create polymorphic index for better query performance
Takarik::Data.connection.exec <<-SQL
  CREATE INDEX IF NOT EXISTS idx_pictures_polymorphic ON pictures (imageable_type, imageable_id)
SQL

# Test models
class User < Takarik::Data::BaseModel
  table_name "users"

  primary_key :id, Int32
  column name, String
  column email, String
  column age, Int32
  column active, Bool
  column created_at, Time
  column updated_at, Time

  has_many posts, dependent: :destroy
  has_many comments

  validates_presence_of :name, :email
  validates_uniqueness_of :email
  validates_length_of :name, minimum: 2, maximum: 50
  validates_format_of :email, with: /\A[\w+\-.]+@[a-z\d\-]+(\.[a-z\d\-]+)*\.[a-z]+\z/i
  validates_numericality_of :age, greater_than: 0, less_than: 150

  scope :active do
    where(active: true)
  end

  scope :adults do
    where("age >=", 18)
  end

  before_save do
    self.email = self.email.try(&.downcase)
  end

  timestamps
end

class Post < Takarik::Data::BaseModel
  table_name "posts"

  primary_key :id, Int32
  column :title, String
  column :content, String
  column :user_id, Int32
  column :published, Bool
  column :created_at, Time
  column :updated_at, Time

  belongs_to :user, optional: true
  has_many :comments, dependent: :destroy

  validates_presence_of :title, :content, :user_id
  validates_length_of :title, minimum: 5, maximum: 100

  scope :published do
    where(published: true)
  end

  scope :recent do
    order("created_at", "DESC").limit(10)
  end

  timestamps
end

class Comment < Takarik::Data::BaseModel
  table_name "comments"

  primary_key :id, Int32
  column :content, String
  column :post_id, Int32
  column :user_id, Int32
  column :created_at, Time
  column :updated_at, Time

  belongs_to :post, optional: true
  belongs_to :user, optional: true

  validates_presence_of :content, :post_id, :user_id
  validates_length_of :content, minimum: 1, maximum: 500

  timestamps
end

# Test models for dependent association testing
class DepartmentDeleteAll < Takarik::Data::BaseModel
  column name, String
  has_many employees, class_name: EmployeeDeleteAll, foreign_key: :department_id, dependent: :delete_all
  timestamps
end

class EmployeeDeleteAll < Takarik::Data::BaseModel
  column name, String
  column department_id, Int64
  belongs_to department, class_name: DepartmentDeleteAll, foreign_key: :department_id, optional: true
  timestamps
end

class CategoryNullify < Takarik::Data::BaseModel
  column name, String
  has_many products, class_name: "ProductNullify", foreign_key: :category_id, dependent: :nullify
  timestamps
end

class ProductNullify < Takarik::Data::BaseModel
  column name, String
  column category_id, Int64?
  belongs_to category, class_name: :CategoryNullify, foreign_key: :category_id, optional: true
  timestamps
end

class CompanyIndependent < Takarik::Data::BaseModel
  column name, String
  has_many employees, class_name: EmployeeIndependent, foreign_key: "company_id"  # No dependent option
  timestamps
end

class EmployeeIndependent < Takarik::Data::BaseModel
  column name, String
  column company_id, Int64
  belongs_to company, class_name: CompanyIndependent, foreign_key: "company_id", optional: true
  timestamps
end

class OrderDependent < Takarik::Data::BaseModel
  column total, Float64
  column customer_id, Int64
  belongs_to customer, class_name: CustomerDependent, foreign_key: "customer_id", dependent: :destroy, optional: true  # Should be ignored
  timestamps
end

class CustomerDependent < Takarik::Data::BaseModel
  column name, String
  has_many orders, class_name: OrderDependent, foreign_key: "customer_id"
  timestamps
end

# Test models using string class names instead of class references
class AuthorString < Takarik::Data::BaseModel
  column name, String
  has_many books, class_name: "BookString", foreign_key: "author_id", dependent: :destroy
  timestamps
end

class BookString < Takarik::Data::BaseModel
  column title, String
  column author_id, Int64
  belongs_to author, class_name: "AuthorString", foreign_key: "author_id", optional: true
  timestamps
end

class PublisherString < Takarik::Data::BaseModel
  column name, String
  has_many magazines, class_name: "MagazineString", foreign_key: "publisher_id", dependent: :nullify
  timestamps
end

class MagazineString < Takarik::Data::BaseModel
  column title, String
  column publisher_id, Int64?
  belongs_to publisher, class_name: "PublisherString", foreign_key: "publisher_id", optional: true
  timestamps
end

# New test models showcasing different parameter styles
class LibrarySymbol < Takarik::Data::BaseModel
  column name, String
  has_many books, class_name: :BookSymbol, foreign_key: :library_id, primary_key: :id, dependent: :destroy
  timestamps
end

class BookSymbol < Takarik::Data::BaseModel
  column title, String
  column library_id, Int64
  belongs_to library, class_name: :LibrarySymbol, foreign_key: :library_id, primary_key: :id, optional: true
  timestamps
end

# Test models for optional associations
class Project < Takarik::Data::BaseModel
  column name, String
  has_many tasks, class_name: Task, foreign_key: :project_id
  timestamps
end

class UserOptional < Takarik::Data::BaseModel
  table_name "users_optional"
  column name, String
  has_many assigned_tasks, class_name: Task, foreign_key: :assignee_id
  timestamps
end

class Task < Takarik::Data::BaseModel
  column title, String
  column project_id, Int64
  column assignee_id, Int64?

  # Required association - project_id cannot be null
  belongs_to project, class_name: Project, foreign_key: :project_id

  # Optional association - assignee_id can be null
  belongs_to assignee, class_name: UserOptional, foreign_key: :assignee_id, optional: true

  timestamps
end

# Clean up before each test
Spec.before_each do
  # Clean up test tables in correct order (child tables first)
  # Clean up many-to-many tables first
  Takarik::Data.connection.exec("DELETE FROM courses_students")
  Takarik::Data.connection.exec("DELETE FROM manifests")
  Takarik::Data.connection.exec("DELETE FROM courses")
  Takarik::Data.connection.exec("DELETE FROM students")
  Takarik::Data.connection.exec("DELETE FROM assemblies")
  Takarik::Data.connection.exec("DELETE FROM parts")

  # Clean up polymorphic tables
  Takarik::Data.connection.exec("DELETE FROM pictures")
  Takarik::Data.connection.exec("DELETE FROM employees")
  Takarik::Data.connection.exec("DELETE FROM products")
  Takarik::Data.connection.exec("DELETE FROM events")

  # Clean up existing test tables
  Takarik::Data.connection.exec("DELETE FROM tasks")
  Takarik::Data.connection.exec("DELETE FROM users_optional")
  Takarik::Data.connection.exec("DELETE FROM projects")
  Takarik::Data.connection.exec("DELETE FROM book_symbols")
  Takarik::Data.connection.exec("DELETE FROM library_symbols")
  Takarik::Data.connection.exec("DELETE FROM magazine_strings")
  Takarik::Data.connection.exec("DELETE FROM publisher_strings")
  Takarik::Data.connection.exec("DELETE FROM book_strings")
  Takarik::Data.connection.exec("DELETE FROM author_strings")
  Takarik::Data.connection.exec("DELETE FROM order_dependents")
  Takarik::Data.connection.exec("DELETE FROM customer_dependents")
  Takarik::Data.connection.exec("DELETE FROM employee_independents")
  Takarik::Data.connection.exec("DELETE FROM company_independents")
  Takarik::Data.connection.exec("DELETE FROM product_nullifies")
  Takarik::Data.connection.exec("DELETE FROM category_nullifies")
  Takarik::Data.connection.exec("DELETE FROM employee_delete_alls")
  Takarik::Data.connection.exec("DELETE FROM department_delete_alls")
  Takarik::Data.connection.exec("DELETE FROM comments")
  Takarik::Data.connection.exec("DELETE FROM posts")
  Takarik::Data.connection.exec("DELETE FROM users")
end

# Test models for polymorphic associations
class Picture < Takarik::Data::BaseModel
  table_name "pictures"

  primary_key :id, Int32
  column :name, String
  column :imageable_id, Int32
  column :imageable_type, String
  timestamps

  # Polymorphic belongs_to association
  belongs_to :imageable, polymorphic: true
end

class Employee < Takarik::Data::BaseModel
  table_name "employees"

  primary_key :id, Int32
  column :name, String
  column :department, String
  timestamps

  # Polymorphic has_many association
  has_many :pictures, as: :imageable, dependent: :destroy
end

class Product < Takarik::Data::BaseModel
  table_name "products"

  primary_key :id, Int32
  column :name, String
  column :price, Float64
  timestamps

  # Polymorphic has_many association
  has_many :pictures, as: :imageable, dependent: :destroy
end

class Event < Takarik::Data::BaseModel
  table_name "events"

  primary_key :id, Int32
  column :title, String
  column :description, String
  timestamps

  # Polymorphic has_many association (no dependent destroy)
  has_many :pictures, as: :imageable
end
