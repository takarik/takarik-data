require "spec"
require "../src/takarik-data"
require "sqlite3"

# Set up test database
Takarik::Data::BaseModel.establish_connection("sqlite3://./test.db")

# Create test tables
Takarik::Data::BaseModel.connection.exec <<-SQL
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

Takarik::Data::BaseModel.connection.exec <<-SQL
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

Takarik::Data::BaseModel.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS comments (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    content TEXT,
    post_id INTEGER,
    user_id INTEGER,
    created_at DATETIME,
    updated_at DATETIME
  )
SQL

# Test models
class User < Takarik::Data::BaseModel
  table_name "users"

  primary_key id, Int32
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

  primary_key id, Int32
  column title, String
  column content, String
  column user_id, Int32
  column published, Bool
  column created_at, Time
  column updated_at, Time

  belongs_to user
  has_many comments, dependent: :destroy

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

  primary_key id, Int32
  column content, String
  column post_id, Int32
  column user_id, Int32
  column created_at, Time
  column updated_at, Time

  belongs_to post
  belongs_to user

  validates_presence_of :content, :post_id, :user_id
  validates_length_of :content, minimum: 1, maximum: 500

  timestamps
end

# Establish connections for all model classes
User.establish_connection("sqlite3://./test.db")
Post.establish_connection("sqlite3://./test.db")
Comment.establish_connection("sqlite3://./test.db")

# Clean up before each test
Spec.before_each do
  Takarik::Data::BaseModel.connection.exec("DELETE FROM comments")
  Takarik::Data::BaseModel.connection.exec("DELETE FROM posts")
  Takarik::Data::BaseModel.connection.exec("DELETE FROM users")
end
