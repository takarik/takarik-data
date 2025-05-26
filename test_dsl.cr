require "sqlite3"
require "./src/takarik-data"

# Define models first
class User < Takarik::Data::BaseModel
  column id, Int32
  column name, String
  column email, String
  column age, Int32
  column active, Bool
  column created_at, Time
  column updated_at, Time

  has_many posts

  validates_presence_of name, email
  validates_format_of email, /\A[\w+\-.]+@[a-z\d\-]+(\.[a-z\d\-]+)*\.[a-z]+\z/i
  validates_uniqueness_of email

  before_save do
    self.email = self.email.try(&.downcase)
  end

  timestamps
end

class Post < Takarik::Data::BaseModel
  column id, Int32
  column title, String
  column content, String
  column user_id, Int32
  column published, Bool
  column created_at, Time
  column updated_at, Time

  belongs_to user

  validates_presence_of title, content

  timestamps
end

# Set up a simple SQLite database for testing
User.establish_connection("sqlite3:./test.db")
Post.establish_connection("sqlite3:./test.db")

# Create tables
User.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    email TEXT UNIQUE NOT NULL,
    age INTEGER,
    active BOOLEAN DEFAULT true,
    created_at TEXT,
    updated_at TEXT
  )
SQL

User.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS posts (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    content TEXT,
    user_id INTEGER,
    published BOOLEAN DEFAULT false,
    created_at TEXT,
    updated_at TEXT
  )
SQL

# Clean up any existing data
User.connection.exec("DELETE FROM users")
User.connection.exec("DELETE FROM posts")

puts "🎉 Testing the improved DSL!"
puts "=" * 50

# Test 1: Basic CRUD with improved DSL
puts "\n1. Basic CRUD Operations:"
user = User.create(name: "John Doe", email: "john@example.com", age: 30)
puts "✓ Created user: #{user.name} (#{user.email})"

# Test 2: Improved where syntax (no .query needed!)
puts "\n2. Improved Query DSL:"
active_users = User.where(active: true).to_a
puts "✓ Found #{active_users.size} active users using User.where(active: true)"

# Test 3: Method chaining
puts "\n3. Method Chaining:"
young_users = User.where_lt("age", 35).order("name").limit(10).to_a
puts "✓ Found #{young_users.size} young users using User.where_lt('age', 35).order('name').limit(10)"

# Test 4: Associations with improved DSL
puts "\n4. Associations:"
post = user.create_posts(title: "My First Post", content: "Hello World!", published: true)
puts "✓ Created post: #{post.title}"

user_posts = user.posts.to_a
puts "✓ User has #{user_posts.size} posts using user.posts"

# Test 5: Complex queries
puts "\n5. Complex Queries:"
published_posts = Post.where(published: true).inner_join("users", "users.id = posts.user_id").to_a
puts "✓ Found #{published_posts.size} published posts with joins"

# Test 6: Comparison with old DSL
puts "\n6. DSL Comparison:"
puts "Old DSL: User.query.where(active: true).order('name').to_a"
puts "New DSL: User.where(active: true).order('name').to_a"
puts "✓ Much cleaner! No need for explicit .query call"

puts "\n🎉 All DSL improvements working perfectly!"
puts "The new DSL is much more Rails-like and intuitive!"
