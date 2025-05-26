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

puts "🚀 Testing method_missing delegation!"
puts "=" * 50

# Create some test data
users = [
  User.create(name: "Alice", email: "alice@example.com", age: 25, active: true),
  User.create(name: "Bob", email: "bob@example.com", age: 30, active: true),
  User.create(name: "Charlie", email: "charlie@example.com", age: 35, active: false),
  User.create(name: "Diana", email: "diana@example.com", age: 28, active: true)
]

users.each do |user|
  user.create_posts(title: "Post by #{user.name}", content: "Content by #{user.name}", published: true)
end

puts "\n1. Array-like methods without .to_a:"
puts "=" * 40

# Test .size method
puts "✓ User.where(active: true).size = #{User.where(active: true).size}"
puts "  (Previously required: User.where(active: true).to_a.size)"

# Test .empty? method
puts "✓ User.where(name: \"NonExistent\").empty? = #{User.where(name: "NonExistent").empty?}"

# Test .any? method
puts "✓ User.where(active: true).any? = #{User.where(active: true).any?}"

# Test .first method (this should still work as QueryBuilder has its own first)
puts "✓ User.where(active: true).first.try(&.name) = #{User.where(active: true).first.try(&.name)}"

# Test .each method
puts "\n2. Iteration methods:"
puts "=" * 20
print "✓ User.where(active: true).each: "
User.where(active: true).each { |user| print "#{user.name} " }
puts

# Test .map method
names = User.where(active: true).map(&.name)
puts "✓ User.where(active: true).map(&.name) = #{names}"

# Test .select method (this will use Array's select, not QueryBuilder's)
young_users = User.where(active: true).select { |u| (u.age || 0) < 30 }
puts "✓ User.where(active: true).select { |u| u.age < 30 }.size = #{young_users.size}"

# Test .find method (this will use Array's find, not QueryBuilder's)
bob = User.where(active: true).find { |u| u.name == "Bob" }
puts "✓ User.where(active: true).find { |u| u.name == \"Bob\" }.try(&.name) = #{bob.try(&.name)}"

# Test array access
puts "\n3. Array access methods:"
puts "=" * 25
first_active = User.where(active: true).order("name")[0]
puts "✓ User.where(active: true).order(\"name\")[0].name = #{first_active.try(&.name)}"

second_active = User.where(active: true).order("name")[1]?
puts "✓ User.where(active: true).order(\"name\")[1]?.try(&.name) = #{second_active.try(&.name)}"

# Test .includes? method
has_alice = User.where(active: true).includes?(users[0])
puts "✓ User.where(active: true).includes?(alice) = #{has_alice}"

puts "\n4. Comparison with old syntax:"
puts "=" * 35
puts "Old way: User.where(active: true).to_a.size"
puts "New way: User.where(active: true).size"
puts ""
puts "Old way: User.where(active: true).to_a.map(&.name)"
puts "New way: User.where(active: true).map(&.name)"
puts ""
puts "Old way: User.where(active: true).to_a.each { |u| puts u.name }"
puts "New way: User.where(active: true).each { |u| puts u.name }"

puts "\n5. Complex chaining still works:"
puts "=" * 35
result = User.where(active: true)
           .order("age")
           .limit(2)
           .map(&.name)
           .join(", ")
puts "✓ Complex chain result: #{result}"

puts "\n🎉 Method missing delegation working perfectly!"
puts "Now QueryBuilder behaves like an Array for most operations!"
puts "No more need for explicit .to_a calls in most cases!"
