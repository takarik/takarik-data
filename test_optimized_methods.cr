require "sqlite3"
require "./src/takarik-data"

# Simple demo models
class User < Takarik::Data::BaseModel
  column id, Int32
  column name, String
  column email, String
  column age, Int32
  column active, Bool
end

# Set up database
User.establish_connection("sqlite3:./demo.db")

# Create tables
User.connection.exec <<-SQL
  CREATE TABLE IF NOT EXISTS users (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT,
    email TEXT,
    age INTEGER,
    active BOOLEAN DEFAULT true
  )
SQL

# Clean up and create test data
User.connection.exec("DELETE FROM users")

# Create a larger dataset to show performance difference
1000.times do |i|
  User.create(
    name: "User#{i}",
    email: "user#{i}@example.com",
    age: 20 + (i % 50),
    active: i % 3 == 0
  )
end

puts "🚀 Testing Optimized vs Delegated Methods"
puts "=" * 50

puts "\n📊 Created 1000 users for testing"

puts "\n🔍 SQL-Optimized Methods (Direct QueryBuilder Implementation):"
puts "-" * 60

# Test empty? - uses SQL COUNT(*)
start_time = Time.monotonic
result1 = User.where(name: "NonExistent").empty?
time1 = Time.monotonic - start_time
puts "✓ User.where(name: 'NonExistent').empty? = #{result1}"
puts "  ↳ Uses SQL: SELECT COUNT(*) FROM users WHERE name = 'NonExistent'"
puts "  ↳ Time: #{time1.total_milliseconds.round(2)}ms"

# Test any? - uses SQL COUNT(*)
start_time = Time.monotonic
result2 = User.where(active: true).any?
time2 = Time.monotonic - start_time
puts "✓ User.where(active: true).any? = #{result2}"
puts "  ↳ Uses SQL: SELECT COUNT(*) FROM users WHERE active = true"
puts "  ↳ Time: #{time2.total_milliseconds.round(2)}ms"

puts "\n🔄 Array-Delegated Methods (via method_missing):"
puts "-" * 50

# Test size - delegates to to_a.size
start_time = Time.monotonic
result3 = User.where(active: true).size
time3 = Time.monotonic - start_time
puts "✓ User.where(active: true).size = #{result3}"
puts "  ↳ Fetches all records then calls .size on array"
puts "  ↳ Time: #{time3.total_milliseconds.round(2)}ms"

# Test includes? - delegates to to_a.includes?
user = User.first!
start_time = Time.monotonic
result4 = User.where(active: true).includes?(user)
time4 = Time.monotonic - start_time
puts "✓ User.where(active: true).includes?(user) = #{result4}"
puts "  ↳ Fetches all records then calls .includes? on array"
puts "  ↳ Time: #{time4.total_milliseconds.round(2)}ms"

puts "\n⚡ Performance Comparison:"
puts "-" * 25
puts "SQL-optimized methods are much faster because they:"
puts "- Use database-level operations (COUNT, etc.)"
puts "- Don't fetch unnecessary data"
puts "- Leverage database indexes"

puts ""
puts "Array-delegated methods:"
puts "- Fetch all matching records into memory"
puts "- Then perform the operation on the array"
puts "- Useful for complex operations not available in SQL"

puts "\n🎯 Best of Both Worlds:"
puts "-" * 25
puts "QueryBuilder now provides:"
puts "- SQL-optimized methods for common operations (empty?, any?, count)"
puts "- Array delegation for complex operations (includes?, sort_by, etc.)"
puts "- Seamless method chaining between both types"
puts "- Clean, Rails-like DSL without performance penalties"
