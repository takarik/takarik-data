# Database Query Logging in Takarik::Data

Takarik::Data now includes comprehensive database query logging functionality. All database queries are automatically logged with execution time and parameters using a Rails ActiveRecord-compatible format.

## Logger Configuration

The logger is named "takarik-data" and can be configured using Crystal's standard logging system:

```crystal
require "takarik-data"

# Set up logging to see query output
Log.setup(:info)

# Or configure specific log level for takarik-data
Log.setup do |c|
  c.bind("takarik-data", :debug, Log::IOBackend.new)
end
```

## What Gets Logged

All database operations are automatically logged, including:

- **SELECT queries** (from QueryBuilder methods like `to_a`, `first`, `count`, etc.)
- **INSERT queries** (from model creation and saving)
- **UPDATE queries** (from model updates)
- **DELETE queries** (from model destruction)
- **DDL queries** (from migrations - CREATE TABLE, DROP TABLE, etc.)
- **Association queries** (from has_many, belongs_to, etc.)

## Log Format

The logging format follows Rails ActiveRecord conventions. Each logged query includes:
- Model name and operation type (Load, Create, Update, Destroy, Count, etc.)
- Execution time in milliseconds
- The SQL statement
- Parameters in Rails format: `[["param1", "value1"], ["param2", "value2"]]`

Example log output:
```
INFO -- takarik-data: User Load (2.4ms)  SELECT * FROM users WHERE name = ?  [["param1", "John Doe"]]
INFO -- takarik-data: User Create (1.2ms)  INSERT INTO users (name, email, created_at, updated_at) VALUES (?, ?, ?, ?)  [["param1", "Jane Doe"], ["param2", "jane@example.com"], ["param3", "2024-01-15T10:30:00Z"], ["param4", "2024-01-15T10:30:00Z"]]
INFO -- takarik-data: User Update (0.9ms)  UPDATE users SET name = ? WHERE id = ?  [["param1", "Jane Smith"], ["param2", "1"]]
INFO -- takarik-data: User Destroy (0.7ms)  DELETE FROM users WHERE id = ?  [["param1", "1"]]
INFO -- takarik-data: User Count (0.1ms)  SELECT COUNT(*) FROM users
INFO -- takarik-data: User Pluck (0.3ms)  SELECT name FROM users
INFO -- takarik-data: Schema (5.2ms)  CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT, email TEXT)
```

## Example Usage

```crystal
require "takarik-data"

# Enable logging
Log.setup(:info)

# Establish connection
Takarik::Data.establish_connection("sqlite3:./app.db")

# Define a model
class User < Takarik::Data::BaseModel
  column :name, String
  column :email, String
  timestamps
end

# All of these operations will be logged:
user = User.create(name: "John", email: "john@example.com")  # Logs: User Create (1.2ms)  INSERT INTO...
users = User.where(name: "John").to_a                       # Logs: User Load (0.8ms)  SELECT * FROM...
count = User.count                                           # Logs: User Count (0.1ms)  SELECT COUNT(*)...
user.name = "Jane"
user.save                                                    # Logs: User Update (0.9ms)  UPDATE users...
user.destroy                                                 # Logs: User Destroy (0.7ms)  DELETE FROM...
```

## Performance Monitoring

The execution time logging helps identify slow queries and performance bottlenecks in your application. You can use this information to:

- Identify N+1 query problems
- Find slow queries that need optimization
- Monitor database performance in production
- Debug query generation issues

## Customizing Log Output

You can customize the log output by configuring Crystal's logging system:

```crystal
# Log to a file
Log.setup do |c|
  c.bind("takarik-data", :info, Log::IOBackend.new(File.open("queries.log", "a")))
end

# Custom formatter
Log.setup do |c|
  backend = Log::IOBackend.new
  backend.formatter = Log::Formatter.new do |entry, io|
    io << "[#{entry.timestamp}] #{entry.severity}: #{entry.message}"
  end
  c.bind("takarik-data", :info, backend)
end
```

## Production Considerations

In production environments, consider:

- Setting appropriate log levels (`:info` or `:warn` instead of `:debug`)
- Rotating log files to prevent disk space issues
- Being mindful of sensitive data in query parameters
- Using structured logging for better analysis

The logging functionality adds minimal overhead to query execution and provides valuable insights into your application's database usage.