# Takarik Data - Crystal ORM Library

A powerful, ActiveRecord-inspired ORM library for Crystal language that provides a clean and intuitive interface for database operations.

## Features

- **ActiveRecord-like API**: Familiar interface for Ruby developers
- **Type Safety**: Full Crystal type system integration
- **Query Builder**: Fluent interface for complex queries
- **Validations**: Comprehensive validation system
- **Associations**: belongs_to, has_many, has_one relationships
- **Callbacks**: before/after hooks for model lifecycle events
- **Scopes**: Reusable query methods
- **Timestamps**: Automatic created_at/updated_at tracking
- **Soft Delete**: Optional soft deletion support
- **Migrations**: Database schema management
- **Multiple Database Support**: PostgreSQL, SQLite3, and more

## Installation

1. Add the dependency to your `shard.yml`:

```yaml
dependencies:
  takarik-data:
    github: your-username/takarik-data
    version: ~> 0.1.0
```

2. Run `shards install`

## Quick Start

### 1. Establish Database Connection

```crystal
require "takarik-data"

# Connect to your database
Takarik::Data::BaseModel.establish_connection("postgresql://user:password@localhost/mydb")
# or for SQLite
# Takarik::Data::BaseModel.establish_connection("sqlite3://./database.db")
```

### 2. Define Your Models

```crystal
class User < Takarik::Data::BaseModel
  table_name "users"

  # Define columns with types
  column id, Int32
  column name, String
  column email, String
  column age, Int32
  column active, Bool

  # Associations
  has_many posts, dependent: :destroy
  has_many comments

  # Validations
  validates_presence_of :name, :email
  validates_uniqueness_of :email
  validates_length_of :name, minimum: 2, maximum: 50
  validates_format_of :email, with: /\A[\w+\-.]+@[a-z\d\-]+(\.[a-z\d\-]+)*\.[a-z]+\z/i
  validates_numericality_of :age, greater_than: 0, less_than: 150

  # Scopes
  scope :active do
    where(active: true)
  end

  scope :adults do
    where_gte("age", 18)
  end

  # Callbacks
  before_save do
    self.email = self.email.try(&.downcase)
  end

  # Enable timestamps and soft delete
  timestamps
  soft_delete
end

class Post < Takarik::Data::BaseModel
  table_name "posts"

  column id, Int32
  column title, String
  column content, String
  column user_id, Int32
  column published, Bool

  belongs_to user
  has_many comments, dependent: :destroy

  validates_presence_of :title, :content, :user_id
  validates_length_of :title, minimum: 5, maximum: 100

  scope :published do
    where(published: true)
  end

  timestamps
end
```

### 3. Basic CRUD Operations

```crystal
# Create
user = User.create(name: "John Doe", email: "john@example.com", age: 30)

# Read
user = User.find(1)
users = User.all
active_users = User.where(active: true)

# Update
user.update(name: "Johnny Doe")
user.name = "John Smith"
user.save

# Delete
user.destroy
```

### 4. Query Builder

```crystal
# Complex queries with method chaining
users = User
  .where(active: true)
  .where_gte("age", 18)
  .order("created_at", "DESC")
  .limit(10)
  .to_a

# Advanced conditions
users = User
  .where_in("age", [25, 30, 35])
  .where_like("name", "John%")
  .where_between("created_at", 1.week.ago, Time.utc)
  .to_a

# Aggregations
total_users = User.count
average_age = User.average("age")
oldest_user_age = User.maximum("age")

# Joins
# Automatic joins using associations (recommended)
posts_with_users = Post.inner_join("user").to_a
users_with_posts = User.left_join("posts").to_a

# Manual joins (still supported)
posts_with_users = Post
  .joins("users", "users.id = posts.user_id")
  .select("posts.*, users.name as user_name")
  .to_a
```

### 5. Associations

```crystal
# belongs_to
post = Post.find(1)
user = post.user

# has_many
user = User.find(1)
posts = user.posts
published_posts = user.posts.where(published: true)

# Creating associated records
user = User.find(1)
post = user.create_post(title: "New Post", content: "Content")
comment = post.create_comment(content: "Great post!", user: user)
```

### 6. Validations

```crystal
user = User.new
user.name = "A"  # Too short
user.email = "invalid"  # Invalid format

if user.valid?
  user.save
else
  puts user.errors_full_messages
  # => ["Name is too short (minimum is 2 characters)", "Email is invalid"]
end
```

### 7. Scopes

```crystal
# Using predefined scopes
active_adults = User.active.adults.to_a

# Chaining with other methods
recent_posts = Post.published.order("created_at", "DESC").limit(5).to_a
```

### 8. Callbacks

```crystal
class User < Takarik::Data::BaseModel
  before_save do
    self.email = self.email.try(&.downcase)
  end

  after_create do
    puts "Welcome #{self.name}!"
  end

  before_destroy do
    puts "Goodbye #{self.name}!"
  end
end
```

### 9. Migrations

```crystal
class CreateUsers < Takarik::Data::Migration
  def up
    create_table "users" do |t|
      t.primary_key
      t.string "name", null: false
      t.string "email", null: false
      t.integer "age"
      t.boolean "active", default: true
      t.timestamps
    end

    add_index "users", "email", unique: true
  end

  def down
    drop_table "users"
  end
end
```

## Advanced Features

### Automatic Joins

Takarik::Data automatically generates join conditions based on your model associations using [Wordsmith](https://github.com/luckyframework/wordsmith) for proper pluralization:

```crystal
# Instead of writing manual join conditions
User.inner_join("posts", "posts.user_id = users.id")

# You can simply use association names
User.inner_join("posts")  # Automatically generates: users.id = posts.user_id
Post.inner_join("user")   # Automatically generates: posts.user_id = users.id

# Supports all join types
User.left_join("posts")
User.right_join("posts")
User.inner_join("posts")

# Chainable with other query methods
active_users_with_posts = User
  .inner_join("posts")
  .where(active: true)
  .where("posts.published", true)
  .order("users.name")
  .to_a

# Works with complex associations
User.inner_join("comments")  # users.id = comments.user_id
Post.inner_join("comments")  # posts.id = comments.post_id

# Manual joins still supported for custom conditions
User.inner_join("posts", "posts.user_id = users.id AND posts.published = 1")
```

### Soft Delete

```crystal
class User < Takarik::Data::BaseModel
  soft_delete
end

user = User.find(1)
user.destroy  # Sets deleted_at timestamp
user.deleted?  # => true

# Restore
user.restore
user.deleted?  # => false

# Query deleted records
User.with_deleted  # Include deleted records
User.only_deleted  # Only deleted records
```

### Timestamps

```crystal
class User < Takarik::Data::BaseModel
  timestamps  # Adds created_at and updated_at columns
end

user = User.create(name: "John")
puts user.created_at  # => 2023-01-01 12:00:00 UTC
puts user.updated_at  # => 2023-01-01 12:00:00 UTC

user.update(name: "Johnny")
puts user.updated_at  # => 2023-01-01 12:05:00 UTC (updated)
```

### Custom Validations

```crystal
class User < Takarik::Data::BaseModel
  validates_presence_of :name, :email
  validates_length_of :name, minimum: 2, maximum: 50
  validates_format_of :email, with: /\A[\w+\-.]+@[a-z\d\-]+(\.[a-z\d\-]+)*\.[a-z]+\z/i
  validates_uniqueness_of :email
  validates_numericality_of :age, greater_than: 0, less_than: 150
end
```

## Database Support

The library supports multiple database backends through Crystal's DB library:

- **PostgreSQL** (via crystal-pg)
- **SQLite3** (via crystal-sqlite3)
- **MySQL** (via crystal-mysql)

## Testing

Run the test suite:

```bash
crystal spec
```

The library includes comprehensive specs covering:
- Basic CRUD operations
- Query builder functionality
- Validations
- Associations
- Callbacks
- Scopes
- Timestamps and soft delete
- Integration tests

## Contributing

1. Fork it (<https://github.com/your-username/takarik-data/fork>)
2. Create your feature branch (`git checkout -b my-new-feature`)
3. Commit your changes (`git commit -am 'Add some feature'`)
4. Push to the branch (`git push origin my-new-feature`)
5. Create a new Pull Request

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## Acknowledgments

- Inspired by Ruby's ActiveRecord
- Built for the Crystal programming language
- Uses Crystal's powerful macro system for code generation
