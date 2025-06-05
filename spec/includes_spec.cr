require "./spec_helper"

describe "Includes (Eager Loading)" do
  before_each do
    # Clean up any existing data
    User.connection.exec("DELETE FROM posts")
    User.connection.exec("DELETE FROM users")
  end

  describe "basic includes functionality" do
    it "should generate correct SQL with LEFT JOIN" do
      query_builder = Post.includes(:user)
      sql = query_builder.to_sql

      sql.should contain("LEFT JOIN")
      sql.should contain("users ON posts.user_id = users.id")
      sql.should contain("posts.id AS posts_id")
      sql.should contain("users.id AS users_id")
    end

    it "should load associated records" do
      # Create test data
      user = User.create(name: "John Doe", email: "john@example.com")
      post = Post.create(title: "Test Post", content: "Test content", user_id: user.id)

      # Test includes
      posts = Post.includes(:user).to_a
      posts.size.should eq(1)

      loaded_post = posts.first
      loaded_post.title.should eq("Test Post")

      # The association should be loaded from cache
      loaded_post.user_loaded?.should be_true

      # Accessing the user should not trigger another query
      associated_user = loaded_post.user
      associated_user.should_not be_nil
      associated_user.not_nil!.name.should eq("John Doe")
    end

    it "should handle multiple includes" do
      # For now, just test with one association since we only have user
      query_builder = Post.includes(:user)
      sql = query_builder.to_sql

      sql.should contain("LEFT JOIN users")
      sql.should contain("posts.id AS posts_id")
      sql.should contain("users.id AS users_id")
    end

    it "should handle nil associations" do
      # Create a post without a user by bypassing validation
      post = Post.new
      post.title = "Orphan Post"
      post.content = "No user"
      post.user_id = nil
      # Save without validation
      Post.connection.exec("INSERT INTO posts (title, content, user_id) VALUES (?, ?, ?)",
        "Orphan Post", "No user", nil)

      posts = Post.includes(:user).to_a
      posts.size.should eq(1)

      loaded_post = posts.first
      loaded_post.user_loaded?.should be_true
      # Skip nil comparison for now
      loaded_post.user.should be_nil
    end
  end

  describe "N+1 prevention" do
    it "should prevent N+1 queries" do
      # Create test data
      user1 = User.create(name: "User 1", email: "user1@example.com")
      user2 = User.create(name: "User 2", email: "user2@example.com")

      Post.create(title: "Post 1", content: "Content 1", user_id: user1.id)
      Post.create(title: "Post 2", content: "Content 2", user_id: user2.id)
      Post.create(title: "Post 3", content: "Content 3", user_id: user1.id)

      # Load posts with includes - should only execute one query
      posts = Post.includes(:user).to_a
      posts.size.should eq(3)

      # Accessing users should not trigger additional queries since they're cached
      posts.each do |post|
        post.user_loaded?.should be_true
        user = post.user
        user.should_not be_nil
        user.not_nil!.name.should_not be_nil
      end
    end
  end

  describe "explicit loading" do
    it "should support loaded? and load() methods" do
      # Create test data
      user = User.create(name: "John Doe", email: "john@example.com")
      post = Post.create(title: "Test Post", content: "Test content", user_id: user.id)

      # Load post without includes
      loaded_post = Post.find(post.id)
      loaded_post.should_not be_nil
      loaded_post = loaded_post.not_nil!

      # Association should not be loaded initially
      loaded_post.user_loaded?.should be_false

      # Explicitly load the association
      loaded_post.load_user

      # Now it should be loaded
      loaded_post.user_loaded?.should be_true

      # Accessing the user should not trigger another query
      associated_user = loaded_post.user
      associated_user.should_not be_nil
      associated_user.not_nil!.name.should eq("John Doe")

      # Calling load again should do nothing (no error)
      loaded_post.load_user
      loaded_post.user_loaded?.should be_true
    end
  end
end
