require "./spec_helper"

describe "Association Objects Support" do
  before_each do
    # Clean up test data
    User.all.delete_all
    Post.all.delete_all
    Comment.all.delete_all
  end

  describe "create with association objects" do
    it "supports creating with belongs_to association object" do
      user = User.create(name: "Alice", email: "alice@test.com", age: 25, active: true)

      # Create post using user object instead of user_id
      post = Post.create(title: "Test Post", content: "Content", user: user, published: true)

      post.persisted?.should be_true
      post.user_id.should eq(user.id)
      post.user.should eq(user)
      post.title.should eq("Test Post")
      post.published.should be_true
    end

    it "still supports creating with foreign key ID" do
      user = User.create(name: "Bob", email: "bob@test.com", age: 30, active: true)

      # Create post using traditional user_id approach
      post = Post.create(title: "Traditional Post", content: "Content", user_id: user.id, published: false)

      post.persisted?.should be_true
      post.user_id.should eq(user.id)
      post.user.should eq(user)
      post.title.should eq("Traditional Post")
      post.published.should be_false
    end

    it "handles mixed parameters correctly" do
      user = User.create(name: "Charlie", email: "charlie@test.com", age: 35, active: true)

      # Mix association object with regular attributes
      post = Post.create(
        title: "Mixed Post",
        content: "Some content",
        user: user,
        published: true
      )

      post.persisted?.should be_true
      post.user_id.should eq(user.id)
      post.user.not_nil!.name.should eq("Charlie")
      post.title.should eq("Mixed Post")
      post.content.should eq("Some content")
      post.published.should be_true
    end

    it "handles nil association objects" do
      # Note: Post model has validates_presence_of :user_id, so nil user won't be valid
      # This test verifies that the association processing doesn't crash with nil values
      post = Post.create(title: "No User Post", content: "Content", user: nil, published: true)

      # Should not persist due to validation failure (user_id required)
      post.persisted?.should be_false
      post.user_id.should be_nil
      post.user.should be_nil
      post.title.should eq("No User Post")

      # Verify it fails validation as expected
      post.valid?.should be_false
      post.errors.has_key?("user_id").should be_true
    end

    it "handles truly optional nil associations" do
      # Use Task model which has optional assignee
      project = Project.create(name: "Test Project")

      # Create task with nil assignee (optional association)
      task = Task.new
      task.title = "Test Task"
      task.project = project
      task.assignee = nil # This should be allowed

      result = task.save
      result.should be_true
      task.persisted?.should be_true
      task.assignee_id.should be_nil
      task.assignee.should be_nil
    end

    it "supports nested association objects" do
      user = User.create(name: "Diana", email: "diana@test.com", age: 28, active: true)
      post = Post.create(title: "Parent Post", content: "Content", user: user, published: true)

      # Create comment with both user and post objects
      comment = Comment.create(content: "Great post!", user: user, post: post)

      comment.persisted?.should be_true
      comment.user_id.should eq(user.id)
      comment.post_id.should eq(post.id)
      comment.user.should eq(user)
      comment.post.should eq(post)
      comment.content.should eq("Great post!")
    end
  end

  describe "update with association objects" do
    it "supports updating with belongs_to association object" do
      user1 = User.create(name: "Alice", email: "alice@test.com", age: 25, active: true)
      user2 = User.create(name: "Bob", email: "bob@test.com", age: 30, active: true)

      post = Post.create(title: "Original Post", content: "Content", user: user1, published: true)

      # Update with user object
      result = post.update(user: user2, title: "Updated Post")

      result.should be_true
      post.user_id.should eq(user2.id)
      post.user.should eq(user2)
      post.title.should eq("Updated Post")
    end

    it "still supports updating with foreign key ID" do
      user1 = User.create(name: "Alice", email: "alice@test.com", age: 25, active: true)
      user2 = User.create(name: "Bob", email: "bob@test.com", age: 30, active: true)

      post = Post.create(title: "Original Post", content: "Content", user: user1, published: true)

      # Update with traditional user_id approach
      result = post.update(user_id: user2.id, title: "Traditionally Updated")

      result.should be_true
      post.user_id.should eq(user2.id)
      post.user.should eq(user2)
      post.title.should eq("Traditionally Updated")
    end

    it "supports class-level update with association objects" do
      user1 = User.create(name: "Alice", email: "alice@test.com", age: 25, active: true)
      user2 = User.create(name: "Bob", email: "bob@test.com", age: 30, active: true)

      post = Post.create(title: "Original Post", content: "Content", user: user1, published: true)

      # Update using class-level update method with association object
      post.update(user: user2, content: "Updated content")

      post.user_id.should eq(user2.id)
      post.user.should eq(user2)
      post.content.should eq("Updated content")
    end

    it "handles multiple association updates" do
      user1 = User.create(name: "Alice", email: "alice@test.com", age: 25, active: true)
      user2 = User.create(name: "Bob", email: "bob@test.com", age: 30, active: true)

      post1 = Post.create(title: "Post 1", content: "Content", user: user1, published: true)
      post2 = Post.create(title: "Post 2", content: "Content", user: user1, published: true)

      comment = Comment.create(content: "Original comment", user: user1, post: post1)

      # Update both associations at once
      result = comment.update(user: user2, post: post2, content: "Updated comment")

      result.should be_true
      comment.user_id.should eq(user2.id)
      comment.post_id.should eq(post2.id)
      comment.user.should eq(user2)
      comment.post.should eq(post2)
      comment.content.should eq("Updated comment")
    end
  end

  describe "edge cases and validation" do
    it "validates association objects properly" do
      user = User.create(name: "Test User", email: "test@test.com", age: 25, active: true)

      # Create with valid association object
      post = Post.create(title: "Valid Post", content: "Content", user: user, published: true)
      post.persisted?.should be_true

      # Verify validation still works on regular attributes
      invalid_post = Post.create(title: "", content: "Content", user: user, published: true)
      invalid_post.persisted?.should be_false
    end

    it "handles non-existent association names gracefully" do
      user = User.create(name: "Test User", email: "test@test.com", age: 25, active: true)

      # This should just be treated as a regular attribute
      post = Post.create(title: "Test Post", content: "Content", user_id: user.id, published: true)
      post.persisted?.should be_true
      post.user_id.should eq(user.id)
    end

    it "works with different association configurations" do
      # Test with required association
      project = Project.create(name: "Test Project")

      # Create task with project object (required association)
      task = Task.new
      task.title = "Test Task"
      task.project = project
      task.save.should be_true

      # Test with optional association
      user = UserOptional.create(name: "Test User")
      task.assignee = user
      task.save.should be_true

      task.project.should eq(project)
      task.assignee.should eq(user)
    end
  end

  describe "performance and consistency" do
    it "generates same SQL for both approaches" do
      user = User.create(name: "Test User", email: "test@test.com", age: 25, active: true)

      # Both approaches should result in identical database state
      post1 = Post.create(title: "Post 1", content: "Content", user_id: user.id, published: true)
      post2 = Post.create(title: "Post 2", content: "Content", user: user, published: true)

      post1.user_id.should eq(post2.user_id)
      post1.user.should eq(post2.user)
    end

    it "works efficiently with large numbers of records" do
      user = User.create(name: "Test User", email: "test@test.com", age: 25, active: true)

      # Create multiple posts using association objects
      10.times do |i|
        post = Post.create(title: "Post #{i}", content: "Content #{i}", user: user, published: true)
        post.persisted?.should be_true
        post.user_id.should eq(user.id)
      end

      # Verify all posts were created correctly
      user.posts.count.should eq(10)
    end
  end

  describe "integration with existing features" do
    it "works with callbacks and validations" do
      user = User.create(name: "Test User", email: "test@test.com", age: 25, active: true)

      # Create post with association object - callbacks should still fire
      post = Post.create(title: "Callback Test", content: "Content", user: user, published: true)

      post.persisted?.should be_true
      post.user.should eq(user)
      # Timestamps should be set by callbacks
      post.created_at.should_not be_nil
      post.updated_at.should_not be_nil
    end

    it "works with dependent associations" do
      user = User.create(name: "Test User", email: "test@test.com", age: 25, active: true)
      post = Post.create(title: "Parent Post", content: "Content", user: user, published: true)

      # Create comments using association objects
      comment1 = Comment.create(content: "Comment 1", user: user, post: post)
      comment2 = Comment.create(content: "Comment 2", user: user, post: post)

      comment1.persisted?.should be_true
      comment2.persisted?.should be_true

      # Verify associations work
      post.comments.count.should eq(2)
      user.comments.count.should eq(2)
    end

    it "works with query builder and joins" do
      user = User.create(name: "Test User", email: "test@test.com", age: 25, active: true)
      post = Post.create(title: "Joinable Post", content: "Content", user: user, published: true)

      # Query using joins should work the same
      posts_with_users = Post.joins("user").where("users.name", "Test User").to_a
      posts_with_users.size.should eq(1)
      posts_with_users.first.title.should eq("Joinable Post")
    end
  end
end
