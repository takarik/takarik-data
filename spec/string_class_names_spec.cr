require "./spec_helper"

describe "String Class Names in Associations" do
  describe "String class names with dependent: :destroy" do
    it "works with has_many dependent: :destroy using string class names" do
      # Create an author with books using string class names
      author = AuthorString.new
      author.name = "J.K. Rowling"
      author.save.should be_true

      # Create books using the build method first
      book1 = author.build_books(title: "Harry Potter 1")
      book2 = author.build_books(title: "Harry Potter 2")

      # Save the books
      book1.save.should be_true
      book2.save.should be_true

      # Verify associations work
      author.books.count.should eq(2)
      book1.author.should eq(author)
      book2.author.should eq(author)

      # Test dependent: :destroy
      author.destroy.should be_true

      # Verify books were destroyed
      BookString.count.should eq(0)
      AuthorString.count.should eq(0)
    end
  end

  describe "String class names with dependent: :nullify" do
    it "works with has_many dependent: :nullify using string class names" do
      # Create a publisher with magazines using string class names
      publisher = PublisherString.new
      publisher.name = "National Geographic"
      publisher.save.should be_true

      # Create magazines using build method first
      mag1 = publisher.build_magazines(title: "National Geographic Magazine")
      mag2 = publisher.build_magazines(title: "National Geographic Kids")

      mag1.save.should be_true
      mag2.save.should be_true

      # Verify associations work
      publisher.magazines.count.should eq(2)
      mag1.publisher.should eq(publisher)
      mag2.publisher.should eq(publisher)

      # Test dependent: :nullify
      publisher.destroy.should be_true

      # Verify magazines still exist but with null publisher_id
      MagazineString.count.should eq(2)
      PublisherString.count.should eq(0)

      # Reload magazines and check publisher_id is null
      mag1_reloaded = MagazineString.find(mag1.id)
      mag2_reloaded = MagazineString.find(mag2.id)

      mag1_reloaded.should_not be_nil
      mag2_reloaded.should_not be_nil
      mag1_reloaded.try(&.publisher_id).should be_nil
      mag2_reloaded.try(&.publisher_id).should be_nil
    end
  end

  describe "Basic association functionality with string class names" do
    it "handles belongs_to associations with string class names" do
      # Create author and book using string class names
      author = AuthorString.new
      author.name = "George Orwell"
      author.save.should be_true

      book = BookString.new
      book.title = "1984"
      book.author = author
      book.save.should be_true

      # Test the association
      book.author.should eq(author)
      author.books.count.should eq(1)
      author.books.first.should eq(book)
    end

    it "handles has_many associations with string class names" do
      # Create publisher with magazines using string class names
      publisher = PublisherString.new
      publisher.name = "Time Inc."
      publisher.save.should be_true

      # Create magazines
      mag1 = publisher.build_magazines(title: "Time Magazine")
      mag2 = publisher.build_magazines(title: "People Magazine")

      mag1.save.should be_true
      mag2.save.should be_true

      # Test associations
      publisher.magazines.count.should eq(2)
      publisher.magazines.to_a.should contain(mag1)
      publisher.magazines.to_a.should contain(mag2)

      mag1.publisher.should eq(publisher)
      mag2.publisher.should eq(publisher)
    end
  end

  describe "Mixed class reference and string approaches" do
    it "works when some models use class references and others use strings" do
      # Create a User (uses class reference) with a Post (uses class reference)
      user = User.new
      user.name = "Test User"
      user.email = "test@example.com"
      user.age = 25
      user.save.should be_true

      post = user.build_posts(title: "Test Post", content: "Test content")
      post.save.should be_true

      # Create an AuthorString (uses string) with a BookString (uses string)
      author = AuthorString.new
      author.name = "Test Author"
      author.save.should be_true

      book = author.build_books(title: "Test Book")
      book.save.should be_true

      # Test both work independently
      user.posts.count.should eq(1)
      author.books.count.should eq(1)

      # Test dependent associations work for both
      user.destroy.should be_true
      Post.count.should eq(0)  # Post should be destroyed with user

      author.destroy.should be_true
      BookString.count.should eq(0)  # BookString should be destroyed with author
    end
  end

  describe "Optional associations parameter" do
    it "validates required associations by default" do
      # Create a task without a project (required association)
      task = Task.new
      task.title = "Test Task"
      # Don't set project - this should fail validation since optional: false is default

      task.save.should be_false
      task.valid?.should be_false
      task.errors.has_key?("project_id").should be_true
      task.errors["project_id"].should contain("can't be blank")
    end

    it "allows optional associations to be null" do
      # Create a project first
      project = Project.new
      project.name = "Test Project"
      project.save.should be_true

      # Create a task with project but without assignee (optional association)
      task = Task.new
      task.title = "Unassigned Task"
      task.project = project
      # Don't set assignee - this should be allowed since it's optional: true

      task.save.should be_true
      task.assignee.should be_nil
      task.assignee_id.should be_nil
    end

    it "allows setting optional associations to nil" do
      # Create a project and user
      project = Project.new
      project.name = "Test Project"
      project.save.should be_true

      user = UserOptional.new
      user.name = "Test User"
      user.save.should be_true

      # Create a task with assignee
      task = Task.new
      task.title = "Assigned Task"
      task.project = project
      task.assignee = user
      task.save.should be_true

      task.assignee.should eq(user)

      # Now remove the assignee (set to nil) - should be allowed
      task.assignee = nil
      task.save.should be_true
      task.assignee.should be_nil
    end

    it "validates required associations even when set to nil" do
      # Create a project and task
      project = Project.new
      project.name = "Test Project"
      project.save.should be_true

      task = Task.new
      task.title = "Test Task"
      task.project = project
      task.save.should be_true

      # Try to remove the required project association
      task.project = nil
      task.save.should be_false
      task.valid?.should be_false
      task.errors.has_key?("project_id").should be_true
      task.errors["project_id"].should contain("can't be blank")
    end
  end
end
