module Takarik::Data
  module Associations
    # Association types
    enum AssociationType
      BelongsTo
      HasMany
      HasOne
    end

    # Association definition
    struct Association
      getter name : String
      getter type : AssociationType
      getter class_name : String
      getter foreign_key : String
      getter primary_key : String
      getter dependent : Symbol?

      def initialize(@name : String, @type : AssociationType, @class_name : String,
                     @foreign_key : String, @primary_key : String = "id", @dependent : Symbol? = nil)
      end
    end

    # Storage for associations
    @@associations = {} of String => Array(Association)

    macro included
      # Class variable to store associations for this model
      @@associations = {} of String => Array(Association)

      # Extend the class with class methods
      extend ClassMethods
    end

    module ClassMethods
      # Class methods
      def add_association(name : String, type : AssociationType, class_name : String,
                             foreign_key : String, primary_key : String, dependent : Symbol?)
        @@associations[self.name] ||= [] of Association
        @@associations[self.name] << Association.new(name, type, class_name, foreign_key, primary_key, dependent)
      end

      def associations
        @@associations[self.name]? || [] of Association
      end
    end

    # Belongs to association
    macro belongs_to(name, class_name = nil, foreign_key = nil, primary_key = "id", dependent = nil)
      {% if class_name %}
        {% class_name = class_name %}
      {% else %}
        {% class_name = name.id.stringify.camelcase %}
      {% end %}

      {% if foreign_key %}
        {% foreign_key = foreign_key %}
      {% else %}
        # Generate foreign key from association name (e.g., "user" -> "user_id")
        {% foreign_key = name.id.stringify + "_id" %}
      {% end %}

      # Add association metadata
      add_association({{name.stringify}}, AssociationType::BelongsTo, {{class_name.stringify}},
                     {{foreign_key.stringify}}, {{primary_key.stringify}}, {{dependent}})

      # Define the getter method
      def {{name.id}}
        foreign_key_value = get_attribute({{foreign_key}})
        return nil unless foreign_key_value

        # Get the associated class
        associated_class = {{class_name.id}}
        associated_class.find(foreign_key_value)
      end

      # Define the setter method
      def {{name.id}}=(record : {{class_name.id}}?)
        if record
          primary_key_value = record.get_attribute({{primary_key}})
          set_attribute({{foreign_key}}, primary_key_value)
        else
          set_attribute({{foreign_key}}, nil)
        end
        record
      end

      # Define the build method
      def build_{{name.id}}(**attributes)
        record = {{class_name.id}}.new
        attributes.each do |key, value|
          record.set_attribute(key.to_s, value.as(DB::Any))
        end
        self.{{name.id}} = record
        record
      end

      # Define the create method
      def create_{{name.id}}(**attributes)
        record = build_{{name.id}}(**attributes)
        record.save
        record
      end
    end

    # Has many association
    macro has_many(name, class_name = nil, foreign_key = nil, primary_key = "id", dependent = nil)
      {% if class_name %}
        {% class_name = class_name %}
      {% else %}
        # Convert association name to class name (e.g., "posts" -> "Post")
        {% class_name = name.id.stringify.gsub(/s$/, "").camelcase %}
      {% end %}

      {% if foreign_key %}
        {% foreign_key = foreign_key %}
      {% else %}
        # Generate foreign key from current class name (e.g., "User" -> "user_id")
        {% foreign_key = @type.name.split("::").last.underscore + "_id" %}
      {% end %}

      # Add association metadata
      add_association({{name.stringify}}, AssociationType::HasMany, {{class_name.stringify}},
                     {{foreign_key.stringify}}, {{primary_key.stringify}}, {{dependent}})

      # Define the getter method
      def {{name.id}}
        primary_key_value = get_attribute({{primary_key}})

        # Get the associated class
        associated_class = {{class_name.id}}

        unless primary_key_value
          # Return empty query builder
          return associated_class.where("1 = ?", 0.as(DB::Any))
        end

        conditions = Hash(String, DB::Any).new
        conditions[{{foreign_key}}] = primary_key_value
        associated_class.where(conditions)
      end

      # Define the build method
      def build_{{name.id}}(**attributes)
        record = {{class_name.id}}.new
        attributes.each do |key, value|
          record.set_attribute(key.to_s, value.as(DB::Any))
        end

        # Set the foreign key
        primary_key_value = get_attribute({{primary_key}})
        record.set_attribute({{foreign_key}}, primary_key_value) if primary_key_value

        record
      end

      # Define the create method
      def create_{{name.id}}(**attributes)
        record = build_{{name.id}}(**attributes)
        record.save
        record
      end
    end

    # Has one association
    macro has_one(name, class_name = nil, foreign_key = nil, primary_key = "id", dependent = nil)
      {% if class_name %}
        {% class_name = class_name %}
      {% else %}
        # Convert association name to class name (e.g., "profile" -> "Profile")
        {% class_name = name.id.stringify.camelcase %}
      {% end %}

      {% if foreign_key %}
        {% foreign_key = foreign_key %}
      {% else %}
        # Generate foreign key from current class name (e.g., "User" -> "user_id")
        {% foreign_key = @type.name.split("::").last.underscore + "_id" %}
      {% end %}

      # Add association metadata
      add_association({{name.stringify}}, AssociationType::HasOne, {{class_name.stringify}},
                     {{foreign_key.stringify}}, {{primary_key.stringify}}, {{dependent}})

      # Define the getter method
      def {{name.id}}
        primary_key_value = get_attribute({{primary_key}})
        return nil unless primary_key_value

        # Get the associated class
        associated_class = {{class_name.id}}
        conditions = Hash(String, DB::Any).new
        conditions[{{foreign_key}}] = primary_key_value
        associated_class.where(conditions).first
      end

      # Define the setter method
      def {{name.id}}=(record : {{class_name.id}}?)
        # Clear existing association
        if existing = {{name.id}}
          existing.set_attribute({{foreign_key}}, nil)
          existing.save
        end

        # Set new association
        if record
          primary_key_value = get_attribute({{primary_key}})
          record.set_attribute({{foreign_key}}, primary_key_value)
        end

        record
      end

      # Define the build method
      def build_{{name.id}}(**attributes)
        record = {{class_name.id}}.new
        attributes.each do |key, value|
          record.set_attribute(key.to_s, value.as(DB::Any))
        end

        # Set the foreign key
        primary_key_value = get_attribute({{primary_key}})
        record.set_attribute({{foreign_key}}, primary_key_value) if primary_key_value

        record
      end

      # Define the create method
      def create_{{name.id}}(**attributes)
        record = build_{{name.id}}(**attributes)
        record.save
        record
      end
    end

    # Instance methods for handling dependent associations
    def destroy_dependent_associations
      self.class.associations.each do |association|
        next unless association.dependent

        case association.dependent
        when :destroy
          destroy_associated_records(association)
        when :delete_all
          delete_associated_records(association)
        when :nullify
          nullify_associated_records(association)
        end
      end
    end

    private def destroy_associated_records(association : Association)
      case association.type
      when .belongs_to?
        # For belongs_to, we don't destroy the parent
        return
      when .has_many?
        records = get_associated_records(association)
        records.each(&.destroy)
      when .has_one?
        record = get_associated_record(association)
        record.try(&.destroy)
      end
    end

    private def delete_associated_records(association : Association)
      case association.type
      when .belongs_to?
        return
      when .has_many?, .has_one?
        primary_key_value = get_attribute(association.primary_key)
        return unless primary_key_value

        # Get the associated class name and convert to actual class
        associated_class_name = association.class_name
        query = "DELETE FROM #{associated_class_name.underscore.pluralize} WHERE #{association.foreign_key} = ?"
        self.class.connection.exec(query, primary_key_value)
      end
    end

    private def nullify_associated_records(association : Association)
      case association.type
      when .belongs_to?
        return
      when .has_many?, .has_one?
        primary_key_value = get_attribute(association.primary_key)
        return unless primary_key_value

        # Get the associated class name and convert to actual class
        associated_class_name = association.class_name
        query = "UPDATE #{associated_class_name.underscore.pluralize} SET #{association.foreign_key} = NULL WHERE #{association.foreign_key} = ?"
        self.class.connection.exec(query, primary_key_value)
      end
    end

    private def get_associated_records(association : Association)
      primary_key_value = get_attribute(association.primary_key)
      return [] of BaseModel unless primary_key_value

      # This is a simplified version - in a real implementation, you'd need
      # to dynamically resolve the class name to the actual class
      [] of BaseModel
    end

    private def get_associated_record(association : Association)
      primary_key_value = get_attribute(association.primary_key)
      return nil unless primary_key_value

      # This is a simplified version - in a real implementation, you'd need
      # to dynamically resolve the class name to the actual class
      nil
    end

    # Override destroy to handle dependent associations
    def destroy
      destroy_dependent_associations
      super
    end
  end
end
