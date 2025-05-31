module Takarik::Data
  module Associations
    # ========================================
    # ENUMS
    # ========================================

    enum AssociationType
      BelongsTo
      HasMany
      HasOne
    end

    # ========================================
    # STRUCTS
    # ========================================

    struct Association
      getter name : String
      getter type : AssociationType
      getter class_type : BaseModel.class
      getter foreign_key : String
      getter primary_key : String
      getter dependent : Symbol?
      getter optional : Bool

      def initialize(@name : String, @type : AssociationType, @class_type : BaseModel.class,
                     @foreign_key : String, @primary_key : String = "id", @dependent : Symbol? = nil, @optional : Bool = false)
      end
    end

    # ========================================
    # MODULE VARIABLES
    # ========================================

    # Storage for associations
    @@associations = {} of String => Array(Association)

    # ========================================
    # INCLUDED HOOK
    # ========================================

    macro included
      # Class variable to store associations for this model
      @@associations = {} of String => Array(Association)

      # Extend the class with class methods
      extend ClassMethods
    end

    # ========================================
    # CLASS METHODS MODULE
    # ========================================

    module ClassMethods
      def add_association(name : String, type : AssociationType, class_type : BaseModel.class,
                             foreign_key : String, primary_key : String, dependent : Symbol?, optional : Bool)
        @@associations[self.name] ||= [] of Association
        @@associations[self.name] << Association.new(name, type, class_type, foreign_key, primary_key, dependent, optional)
      end

      def associations
        @@associations[self.name]? || [] of Association
      end
    end

    # ========================================
    # INSTANCE METHODS
    # ========================================

    def destroy_dependent_associations(connection = nil)
      self.class.associations.each do |association|
        next unless association.dependent

        case association.dependent
        when :destroy
          destroy_associated_records(association, connection)
        when :delete_all
          delete_associated_records(association, connection)
        when :nullify
          nullify_associated_records(association, connection)
        end
      end
    end

    # ========================================
    # PRIVATE METHODS
    # ========================================

    private def destroy_associated_records(association : Association, connection = nil)
      case association.type
      when .belongs_to?
        # For belongs_to, we don't destroy the parent
        return
      when .has_many?
        # For has_many with dependent: :destroy, we need to call destroy on each record
        # to trigger their callbacks and nested dependent associations
        records = get_associated_records(association)
        records.each do |record|
          # Use transaction-aware destroy if connection is provided
          if connection
            record.destroy_with_connection(connection)
          else
            record.destroy
          end
        end
      when .has_one?
        # Same approach for has_one
        record = get_associated_record(association)
        if record
          if connection
            record.destroy_with_connection(connection)
          else
            record.destroy
          end
        end
      end
    end

    private def delete_associated_records(association : Association, connection = nil)
      case association.type
      when .belongs_to?
        return
      when .has_many?, .has_one?
        primary_key_value = get_attribute(association.primary_key)
        return unless primary_key_value

        # Use the class to get table name
        table_name = association.class_type.table_name
        query = "DELETE FROM #{table_name} WHERE #{association.foreign_key} = ?"
        conn = connection || self.class.connection
        conn.exec(query, primary_key_value)
      end
    end

    private def nullify_associated_records(association : Association, connection = nil)
      case association.type
      when .belongs_to?
        return
      when .has_many?, .has_one?
        primary_key_value = get_attribute(association.primary_key)
        return unless primary_key_value

        # Use the class to get table name
        table_name = association.class_type.table_name
        query = "UPDATE #{table_name} SET #{association.foreign_key} = NULL WHERE #{association.foreign_key} = ?"
        conn = connection || self.class.connection
        conn.exec(query, primary_key_value)
      end
    end

    # Clean class-based association record retrieval
    private def get_associated_records(association : Association) : Array(BaseModel)
      primary_key_value = get_attribute(association.primary_key)
      return [] of BaseModel unless primary_key_value

      # Use the actual class type directly
      conditions = Hash(String, DB::Any).new
      conditions[association.foreign_key] = primary_key_value
      association.class_type.where(conditions).to_a.map(&.as(BaseModel))
    end

    private def get_associated_record(association : Association) : BaseModel?
      primary_key_value = get_attribute(association.primary_key)
      return nil unless primary_key_value

      # Use the actual class type directly
      conditions = Hash(String, DB::Any).new
      conditions[association.foreign_key] = primary_key_value
      association.class_type.where(conditions).first.try(&.as(BaseModel))
    end

    # ========================================
    # ASSOCIATION MACROS
    # ========================================

    macro belongs_to(name, class_name = nil, foreign_key = nil, primary_key = "id", dependent = nil, optional = false)
      # Determine the class type from class_name parameter
      {% if class_name %}
        {% if class_name.is_a?(StringLiteral) %}
          # Handle string class name - convert to class reference
          {% class_type = class_name.camelcase.id %}
        {% elsif class_name.is_a?(SymbolLiteral) %}
          # Handle symbol class name - convert to class reference
          {% class_type = class_name.id.stringify.camelcase.id %}
        {% else %}
          # Handle class reference directly
          {% class_type = class_name.id %}
        {% end %}
      {% else %}
        # Convert association name to class name (e.g., "user" -> "User")
        {% class_type = name.id.stringify.camelcase.id %}
      {% end %}

      # Handle foreign key parameter
      {% if foreign_key %}
        {% if foreign_key.is_a?(SymbolLiteral) %}
          {% foreign_key_str = foreign_key.id.stringify %}
        {% else %}
          {% foreign_key_str = foreign_key %}
        {% end %}
      {% else %}
        # Generate foreign key from association name (e.g., "user" -> "user_id")
        {% foreign_key_str = name.id.stringify + "_id" %}
      {% end %}

      # Handle primary key parameter
      {% if primary_key.is_a?(SymbolLiteral) %}
        {% primary_key_str = primary_key.id.stringify %}
      {% else %}
        {% primary_key_str = primary_key %}
      {% end %}

      # Ensure dependent is a symbol or nil
      {% if dependent && !dependent.is_a?(SymbolLiteral) %}
        {% raise "dependent option must be a symbol (e.g., :destroy, :delete_all, :nullify)" %}
      {% end %}

      # Add association metadata with actual class
      add_association({{name.id.stringify}}, AssociationType::BelongsTo, {{class_type}},
                     {{foreign_key_str}}, {{primary_key_str}}, {{dependent}}, {{optional}})

      # Add validation for required associations (when optional: false)
      {% unless optional %}
        validates_presence_of {{foreign_key_str.id}}
      {% end %}

      # Define the getter method
      def {{name.id}}
        foreign_key_value = get_attribute({{foreign_key_str}})
        return nil unless foreign_key_value

        {{class_type}}.find(foreign_key_value)
      end

      # Define the setter method
      def {{name.id}}=(record : {{class_type}}?)
        if record
          primary_key_value = record.get_attribute({{primary_key_str}})
          set_attribute({{foreign_key_str}}, primary_key_value)
        else
          {% if optional %}
            set_attribute({{foreign_key_str}}, nil)
          {% else %}
            # For required associations, setting to nil should be allowed during object construction
            # but validation will catch it during save
            set_attribute({{foreign_key_str}}, nil)
          {% end %}
        end
        record
      end

      # Define the build method
      def build_{{name.id}}(**attributes)
        record = {{class_type}}.new
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

    macro has_many(name, class_name = nil, foreign_key = nil, primary_key = "id", dependent = nil)
      # Determine the class type from class_name parameter
      {% if class_name %}
        {% if class_name.is_a?(StringLiteral) %}
          # Handle string class name - convert to class reference
          {% class_type = class_name.camelcase.id %}
        {% elsif class_name.is_a?(SymbolLiteral) %}
          # Handle symbol class name - convert to class reference
          {% class_type = class_name.id.stringify.camelcase.id %}
        {% else %}
          # Handle class reference directly
          {% class_type = class_name.id %}
        {% end %}
      {% else %}
        # Convert association name to class name (e.g., "posts" -> "Post")
        # For irregular plurals, users should specify class_name explicitly
        {% class_type = name.id.stringify.gsub(/s$/, "").camelcase.id %}
      {% end %}

      # Handle foreign key parameter
      {% if foreign_key %}
        {% if foreign_key.is_a?(SymbolLiteral) %}
          {% foreign_key_str = foreign_key.id.stringify %}
        {% else %}
          {% foreign_key_str = foreign_key %}
        {% end %}
      {% else %}
        # Generate foreign key from current class name (e.g., "User" -> "user_id")
        {% foreign_key_str = @type.name.split("::").last.underscore + "_id" %}
      {% end %}

      # Handle primary key parameter
      {% if primary_key.is_a?(SymbolLiteral) %}
        {% primary_key_str = primary_key.id.stringify %}
      {% else %}
        {% primary_key_str = primary_key %}
      {% end %}

      # Ensure dependent is a symbol or nil
      {% if dependent && !dependent.is_a?(SymbolLiteral) %}
        {% raise "dependent option must be a symbol (e.g., :destroy, :delete_all, :nullify)" %}
      {% end %}

      # Add association metadata with actual class
      add_association({{name.id.stringify}}, AssociationType::HasMany, {{class_type}},
                     {{foreign_key_str}}, {{primary_key_str}}, {{dependent}}, false)

      # Define the getter method
      def {{name.id}}
        primary_key_value = get_attribute({{primary_key_str}})

        unless primary_key_value
          # Return empty query builder
          return {{class_type}}.where("1 = ?", 0)
        end

        conditions = Hash(String, DB::Any).new
        conditions[{{foreign_key_str}}] = primary_key_value
        {{class_type}}.where(conditions)
      end

      # Define the build method
      def build_{{name.id}}(**attributes)
        record = {{class_type}}.new
        attributes.each do |key, value|
          record.set_attribute(key.to_s, value.as(DB::Any))
        end

        # Set the foreign key
        primary_key_value = get_attribute({{primary_key_str}})
        record.set_attribute({{foreign_key_str}}, primary_key_value) if primary_key_value

        record
      end

      # Define the create method
      def create_{{name.id}}(**attributes)
        record = build_{{name.id}}(**attributes)
        record.save
        record
      end
    end

    macro has_one(name, class_name = nil, foreign_key = nil, primary_key = "id", dependent = nil)
      # Determine the class type from class_name parameter
      {% if class_name %}
        {% if class_name.is_a?(StringLiteral) %}
          # Handle string class name - convert to class reference
          {% class_type = class_name.camelcase.id %}
        {% elsif class_name.is_a?(SymbolLiteral) %}
          # Handle symbol class name - convert to class reference
          {% class_type = class_name.id.stringify.camelcase.id %}
        {% else %}
          # Handle class reference directly
          {% class_type = class_name.id %}
        {% end %}
      {% else %}
        # Convert association name to class name (e.g., "profile" -> "Profile")
        {% class_type = name.id.stringify.camelcase.id %}
      {% end %}

      # Handle foreign key parameter
      {% if foreign_key %}
        {% if foreign_key.is_a?(SymbolLiteral) %}
          {% foreign_key_str = foreign_key.id.stringify %}
        {% else %}
          {% foreign_key_str = foreign_key %}
        {% end %}
      {% else %}
        # Generate foreign key from current class name (e.g., "User" -> "user_id")
        {% foreign_key_str = @type.name.split("::").last.underscore + "_id" %}
      {% end %}

      # Handle primary key parameter
      {% if primary_key.is_a?(SymbolLiteral) %}
        {% primary_key_str = primary_key.id.stringify %}
      {% else %}
        {% primary_key_str = primary_key %}
      {% end %}

      # Ensure dependent is a symbol or nil
      {% if dependent && !dependent.is_a?(SymbolLiteral) %}
        {% raise "dependent option must be a symbol (e.g., :destroy, :delete_all, :nullify)" %}
      {% end %}

      # Add association metadata with actual class
      add_association({{name.id.stringify}}, AssociationType::HasOne, {{class_type}},
                     {{foreign_key_str}}, {{primary_key_str}}, {{dependent}}, false)

      # Define the getter method
      def {{name.id}}
        primary_key_value = get_attribute({{primary_key_str}})
        return nil unless primary_key_value

        conditions = Hash(String, DB::Any).new
        conditions[{{foreign_key_str}}] = primary_key_value
        {{class_type}}.where(conditions).first
      end

      # Define the setter method
      def {{name.id}}=(record : {{class_type}}?)
        # Clear existing association
        if existing = {{name.id}}
          existing.set_attribute({{foreign_key_str}}, nil)
          existing.save
        end

        # Set new association
        if record
          primary_key_value = get_attribute({{primary_key_str}})
          record.set_attribute({{foreign_key_str}}, primary_key_value)
        end

        record
      end

      # Define the build method
      def build_{{name.id}}(**attributes)
        record = {{class_type}}.new
        attributes.each do |key, value|
          record.set_attribute(key.to_s, value.as(DB::Any))
        end

        # Set the foreign key
        primary_key_value = get_attribute({{primary_key_str}})
        record.set_attribute({{foreign_key_str}}, primary_key_value) if primary_key_value

        record
      end

      # Define the create method
      def create_{{name.id}}(**attributes)
        record = build_{{name.id}}(**attributes)
        record.save
        record
      end
    end
  end
end
