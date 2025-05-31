module Takarik::Data
  module Associations
    # ========================================
    # ENUMS
    # ========================================

    enum AssociationType
      BelongsTo
      HasMany
      HasOne
      HasManyThrough
      HasAndBelongsToMany
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
      getter through : String?
      getter join_table : String?

      def initialize(@name : String, @type : AssociationType, @class_type : BaseModel.class,
                     @foreign_key : String, @primary_key : String = "id", @dependent : Symbol? = nil,
                     @optional : Bool = false, @through : String? = nil, @join_table : String? = nil)
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
                             foreign_key : String, primary_key : String, dependent : Symbol?, optional : Bool,
                             through : String? = nil, join_table : String? = nil)
        @@associations[self.name] ||= [] of Association
        @@associations[self.name] << Association.new(name, type, class_type, foreign_key, primary_key, dependent, optional, through, join_table)
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

    # Regular has_many association
    macro has_many(name, class_name = nil, foreign_key = nil, primary_key = "id", dependent = nil)
      # Determine the class type from class_name parameter
      {% if class_name %}
        {% if class_name.is_a?(StringLiteral) %}
          {% class_type = class_name.camelcase.id %}
        {% elsif class_name.is_a?(SymbolLiteral) %}
          {% class_type = class_name.id.stringify.camelcase.id %}
        {% else %}
          {% class_type = class_name.id %}
        {% end %}
      {% else %}
        # Convert plural association name to singular class name using inline singularization
        {%
          name_str = name.id.stringify
          if name_str.ends_with?("ies") && name_str.size > 3
            singular_name = name_str[0..-4] + "y"
          elsif name_str.ends_with?("ves") && name_str.size > 3
            singular_name = name_str[0..-4] + "f"
          elsif name_str.ends_with?("s") && !name_str.ends_with?("ss") && name_str.size > 1
            singular_name = name_str[0..-2]
          else
            singular_name = name_str
          end
          class_type = singular_name.camelcase.id
        %}
      {% end %}

      # Handle foreign key parameter
      {% if foreign_key %}
        {% if foreign_key.is_a?(SymbolLiteral) %}
          {% foreign_key_str = foreign_key.id.stringify %}
        {% else %}
          {% foreign_key_str = foreign_key %}
        {% end %}
      {% else %}
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

      # Add association metadata
      add_association({{name.id.stringify}}, AssociationType::HasMany, {{class_type}},
                     {{foreign_key_str}}, {{primary_key_str}}, {{dependent}}, false)

      # Define the getter method
      def {{name.id}}
        primary_key_value = get_attribute({{primary_key_str}})

        unless primary_key_value
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

    # has_many :through association (macro overload)
    macro has_many(name, class_name = nil, foreign_key = nil, primary_key = "id", dependent = nil, through = nil)
      {% if through %}
        # Convert through parameter
        {% through_str = through.is_a?(SymbolLiteral) ? through.id.stringify : through %}

        # Determine the class type from class_name parameter
        {% if class_name %}
          {% if class_name.is_a?(StringLiteral) %}
            {% class_type = class_name.camelcase.id %}
          {% elsif class_name.is_a?(SymbolLiteral) %}
            {% class_type = class_name.id.stringify.camelcase.id %}
          {% else %}
            {% class_type = class_name.id %}
          {% end %}
        {% else %}
          # Convert plural association name to singular class name using inline singularization
          {%
            name_str = name.id.stringify
            if name_str.ends_with?("ies") && name_str.size > 3
              singular_name = name_str[0..-4] + "y"
            elsif name_str.ends_with?("ves") && name_str.size > 3
              singular_name = name_str[0..-4] + "f"
            elsif name_str.ends_with?("s") && !name_str.ends_with?("ss") && name_str.size > 1
              singular_name = name_str[0..-2]
            else
              singular_name = name_str
            end
            class_type = singular_name.camelcase.id
          %}
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

        # Add association metadata
        add_association({{name.id.stringify}}, AssociationType::HasManyThrough, {{class_type}},
                       "", {{primary_key_str}}, {{dependent}}, false, {{through_str}})

        # Define the getter method for has_many :through
        def {{name.id}}
          primary_key_value = get_attribute({{primary_key_str}})
          unless primary_key_value
            return [] of {{class_type}}
          end

          # Build query through the intermediate association
          # Convert through association name to class name using inline singularization
          {%
            through_str_val = through_str
            if through_str_val.ends_with?("ies") && through_str_val.size > 3
              through_singular = through_str_val[0..-4] + "y"
            elsif through_str_val.ends_with?("ves") && through_str_val.size > 3
              through_singular = through_str_val[0..-4] + "f"
            elsif through_str_val.ends_with?("s") && !through_str_val.ends_with?("ss") && through_str_val.size > 1
              through_singular = through_str_val[0..-2]
            else
              through_singular = through_str_val
            end
            through_class_type = through_singular.camelcase.id
          %}

          through_class_name = {{through_class_type}}
          intermediate_table = through_class_name.table_name

          # Determine foreign keys for the relationship
          current_foreign_key = {{@type.name.split("::").last.underscore + "_id"}}
          target_foreign_key = {{class_type.stringify.underscore + "_id"}}

          # Build the join query
          query = "SELECT #{{{class_type}}.table_name}.* FROM #{{{class_type}}.table_name} " \
                  "INNER JOIN #{intermediate_table} ON #{{{class_type}}.table_name}.id = #{intermediate_table}.#{target_foreign_key} " \
                  "WHERE #{intermediate_table}.#{current_foreign_key} = ?"

          results = [] of {{class_type}}
          {{class_type}}.connection.query(query, primary_key_value) do |rs|
            while rs.move_next
              instance = {{class_type}}.new
              instance.load_from_result_set(rs)
              results << instance
            end
          end

          results
        end
      {% else %}
        # This shouldn't happen since this macro is only for through associations
        {% raise "has_many with through parameter expected, but through is nil" %}
      {% end %}
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

    # has_and_belongs_to_many macro for direct many-to-many associations
    macro has_and_belongs_to_many(name, class_name = nil, join_table = nil, foreign_key = nil, association_foreign_key = nil)
      # Determine the class type from class_name parameter
      {% if class_name %}
        {% if class_name.is_a?(StringLiteral) %}
          {% class_type = class_name.camelcase.id %}
        {% elsif class_name.is_a?(SymbolLiteral) %}
          {% class_type = class_name.id.stringify.camelcase.id %}
        {% else %}
          {% class_type = class_name.id %}
        {% end %}
      {% else %}
        # Convert plural association name to singular class name using inline singularization
        {%
          name_str = name.id.stringify
          if name_str.ends_with?("ies") && name_str.size > 3
            singular_name = name_str[0..-4] + "y"
          elsif name_str.ends_with?("ves") && name_str.size > 3
            singular_name = name_str[0..-4] + "f"
          elsif name_str.ends_with?("s") && !name_str.ends_with?("ss") && name_str.size > 1
            singular_name = name_str[0..-2]
          else
            singular_name = name_str
          end
          class_type = singular_name.camelcase.id
        %}
      {% end %}

      # Determine join table name
      {% if join_table %}
        {% if join_table.is_a?(SymbolLiteral) %}
          {% join_table_str = join_table.id.stringify %}
        {% else %}
          {% join_table_str = join_table %}
        {% end %}
      {% else %}
        # Generate join table name from model names in alphabetical order using inline pluralization
        {%
          # Pluralize current class name
          current_class_name = @type.name.split("::").last.underscore
          if current_class_name.ends_with?("y") && !["ay", "ey", "iy", "oy", "uy"].any? { |ending| current_class_name.ends_with?(ending) }
            current_table = current_class_name[0..-2] + "ies"
          elsif current_class_name.ends_with?("s") || current_class_name.ends_with?("sh") || current_class_name.ends_with?("ch") || current_class_name.ends_with?("x") || current_class_name.ends_with?("z")
            current_table = current_class_name + "es"
          elsif current_class_name.ends_with?("f")
            current_table = current_class_name[0..-2] + "ves"
          elsif current_class_name.ends_with?("fe")
            current_table = current_class_name[0..-3] + "ves"
          else
            current_table = current_class_name + "s"
          end

          # Pluralize target class name
          target_class_name = class_type.stringify.underscore
          if target_class_name.ends_with?("y") && !["ay", "ey", "iy", "oy", "uy"].any? { |ending| target_class_name.ends_with?(ending) }
            target_table = target_class_name[0..-2] + "ies"
          elsif target_class_name.ends_with?("s") || target_class_name.ends_with?("sh") || target_class_name.ends_with?("ch") || target_class_name.ends_with?("x") || target_class_name.ends_with?("z")
            target_table = target_class_name + "es"
          elsif target_class_name.ends_with?("f")
            target_table = target_class_name[0..-2] + "ves"
          elsif target_class_name.ends_with?("fe")
            target_table = target_class_name[0..-3] + "ves"
          else
            target_table = target_class_name + "s"
          end

          if current_table < target_table
            join_table_str = current_table + "_" + target_table
          else
            join_table_str = target_table + "_" + current_table
          end
        %}
      {% end %}

      # Determine foreign keys
      {% if foreign_key %}
        {% if foreign_key.is_a?(SymbolLiteral) %}
          {% foreign_key_str = foreign_key.id.stringify %}
        {% else %}
          {% foreign_key_str = foreign_key %}
        {% end %}
      {% else %}
        {% foreign_key_str = @type.name.split("::").last.underscore + "_id" %}
      {% end %}

      {% if association_foreign_key %}
        {% if association_foreign_key.is_a?(SymbolLiteral) %}
          {% association_foreign_key_str = association_foreign_key.id.stringify %}
        {% else %}
          {% association_foreign_key_str = association_foreign_key %}
        {% end %}
      {% else %}
        {% association_foreign_key_str = class_type.stringify.underscore + "_id" %}
      {% end %}

      # Add association metadata
      add_association({{name.id.stringify}}, AssociationType::HasAndBelongsToMany, {{class_type}},
                     {{foreign_key_str}}, "id", nil, false, nil, {{join_table_str}})

      # Define the getter method - returns an array of associated records
      def {{name.id}}
        primary_key_value = get_attribute("id")
        unless primary_key_value
          return [] of {{class_type}}
        end

        # Query through join table
        query = "SELECT #{{{class_type}}.table_name}.* FROM #{{{class_type}}.table_name} " \
                "INNER JOIN #{{{join_table_str}}} ON #{{{class_type}}.table_name}.id = #{{{join_table_str}}}.#{{{association_foreign_key_str}}} " \
                "WHERE #{{{join_table_str}}}.#{{{foreign_key_str}}} = ?"

        results = [] of {{class_type}}
        {{class_type}}.connection.query(query, primary_key_value) do |rs|
          while rs.move_next
            instance = {{class_type}}.new
            instance.load_from_result_set(rs)
            results << instance
          end
        end

        results
      end

      # Define method to add a single association
      {%
        name_str = name.id.stringify
        if name_str.ends_with?("ies") && name_str.size > 3
          singular_name = name_str[0..-4] + "y"
        elsif name_str.ends_with?("ves") && name_str.size > 3
          singular_name = name_str[0..-4] + "f"
        elsif name_str.ends_with?("s") && !name_str.ends_with?("ss") && name_str.size > 1
          singular_name = name_str[0..-2]
        else
          singular_name = name_str
        end
      %}
      def add_{{singular_name.id}}(record : {{class_type}})
        primary_key_value = get_attribute("id")
        target_key_value = record.get_attribute("id")

        return record unless primary_key_value && target_key_value

        # Check if association already exists
        existing_query = "SELECT COUNT(*) FROM #{{{join_table_str}}} WHERE #{{{foreign_key_str}}} = ? AND #{{{association_foreign_key_str}}} = ?"
        count = self.class.connection.scalar(existing_query, primary_key_value, target_key_value).as(Int64)

        if count == 0
          # Insert new association
          insert_query = "INSERT INTO #{{{join_table_str}}} (#{{{foreign_key_str}}}, #{{{association_foreign_key_str}}}) VALUES (?, ?)"
          self.class.connection.exec(insert_query, primary_key_value, target_key_value)
        end

        record
      end

      # Define method to remove a single association
      def remove_{{singular_name.id}}(record : {{class_type}})
        primary_key_value = get_attribute("id")
        target_key_value = record.get_attribute("id")

        return record unless primary_key_value && target_key_value

        delete_query = "DELETE FROM #{{{join_table_str}}} WHERE #{{{foreign_key_str}}} = ? AND #{{{association_foreign_key_str}}} = ?"
        self.class.connection.exec(delete_query, primary_key_value, target_key_value)
        record
      end

      # Define method to clear all associations
      def clear_{{name.id}}
        primary_key_value = get_attribute("id")
        return unless primary_key_value

        clear_query = "DELETE FROM #{{{join_table_str}}} WHERE #{{{foreign_key_str}}} = ?"
        self.class.connection.exec(clear_query, primary_key_value)
      end
    end
  end
end
