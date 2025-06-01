require "db"
require "json"
require "./string"
require "./query_builder"
require "./validations"
require "./associations"

module Takarik::Data
  # Module-level connection variable that's shared across all classes
  @@connection : DB::Database?

  def self.connection
    @@connection || raise "Database connection not established. Call Takarik::Data.establish_connection first."
  end

  def self.establish_connection(database_url : String)
    @@connection = DB.open(database_url)
  end

  # Base class for all ORM models, providing ActiveRecord-like functionality
  # but designed specifically for Crystal language features
  abstract class BaseModel
    include JSON::Serializable
    include Validations
    include Associations

    # ========================================
    # CLASS VARIABLES
    # ========================================

    # Class variable to store column names for each model
    @@column_names = {} of String => Array(String)

    # Class variable to store primary key name for each model
    @@primary_key_name = "id"

    # ========================================
    # INSTANCE VARIABLES
    # ========================================

    @attributes = {} of String => DB::Any
    @persisted = false
    @changed_attributes = Set(String).new
    @_last_action : Symbol?

    # ========================================
    # CLASS METHODS - CONFIGURATION
    # ========================================

    def self.connection
      Takarik::Data.connection
    end

    def self.table_name
      self.name.split("::").last.tableize
    end

    def self.primary_key
      @@primary_key_name
    end

    def self.column_names
      @@column_names[self.name]? || [] of String
    end

    def self.add_column_name(name : String)
      @@column_names[self.name] ||= [] of String
      @@column_names[self.name] << name unless @@column_names[self.name].includes?(name)
    end

    # ========================================
    # CLASS METHODS - QUERY BUILDING
    # ========================================

    def self.all
      QueryBuilder(self).new(self)
    end

    def self.where(conditions : Hash(String, DB::Any))
      all.where(conditions)
    end

    def self.where(**conditions)
      all.where(**conditions)
    end

    def self.where(condition : String, *params : DB::Any)
      all.where(condition, *params)
    end

    def self.where(column_with_operator : String, value : DB::Any)
      all.where(column_with_operator, value)
    end

    def self.where(column : String, values : Array(DB::Any))
      all.where(column, values)
    end

    # New clean syntax methods
    def self.not(conditions : Hash(String, DB::Any))
      all.not(conditions)
    end

    def self.not(**conditions)
      all.not(**conditions)
    end

    def self.not(column : String, values : Array(DB::Any))
      all.not(column, values)
    end

    def self.not(condition : String, param : DB::Any)
      all.not(condition, param)
    end

    def self.not(condition : String, *params : DB::Any)
      all.not(condition, *params)
    end

    def self.not(column_with_operator : String, value : DB::Any)
      all.not(column_with_operator, value)
    end

    def self.associated(association_name : String | Symbol)
      all.associated(association_name)
    end

    def self.missing(association_name : String | Symbol)
      all.missing(association_name)
    end

    # Logical operator methods
    def self.or(conditions : Hash(String, DB::Any))
      all.or(conditions)
    end

    def self.or(**conditions)
      all.or(**conditions)
    end

    def self.or(condition : String, param : DB::Any)
      all.or(condition, param)
    end

    def self.or(condition : String, *params : DB::Any)
      all.or(condition, *params)
    end

    def self.or(column_with_operator : String, value : DB::Any)
      all.or(column_with_operator, value)
    end

    def self.or(column : String, values : Array(DB::Any))
      all.or(column, values)
    end

    def self.select(*columns : String)
      all.select(*columns)
    end

    def self.select(columns : Array(String))
      all.select(columns)
    end

    def self.order(column : String, direction : String = "ASC")
      all.order(column, direction)
    end

    def self.order(**columns)
      all.order(**columns)
    end

    def self.limit(count : Int32)
      all.limit(count)
    end

    def self.offset(count : Int32)
      all.offset(count)
    end

    def self.join(table : String, on : String)
      all.join(table, on)
    end

    def self.join(association_name : String | Symbol)
      all.join(association_name)
    end

    def self.inner_join(table : String, on : String)
      all.inner_join(table, on)
    end

    def self.inner_join(association_name : String | Symbol)
      all.inner_join(association_name)
    end

    def self.left_join(table : String, on : String)
      all.left_join(table, on)
    end

    def self.left_join(association_name : String | Symbol)
      all.left_join(association_name)
    end

    def self.right_join(table : String, on : String)
      all.right_join(table, on)
    end

    def self.right_join(association_name : String | Symbol)
      all.right_join(association_name)
    end

    def self.group(*columns : String)
      all.group(*columns)
    end

    def self.having(condition : String, *params : DB::Any)
      all.having(condition, *params)
    end

    def self.page(page_number : Int32, per_page : Int32)
      all.page(page_number, per_page)
    end

    # ========================================
    # CLASS METHODS - AGGREGATION
    # ========================================

    def self.sum(column : String)
      all.sum(column)
    end

    def self.average(column : String)
      all.average(column)
    end

    def self.minimum(column : String)
      all.minimum(column)
    end

    def self.maximum(column : String)
      all.maximum(column)
    end

    # ========================================
    # CLASS METHODS - FINDERS
    # ========================================

    def self.find(id)
      all.where(primary_key, id).first
    end

    def self.find!(id)
      find(id) || raise "Record not found with #{primary_key}=#{id}"
    end

    def self.first
      all.first
    end

    def self.first!
      all.first!
    end

    def self.last
      all.order(primary_key, "DESC").first
    end

    def self.count
      all.count
    end

    # ========================================
    # CLASS METHODS - CREATION
    # ========================================

    def self.create(attributes : Hash(String, DB::Any))
      instance = new
      attributes.each do |key, value|
        instance.set_attribute(key, value)
      end
      instance.save
      instance
    end

    def self.create(**attributes)
      # Process association objects first
      processed_attributes = process_association_attributes_for_create(attributes)
      create(processed_attributes)
    end

    # ========================================
    # INITIALIZE
    # ========================================

    def initialize
      @attributes = {} of String => DB::Any
      @persisted = false
      @changed_attributes = Set(String).new
      @_last_action = nil

      # Run after_initialize callbacks automatically for new instances
      run_after_initialize_callbacks
    end

    # ========================================
    # INSTANCE METHODS - STATE
    # ========================================

    def persisted?
      @persisted
    end

    def new_record?
      !@persisted
    end

    def changed?
      @changed_attributes.any?
    end

    def changed_attributes
      @changed_attributes.to_a
    end

    # ========================================
    # INSTANCE METHODS - ATTRIBUTES
    # ========================================

    def set_attribute(name : String, value : DB::Any)
      @attributes[name] = value
      @changed_attributes << name

      # Also set the instance variable if it exists
      set_single_instance_variable(name, value)
    end

    def get_attribute(name : String)
      @attributes[name]?
    end

    def to_h
      @attributes.dup
    end

    # ========================================
    # INSTANCE METHODS - PERSISTENCE
    # ========================================
    #
    # These methods implement real database transactions with proper callback execution:
    #
    # Execution order:
    # 1. before_* callbacks (outside transaction)
    # 2. START TRANSACTION
    # 3. Database operation (INSERT/UPDATE/DELETE)
    # 4. after_create/after_update/after_destroy callbacks (inside transaction)
    # 5. after_save callbacks (inside transaction, for create/update only)
    # 6. COMMIT TRANSACTION (automatic if no exceptions)
    # 7. after_commit callbacks (after successful commit)
    #
    # On failure or exception:
    # - ROLLBACK TRANSACTION (automatic on exception, explicit on failed operation)
    # - after_rollback callbacks (after rollback)
    #
    def save
      # Run before_validation callbacks
      run_before_validation_callbacks

      # Run validation if the model includes validations
      {% if @type.ancestors.any? { |a| a.name.stringify.includes?("Validations") } %}
        valid_result = valid?

        # Run after_validation callbacks
        run_after_validation_callbacks

        return false unless valid_result
      {% else %}
        # Run after_validation callbacks even if no validations module
        run_after_validation_callbacks
      {% end %}

      result = if new_record?
                 insert_record
               else
                 update_record if changed?
               end

      result
    end

    def save!
      # Run before_validation callbacks
      run_before_validation_callbacks

      # Run validation if the model includes validations
      {% if @type.ancestors.any? { |a| a.name.stringify.includes?("Validations") } %}
        valid_result = valid?

        # Run after_validation callbacks
        run_after_validation_callbacks

        unless valid_result
          raise Takarik::Data::Validations::ValidationError.new(@validation_errors)
        end
      {% else %}
        # Run after_validation callbacks even if no validations module
        run_after_validation_callbacks
      {% end %}

      result = if new_record?
                 insert_record
               else
                 update_record if changed?
               end

      result || raise "Failed to save record"
    end

    def update(attributes : Hash(String, DB::Any))
      # Check if any attributes are association objects and process them
      processed_attributes = process_db_any_attributes_for_associations(attributes)
      processed_attributes.each do |key, value|
        set_attribute(key, value)
      end
      save
    end

    def update(**attributes)
      # Process association objects first
      processed_attributes = process_association_attributes_for_update(attributes)
      update(processed_attributes)
    end

    def touch(*attributes)
      return false if new_record?

      current_time = Time.utc

      # If no specific attributes provided, touch updated_at by default
      if attributes.empty?
        # Try to set updated_at if it exists
        begin
          self.updated_at = current_time
        rescue
          # Ignore if updated_at doesn't exist
        end
      else
        # Touch specified attributes
        attributes.each do |attr|
          attr_name = attr.to_s
          set_attribute(attr_name, current_time.as(DB::Any))
        end

        # Always update updated_at if it exists and wasn't already specified
        unless attributes.any? { |attr| attr.to_s == "updated_at" }
          begin
            self.updated_at = current_time
          rescue
            # Ignore if updated_at doesn't exist
          end
        end
      end

      # Save without validation (like ActiveRecord touch)
      result = update_record if changed?

      # Run after_touch callbacks if the update was successful
      if result
        run_after_touch_callbacks
      end

      result
    end

    def destroy
      return false if new_record?

      # Run before_destroy callbacks
      run_before_destroy_callbacks

      query = "DELETE FROM #{self.class.table_name} WHERE #{self.class.primary_key} = ?"
      id_value = get_attribute(self.class.primary_key)

      begin
        self.class.connection.transaction do |tx|
          result = tx.connection.exec(query, id_value)

          if result.rows_affected > 0
            @persisted = false

            # Call dependent associations handling after main record is destroyed but within transaction
            {% if @type.ancestors.any? { |a| a.name.stringify.includes?("Associations") } %}
              destroy_dependent_associations(tx.connection)
            {% end %}

            # Set the action for callbacks
            @_last_action = :destroy

            # Run after_destroy callbacks (inside transaction)
            run_after_destroy_callbacks

            # Transaction will commit automatically here
          else
            # Explicitly rollback the transaction
            tx.rollback
            @_last_action = :destroy
            execute_rollback_callbacks(:destroy)
            return false
          end
        end

        # Transaction committed successfully - run after_commit callbacks
        @_last_action = :destroy
        execute_commit_callbacks(:destroy)
        true

      rescue ex
        # Transaction was rolled back due to exception - run after_rollback callbacks
        @_last_action = :destroy
        execute_rollback_callbacks(:destroy)
        raise ex
      end
    end

    # Transaction-aware destroy method for use within existing transactions
    def destroy_with_connection(connection)
      return false if new_record?

      # Run before_destroy callbacks
      run_before_destroy_callbacks

      query = "DELETE FROM #{self.class.table_name} WHERE #{self.class.primary_key} = ?"
      id_value = get_attribute(self.class.primary_key)

      result = connection.exec(query, id_value)

      if result.rows_affected > 0
        @persisted = false

        # Call dependent associations handling within the same transaction
        {% if @type.ancestors.any? { |a| a.name.stringify.includes?("Associations") } %}
          destroy_dependent_associations(connection)
        {% end %}

        # Set the action for callbacks
        @_last_action = :destroy

        # Run after_destroy callbacks (inside transaction)
        run_after_destroy_callbacks

        true
      else
        false
      end
    end

    def reload
      return self if new_record?

      id_value = get_attribute(self.class.primary_key)
      fresh_record = self.class.find(id_value)

      if fresh_record
        @attributes = fresh_record.@attributes
        sync_instance_variables_from_attributes
        @changed_attributes.clear
        self
      else
        raise "Record no longer exists"
      end
    end

    # ========================================
    # PRIVATE METHODS - PERSISTENCE
    # ========================================

    private def insert_record
      # Run before_save callbacks first
      run_before_save_callbacks

      # Run before_create callbacks
      run_before_create_callbacks

      columns = @attributes.keys
      return false if columns.empty?

      placeholders = (["?"] * columns.size).join(", ")
      query = "INSERT INTO #{self.class.table_name} (#{columns.join(", ")}) VALUES (#{placeholders})"

      begin
        self.class.connection.transaction do |tx|
          result = tx.connection.exec(query, args: @attributes.values.to_a)

          if result.rows_affected > 0
            # Get the inserted ID if it's an auto-increment primary key and not already set
            primary_key_name = self.class.primary_key
            unless @attributes.has_key?(primary_key_name)
              id_value = result.last_insert_id
              @attributes[primary_key_name] = id_value.as(DB::Any)
              # Also set the instance variable directly without going through set_attribute
              # to avoid adding it to changed_attributes
              set_single_instance_variable(primary_key_name, id_value.as(DB::Any))
            end
            @persisted = true
            @changed_attributes.clear

            # Set the action for callbacks
            @_last_action = :create

            # Run after_create callbacks (inside transaction)
            run_after_create_callbacks

            # Run after_save callbacks (inside transaction)
            run_after_save_callbacks

            # Transaction will commit automatically here
          else
            # Explicitly rollback the transaction
            tx.rollback
            @_last_action = :create
            execute_rollback_callbacks(:create)
            return false
          end
        end

        # Transaction committed successfully - run after_commit callbacks
        @_last_action = :create
        execute_commit_callbacks(:create)
        true

      rescue ex
        # Transaction was rolled back due to exception - run after_rollback callbacks
        @_last_action = :create
        execute_rollback_callbacks(:create)
        raise ex
      end
    end

    private def update_record
      return false if @changed_attributes.empty?

      # Run before_save callbacks first
      run_before_save_callbacks

      # Run before_update callbacks
      run_before_update_callbacks

      set_clause = @changed_attributes.map { |attr| "#{attr} = ?" }.join(", ")
      query = "UPDATE #{self.class.table_name} SET #{set_clause} WHERE #{self.class.primary_key} = ?"

      changed_values = @changed_attributes.map { |attr| @attributes[attr] }.to_a
      id_value = get_attribute(self.class.primary_key)
      args = changed_values + [id_value]

      begin
        self.class.connection.transaction do |tx|
          result = tx.connection.exec(query, args: args)

          if result.rows_affected > 0
            @changed_attributes.clear

            # Set the action for callbacks
            @_last_action = :update

            # Run after_update callbacks (inside transaction)
            run_after_update_callbacks

            # Run after_save callbacks (inside transaction)
            run_after_save_callbacks

            # Transaction will commit automatically here
          else
            # Explicitly rollback the transaction
            tx.rollback
            @_last_action = :update
            execute_rollback_callbacks(:update)
            return false
          end
        end

        # Transaction committed successfully - run after_commit callbacks
        @_last_action = :update
        execute_commit_callbacks(:update)
        true

      rescue ex
        # Transaction was rolled back due to exception - run after_rollback callbacks
        @_last_action = :update
        execute_rollback_callbacks(:update)
        raise ex
      end
    end

    # Process Hash(String, DB::Any) attributes to handle association objects
    private def process_db_any_attributes_for_associations(attributes : Hash(String, DB::Any))
      processed = Hash(String, DB::Any).new

      # Get all belongs_to associations for this model
      belongs_to_associations = self.class.associations.select(&.type.belongs_to?)

      attributes.each do |key, value|
        # Check if this key corresponds to a belongs_to association
        association = belongs_to_associations.find { |a| a.name == key }

        if association && value.is_a?(BaseModel)
          # Extract the primary key from the model object
          primary_key_value = value.get_attribute(association.primary_key)
          processed[association.foreign_key] = primary_key_value
        else
          # Regular attribute - keep as is
          processed[key] = value
        end
      end

      processed
    end

    # Process attributes to handle association objects for update
    private def process_association_attributes_for_update(attributes)
      processed = Hash(String, DB::Any).new

      # Get all belongs_to associations for this model (including polymorphic)
      belongs_to_associations = self.class.associations.select { |a| a.type.belongs_to? || a.type.belongs_to_polymorphic? }

      attributes.each do |key, value|
        key_str = key.to_s

        # Check if this key corresponds to a belongs_to association
        association = belongs_to_associations.find { |a| a.name == key_str }

        if association && value.is_a?(BaseModel)
          if association.type.belongs_to_polymorphic?
            # Handle polymorphic association
            primary_key_value = value.get_attribute(association.primary_key)
            processed[association.foreign_key] = primary_key_value
            processed[association.polymorphic_type.not_nil!] = value.class.name.split("::").last
          else
            # Handle regular belongs_to association
            primary_key_value = value.get_attribute(association.primary_key)
            processed[association.foreign_key] = primary_key_value
          end
        else
          # Regular attribute - convert to DB::Any
          processed[key_str] = value.as(DB::Any)
        end
      end

      processed
    end

    # ========================================
    # INSTANCE METHODS - COMPARISON
    # ========================================

    def ==(other : BaseModel)
      return false unless other.class == self.class
      return false if new_record? || other.new_record?

      self_id = get_attribute(self.class.primary_key)
      other_id = other.get_attribute(self.class.primary_key)
      self_id == other_id
    end

    # ========================================
    # PROTECTED METHODS
    # ========================================

    protected def load_from_result_set(rs : DB::ResultSet)
      rs.column_names.each_with_index do |column_name, index|
        value = rs.read
        @attributes[column_name] = value
      end

      sync_instance_variables_from_attributes
      @persisted = true
      @changed_attributes.clear

      # Run after_find callbacks since we loaded from database
      run_after_find_callbacks
    end

    protected def load_from_prefixed_result_set(rs : DB::ResultSet)
      table_prefix = "#{self.class.table_name.gsub("\"", "")}_"

      rs.column_names.each_with_index do |column_name, index|
        value = rs.read

        # Only process columns that belong to this table (have the correct prefix)
        if column_name.starts_with?(table_prefix)
          # Remove the table prefix to get the actual column name
          actual_column_name = column_name[table_prefix.size..-1]
          @attributes[actual_column_name] = value
        end
      end

      sync_instance_variables_from_attributes
      @persisted = true
      @changed_attributes.clear

      # Run after_find callbacks since we loaded from database
      run_after_find_callbacks
    end

    # ========================================
    # PRIVATE METHODS - HELPERS
    # ========================================

    private def set_single_instance_variable(column_name : String, value : DB::Any)
      assign_instance_variables_from_attributes
    end

    private def sync_instance_variables_from_attributes
      @attributes.each do |column_name, value|
        assign_instance_variables_from_attributes
      end
    end

    # Process attributes to handle association objects for create
    private def self.process_association_attributes_for_create(attributes)
      processed = Hash(String, DB::Any).new

      # Get all belongs_to associations for this model (including polymorphic)
      belongs_to_associations = associations.select { |a| a.type.belongs_to? || a.type.belongs_to_polymorphic? }

      attributes.each do |key, value|
        key_str = key.to_s

        # Check if this key corresponds to a belongs_to association
        association = belongs_to_associations.find { |a| a.name == key_str }

        if association && value.is_a?(BaseModel)
          if association.type.belongs_to_polymorphic?
            # Handle polymorphic association
            primary_key_value = value.get_attribute(association.primary_key)
            processed[association.foreign_key] = primary_key_value
            processed[association.polymorphic_type.not_nil!] = value.class.name.split("::").last
          else
            # Handle regular belongs_to association
            primary_key_value = value.get_attribute(association.primary_key)
            processed[association.foreign_key] = primary_key_value
          end
        else
          # Regular attribute - convert to DB::Any
          processed[key_str] = value.as(DB::Any)
        end
      end

      processed
    end

    # ========================================
    # PRIVATE METHODS - CALLBACKS
    # ========================================

    private def run_before_save_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("before_save_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_save_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_save_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_before_create_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("before_create_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_create_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_create_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_before_update_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("before_update_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_update_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_update_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_before_destroy_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("before_destroy_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_destroy_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_destroy_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_before_validation_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("before_validation_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_validation_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_validation_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_commit_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_commit_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_rollback_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_rollback_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_initialize_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_initialize_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_find_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_find_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    private def run_after_touch_callbacks
      {% for method in @type.methods %}
        {% if method.name.stringify.starts_with?("after_touch_callback_") %}
          {{method.name.id}}
        {% end %}
      {% end %}
    end

    # ========================================
    # MACROS - CONFIGURATION
    # ========================================

    macro inherited
      # Call internal macro to set up defaults (can be overridden by explicit primary_key call)
      setup_default_primary_key

      # Register this class for polymorphic lookups
      {% if @type.ancestors.any? { |a| a.name.stringify.includes?("Associations") } %}
        Takarik::Data::Associations.register_polymorphic_class({{@type.name.split("::").last}}, {{@type}})
      {% end %}
    end

    macro setup_default_primary_key
      # Define class variable for this model's primary key
      @@primary_key_name = "id"

      # Automatically define id property if no primary_key macro is called
      define_property_with_accessors(id, Int64)
    end

    macro timestamps
      define_property_with_accessors(created_at, Time)
      define_property_with_accessors(updated_at, Time)

      before_create do
        now = Time.utc
        self.created_at = now
        self.updated_at = now
      end

      before_update do
        self.updated_at = Time.utc
      end
    end

    macro primary_key(name, type = Int64)
      # Override the default primary key
      @@primary_key_name = {{name.id.stringify}}

      # Define the property for the primary key
      define_property_with_accessors({{name}}, {{type}})
    end

    macro column(name, type, **options)
      define_property_with_accessors({{name}}, {{type}})

      # Add to JSON serialization
      def {{name.id}}_json
        @{{name.id}}.try(&.to_json) || "null"
      end
    end

    macro table_name(name)
      def self.table_name
        {% if name.is_a?(SymbolLiteral) %}
          {{name.id.stringify}}
        {% else %}
          {{name}}
        {% end %}
      end
    end

    macro scope(name, &block)
      def self.{{name.id}}
        {{block.body}}
      end
    end

    # ========================================
    # MACROS - CALLBACKS
    # ========================================

    macro check_callback_conditions(condition_if, condition_unless, on_condition = nil, current_action = nil)
      {% if condition_if || condition_unless || on_condition %}
        # Check on conditions first
        {% if on_condition && current_action %}
          {% if on_condition.is_a?(ArrayLiteral) %}
            return unless {{on_condition}}.includes?({{current_action}})
          {% else %}
            return unless {{on_condition}} == {{current_action}}
          {% end %}
        {% end %}

        # Check if conditions (all must be true)
        {% if condition_if %}
          {% if condition_if.is_a?(ArrayLiteral) %}
            # Handle array of conditions - all must be true
            {% for condition in condition_if %}
              {% if condition.is_a?(SymbolLiteral) %}
                return unless {{condition.id}}
              {% elsif condition.is_a?(ProcLiteral) %}
                condition_proc = {{condition}}
                {% if condition.args.size > 0 %}
                  # Proc with parameter(s)
                  return unless condition_proc.call(self)
                {% else %}
                  # Proc without parameters
                  return unless condition_proc.call
                {% end %}
              {% else %}
                {% raise "Unsupported callback condition type: #{condition.class_name}. Use Symbol or Proc." %}
              {% end %}
            {% end %}
          {% elsif condition_if.is_a?(SymbolLiteral) %}
            return unless {{condition_if.id}}
          {% elsif condition_if.is_a?(ProcLiteral) %}
            condition_proc = {{condition_if}}
            {% if condition_if.args.size > 0 %}
              # Proc with parameter(s)
              return unless condition_proc.call(self)
            {% else %}
              # Proc without parameters
              return unless condition_proc.call
            {% end %}
          {% else %}
            {% raise "Unsupported callback condition type: #{condition_if.class_name}. Use Symbol or Proc." %}
          {% end %}
        {% end %}

        # Check unless conditions (all must be false)
        {% if condition_unless %}
          {% if condition_unless.is_a?(ArrayLiteral) %}
            # Handle array of conditions - all must be false
            {% for condition in condition_unless %}
              {% if condition.is_a?(SymbolLiteral) %}
                return if {{condition.id}}
              {% elsif condition.is_a?(ProcLiteral) %}
                condition_proc = {{condition}}
                {% if condition.args.size > 0 %}
                  # Proc with parameter(s)
                  return if condition_proc.call(self)
                {% else %}
                  # Proc without parameters
                  return if condition_proc.call
                {% end %}
              {% else %}
                {% raise "Unsupported callback condition type: #{condition.class_name}. Use Symbol or Proc." %}
              {% end %}
            {% end %}
          {% elsif condition_unless.is_a?(SymbolLiteral) %}
            return if {{condition_unless.id}}
          {% elsif condition_unless.is_a?(ProcLiteral) %}
            condition_proc = {{condition_unless}}
            {% if condition_unless.args.size > 0 %}
              # Proc with parameter(s)
              return if condition_proc.call(self)
            {% else %}
              # Proc without parameters
              return if condition_proc.call
            {% end %}
          {% else %}
            {% raise "Unsupported callback condition type: #{condition_unless.class_name}. Use Symbol or Proc." %}
          {% end %}
        {% end %}
      {% end %}
    end

    macro before_save(method_name = nil, if condition_if = nil, unless condition_unless = nil, on on_condition = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("before_save_callback_") }.size %}

      {% if method_name %}
        def before_save_callback_{{callback_num}}
          {% if on_condition %}
            current_action = new_record? ? :create : :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{method_name.id}}
        end
      {% else %}
        def before_save_callback_{{callback_num}}
          {% if on_condition %}
            current_action = new_record? ? :create : :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{block.body}}
        end
      {% end %}
    end

    macro after_save(method_name = nil, if condition_if = nil, unless condition_unless = nil, on on_condition = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_save_callback_") }.size %}

      {% if method_name %}
        def after_save_callback_{{callback_num}}
          {% if on_condition %}
            current_action = @_last_action || (new_record? ? :create : :update)
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{method_name.id}}
        end
      {% else %}
        def after_save_callback_{{callback_num}}
          {% if on_condition %}
            current_action = @_last_action || (new_record? ? :create : :update)
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{block.body}}
        end
      {% end %}
    end

    macro before_create(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("before_create_callback_") }.size %}

      {% if method_name %}
        def before_create_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def before_create_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro after_create(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_create_callback_") }.size %}

      {% if method_name %}
        def after_create_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def after_create_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro before_update(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("before_update_callback_") }.size %}

      {% if method_name %}
        def before_update_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def before_update_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro after_update(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_update_callback_") }.size %}

      {% if method_name %}
        def after_update_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def after_update_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro before_destroy(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("before_destroy_callback_") }.size %}

      {% if method_name %}
        def before_destroy_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def before_destroy_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro after_destroy(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_destroy_callback_") }.size %}

      {% if method_name %}
        def after_destroy_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def after_destroy_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro before_validation(method_name = nil, if condition_if = nil, unless condition_unless = nil, on on_condition = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("before_validation_callback_") }.size %}

      {% if method_name %}
        def before_validation_callback_{{callback_num}}
          {% if on_condition %}
            current_action = new_record? ? :create : :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{method_name.id}}
        end
      {% else %}
        def before_validation_callback_{{callback_num}}
          {% if on_condition %}
            current_action = new_record? ? :create : :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{block.body}}
        end
      {% end %}
    end

    macro after_validation(method_name = nil, if condition_if = nil, unless condition_unless = nil, on on_condition = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_validation_callback_") }.size %}

      {% if method_name %}
        def after_validation_callback_{{callback_num}}
          {% if on_condition %}
            current_action = new_record? ? :create : :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{method_name.id}}
        end
      {% else %}
        def after_validation_callback_{{callback_num}}
          {% if on_condition %}
            current_action = new_record? ? :create : :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{block.body}}
        end
      {% end %}
    end

    # Generic commit callbacks (ActiveRecord pattern)
    macro after_commit(method_name = nil, if condition_if = nil, unless condition_unless = nil, on on_condition = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_commit_callback_") }.size %}

      {% if method_name %}
        def after_commit_callback_{{callback_num}}
          {% if on_condition %}
            # For after_commit, we need to track what action was performed
            current_action = @_last_action || :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{method_name.id}}
        end
      {% else %}
        def after_commit_callback_{{callback_num}}
          {% if on_condition %}
            # For after_commit, we need to track what action was performed
            current_action = @_last_action || :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{block.body}}
        end
      {% end %}
    end

    macro after_rollback(method_name = nil, if condition_if = nil, unless condition_unless = nil, on on_condition = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_rollback_callback_") }.size %}

      {% if method_name %}
        def after_rollback_callback_{{callback_num}}
          {% if on_condition %}
            # For after_rollback, we need to track what action was being performed
            current_action = @_last_action || :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{method_name.id}}
        end
      {% else %}
        def after_rollback_callback_{{callback_num}}
          {% if on_condition %}
            # For after_rollback, we need to track what action was being performed
            current_action = @_last_action || :update
            check_callback_conditions({{condition_if}}, {{condition_unless}}, {{on_condition}}, current_action)
          {% else %}
            check_callback_conditions({{condition_if}}, {{condition_unless}})
          {% end %}
          {{block.body}}
        end
      {% end %}
    end

    # Object lifecycle callbacks (ActiveRecord pattern)
    macro after_initialize(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_initialize_callback_") }.size %}

      {% if method_name %}
        def after_initialize_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def after_initialize_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro after_find(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_find_callback_") }.size %}

      {% if method_name %}
        def after_find_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def after_find_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    macro after_touch(method_name = nil, if condition_if = nil, unless condition_unless = nil, &block)
      {% callback_num = @type.methods.select { |m| m.name.stringify.starts_with?("after_touch_callback_") }.size %}

      {% if method_name %}
        def after_touch_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{method_name.id}}
        end
      {% else %}
        def after_touch_callback_{{callback_num}}
          check_callback_conditions({{condition_if}}, {{condition_unless}})
          {{block.body}}
        end
      {% end %}
    end

    # ========================================
    # MACROS - PROPERTY DEFINITION
    # ========================================

    macro define_property_with_accessors(name, type)
      # Register this column name (without quotes)
      add_column_name({{name.id.stringify}})

      {% if type == Int32 %}
        property {{name.id}} : Int32?
      {% elsif type == Int64 %}
        property {{name.id}} : Int64?
      {% elsif type == String %}
        property {{name.id}} : String?
      {% elsif type == Bool %}
        property {{name.id}} : Bool?
      {% elsif type == Time %}
        property {{name.id}} : Time?
      {% elsif type == Float64 %}
        property {{name.id}} : Float64?
      {% else %}
        property {{name.id}} : {{type}}?
      {% end %}

      # Override setter to track changes and sync with attributes
      def {{name.id}}=(value : {{type}}?)
        old_value = @{{name.id}}
        @{{name.id}} = value

        # Also update the attributes hash
        if value.nil?
          @attributes.delete({{name.id.stringify}})
        else
          @attributes[{{name.id.stringify}}] = value.as(DB::Any)
        end

        # Track changes
        if old_value != value
          @changed_attributes << {{name.id.stringify}}
        end
        value
      end

      # Override getter to return from instance variable or attributes
      def {{name.id}}
        if @{{name.id}}
          @{{name.id}}
        elsif @attributes.has_key?({{name.id.stringify}})
          value = @attributes[{{name.id.stringify}}]
          {% if type == Int32 %}
            case value
            when Int32
              value
            when Int64
              value.to_i32
            when String
              value.to_i32?
            else
              nil
            end
          {% elsif type == Int64 %}
            case value
            when Int64
              value
            when Int32
              value.to_i64
            when String
              value.to_i64?
            else
              nil
            end
          {% elsif type == String %}
            case value
            when String
              value
            when Nil
              nil
            else
              value.to_s
            end
          {% elsif type == Bool %}
            case value
            when 1, "1", "true", "t", true
              true
            when 0, "0", "false", "f", false, nil
              false
            else
              nil
            end
          {% elsif type == Float64 %}
            case value
            when Float64
              value
            when Int32, Int64
              value.to_f64
            when String
              value.to_f64?
            else
              nil
            end
          {% elsif type == Time %}
            value.is_a?(Time) ? value : nil
          {% else %}
            value.as?({{type}})
          {% end %}
        else
          nil
        end
      end
    end

    macro assign_instance_variables_from_attributes
      {% begin %}
        case column_name
        {% for ivar in @type.instance_vars %}
          {% excluded_vars = ["attributes", "persisted", "changed_attributes", "validation_errors", "_last_action"] %}
          {% unless excluded_vars.includes?(ivar.name.stringify) %}
            when {{ivar.name.stringify}}
              # Extract the type from the instance variable type and do direct assignment
              {% if ivar.type.stringify.includes?("Int32") %}
                @{{ivar.name}} = case value
                when Int32
                  value
                when Int64
                  value.to_i32
                when String
                  value.to_i32?
                else
                  nil
                end
              {% elsif ivar.type.stringify.includes?("Int64") %}
                @{{ivar.name}} = case value
                when Int64
                  value
                when Int32
                  value.to_i64
                when String
                  value.to_i64?
                else
                  nil
                end
              {% elsif ivar.type.stringify.includes?("String") %}
                @{{ivar.name}} = case value
                when String
                  value
                when Nil
                  nil
                else
                  value.to_s
                end
              {% elsif ivar.type.stringify.includes?("Bool") %}
                @{{ivar.name}} = case value
                when 1, "1", "true", "t", true
                  true
                when 0, "0", "false", "f", false, nil
                  false
                else
                  nil
                end
              {% elsif ivar.type.stringify.includes?("Float64") %}
                @{{ivar.name}} = case value
                when Float64
                  value
                when Int32, Int64
                  value.to_f64
                when String
                  value.to_f64?
                else
                  nil
                end
              {% elsif ivar.type.stringify.includes?("Time") %}
                @{{ivar.name}} = value.is_a?(Time) ? value : nil
              {% else %}
                @{{ivar.name}} = value
              {% end %}
          {% end %}
        {% end %}
        end
      {% end %}
    end

    macro generate_base_model_where_overloads
      {% for type in [Int32, Int64, String, Float32, Float64, Bool, Time] %}
        # ========================================
        # WHERE CLASS METHOD OVERLOADS FOR {{type}}
        # ========================================

        def self.where(condition : String, param : {{type}})
          all.where(condition, param)
        end

        def self.where(condition : String, *params : {{type}})
          all.where(condition, *params)
        end

        def self.where(column : String, values : Array({{type}}))
          all.where(column, values)
        end

        def self.where(column_with_operator : String, value : {{type}})
          all.where(column_with_operator, value)
        end

        # ========================================
        # NOT CLASS METHOD OVERLOADS FOR {{type}}
        # ========================================

        def self.not(condition : String, param : {{type}})
          all.not(condition, param)
        end

        def self.not(condition : String, *params : {{type}})
          all.not(condition, *params)
        end

        def self.not(column_with_operator : String, value : {{type}})
          all.not(column_with_operator, value)
        end

        def self.not(column : String, values : Array({{type}}))
          all.not(column, values)
        end

        # ========================================
        # OR CLASS METHOD OVERLOADS FOR {{type}}
        # ========================================

        def self.or(condition : String, param : {{type}})
          all.or(condition, param)
        end

        def self.or(condition : String, *params : {{type}})
          all.or(condition, *params)
        end

        def self.or(column_with_operator : String, value : {{type}})
          all.or(column_with_operator, value)
        end

        def self.or(column : String, values : Array({{type}}))
          all.or(column, values)
        end
      {% end %}

      # ========================================
      # RANGE CLASS METHOD OVERLOADS
      # ========================================

      {% for type in [Int32, Int64, Float32, Float64, Time, String] %}
        def self.where(column : String, range : Range({{type}}, {{type}}))
          all.where(column, range)
        end

        def self.not(column : String, range : Range({{type}}, {{type}}))
          all.not(column, range)
        end

        def self.or(column : String, range : Range({{type}}, {{type}}))
          all.or(column, range)
        end
      {% end %}
    end

    generate_base_model_where_overloads

    # Helper methods for DRY callback execution
    private def execute_commit_callbacks(action : Symbol)
      @_last_action = action
      run_after_commit_callbacks
    end

    private def execute_rollback_callbacks(action : Symbol)
      @_last_action = action
      run_after_rollback_callbacks
    end
  end
end
