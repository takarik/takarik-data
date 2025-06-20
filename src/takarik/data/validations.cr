module Takarik::Data
  module Validations
    # ========================================
    # EXCEPTION CLASSES
    # ========================================

    class ValidationError < Exception
      getter errors : Hash(String, Array(String))

      def initialize(@errors : Hash(String, Array(String)))
        message = @errors.map { |field, msgs| "#{field}: #{msgs.join(", ")}" }.join("; ")
        super(message)
      end
    end

    # ========================================
    # STRUCTS
    # ========================================

    struct ValidationResult
      getter valid : Bool
      getter errors : Hash(String, Array(String))

      def initialize(@valid : Bool, @errors : Hash(String, Array(String)) = {} of String => Array(String))
      end

      def valid?
        @valid
      end

      def invalid?
        !@valid
      end
    end

    # ========================================
    # MODULE VARIABLES
    # ========================================

    # Validation rules storage
    @@validation_rules = {} of String => Array(Proc(BaseModel, ValidationResult))

    # ========================================
    # INCLUDED HOOK
    # ========================================

    macro included
      # Instance variable to store validation errors
      @validation_errors = {} of String => Array(String)

      # Class variable to store validation rules for this model
      @@validation_rules = {} of String => Array(Proc(BaseModel, ValidationResult))

      # Extend the class with class methods
      extend ClassMethods
    end

    # ========================================
    # CLASS METHODS MODULE
    # ========================================

    module ClassMethods
      def add_validation(field : String, &block : BaseModel -> ValidationResult)
        @@validation_rules[field] ||= [] of Proc(BaseModel, ValidationResult)
        @@validation_rules[field] << block
      end
    end

    # ========================================
    # INSTANCE METHODS
    # ========================================

    def valid?
      validate.valid?
    end

    def invalid?
      !valid?
    end

    def validate
      @validation_errors.clear
      all_valid = true

      @@validation_rules.each do |field, rules|
        rules.each do |rule|
          result = rule.call(self)
          unless result.valid?
            all_valid = false
            result.errors.each do |error_field, messages|
              @validation_errors[error_field] ||= [] of String
              @validation_errors[error_field].concat(messages)
            end
          end
        end
      end

      ValidationResult.new(all_valid, @validation_errors.dup)
    end

    def errors
      @validation_errors
    end

    def errors_full_messages
      @validation_errors.flat_map do |field, messages|
        messages.map { |message| "#{field.capitalize} #{message}" }
      end
    end

    # ========================================
    # VALIDATION MACROS
    # ========================================

    # Generic validates macro (Rails-style) - dispatches to specific validation macros
    macro validates(field, **options)
      {% if options[:presence] %}
        validates_presence_of {{field}}
      {% end %}

      {% if options[:length] %}
        {% length_opts = options[:length] %}
        validates_length_of {{field}}{% for key, value in length_opts %}, {{key}}: {{value}}{% end %}
      {% end %}

      {% if options[:format] %}
        {% format_opts = options[:format] %}
        validates_format_of {{field}}, with: {{format_opts[:with]}}{% if format_opts[:message] %}, message: {{format_opts[:message]}}{% end %}
      {% end %}

      {% if options[:uniqueness] %}
        validates_uniqueness_of {{field}}
      {% end %}

      {% if options[:numericality] %}
        {% num_opts = options[:numericality] %}
        validates_numericality_of {{field}}{% for key, value in num_opts %}, {{key}}: {{value}}{% end %}
      {% end %}
    end

    # Specific validation macros
    macro validates_presence_of(*fields)
      {% for field in fields %}
        {% field_name = field.stringify.gsub(/^:/, "") %}
        add_validation({{field_name}}) do |record|
          value = record.get_attribute({{field_name}})
          if value.nil? || (value.is_a?(String) && value.to_s.strip.empty?)
            errors = Hash(String, Array(String)).new
            errors[{{field_name}}] = ["can't be blank"]
            ValidationResult.new(false, errors)
          else
            ValidationResult.new(true)
          end
        end
      {% end %}
    end

    macro validates_length_of(field, **options)
      {% field_name = field.stringify.gsub(/^:/, "") %}
      add_validation({{field_name}}) do |record|
        value = record.get_attribute({{field_name}})
        errors = [] of String

        if value && value.is_a?(String)
          str_value = value.to_s

          {% if options[:minimum] %}
            if str_value.size < {{options[:minimum]}}
              errors << "is too short (minimum is {{options[:minimum]}} characters)"
            end
          {% end %}

          {% if options[:maximum] %}
            if str_value.size > {{options[:maximum]}}
              errors << "is too long (maximum is {{options[:maximum]}} characters)"
            end
          {% end %}

          {% if options[:is] %}
            if str_value.size != {{options[:is]}}
              errors << "is the wrong length (should be {{options[:is]}} characters)"
            end
          {% end %}

          {% if options[:in] || options[:within] %}
            {% range = options[:in] || options[:within] %}
            {% if range.is_a?(RangeLiteral) %}
              {% range_start = range.begin %}
              {% range_end = range.end %}
              unless str_value.size >= {{range_start}} && str_value.size <= {{range_end}}
                errors << "is the wrong length (should be within {{range}})"
              end
            {% else %}
              # Handle runtime range
              unless ({{range}}).includes?(str_value.size)
                errors << "is the wrong length (should be within {{range}})"
              end
            {% end %}
          {% end %}
        end

        if errors.empty?
          ValidationResult.new(true)
        else
          error_hash = Hash(String, Array(String)).new
          error_hash[{{field_name}}] = errors
          ValidationResult.new(false, error_hash)
        end
      end
    end

    macro validates_format_of(field, with regex, message = nil)
      {% field_name = field.stringify.gsub(/^:/, "") %}
      add_validation({{field_name}}) do |record|
        value = record.get_attribute({{field_name}})

        if value && value.is_a?(String)
          str_value = value.to_s
          unless str_value.matches?({{regex}})
            message = {{message}} || "is invalid"
            error_hash = Hash(String, Array(String)).new
            error_hash[{{field_name}}] = [message]
            ValidationResult.new(false, error_hash)
          else
            ValidationResult.new(true)
          end
        else
          ValidationResult.new(true)
        end
      end
    end

    macro validates_uniqueness_of(*fields)
      {% for field in fields %}
        {% field_name = field.stringify.gsub(/^:/, "") %}
        add_validation({{field_name}}) do |record|
          value = record.get_attribute({{field_name}})

          if value
            query = "SELECT COUNT(*) FROM #{record.class.table_name} WHERE #{{{field_name}}} = ?"
            args = [value]

            # Exclude current record if it's persisted
            if record.persisted?
              id_value = record.get_attribute(record.class.primary_key)
              if id_value
                query += " AND #{record.class.primary_key} != ?"
                args << id_value
              end
            end

            count = Takarik::Data.scalar_with_logging(record.class.connection, query, args).as(Int64)

            if count > 0
              error_hash = Hash(String, Array(String)).new
              error_hash[{{field_name}}] = ["has already been taken"]
              ValidationResult.new(false, error_hash)
            else
              ValidationResult.new(true)
            end
          else
            ValidationResult.new(true)
          end
        end
      {% end %}
    end

    macro validates_numericality_of(field, **options)
      {% field_name = field.stringify.gsub(/^:/, "") %}
      add_validation({{field_name}}) do |record|
        value = record.get_attribute({{field_name}})
        errors = [] of String

        if value
          numeric_value = case value
                         when Int32, Int64, Float64
                           value.as(Number)
                         when String
                           begin
                             value.to_s.to_f64
                           rescue
                             errors << "is not a number"
                             nil
                           end
                         else
                           errors << "is not a number"
                           nil
                         end

          if numeric_value && errors.empty?
            {% if options[:greater_than] %}
              if numeric_value <= {{options[:greater_than]}}
                errors << "must be greater than {{options[:greater_than]}}"
              end
            {% end %}

            {% if options[:greater_than_or_equal_to] %}
              if numeric_value < {{options[:greater_than_or_equal_to]}}
                errors << "must be greater than or equal to {{options[:greater_than_or_equal_to]}}"
              end
            {% end %}

            {% if options[:less_than] %}
              if numeric_value >= {{options[:less_than]}}
                errors << "must be less than {{options[:less_than]}}"
              end
            {% end %}

            {% if options[:less_than_or_equal_to] %}
              if numeric_value > {{options[:less_than_or_equal_to]}}
                errors << "must be less than or equal to {{options[:less_than_or_equal_to]}}"
              end
            {% end %}

            {% if options[:equal_to] %}
              if numeric_value != {{options[:equal_to]}}
                errors << "must be equal to {{options[:equal_to]}}"
              end
            {% end %}

            {% if options[:only_integer] %}
              unless numeric_value == numeric_value.to_i
                errors << "must be an integer"
              end
            {% end %}

            {% if options[:odd] %}
              unless numeric_value.to_i.odd?
                errors << "must be odd"
              end
            {% end %}

            {% if options[:even] %}
              unless numeric_value.to_i.even?
                errors << "must be even"
              end
            {% end %}
          end
        end

        if errors.empty?
          ValidationResult.new(true)
        else
          error_hash = Hash(String, Array(String)).new
          error_hash[{{field_name}}] = errors
          ValidationResult.new(false, error_hash)
        end
      end
    end
  end
end
