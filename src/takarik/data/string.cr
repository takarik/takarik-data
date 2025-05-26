require "wordsmith"

# String extensions for inflection using Wordsmith
class String
  def underscore
    Wordsmith::Inflector.underscore(self)
  end

  def camelcase
    Wordsmith::Inflector.camelize(self)
  end

  def pluralize
    Wordsmith::Inflector.pluralize(self)
  end

  def singularize
    Wordsmith::Inflector.singularize(self)
  end
end
