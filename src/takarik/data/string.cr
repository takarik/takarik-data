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

  def classify
    Wordsmith::Inflector.classify(self)
  end

  def tableize
    Wordsmith::Inflector.tableize(self)
  end

  def foreign_key
    Wordsmith::Inflector.foreign_key(self)
  end
end
