class ActiveDocument::Base
  def self.path(path = nil)
    if path
      @path = path
    else
      @path || (base_class? ? ActiveDocument::DEFAULT_PATH : super)
    end
  end

  def self.database_name(database_name = nil)
    if database_name
      raise 'cannot modify database_name after db has been initialized' if @database_name
      @database_name = database_name
    else
      return nil if self == ActiveDocument::Base
      @database_name ||= base_class? ? name.underscore.gsub('/', '-').pluralize : super
    end
  end
  
  def self.base_class?
    self == base_class
  end

  def self.base_class(klass = self)
    if klass == ActiveDocument::Base or klass.superclass == ActiveDocument::Base
      klass
    else
      base_class(klass.superclass)
    end
  end

  @@environment = {}
  def self.environment
    @@environment[path] ||= ActiveDocument::Environment.new(path)
  end

  def self.transaction(&block)
    environment.transaction(&block)
  end

  def self.create(*args)
    model = new(*args)
    model.save
    model
  end

  def self.index_by(field, opts = {})
    field = field.to_sym
    raise "index on #{field} already exists" if databases[field]
    databases[field] = ActiveDocument::Database.new(opts.merge(:field => field, :model_class => self))    
  end

  def self.id(field_or_fields)
    if field_or_fields.kind_of?(Array)
      define_method(:id) do
        field_or_fields.collect {|field| self.send(field)}.flatten
      end

      (class << self; self; end).instance_eval do
        define_method("find_by_#{field_or_fields.first}") do |*keys|
          opts = keys.last.kind_of?(Hash) ? keys.pop : {}
          opts[:partial] = true
          database(:id).find(keys, opts)
        end
      end
    else
      define_method(:id) do
        self.send(field_or_fields)
      end
    end
  end

  def self.databases
    @databases ||= { :id  => ActiveDocument::Database.new(:model_class => self, :unique => true) }
  end      

  def self.open_database
    environment.open
    databases[:id].open # Must be opened first for associate to work.
    databases.values.each {|database| database.open}
  end

  def self.close_database
    databases.values.each {|database| database.close}
    environment.close
  end

  def self.database(field = :id)
    field = field.to_sym
    database = databases[field]
    database ||= base_class.database(field) unless base_class?
    database
  end

  def self.find_by(field, *keys)
    opts = keys.last.kind_of?(Hash) ? keys.pop : {}
    database(field).find(keys, opts)
  end

  def self.find(id, opts = {})
    doc = database.find([id], opts).first
    raise ActiveDocument::DocumentNotFound, "Couldn't find #{name} with id #{id.inspect}" unless doc
    doc
  end

  def self.method_missing(method_name, *args)
    method_name = method_name.to_s
    if method_name =~ /^find_by_(\w+)$/
      field = $1.to_sym
      return find_by(field, *args) if databases[field]
    end
    raise NoMethodError, "undefined method `#{method_name}' for #{self}"
  end

  def self.timestamps
    reader(:created_at, :updated_at)
  end

  def self.reader(*attrs)
    attrs.each do |attr|
      define_method(attr) do
        attributes[attr]
      end
    end
  end

  def self.writer(*attrs)
    attrs.each do |attr|
      define_method("#{attr}=") do |value|
        raise 'cannot modify readonly document' if readonly?
        attributes[attr] = value
      end
    end
  end

  def self.accessor(*attrs)
    reader(*attrs)
    writer(*attrs)
  end
    
  def initialize(attributes = {})
    if attributes.kind_of?(String)
      @attributes, @saved_attributes = Marshal.load(attributes)
    else
      @attributes = attributes
    end
  end

  attr_reader :saved_attributes

  def attributes
    @attributes ||= Marshal.load(Marshal.dump(saved_attributes))
  end

  def ==(other)
    return false if other.nil?
    attributes == other.attributes
  end

  def new_record?
    @saved_attributes.nil?
  end

  def changed?(field = nil)
    return false unless @attributes and @saved_attributes

    if field
      attributes[field] != saved_attributes[field]
    else
      attributes != saved_attributes
    end
  end

  def save
    attributes[:updated_at] = Time.now if respond_to?(:updated_at)
    attributes[:created_at] = Time.now if respond_to?(:created_at) and new_record?
    @saved_attributes = attributes
    @attributes       = nil
    self.class.database.save(self)

    true
  end

  def _dump(ignored)
    Marshal.dump([@attributes, @saved_attributes])
  end

  def self._load(data)
    new(data)
  end
end
