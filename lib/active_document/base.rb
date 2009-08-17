class ActiveDocument::Base
  def self.path(path = nil)
    if path
      @path = path
    else
      @path || (base_class? ? DEFAULT_PATH : super)
    end
  end

  def self.database_name(database_name = nil)
    if database_name
      raise 'cannot modify database_name after db has been initialized' if @database_name
      @database_name = database_name
    else
      return nil if self == ActiveDocument::Base
      @database_name ||= base_class? ? name.underscore.pluralize : super
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
  
  def self.databases
    @databases ||= { :id => ActiveDocument::Database.new(:model_class => self, :unique => true) }
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
  
  def self.find(key)
    doc = find_by(:id, key).first
    raise ActiveDocument::DocumentNotFound, "Couldn't find #{name} with key #{key}" unless doc
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
    
  def initialize(attributes = {}, new_record = true)
    @attributes = attributes
    @new_record = new_record
  end

  attr_reader :attributes

  def ==(other)
    attributes == other.attributes
  end

  def new_record?
    @new_record
  end

  def readonly?
    false # not @new_record and not @writable
  end

  def save
    raise 'cannot save readonly document' if readonly?

    attributes[:updated_at] = Time.now if respond_to?(:updated_at)    
    if new_record?      
      attributes[:created_at] = Time.now if respond_to?(:created_at)
      @new_record = false
    end

    self.class.database.save(self)
    true
  end

  def _dump(ignored)
    data = [@attributes]
    data << true if new_record?
    Marshal.dump(data)
  end

  def self._load(str)
    attributes, new_record = Marshal.load(str)
    new(attributes, new_record)
  end
end
