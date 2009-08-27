class ActiveDocument::Base
  def self.path(path = nil)
    if path
      @path = path
    else
      @path ||= (self == ActiveDocument::Base ? ActiveDocument::DEFAULT_PATH : ActiveDocument::Base.path)
    end
  end

  def self.database_name(database_name = nil)
    if database_name
      raise 'cannot modify database_name after db has been initialized' if @database_name
      @database_name = database_name
    else
      return if self == ActiveDocument::Base 
      @database_name ||= name.underscore.gsub('/', '-').pluralize
    end
  end

  @@environment = {}
  def self.environment
    @@environment[path] ||= ActiveDocument::Environment.new(path)
  end

  def self.transaction(&block)
    open_database
    environment.transaction(&block)
  end

  def transaction(&block)
    self.class.transaction(&block)
  end

  def self.create(*args)
    model = new(*args)
    model.save
    model
  end

  def self.primary_key(field_or_fields)
    databases[:primary_key] = environment.database(:model_class => self, :unique => true)

    field = define_field_accessor(field_or_fields)
    define_find_methods(field, :field => :primary_key) # find_by_field1_and_field2
    
    define_field_accessor(field_or_fields, :primary_key)
    define_find_methods(:primary_key) # find_by_primary_key

    # Define shortcuts for partial keys.
    if field_or_fields.kind_of?(Array) and not respond_to?(field_or_fields.first)
      define_find_methods(field_or_fields.first, :field => :primary_key, :partial => true) # find_by_field1
    end
  end

  def self.index_by(field_or_fields, opts = {})
    field = define_field_accessor(field_or_fields)
    raise "index on #{field} already exists" if databases[field]
    databases[field] = environment.database(opts.merge(:field => field, :model_class => self))
    define_find_methods(field) # find_by_field1_and_field2

    # Define shortcuts for partial keys.
    if field_or_fields.kind_of?(Array) and not respond_to?(field_or_fields.first)
      define_find_methods(field_or_fields.first, :field => field, :partial => true) # find_by_field1
    end
  end

  def self.databases
    @databases ||= {}
  end      

  def self.open_database
    unless @database_open
      environment.open
      databases[:primary_key].open # Must be opened first for associate to work.
      databases.values.each {|database| database.open}
      @database_open = true
      at_exit { close_database }
    end
  end

  def self.close_database
    if @database_open
      databases.values.each {|database| database.close}
      environment.close
      @database_open = false
    end
  end

  def self.database(field = nil)
    open_database # Make sure the database is open.
    field ||= :primary_key
    field = field.to_sym
    return if self == ActiveDocument::Base
    databases[field] ||= super
  end

  def database(field = nil)
    self.class.database(field)
  end

  def self.find_by(field, *keys)
    opts = extract_opts(keys)
    keys << :all if keys.empty?
    database(field).find(keys, opts)
  end

  def self.find(key, opts = {})
    doc = database.find([key], opts).first
    raise ActiveDocument::DocumentNotFound, "Couldn't find #{name} with id #{key.inspect}" unless doc
    doc
  end
  
  def self.define_field_accessor(field_or_fields, field = nil)    
    if field_or_fields.kind_of?(Array)
      field ||= field_or_fields.join('_and_').to_sym
      define_method(field) do
        field_or_fields.collect {|f| self.send(f)}.flatten
      end
    elsif field
      define_method(field) do
        self.send(field_or_fields)
      end
    else
      field = field_or_fields.to_sym
    end
    field
  end

  def self.define_find_methods(name, opts = {})
    field = opts[:field] || name

    (class << self; self; end).instance_eval do
      define_method("find_by_#{name}") do |*args|
        merge_opts(args, :limit => 1, :partial => opts[:partial])
        find_by(field, *args).first
      end

      define_method("find_all_by_#{name}") do |*args|
        merge_opts(args, :partial => opts[:partial])
        find_by(field, *args)
      end
    end
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
        attributes[attr] = value
      end
    end
  end

  def self.accessor(*attrs)
    reader(*attrs)
    writer(*attrs)
  end
  
  def self.save_method(method_name)
    define_method("#{method_name}!") do |*args|
      value = send(method_name, *args)
      save
      value
    end
  end

  def initialize(attributes = {})
    if attributes.kind_of?(String)
      @attributes, @saved_attributes = Marshal.load(attributes)      
    else
      @attributes = attributes
    end
    @attributes       = HashWithIndifferentAccess.new(@attributes)       if @attributes
    @saved_attributes = HashWithIndifferentAccess.new(@saved_attributes) if @saved_attributes
  end

  attr_reader :saved_attributes

  def attributes
    @attributes ||= Marshal.load(Marshal.dump(saved_attributes))
  end

  def to_json(*fields)
    if fields.empty?
      attributes.to_json
    else
      slice = {}
      fields.each do |field|
        slice[field] = attributes[field]
      end
      slice.to_json
    end
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
      send(field) != saved.send(field)
    else
      attributes != saved_attributes
    end
  end

  def saved
    raise 'no saved attributes for new record' if new_record?
    @saved ||= self.class.new(saved_attributes)
  end

  def save
    attributes[:updated_at] = Time.now if respond_to?(:updated_at)
    attributes[:created_at] = Time.now if respond_to?(:created_at) and new_record?

    opts = {}
    if changed?(:primary_key)
      opts[:create] = true
      saved.destroy
    else
      opts[:create] = new_record?
    end

    @saved_attributes = attributes
    @attributes       = nil
    @saved            = nil
    database.save(self, opts)
  end

  def destroy
    database.delete(self)
  end

  def _dump(ignored)
    attributes       = @attributes.to_hash       if @attributes
    saved_attributes = @saved_attributes.to_hash if @saved_attributes
    Marshal.dump([attributes, saved_attributes])
  end

  def self._load(data)
    new(data)
  end

private

  def self.extract_opts(args)
    args.last.kind_of?(Hash) ? args.pop : {}
  end

  def self.merge_opts(args, opts)
    args << extract_opts(args).merge(opts)
  end
end
