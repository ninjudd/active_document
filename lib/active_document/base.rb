class ActiveDocument::Base
  def self.path(path = nil)    
    @path = path if path
    @path ||= (self == ActiveDocument::Base ? nil : ActiveDocument::Base.path)
  end

  def self.db_config(config = {})
    @db_config ||= {}
    @db_config.merge!(config)
    ActiveDocument.db_config.merge(@db_config)
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

  def self.database
    @database
  end

  def database
    self.class.database
  end

  def self.transaction(&block)
    database.transaction(&block)
  end

  def transaction(&block)
    self.class.transaction(&block)
  end

  def self.create(*args)
    model = new(*args)
    model.save
    model
  end

  def self.primary_key(field_or_fields, opts = {})
    raise 'primary key already defined' if @database

    if @partition_by = opts[:partition_by]
      @database = ActiveDocument::PartitionedDatabase.new(
        :model_class  => self,
        :environment  => environment,
        :partition_by => @partition_by
      )
      (class << self; self; end).instance_eval do
        alias_method opts[:partition_by].to_s.pluralize, :partitions
        alias_method "with_#{opts[:partition_by]}", :with_partition
        alias_method "with_each_#{opts[:partition_by]}", :with_each_partition
      end
    else
      @database = environment.new_database(:model_class => self)
    end

    field = define_field_accessor(field_or_fields)
    define_find_methods(field, :field => :primary_key) # find_by_field1_and_field2
    
    define_field_accessor(field_or_fields, :primary_key)
    define_find_methods(:primary_key) # find_by_primary_key

    # Define shortcuts for partial keys.
    define_partial_shortcuts(field_or_fields, :primary_key)
  end

  def self.partitions
    database.partitions
  end

  def self.with_partition(partition, &block)
    database.with_partition(partition, &block)
  end

  def self.with_each_partition(&block)
    database.partitions.each do |partition|
      database.with_partition(partition, &block)
    end
  end

  def self.partition_by
    @partition_by
  end

  def partition_by
    self.class.partition_by
  end

  def partition
    send(partition_by) if partition_by
  end

  def self.index_by(field_or_fields, opts = {})
    raise "cannot have a multi_key index on an aggregate key" if opts[:multi_key] and field_or_fields.kind_of?(Array)

    field = define_field_accessor(field_or_fields)
    database.index_by(field, opts)

    field_name = opts[:multi_key] ? field.to_s.singularize : field
    define_find_methods(field_name, :field => field) # find_by_field1_and_field2

    # Define shortcuts for partial keys.
    define_partial_shortcuts(field_or_fields, field)
  end

  def self.close_environment
    # Will close all databases in the environment.
    environment.close
  end

  def self.find_by(field, *keys)
    opts = extract_opts(keys)
    opts[:field] = field
    keys << :all if keys.empty?
    database.find(keys, opts)
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

  def self.define_find_methods(name, config = {})
    field = config[:field] || name

    (class << self; self; end).instance_eval do
      define_method("find_by_#{name}") do |*args|
        modify_opts(args) do |opts|
          opts[:limit] = 1
          opts[:partial] ||= config[:partial]
        end
        find_by(field, *args).first
      end

      define_method("find_all_by_#{name}") do |*args|
        modify_opts(args) do |opts|
          opts[:partial] ||= config[:partial]
        end
        find_by(field, *args)
      end
    end
  end

  def self.define_partial_shortcuts(fields, primary_field)
    return unless fields.kind_of?(Array)

    (fields.size - 1).times do |i|
      name = fields[0..i].join('_and_')
      next if respond_to?("find_by_#{name}")
      define_find_methods(name, :field => primary_field, :partial => true)
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

  def self.bool_reader(*attrs)
    attrs.each do |attr|
      define_method(attr) do
        !!attributes[attr]
      end

      define_method("#{attr}?") do
        !!attributes[attr]
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

  def self.bool_accessor(*attrs)
    bool_reader(*attrs)
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

    if partition_by and partition.nil?
      set_method = "#{partition_by}="
      self.send(set_method, database.partition) if respond_to?(set_method)
    end
  end

  attr_reader :saved_attributes

  def attributes
    @attributes ||= Marshal.load(Marshal.dump(saved_attributes))
  end

  save_method :update_attributes
  def update_attributes(attrs = {})
    attrs.each do |field, value|
      self.send("#{field}=", value)
    end
  end

  def to_json(*args)
    attributes.to_json(*args)
  end

  def ==(other)
    return false unless other.class == self.class
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
    time = Time.now
    attributes[:updated_at] = time if respond_to?(:updated_at)
    attributes[:created_at] = time if respond_to?(:created_at) and new_record?

    opts = {}
    if changed?(:primary_key) or (partition_by and changed?(partition_by))
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

  def self.modify_opts(args)
    opts = extract_opts(args)
    yield(opts)
    args << opts
  end
end
