class ActiveDocument::PartitionedDatabase
  def initialize(opts)
    @environment  = opts[:environment]
    @model_class  = opts[:model_class]
    @partition_by = opts[:partition_by]
    @base_name    = opts[:base_name] || @model_class.database_name
  end
  attr_reader :environment, :model_class, :partition_by, :base_name, :partition

  def indexes
    @indexes ||= {}
  end

  def index_by(field, opts = {})
    raise "index on #{field} already exists" if indexes[field]
    indexes[field] = opts
  end

  def databases
    @databases ||= {}
  end

  def database(partition)
    raise 'partition value required' if partition.nil?
    partition = partition.to_s
    databases[partition] ||= begin
      database = environment.new_database(
        :model_class => model_class,
        :name        => [partition, base_name].join('-')
      )
      indexes.each do |field, opts|
        database.index_by(field, opts)
      end
      database
    end
  end

  def partitions
    Dir[environment.path + "/*-#{base_name}"].collect do |file|
      File.basename(file).split('-').first
    end
  end

  def with_partition(partition)
    @partition, old_partition = partition, @partition
    yield
  ensure
    @partition = old_partition
  end

  def close
    partitions.each do |value, partition|
      partition.close
    end
    @partitions.clear
  end

  def transaction(&block)
    environment.transaction(&block)
  end

  def find(keys, opts = {}, &block)
    partition = opts[partition_by] || self.partition
    database(partition).find(keys, opts, &block)
  end

  def save(model, opts = {})
    partition = model.send(partition_by)
    database(partition).save(model, opts)
  end

  def delete(model)
    partition = model.send(partition_by)
    database(partition).delete(model)
  end

  # Deletes all records in the database. Beware!
  def truncate!
    partitions.each do |partition|
      database(partition).truncate!
    end
  end
end
