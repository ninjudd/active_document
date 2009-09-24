class ActiveDocument::Database
  def initialize(opts)
    @environment = opts[:environment]
    @model_class = opts[:model_class]
    @field       = opts[:field]
    @unique      = opts[:unique]
    @multi_key   = opts[:multi_key]
    @name        = [@model_class.database_name, @field].compact.join('_by_')
  end
  attr_reader :environment, :model_class, :primary_database, :field, :name

  define_method(:unique?)    { @unique                }
  define_method(:multi_key?) { @multi_key             }
  define_method(:primary?)   { @primary_database.nil? }

  def config
    model_class.db_config
  end
  
  def index
    @index ||= {}
  end

  def indexes
    index.values
  end

  def index_by(field, opts = {})
    raise 'creating an index only allowed on primary database' unless primary?
    raise "index on #{field} already exists" if index[field]
    index[field] = environment.new_database(opts.merge(:model_class => model_class, :field => field))
  end

  def db
    # Ensure that the primary database is open.
    primary_database.db unless primary?

    if @db.nil?
      @db = environment.env.db
      @db.flags = Bdb::DB_DUPSORT unless unique?
      @db.pagesize = config[:page_size] if config[:page_size]
      @db.open(nil, name, nil, Bdb::Db::BTREE, Bdb::DB_CREATE | Bdb::DB_AUTO_COMMIT, 0)
      indexes.each do |index|
        index.associate(self)
      end
    end
    @db
  rescue Exception => e
    raise ActiveDocument.wrap_error(e)
  end

  def associate(primary)
    @primary_database = primary

    index_callback = lambda do |db, key, data|
      model = Marshal.load(data)
      return unless model.kind_of?(model_class)
      
      index_key = model.send(field)
      if multi_key? and index_key.kind_of?(Array)
        # Index multiple keys. If the key is an array, you must wrap it with an outer array.
        index_key.collect {|k| Tuple.dump(k)}
      elsif index_key
        # Index a single key.
        Tuple.dump(index_key)
      end
    end
    primary.db.associate(nil, db, Bdb::DB_CREATE, index_callback)
  end

  def close
    return unless @db
    @db.close(0)
    @db = nil
    indexes.each do |index|
      index.close
    end
  rescue Bdb::DbError => e
    raise ActiveDocument.wrap_error(e)
  end

  def transaction(&block)
    if primary?
      environment.transaction(&block)
    else
      raise 'cannot start a transaction on a secondary database' if block_given?
      environment.transaction
    end
  end

  def find(keys, opts = {}, &block)
    # Delegate find to a secondary index if necessary.
    field = opts[:field]
    return index[field].find(keys, opts, &block) if primary? and field and field != :primary_key

    models = block_given? ? BlockArray.new(block) : []
    flags  = opts[:modify] ? Bdb::DB_RMW : 0

    keys.uniq.each do |key|
      if opts[:partial] and not key.kind_of?(Range)
        first = [*key]
        last  = first + [true]
        key   = first..last
      end

      if key == :all
        with_cursor do |cursor|
          if opts[:reverse]
            k,v  = cursor.get(nil, nil, Bdb::DB_LAST | flags)          # Start at the last item.
            iter = lambda {cursor.get(nil, nil, Bdb::DB_PREV | flags)} # Move backward.
          else
            k,v  = cursor.get(nil, nil, Bdb::DB_FIRST | flags)         # Start at the first item.
            iter = lambda {cursor.get(nil, nil, Bdb::DB_NEXT | flags)} # Move forward.
          end

          while k
            models << Marshal.load(v)
            break if opts[:limit] and models.size == opts[:limit]
            k,v = iter.call
          end
        end
      elsif key.kind_of?(Range)
        # Fetch a range of keys.
        with_cursor do |cursor|
          first = Tuple.dump(key.first)
          last  = Tuple.dump(key.last)        
          
          # Return false once we pass the end of the range.
          cond = key.exclude_end? ? lambda {|k| k < last} : lambda {|k| k <= last}
          if opts[:reverse]
            iter = lambda {cursor.get(nil, nil, Bdb::DB_PREV | flags)} # Move backward.
            
            # Position the cursor at the end of the range.
            k,v = cursor.get(last, nil, Bdb::DB_SET_RANGE | flags) || cursor.get(nil, nil, Bdb::DB_LAST | flags)
            while k and not cond.call(k)
              k,v = iter.call
            end
            
            cond = lambda {|k| k >= first} # Change the condition to stop when we move past the start.
          else
            k,v  = cursor.get(first, nil, Bdb::DB_SET_RANGE | flags)   # Start at the beginning of the range.
            iter = lambda {cursor.get(nil, nil, Bdb::DB_NEXT | flags)} # Move forward.
          end
          
          while k and cond.call(k)
            models << Marshal.load(v)
            break if opts[:limit] and models.size == opts[:limit]
            k,v = iter.call
          end
        end
      else
        if unique?
          # There can only be one item for each key.
          data = db.get(transaction, Tuple.dump(key), nil, flags)
          models << Marshal.load(data) if data
        else
          # Have to use a cursor because there may be multiple items with each key.
          with_cursor do |cursor|
            k,v = cursor.get(Tuple.dump(key), nil, Bdb::DB_SET | flags)
            while k
              models << Marshal.load(v)
              break if opts[:limit] and models.size == opts[:limit]
              k,v = cursor.get(nil, nil, Bdb::DB_NEXT_DUP | flags)
            end
          end
        end
      end
      break if opts[:limit] and models.size == opts[:limit]
    end

    block_given? ? nil : models
  rescue Bdb::DbError => e
    e = ActiveDocument.wrap_error(e)
    retry if transaction.nil? and e.kind_of?(ActiveDocument::Deadlock)
    raise e
  end

  def save(model, opts = {})
    assert_primary!
    key   = Tuple.dump(model.primary_key)
    data  = Marshal.dump(model)
    flags = opts[:create] ? Bdb::DB_NOOVERWRITE : 0
    db.put(transaction, key, data, flags)
  rescue Bdb::DbError => e
    e = ActiveDocument.wrap_error(e, model)
    retry if transaction.nil? and e.kind_of?(ActiveDocument::Deadlock)
    raise e
  end

  def delete(model)
    assert_primary!
    key = Tuple.dump(model.primary_key)
    db.del(transaction, key, 0)
  rescue Bdb::DbError => e
    e = ActiveDocument.wrap_error(e)
    retry if transaction.nil? and e.kind_of?(ActiveDocument::Deadlock)
    raise e
  end

  # Deletes all records in the database. Beware!
  def truncate!
    assert_primary!
    db.truncate(transaction)
  rescue Bdb::DbError => e
    raise ActiveDocument.wrap_error(e)
  end

private
  
  def with_cursor
    cursor = db.cursor(transaction, 0)
    yield(cursor)
  ensure
    cursor.close if cursor
  end

  def assert_primary!
    raise 'cannot perform operation on secondary database' unless primary?
  end
end

# This allows us to support a block in find without changing the syntax.
class BlockArray
  def initialize(block)
    @block = block
    @size  = 0
  end
  attr_reader :size
  
  def <<(item)
    @size += 1
    @block.call(item)
  end
end

# Array comparison should try Tuple comparison first.
class Array
  cmp = instance_method(:<=>)

  define_method(:<=>) do |other|
    begin
      Tuple.dump(self) <=> Tuple.dump(other)
    rescue TypeError => e
      cmp.bind(self).call(other)
    end
  end
end
