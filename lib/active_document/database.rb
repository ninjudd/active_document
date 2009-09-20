class ActiveDocument::Database
  def initialize(opts)
    @environment = opts[:environment]
    @model_class = opts[:model_class]
    @field       = opts[:field]
    @unique      = opts[:unique]
    @multi_key   = opts[:multi_key]
    @name        = [@model_class.database_name, @field].compact.join('_by_')
  end

  attr_reader :environment, :model_class, :field, :db, :name

  define_method(:unique?)    { @unique    }
  define_method(:multi_key?) { @multi_key }
  define_method(:primary?)   { !field     }

  def secondary_database(opts)
    raise 'creating a secondary database only allowed on primary' unless primary?
    returning environment.database(opts.merge(:model_class => model_class)) do |db|
      secondary_databases << db
    end
  end
  
  def secondary_databases
    @secondary_databases ||= []
  end

  def transaction(&block)
    environment.transaction(&block)
  end

  def find(keys, opts = {}, &block)
    models = block_given? ? BlockArray.new(block) : []
    flags  = opts[:modify] ? Bdb::DB_RMW : 0

    keys.uniq.each do |key|
      if opts[:partial] and not key.kind_of?(Range)
        first = [*key]
        last  = first + [true]
        key   = first..last
      end

      if key == :all
        begin
          cursor = db.cursor(transaction, 0)
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
        ensure
          cursor.close
        end
      elsif key.kind_of?(Range)
        # Fetch a range of keys.
        begin
          cursor = db.cursor(transaction, 0)
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
        ensure
          cursor.close
        end
      else
        if unique?
          # There can only be one item for each key.
          data = db.get(transaction, Tuple.dump(key), nil, flags)
          models << Marshal.load(data) if data
        else
          # Have to use a cursor because there may be multiple items with each key.
          begin
            cursor = db.cursor(transaction, 0)
            k,v = cursor.get(Tuple.dump(key), nil, Bdb::DB_SET | flags)
            while k
              models << Marshal.load(v)
              break if opts[:limit] and models.size == opts[:limit]
              k,v = cursor.get(nil, nil, Bdb::DB_NEXT_DUP | flags)
            end
          ensure
            cursor.close
          end
        end
      end
      break if opts[:limit] and models.size == opts[:limit]
    end

    block_given? ? nil : models
  rescue Bdb::DbError => e
    e = wrap_error(e)
    retry if transaction.nil? and e.kind_of?(ActiveDocument::Deadlock)
    raise e
  end

  def save(model, opts = {})
    key   = Tuple.dump(model.primary_key)
    data  = Marshal.dump(model)
    flags = opts[:create] ? Bdb::DB_NOOVERWRITE : 0
    db.put(transaction, key, data, flags)
  rescue Bdb::DbError => e
    e = wrap_error(e, model)
    retry if transaction.nil? and e.kind_of?(ActiveDocument::Deadlock)
    raise e
  end

  def delete(model)
    key = Tuple.dump(model.primary_key)
    db.del(transaction, key, 0)
  rescue Bdb::DbError => e
    e = wrap_error(e)
    retry if transaction.nil? and e.kind_of?(ActiveDocument::Deadlock)
    raise e
  end

  def truncate
    # Delete all records in the database. Beware!
    db.truncate(transaction)
  rescue Bdb::DbError => e
    raise wrap_error(e)
  end

  def open(config)
    return if @db
    @db = environment.db
    @db.flags = Bdb::DB_DUPSORT unless unique?
    @db.pagesize = config[:page_size] if config[:page_size]
    @db.open(nil, name, nil, Bdb::Db::BTREE, Bdb::DB_CREATE | Bdb::DB_AUTO_COMMIT, 0)
      
    secondary_databases.each do |secondary|
      secondary.open(config)
      associate(secondary)
    end
  rescue Exception => e
    raise wrap_error(e)
  end

  def close
    return unless @db
    @db.close(0)
    secondary_databases.each {|secondary| secondary.close}
    @db = nil
  end

private

  def associate(secondary)
    index_callback = lambda do |db, key, data|
      model = Marshal.load(data)
      return unless model.kind_of?(model_class)
      
      index_key = model.send(secondary.field)
      if secondary.multi_key? and index_key.kind_of?(Array)
        # Index multiple keys. If the key is an array, you must wrap it with an outer array.
        index_key.collect {|k| Tuple.dump(k)}
      elsif index_key
        # Index a single key.
        Tuple.dump(index_key)
      end
    end
    db.associate(nil, secondary.db, Bdb::DB_CREATE, index_callback)
  end
  
  def wrap_error(e, model = nil)
    error = case e.code
    when Bdb::DB_RUNRECOVERY     : ActiveDocument::RunRecovery.new(e.message)
    when Bdb::DB_LOCK_DEADLOCK   : ActiveDocument::Deadlock.new(e.message)
    when Bdb::DB_LOCK_NOTGRANTED : ActiveDocument::Timeout.new(e.message)
    when Bdb::DB_KEYEXIST
      ActiveDocument::DuplicatePrimaryKey.new("primary key #{model.primary_key.inspect} already exists")
    else
      return e
    end
    error.set_backtrace(e.backtrace)
    error
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
