class ActiveDocument::Database
  def initialize(opts)
    @model_class = opts[:model_class]
    @field       = opts[:field]
    @unique      = opts[:unique]
    @suffix      = opts[:suffix] || (@field ? "by_#{@field}" : nil)
  end

  attr_accessor :model_class, :field, :db, :suffix

  def unique?
    @unique
  end
  
  def environment
    model_class.environment
  end

  def primary_db
    model_class.database.db if field
  end
  
  def name
    @name ||= [model_class.database_name, suffix].compact.join('_')
  end

  def transaction
    environment.transaction
  end

  def find(keys, opts = {}, &block)
    models = block_given? ? BlockArray.new(block) : []
    flags  = opts[:modify] ? Bdb::DB_RMW : 0

    keys.uniq.each do |key|
      if opts[:partial] and not key.kind_of?(Range)
        first = [*key]
        last  = first + [true]
        key = first..last
      end

      if key == :all
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
        cursor.close
      elsif key.kind_of?(Range)
        # Fetch a range of keys.
        cursor = db.cursor(transaction, 0)
        first = Tuple.dump(key.first)
        last  = Tuple.dump(key.last)        

        # Return false once we pass the end of the range.
        cond = key.exclude_end? ? lambda {|k| k < last} : lambda {|k| k <= last}

        if opts[:reverse]
          # Position the cursor at the end of the range.
          k,v = cursor.get(last, nil, Bdb::DB_SET_RANGE | flags)
          while k and not cond.call(k)
            k,v = iter.call
          end

          iter = lambda {cursor.get(nil, nil, Bdb::DB_PREV | flags)} # Move backward.
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
        cursor.close
      else
        if unique?
          # There can only be one item for each key.
          data = db.get(transaction, Tuple.dump(key), nil, flags)
          models << Marshal.load(data) if data
        else
          # Have to use a cursor because there may be multiple items with each key.
          cursor = db.cursor(transaction, 0)
          k,v = cursor.get(Tuple.dump(key), nil, Bdb::DB_SET | flags)
          while k
            models << Marshal.load(v)
            break if opts[:limit] and models.size == opts[:limit]
            k,v = cursor.get(nil, nil, Bdb::DB_NEXT_DUP | flags)
          end
          cursor.close
        end
      end
      break if opts[:limit] and models.size == opts[:limit]
    end

    block_given? ? nil : models
  end

  def save(model, opts = {})
    key   = Tuple.dump(model.primary_key)
    data  = Marshal.dump(model)
    flags = opts[:create] ? Bdb::DB_NOOVERWRITE : 0
    db.put(transaction, key, data, flags)
  rescue Bdb::DbError => e
    if e.message =~ /DB_KEYEXIST/
      raise ActiveDocument::DuplicatePrimaryKey, "primary key #{model.primary_key.inspect} already exists"
    else
      raise e
    end
  end

  def delete(model)
    key = Tuple.dump(model.primary_key)
    db.del(transaction, key, 0)
  end

  def truncate
    # Delete all records in the database. Beware!
    db.truncate(transaction)
  end

  def open
    if @db.nil?
      @db = environment.db
      @db.flags = Bdb::DB_DUPSORT unless unique?
      @db.open(nil, name, nil, Bdb::Db::BTREE, Bdb::DB_CREATE | Bdb::DB_AUTO_COMMIT, 0)
      
      if primary_db
        index_callback = lambda do |db, key, data|
          model = Marshal.load(data)
          return unless model.kind_of?(model_class)
          
          index_key = model.send(field)
          if index_key.kind_of?(Array)
            # Index multiple keys. If the key is an array, you must wrap it with an outer array.
            index_key.collect {|k| Tuple.dump(k)}
          elsif index_key
            # Index a single key.
            Tuple.dump(index_key)
          end
        end
        
        primary_db.associate(nil, @db, 0, index_callback)
      end
    end
  end

  def close
    if @db
      @db.close(0)
      @db = nil
    end
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
