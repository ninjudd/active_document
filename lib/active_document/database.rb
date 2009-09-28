class ActiveDocument::Database
  def initialize(opts)
    @environment = opts[:environment]
    @model_class = opts[:model_class]
    @name        = opts[:name] || @model_class.database_name
  end
  attr_reader :environment, :model_class, :name

  def env
    environment.env
  end

  def config
    model_class.db_config
  end
  
  def indexes
    @indexes ||= {}
  end

  def index_by(field, opts = {})
    raise "index on #{field} already exists" if indexes[field]
    indexes[field] = opts
  end

  def db(index = nil)
    if @db.nil?
      @db = {}
      transaction(false) do
        db = env.db
        db.pagesize = config[:page_size] if config[:page_size]
        db.open(transaction, name, nil, Bdb::Db::BTREE, Bdb::DB_CREATE, 0)
        @db[:primary_key] = db

        indexes.each do |field, opts|
          index_callback = lambda do |db, key, data|
            model = Marshal.load(data)          
            index_key = model.send(field)
            if opts[:multi_key] and index_key.kind_of?(Array)
              # Index multiple keys. If the key is an array, you must wrap it with an outer array.
              index_key.collect {|k| Tuple.dump(k)}
            elsif index_key
              # Index a single key.
              Tuple.dump(index_key)
            end
          end
          index_db = env.db
          index_db.flags = Bdb::DB_DUPSORT unless opts[:unique]
          index_db.pagesize = config[:page_size] if config[:page_size]
          index_db.open(transaction, "#{name}_by_#{field}", nil, Bdb::Db::BTREE, Bdb::DB_CREATE, 0)
          db.associate(transaction, index_db, Bdb::DB_CREATE, index_callback)
          @db[field] = index_db
        end
      end
    end
    @db[index || :primary_key]
  rescue Exception => e
    raise ActiveDocument.wrap_error(e)
  end

  def close
    return unless @db
    synchronize do
      @db.each {|field, db| db.close(0)}
      @db = nil
    end
  rescue Bdb::DbError => e
    raise ActiveDocument.wrap_error(e)
  end

  def transaction(nested = true, &block)
    environment.transaction(nested, &block)
  end

  def synchronize(&block)
    environment.synchronize(&block)
  end

  def find(keys, opts = {}, &block)
    db     = db(opts[:field])
    models = block_given? ? BlockArray.new(block) : []
    flags  = opts[:modify] ? Bdb::DB_RMW : 0

    keys.uniq.each do |key|
      if opts[:partial] and not key.kind_of?(Range)
        first = [*key]
        last  = first + [true]
        key   = first..last
      end

      if key == :all
        with_cursor(db) do |cursor|
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
        with_cursor(db) do |cursor|
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
        if (db.flags & Bdb::DB_DUPSORT) == 0
          synchronize do
            # There can only be one item for each key.
            data = db.get(transaction, Tuple.dump(key), nil, flags)
            models << Marshal.load(data) if data
          end
        else
          # Have to use a cursor because there may be multiple items with each key.
          with_cursor(db) do |cursor|
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
    synchronize do
      key   = Tuple.dump(model.primary_key)
      data  = Marshal.dump(model)
      flags = opts[:create] ? Bdb::DB_NOOVERWRITE : 0
      db.put(transaction, key, data, flags)
    end
  rescue Bdb::DbError => e
    e = ActiveDocument.wrap_error(e, model)
    retry if transaction.nil? and e.kind_of?(ActiveDocument::Deadlock)
    raise e
  end

  def delete(model)
    synchronize do
      key = Tuple.dump(model.primary_key)
      db.del(transaction, key, 0)
    end
  rescue Bdb::DbError => e
    e = ActiveDocument.wrap_error(e)
    retry if transaction.nil? and e.kind_of?(ActiveDocument::Deadlock)
    raise e
  end

  # Deletes all records in the database. Beware!
  def truncate!
    synchronize do
      db.truncate(transaction)
    end
  rescue Bdb::DbError => e
    raise ActiveDocument.wrap_error(e)
  end

private
  
  def with_cursor(db)
    synchronize do
      begin
        cursor = db.cursor(transaction, 0)
        yield(cursor)
      ensure
        cursor.close if cursor
      end
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
