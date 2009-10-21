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
        primary_db = env.db
        primary_db.pagesize = config[:page_size] if config[:page_size]
        primary_db.open(transaction, name, nil, Bdb::Db::BTREE, Bdb::DB_CREATE, 0)
        @db[:primary_key] = primary_db

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
          primary_db.associate(transaction, index_db, Bdb::DB_CREATE, index_callback)
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
    if opts[:page].is_a?(Array)
      raise 'page markers not supported with multiple keys' if keys.size != 1
      page_key, opts[:offset] = opts.delete(:page)
      page_key = Tuple.dump(page_key)
    end

    db     = db(opts[:field])
    models = ResultSet.new(opts, &block)
    flags  = opts[:modify] ? Bdb::DB_RMW : 0
    flags  = 0 if environment.disable_transactions?

    keys.each do |key|
      if opts[:partial] and not key.kind_of?(Range)
        first = [*key]
        last  = first + [true]
        key   = first..last
      end

      if key == :all
        with_cursor(db) do |cursor|          
          # Go directly to the page marker if there is a page_key.
          page_marker = cursor.get(page_key, nil, Bdb::DB_SET | flags) if page_key

          if opts[:reverse]
            k,v  = page_marker || cursor.get(nil, nil, Bdb::DB_LAST | flags) # Start at the last item.
            iter = lambda {cursor.get(nil, nil, Bdb::DB_PREV | flags)}       # Move backward.
          else
            k,v  = page_marker || cursor.get(nil, nil, Bdb::DB_FIRST | flags) # Start at the first item.
            iter = lambda {cursor.get(nil, nil, Bdb::DB_NEXT | flags)}        # Move forward.
          end

          while k
            models << Marshal.load(v)
            k,v = iter.call
          end
        end
      elsif key.kind_of?(Range)
        # Fetch a range of keys.
        with_cursor(db) do |cursor|
          first = page_key || Tuple.dump(key.first)
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
              k,v = cursor.get(nil, nil, Bdb::DB_NEXT_DUP | flags)
            end
          end
        end
      end
    end
    model_class.set_page_marker
    models.to_a
  rescue ResultSet::LimitReached
    model_class.set_page_marker(models.page_key, models.page_offset)
    models.to_a
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

  class ResultSet
    class LimitReached < Exception; end

    def initialize(opts, &block)
      @block  = block
      @field  = opts[:field] || :primary_key
      @models = []
      @count  = 0
      @limit  = opts[:limit] || opts[:per_page]
      @limit  = @limit.to_i if @limit
      @offset = opts[:offset] || (opts[:page] ? @limit * (opts[:page] - 1) : 0)
      @offset = @offset.to_i if @offset
      @page_offset = 0
    end
    attr_reader :count, :limit, :offset, :page_key, :page_offset, :field

    def to_a
      @models.dup
    end

    def <<(model)
      @count += 1
      return if count <= offset

      if limit
        key = model.send(field)
        if key == page_key
          @page_offset += 1
        else
          @page_key = key
          @page_offset = 0
        end
        raise LimitReached if count > limit + offset
      end

      @block ? @block.call(model) : @models << model
    end
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
