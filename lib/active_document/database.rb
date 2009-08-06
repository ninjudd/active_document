class ActiveDocument::Database
  def initialize(opts)
    @model_class = opts[:model_class]
    @field       = opts[:field]
    @unique      = opts[:unique]
  end

  attr_accessor :model_class, :field, :db

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
    @name ||= field ? "#{model_class.database_name}_by_#{field}" : model_class.database_name
  end

  def transaction
    environment.transaction
  end

  def db
    if @db.nil?
      @db = environment.db

      db.flags = Bdb::DB_DUPSORT unless unique?
      db.btree_compare = lambda do |db, key1, key2|
        Marshal.load(key1) <=> Marshal.load(key2)
      end
      db.open(nil, name, nil, Bdb::Db::BTREE, Bdb::DB_CREATE | Bdb::DB_AUTO_COMMIT, 0)
      
      if primary_db
        index_callback = lambda do |db, key, data|
          model = Marshal.load(data)
          return unless model.kind_of?(model_class)
          
          index_key = model.send(field)
          if index_key.kind_of?(Array)
            # Index multiple keys. If the key is an array, you must wrap it with an outer array.
            index_key.collect {|k| Marshal.dump(k)}
          elsif index_key
            # Index a single key.
            Marshal.dump(index_key)
          end
        end
        
        primary_db.associate(nil, db, 0, index_callback)
      end
      at_exit { db.close(0) }
    end
    @db
  end

  def find(keys, opts = {})
    models = []

    if keys.kind_of?(Range)
      start_key = Marshal.dump(keys.first)
      cursor = db.cursor(transaction, 0)
      key, data = cursor.get(start_key, nil, Bdb::DB_SET_RANGE)
      while keys.include?(Marshal.load(key))
        models << Marshal.load(data)
        break if opts[:limit] and models.size == opts[:limit]
        key, data = cursor.get(nil, nil, Bdb::DB_NEXT)
      end
      cursor.close
    else
      keys.uniq.each do |key|
        key = Marshal.dump(key)
        if unique?
          # There can only be one item for each key.
          data = db.get(transaction, key, nil, 0)
          models << Marshal.load(data) if data
        else
          # Have to use a cursor because there may be multiple items with each key.
          cursor = db.cursor(transaction, 0)
          data = cursor.get(key, nil, Bdb::DB_SET)
          while data
            models << Marshal.load(data[1])
            break if opts[:limit] and models.size == opts[:limit]
            data = cursor.get(nil, nil, Bdb::DB_NEXT_DUP)
          end
          cursor.close
        end
      end
      break if opts[:limit] and models.size == opts[:limit]
    end
    models
  end

  def save(model)
    id = Marshal.dump(model.id)
    db.put(nil, id, Marshal.dump(model), 0)
  end
end
