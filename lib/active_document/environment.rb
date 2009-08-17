class ActiveDocument::Environment
  def initialize(path)
    @path = path
    at_exit { close }
  end

  attr_reader :path, :env

  def db
    env.db
  end

  def open
    if @env.nil?
      @env = Bdb::Env.new(0)
      env_flags =  Bdb::DB_CREATE    | # Create the environment if it does not already exist.
                   Bdb::DB_INIT_TXN  | # Initialize transactions
                   Bdb::DB_INIT_LOCK | # Initialize locking.
                   Bdb::DB_INIT_LOG  | # Initialize logging
                   Bdb::DB_INIT_MPOOL  # Initialize the in-memory cache.
      @env.open(path, env_flags, 0);
    end
  end

  def close
    if @env
      @env.close
      @env = nil
    end
  end

  def transaction
    if block_given?
      parent = @transaction
      @transaction = env.txn_begin(nil, 0)
      begin
        yield
        @transaction.commit(0)
      rescue Exception => e
        @transaction.abort
        raise e
      ensure
        @transaction = parent
      end
    else
      @transaction
    end
  end
end
