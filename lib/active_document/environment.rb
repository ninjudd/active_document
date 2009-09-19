class ActiveDocument::Environment
  def initialize(path)
    @path = path
  end
  attr_reader :path, :env

  def database(opts)
    opts[:environment] = self
    ActiveDocument::Database.new(opts)
  end

  def db
    env.db
  end

  CACHE_SIZE   = 10 * 1024 * 1024
  LOCK_TIMEOUT = 10 * 1000 * 1000
  TXN_TIMEOUT  = 10 * 1000 * 1000

  def open
    if @env.nil?
      @env = Bdb::Env.new(0)
      env_flags = Bdb::DB_CREATE | Bdb::DB_INIT_TXN | Bdb::DB_INIT_LOCK | Bdb::DB_INIT_LOG |
                  Bdb::DB_REGISTER | Bdb::DB_RECOVER | Bdb::DB_INIT_MPOOL
      @env.cachesize = CACHE_SIZE
      @env.set_timeout(TXN_TIMEOUT, Bdb::DB_SET_TXN_TIMEOUT)
      @env.set_timeout(LOCK_TIMEOUT, Bdb::DB_SET_LOCK_TIMEOUT)
      @env.set_lk_detect(Bdb::DB_LOCK_RANDOM)
      @env.flags_on = Bdb::DB_TXN_WRITE_NOSYNC | Bdb::DB_TIME_NOTGRANTED
      @env.open(path, env_flags, 0)
    end
  end

  def close
    if @env
      @env.close
      @env = nil
    end
  end

  def transaction
    return @transaction unless block_given?

    parent = @transaction
    begin
      @transaction = env.txn_begin(nil, 0)
      value = yield
      @transaction.commit(0)
      value
    rescue Exception => e
      @transaction.abort
      retry if parent.nil? and e.kind_of?(ActiveDocument::Deadlock)
      raise e
    ensure
      @transaction = parent
    end
  end
end
