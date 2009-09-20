class ActiveDocument::Environment
  def initialize(path)
    @path = path
  end
  attr_reader :path, :env

  def database(opts)
    opts[:environment] = self
    returning ActiveDocument::Database.new(opts) do |db|
      databases << db if db.primary?
    end
  end

  def databases
    @databases ||= []
  end

  def db
    env.db
  end

  def config
    ActiveDocument.env_config
  end

  def open
    return if @env
    @env = Bdb::Env.new(0)
    env_flags = Bdb::DB_CREATE | Bdb::DB_INIT_TXN | Bdb::DB_INIT_LOCK | Bdb::DB_INIT_LOG |
                Bdb::DB_REGISTER | Bdb::DB_RECOVER | Bdb::DB_INIT_MPOOL
    @env.cachesize = config[:cache_size] if config[:cache_size]
    @env.set_timeout(config[:txn_timeout],  Bdb::DB_SET_TXN_TIMEOUT)  if config[:txn_timeout]
    @env.set_timeout(config[:lock_timeout], Bdb::DB_SET_LOCK_TIMEOUT) if config[:lock_timeout]
    @env.mutex_set_max(config[:max_mutexes])  if config[:max_mutexes]
    @env.set_lk_max_locks(config[:max_locks]) if config[:max_locks]
    @env.set_lk_detect(Bdb::DB_LOCK_RANDOM)
    @env.flags_on = Bdb::DB_TXN_WRITE_NOSYNC | Bdb::DB_TIME_NOTGRANTED
    @env.open(path, env_flags, 0)
  end

  def close
    return unless @env
    databases.each {|database| database.close}
    @env.close
    @env = nil
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
