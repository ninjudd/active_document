class ActiveDocument::Environment
  def initialize(path)
    @path = path
  end
  attr_reader :path

  def new_database(opts)
    opts[:environment] = self
    returning ActiveDocument::Database.new(opts) do |db|
      databases << db
    end
  end

  def databases
    @databases ||= []
  end

  def config
    ActiveDocument.env_config
  end

  def env
    if @env.nil?
      synchronize do
        @env = Bdb::Env.new(0)
        env_flags = Bdb::DB_CREATE | Bdb::DB_INIT_TXN | Bdb::DB_INIT_LOCK | Bdb::DB_INIT_LOG |
                    Bdb::DB_REGISTER | Bdb::DB_RECOVER | Bdb::DB_INIT_MPOOL
        @env.cachesize = config[:cache_size] if config[:cache_size]
        @env.set_timeout(config[:txn_timeout],  Bdb::DB_SET_TXN_TIMEOUT)  if config[:txn_timeout]
        @env.set_timeout(config[:lock_timeout], Bdb::DB_SET_LOCK_TIMEOUT) if config[:lock_timeout]
        @env.set_lk_max_locks(config[:max_locks]) if config[:max_locks]
        @env.set_lk_detect(Bdb::DB_LOCK_RANDOM)
        @env.flags_on = Bdb::DB_TXN_WRITE_NOSYNC | Bdb::DB_TIME_NOTGRANTED
        @env.open(path, env_flags, 0)

        @exit_handler ||= at_exit { close }
      end
    end
    @env
  rescue Bdb::DbError => e
    raise ActiveDocument.wrap_error(e)
  end

  def close
    return unless @env
    synchronize do
      databases.each {|database| database.close}
      @env.close
      @env = nil
    end
  rescue Bdb::DbError => e
    raise ActiveDocument.wrap_error(e)
  end

  def transaction(nested = true)
    return @transaction unless block_given?
    return yield if disable_transactions?

    synchronize do
      parent = @transaction
      begin
        @transaction = env.txn_begin(nested ? parent : nil, 0)
        value = yield
        @transaction.commit(0)
        @transaction = nil
        value
      ensure
        @transaction.abort if @transaction
        @transaction = parent
      end
    end
  rescue Bdb::DbError, ActiveDocument::Error => e
    e = ActiveDocument.wrap_error(e)
    retry if @transaction.nil? and e.kind_of?(ActiveDocument::Deadlock)
    raise e
  end

  def checkpoint(opts = {})
    env.txn_checkpoint(opts[:kbyte] || 0, opts[:min] || 0, opts[:force] ? Bdb::DB_FORCE : 0)
  end

  def disable_transactions?
    config[:disable_transactions]
  end

  def synchronize
    @mutex ||= Mutex.new
    if @thread_id == thread_id
      yield
    else
      @mutex.synchronize do
        begin
          @thread_id = thread_id
          Thread.exclusive { yield }
        ensure
          @thread_id = nil
        end
      end
    end
  end

  def thread_id
    Thread.current.object_id
  end
end
