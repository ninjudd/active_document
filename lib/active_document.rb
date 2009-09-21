module ActiveDocument
  class Error               < StandardError; end
  class DocumentNotFound    < Error;         end
  class DuplicatePrimaryKey < Error;         end
  class RunRecovery         < Error;         end
  class Deadlock            < Error;         end
  class Timeout             < Error;         end

  def self.env_config(config = {})
    @env_config ||= {
      :max_locks    => 5000,
      :lock_timeout => 30 * 1000 * 1000,
      :txn_timeout  => 30 * 1000 * 1000,
      :cache_size   => 1  * 1024 * 1024,
    }
    @env_config.merge!(config)
  end

  def self.db_config(config = {})
    @db_config ||= {}
    @db_config.merge!(config)
  end

  def self.default_path=(path)
    ActiveDocument::Base.path(path)
  end

  def self.default_path
    ActiveDocument::Base.path
  end
end

require 'bdb'
require 'tuple'
require 'active_support'
require 'active_document/database'
require 'active_document/environment'
require 'active_document/base'
