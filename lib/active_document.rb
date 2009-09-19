module ActiveDocument
  class Error               < StandardError; end
  class DocumentNotFound    < Error;         end
  class DuplicatePrimaryKey < Error;         end
  class RunRecovery         < Error;         end
  class Deadlock            < Error;         end
  class Timeout             < Error;         end
end

require 'bdb'
require 'tuple'
require 'active_support'
require 'active_document/database'
require 'active_document/environment'
require 'active_document/base'
