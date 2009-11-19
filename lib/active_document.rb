module ActiveDocument
  class DocumentNotFound    < StandardError; end
  class DuplicatePrimaryKey < StandardError; end
end

require 'bdb/database'
require 'bdb/partitioned_database'
require 'active_support'
require 'active_document/base'
