module ActiveDocument
  class ActiveDocumentError < StandardError; end
  class DocumentNotFound < ActiveDocumentError; end
  class DuplicatePrimaryKey < ActiveDocumentError; end
end

require 'bdb'
require 'tuple'
require 'active_support/inflector'
require 'active_support/core_ext/hash/indifferent_access.rb'
require 'active_document/database'
require 'active_document/environment'
require 'active_document/base'
