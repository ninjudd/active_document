module ActiveDocument
  class ActiveDocumentError < StandardError; end
  class DocumentNotFound < ActiveDocumentError; end
end

require 'bdb'
require 'active_support/inflector'
require 'active_document/database'
require 'active_document/environment'
require 'active_document/base'
