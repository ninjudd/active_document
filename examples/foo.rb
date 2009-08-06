require 'rubygems'
require 'active_document'

class Foo < ActiveDocument::Base
  path '/tmp/data'

  reader :roo
  writer :woo
  accessor :foo, :bar, :key
end
