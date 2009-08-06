require 'rubygems'
require 'test/unit'
require 'shoulda'
require 'mocha'
require 'pp'

$LOAD_PATH.unshift(File.dirname(__FILE__) + '/../lib')
require 'active_document'

TEST_DIR = '/tmp/active_document_test'
FileUtils.rmtree TEST_DIR
FileUtils.mkdir  TEST_DIR
