require File.dirname(__FILE__) + '/test_helper'

BDB_PATH = File.dirname(__FILE__) + '/tmp'

class Foo < ActiveDocument::Base
  path BDB_PATH
  accessor :foo, :bar, :id

  index_by :foo
  index_by :bar, :unique => true
end

class ActiveDocumentTest < Test::Unit::TestCase
  context 'with db open' do
    setup do
      FileUtils.mkdir BDB_PATH
      Foo.open_database
    end
    
    teardown do
      Foo.close_database
      FileUtils.rmtree BDB_PATH      
    end

    should 'find in database after save' do
      f = Foo.new(:foo => 'BAR', :id => 1)
      f.save
      
      assert_equal f, Foo.find(1)
    end
    
    should 'raise exception if not found' do
      assert_raises(ActiveDocument::DocumentNotFound) do
        Foo.find(7)
      end
    end
    
    should 'find by key' do
      f = Foo.new(:foo => 'BAR', :id => 1)
      f.save
      
      assert_equal f, Foo.find_by_id(1).first
    end
    
    should 'find by secondary indexes' do
      f1 = Foo.new(:foo => 'BAR', :bar => 'FOO', :id => 1)
      f1.save
      
      f2 = Foo.new(:foo => 'BAR', :bar => 'FU', :id => 2)
      f2.save
      
      assert_equal f1, Foo.find_by_bar('FOO').first
      assert_equal f2, Foo.find_by_bar('FU').first
      assert_equal [f1,f2], Foo.find_by_foo('BAR')
    end
    
    should 'find by range' do
      (1..20).each do |i|
        Foo.new(:id => i, :foo => "foo-#{i}").save
      end
      
      assert_equal (5..17).to_a, Foo.find_by_id(5..17).collect {|f| f.id}
      assert_equal (5..14).to_a, Foo.find_by_id(5..17, :limit => 10).collect {|f| f.id}

      # Mixed keys and ranges.
      assert_equal (1..4).to_a + (16..20).to_a, Foo.find_by_id(1..3, 4, 16..20).collect {|f| f.id}
    end
  end
end
