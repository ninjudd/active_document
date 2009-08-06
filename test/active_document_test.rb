require File.dirname(__FILE__) + '/test_helper'

class Foo < ActiveDocument::Base
  path TEST_DIR
  accessor :foo, :bar, :id

  index_by :foo
  index_by :bar, :unique => true
end

class ActiveDocumentTest < Test::Unit::TestCase
  should "find in database after save" do
    f = Foo.new(:foo => 'BAR', :id => 1)
    f.save

    assert_equal f, Foo.find(1)
  end

  should "raise exception if not found" do
    assert_raises(ActiveDocument::DocumentNotFound) do
      Foo.find(7)
    end
  end

  should "find by key" do
    f = Foo.new(:foo => 'BAR', :id => 1)
    f.save

    assert_equal f, Foo.find_by_id(1).first
  end

  should "find by secondary indexes" do
    f1 = Foo.new(:foo => 'BAR', :bar => 'FOO', :id => 1)
    f1.save

    f2 = Foo.new(:foo => 'BAR', :bar => 'FU', :id => 2)
    f2.save

    assert_equal f1, Foo.find_by_bar('FOO').first
    assert_equal f2, Foo.find_by_bar('FU').first
    assert_equal [f1,f2], Foo.find_by_foo('BAR')
  end

  should "find by range" do
    
  end
end
