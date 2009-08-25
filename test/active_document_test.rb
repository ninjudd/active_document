require File.dirname(__FILE__) + '/test_helper'

BDB_PATH = File.dirname(__FILE__) + '/tmp'

class Foo < ActiveDocument::Base
  path BDB_PATH
  accessor :foo, :bar, :id

  primary_key :id
  index_by :foo
  index_by :bar, :unique => true
end

class Bar < ActiveDocument::Base
  path BDB_PATH
  accessor :foo, :bar

  primary_key [:foo, :bar]
  index_by :bar
end

class ActiveDocumentTest < Test::Unit::TestCase
  context 'with foo db open' do
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
    
    should 'find_by_primary_key' do
      f = Foo.new(:foo => 'BAR', :id => 1)
      f.save
      
      assert_equal f, Foo.find_by_primary_key(1)
      assert_equal f, Foo.find_by_id(1)
    end

    should 'destroy' do
      f = Foo.new(:foo => 'BAR', :id => 1)
      f.save
      
      assert_equal f, Foo.find_by_id(1)

      f.destroy

      assert_equal nil, Foo.find_by_id(1)
    end

    should 'change primary key' do
      f = Foo.new(:foo => 'BAR', :id => 1)
      f.save
      
      assert_equal f, Foo.find_by_id(1)

      f.id = 2
      f.save

      assert_equal nil, Foo.find_by_id(1)
      assert_equal 2,   Foo.find_by_id(2).id
    end

    should 'not overwrite existing model' do
      b1 = Bar.new(:foo => 'foo', :bar => 'bar')
      b1.save
      
      assert_raises(ActiveDocument::DuplicatePrimaryKey) do
        b2 = Bar.new(:foo => 'foo', :bar => 'bar')
        b2.save
      end
    end
    
    should 'find by secondary indexes' do
      f1 = Foo.new(:foo => 'BAR', :bar => 'FOO', :id => 1)
      f1.save
      
      f2 = Foo.new(:foo => 'BAR', :bar => 'FU', :id => 2)
      f2.save
      
      assert_equal f1,      Foo.find_by_bar('FOO')
      assert_equal f2,      Foo.find_by_bar('FU')
      assert_equal [f1,f2], Foo.find_all_by_foo('BAR')
    end
    
    should 'find by range' do
      (1..20).each do |i|
        Foo.new(:id => i, :foo => "foo-#{i}").save
      end
      
      assert_equal (5..17).to_a, Foo.find_all_by_id(5..17).collect {|f| f.id}
      assert_equal (5..14).to_a, Foo.find_all_by_id(5..17, :limit => 10).collect {|f| f.id}

      # Mixed keys and ranges.
      assert_equal (1..4).to_a + (16..20).to_a, Foo.find_all_by_id(1..3, 4, 16..20).collect {|f| f.id}
    end

    should 'find all' do
      (1..20).each do |i|
        Foo.new(:id => i, :foo => "foo-#{i}").save
      end
      
      assert_equal (1..20).to_a, Foo.find_all_by_id.collect {|f| f.id}
      assert_equal 1, Foo.find_by_id.id # First
    end

    should 'find with reverse' do
      (1..20).each do |i|
        Foo.new(:id => i, :foo => "foo-#{i}").save
      end
      
      assert_equal (1..20).to_a.reverse, Foo.find_all_by_id(:reverse => true).collect {|f| f.id}
      assert_equal (5..17).to_a.reverse, Foo.find_all_by_id(5..17, :reverse => true).collect {|f| f.id}
      assert_equal 20, Foo.find_by_id(:reverse => true).id # Last
    end
  end

  context 'with bar db open' do
    setup do
      FileUtils.mkdir BDB_PATH
      Bar.open_database
    end

    teardown do
      Bar.close_database
      FileUtils.rmtree BDB_PATH
    end

    should 'find_by_primary_key and find by id fields' do
      100.times do |i|
        100.times do |j|
          b = Bar.new(:foo => i, :bar => j)
          b.save
        end
      end

      assert_equal [5, 5],   Bar.find_by_primary_key([5, 5]).primary_key
      assert_equal [52, 52], Bar.find_by_foo_and_bar([52, 52]).foo_and_bar
      assert_equal (0..99).collect {|i| [42, i]}, Bar.find_all_by_foo(42).collect {|b| b.primary_key}
      assert_equal (0..99).collect {|i| [i, 52]}, Bar.find_all_by_bar(52).collect {|b| b.primary_key}
    end
  end

end
