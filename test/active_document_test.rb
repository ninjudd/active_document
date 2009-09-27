require File.dirname(__FILE__) + '/test_helper'

ActiveDocument.default_path = File.dirname(__FILE__) + '/tmp'
FileUtils.rmtree ActiveDocument.default_path
FileUtils.mkdir  ActiveDocument.default_path

class Foo < ActiveDocument::Base
  accessor :foo, :bar, :id

  primary_key :id
  index_by :foo, :multi_key => true
  index_by :bar, :unique => true
end

class Bar < ActiveDocument::Base
  accessor :foo, :bar

  primary_key [:foo, :bar]
  index_by :bar
end

class Baz < ActiveDocument::Base
  accessor :foo, :bar, :baz

  primary_key [:foo, :bar], :partition_by => :baz
  index_by :bar
end

class User < ActiveDocument::Base
  accessor :first_name, :last_name, :username, :email_address, :tags

  primary_key :username
  index_by [:last_name, :first_name]
  index_by :email_address, :unique => true
  index_by :tags, :multi_key => true
end

class ActiveDocumentTest < Test::Unit::TestCase
  context 'with empty foo db' do
    setup do
      Foo.database.truncate!
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
    
    should 'find by secondary indexes' do
      f1 = Foo.new(:foo => ['BAR', 'BAZ'], :bar => 'FOO', :id => 1)
      f1.save
      
      f2 = Foo.new(:foo => 'BAR', :bar => 'FU', :id => 2)
      f2.save
      
      assert_equal f1,      Foo.find_by_bar('FOO')
      assert_equal f2,      Foo.find_by_bar('FU')
      assert_equal [f1,f2], Foo.find_all_by_foo('BAR')
      assert_equal [f1],    Foo.find_all_by_foo('BAZ')
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

  context 'with empty bar db' do
    setup do
      Bar.database.truncate!
    end

    should 'not overwrite existing model' do
      b1 = Bar.new(:foo => 'foo', :bar => 'bar')
      b1.save
      
      assert_raises(ActiveDocument::DuplicatePrimaryKey) do
        b2 = Bar.new(:foo => 'foo', :bar => 'bar')
        b2.save
      end
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

  context 'with empty baz db' do
    setup do
      Baz.database.truncate!
    end

    should 'partition_by baz' do
      b1 = Baz.new(:foo => 'foo', :bar => 'bar', :baz => 1)
      b1.save

      b2 = Baz.new(:foo => 'foo', :bar => 'bar', :baz => 2)
      b2.save

      assert_equal b1, Baz.find(['foo','bar'], :baz => 1)
      assert_equal b2, Baz.find(['foo','bar'], :baz => 2)
    end

    should 'find and save with partition' do
      10.times do |i|
        Baz.with_baz(i) do
          assert_equal nil, Baz.find_by_primary_key(['foo','bar'])

          b = Baz.new(:foo => 'foo', :bar => 'bar')
          b.save

          assert_equal i, b.baz
          assert_equal b, Baz.find(['foo','bar'])
        end
      end
      assert_equal (0...10).collect {|i| i.to_s}, Baz.partitions
    end
  end

  context 'with empty user db' do
    setup do
      User.database.truncate!

      @john = User.create(
        :first_name => 'John',
        :last_name  => 'Stewart',
        :username   => 'lefty',
        :email_address => 'john@thedailyshow.com',
        :tags => [:funny, :liberal]
      )

      @martha = User.create(
        :first_name => 'Martha',
        :last_name  => 'Stewart',
        :username   => 'helen',
        :email_address => 'martha@marthastewart.com',
        :tags => [:conservative, :convict]
      )

      @martha = User.create(
        :first_name => 'Stephen',
        :last_name  => 'Colbert',
        :username   => 'steve',
        :email_address => 'stephen@thereport.com',
        :tags => [:conservative, :funny]
      )
    end
    
    should 'find_all_by_username' do
      assert_equal ['helen', 'lefty', 'steve'], User.find_all_by_username.collect {|u| u.username}
    end

    should 'find_all_by_last_name_and_first_name' do
      assert_equal ['steve', 'lefty', 'helen'], User.find_all_by_last_name_and_first_name.collect {|u| u.username}
    end

    should 'find_all_by_last_name' do
      assert_equal ['John', 'Martha'], User.find_all_by_last_name('Stewart').collect {|u| u.first_name}
    end

    should 'find_all_by_tag' do
      assert_equal ['lefty', 'steve'], User.find_all_by_tag(:funny).collect {|u| u.username}
    end
  end
end
