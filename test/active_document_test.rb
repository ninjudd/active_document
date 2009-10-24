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
  index_by :foo
end

class Baz < ActiveDocument::Base
  accessor :foo, :bar, :baz

  primary_key [:foo, :bar], :partition_by => :baz
  index_by :bar
end

class User < ActiveDocument::Base
  accessor :first_name, :last_name, :username, :email_address, :tags
  timestamps

  primary_key :username
  index_by [:last_name, :first_name]
  index_by :email_address, :unique => true
  index_by :tags, :multi_key => true
end

class View < ActiveDocument::Base
  reader :profile_id, :user_id, :count
  timestamps

  primary_key [:profile_id, :user_id]
  index_by [:user_id,    :updated_at]
  index_by [:profile_id, :updated_at]
  
  save_method :increment
  def increment
    attributes[:count] += 1
  end

  def self.increment!(profile_id, user_id)
    transaction do
      view = find_by_primary_key([profile_id, user_id]) #, :modify => true)
      if view
        view.increment!
      else
        view = create(:profile_id => profile_id, :user_id => user_id, :count => 1)
      end
    end    
  end
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

    should 'find with limit and offset' do
      (1..100).each do |i|
        Foo.new(:id => i, :bar => i + 42, :foo => i % 20).save
      end

      assert_equal [5, 5, 5, 5, 6, 6, 6],
        Foo.find_all_by_foo(5..14, :limit => 7, :offset => 1).collect {|f| f.foo}
      assert_equal 6, Foo.page_key
      assert_equal 3, Foo.page_offset

      assert_equal [6, 6, 7, 7, 7, 7, 7, 8, 8, 8, 8],
        Foo.find_all_by_foo(Foo.page_key..14, :limit => 11, :offset => Foo.page_offset).collect {|f| f.foo}
      assert_equal 8, Foo.page_key
      assert_equal 4, Foo.page_offset

      assert_equal [8, 9, 9, 9, 9, 9, 10, 10, 10, 10, 10, 11, 11, 11, 11, 11],
        Foo.find_all_by_foo(Foo.page_key..14, :limit => 16, :offset => Foo.page_offset).collect {|f| f.foo}
      assert_equal 12, Foo.page_key
      assert_equal 0,  Foo.page_offset

      assert_equal [12, 12, 12, 12, 12, 13, 13, 13, 13, 13, 14, 14, 14, 14, 14],
        Foo.find_all_by_foo(Foo.page_key..14, :offset => Foo.page_offset).collect {|f| f.foo}
      assert_equal nil, Foo.page_key
      assert_equal nil, Foo.page_offset
    end

    should 'add locator_key to models' do
      Foo.new(:id => 1, :foo => [1, 2, 3]).save
      Foo.new(:id => 2, :foo => [4, 5, 6]).save
      Foo.new(:id => 3, :foo => [6, 7, 8]).save

      Foo.find_all_by_foo(2..4).each_with_index do |foo, i|
        assert_equal [i + 2], foo.locator_key
      end

      Foo.find_all_by_foo(6).each do |foo|
        assert_equal [6], foo.locator_key
      end

      i = 1
      Foo.find_all_by_foo.each do |foo|
        key = i > 6 ? [i - 1] : [i]
        assert_equal key, foo.locator_key
        i += 1
      end
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

    should 'count' do
      (1..21).each do |i|
        Bar.new(:foo => i % 7, :bar => i % 3).save
      end
      
      assert_equal 1, Bar.count(:primary_key, [6,2])
      assert_equal 0, Bar.count(:primary_key, [2,6])

      3.times {|i| assert_equal 7, Bar.count(:bar, i)}
      assert_equal 0, Bar.count(:bar, 3)

      7.times {|i| assert_equal 3, Bar.count(:foo, i)}
      assert_equal 0, Bar.count(:foo, 7)
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

      @steve = User.create(
        :first_name => 'Stephen',
        :last_name  => 'Colbert',
        :username   => 'steve',
        :email_address => 'stephen@thereport.com',
        :tags => [:conservative, :funny]
      )

      @will = User.create(
        :first_name => 'Will',
        :last_name  => 'Smith',
        :username   => 'legend',
        :email_address => 'will@smith.com',
        :tags => [:actor, :rapper]
      )
    end
    
    should 'find_all_by_username' do
      assert_equal ['helen', 'lefty', 'legend', 'steve'], User.find_all_by_username.collect {|u| u.username}
    end

    should 'find_all_by_last_name_and_first_name' do
      assert_equal ['steve', 'legend', 'lefty', 'helen'], User.find_all_by_last_name_and_first_name.collect {|u| u.username}
    end

    should 'find_all_by_last_name' do
      assert_equal ['John', 'Martha'], User.find_all_by_last_name('Stewart').collect {|u| u.first_name}
    end

    should 'find_all_by_tag' do
      assert_equal ['lefty', 'steve'], User.find_all_by_tag(:funny).collect {|u| u.username}
    end

    should 'find with keys' do
      assert_equal ['lefty', 'helen', 'legend'],
        User.find_all_by_last_name("Stewart", "Smith").collect {|u| u.username}
    end

    should 'find with range' do
      assert_equal ['legend', 'lefty', 'helen'],
        User.find_all_by_last_name("Smith".."Stuart").collect {|u| u.username}
    end

    should 'find with range and key' do
      assert_equal ['legend', 'lefty', 'helen', 'steve'],
        User.find_all_by_last_name("Smith".."Stuart", "Colbert").collect {|u| u.username}
    end

    should 'find with ranges' do
      assert_equal ['steve', 'legend', 'lefty', 'helen'],
        User.find_all_by_last_name("Aardvark".."Daisy", "Smith".."Stuart").collect {|u| u.username}
    end

    should 'find with limit' do
      assert_equal ["helen", "lefty"], User.find_all_by_username(:limit => 2).collect {|u| u.username}
    end

    should 'find with limit and offset' do
      assert_equal ["legend", "steve"], User.find_all_by_username(:limit => 2, :offset => 2).collect {|u| u.username}
    end

    should 'find with page' do
      assert_equal ["helen", "lefty"],  User.find_all_by_username(:per_page => 2, :page => 1).collect {|u| u.username}
      assert_equal ["legend", "steve"], User.find_all_by_username(:per_page => 2, :page => 2).collect {|u| u.username}
      assert_equal ["helen", "lefty"],  User.find_all_by_username(:limit => 2, :page => 1).collect {|u| u.username}
      assert_equal ["legend", "steve"], User.find_all_by_username(:limit => 2, :page => 2).collect {|u| u.username}
    end

    should 'find with page_marker' do
      assert_equal ["helen", "lefty"],  User.find_all_by_username(:limit => 2).collect {|u| u.username}
      assert_equal ["legend", "steve"], User.find_all_by_username(:page => User.page_marker).collect {|u| u.username}
    end

    should "mark deleted but don't destroy record" do
      assert !@martha.deleted?
      assert !User.find_by_username('helen').deleted?

      @martha.delete!

      assert @martha.deleted?
      assert User.find_by_username('helen').deleted?
    end
  end

  context 'with empty views db' do
    setup do
      View.database.truncate!
    end

    N = 10000
    P = 1
    U = 1

    should 'increment views randomly without corrupting secondary index' do
      N.times do
        profile_id = rand(P)
        user_id    = rand(U)
        View.increment!(profile_id, user_id)
      end
      assert true
    end
  end
end
