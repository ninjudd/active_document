require File.dirname(__FILE__) + '/test_helper'

ActiveDocument.default_path = File.dirname(__FILE__) + '/tmp'
ActiveDocument.env_config :cache_size => 1 * 1024 * 1024
ActiveDocument.db_config  :page_size => 512

class Foo < ActiveDocument::Base
  accessor :bar, :id

  primary_key :id
end

class DeadlockTest < Test::Unit::TestCase
  context 'with empty and closed environment' do
    setup do
      FileUtils.rmtree Foo.path
      FileUtils.mkdir  Foo.path
      Foo.close_environment
    end

    N = 10000 # total number of records
    R = 10    # number of readers
    W = 10    # number of writers
    T = 20    # reads per transaction
    L = 100   # logging frequency

    should 'detect deadlock' do
      pids = []

      W.times do |n|
        pids << fork(&writer)
      end

      sleep(1)

      R.times do
        pids << fork(&reader)
      end

      # Make sure that all processes finish with no errors.
      pids.each do |pid|
        Process.wait(pid)
        assert_equal status, $?.exitstatus
      end
    end

    C = 10
    should 'detect unclosed resources' do
      threads = []

      threads << Thread.new do
        C.times do
          sleep(10)

          pid = fork do
            cursor = Foo.database.db.cursor(nil, 0)
            cursor.get(nil, nil, Bdb::DB_FIRST)
            exit!(1)
          end
          puts "\n====simulating exit with unclosed resources ===="
          Process.wait(pid)
          assert_equal 1, $?.exitstatus
        end
      end

      threads << Thread.new do
        C.times do
          pid = fork(&writer(1000))
          Process.wait(pid)
          assert [0,9].include?($?.exitstatus)
        end
      end

      sleep(3)

      threads << Thread.new do
        C.times do
          pid = fork(&reader(1000))
          Process.wait(pid)
          assert [0,9].include?($?.exitstatus)
        end
      end
      
      threads.each {|t| t.join}
    end
  end

  def reader(n = N)
    lambda do
      T.times do
        (1...n).to_a.shuffle.each_slice(T) do |ids|
          Foo.transaction do
            ids.each {|id| Foo.find_by_id(id)}
          end
          log('r')
        end
      end
      Foo.close_environment
    end
  end

  def writer(n = N)
    lambda do
      (1...n).to_a.shuffle.each_with_index do |id, i|
        Foo.transaction do
          begin
            Foo.create(:id => id, :bar => "bar" * 1000 + "anne #{rand}")
          rescue ActiveDocument::DuplicatePrimaryKey => e
            Foo.find_by_id(id).destroy
            retry
          end
        end
        log('w')
      end
      Foo.close_environment
    end
  end

  def log(action)
    @count ||= Hash.new(0)
    if @count[action] % L == 0
      print action.to_s
      $stdout.flush
    end
    @count[action] += 1
  end
end
