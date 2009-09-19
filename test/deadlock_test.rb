require File.dirname(__FILE__) + '/test_helper'

BDB_PATH = File.dirname(__FILE__) + '/tmp'

class Foo < ActiveDocument::Base
  path BDB_PATH
  accessor :bar, :id

  db_config :pagesize => 512

  primary_key :id
end

class DeadlockTest < Test::Unit::TestCase
  context 'with db path' do
    setup do
      FileUtils.mkdir BDB_PATH
    end

    teardown do
      FileUtils.rmtree BDB_PATH
    end

    N = 5000 # total number of records
    R = 10   # number of readers
    W = 10   # number of writers
    T = 20   # reads per transaction

    should 'detect deadlock' do
      pids = []
      R.times do
        pids << Process.fork(&reader)
      end

      W.times do |n|
        pids << Process.fork(&writer(n))
      end
      
      # Just make sure that all processes finish with no errors.
      pids.each do |pid| 
        Process.wait(pid)
        assert_equal 0, $?.exitstatus
      end
    end
  end

  def reader
    lambda do
      Foo.open_database
      (1...N).to_a.shuffle.each_slice(T) do |ids|
        Foo.transaction do
          ids.each {|id| Foo.find_by_id(id)}
        end
      end
      
      Foo.close_database
    end
  end

  def writer(n)
    lambda do
      Foo.open_database
      N.times do |id|
        next unless id % W == n
        Foo.create(:id => id, :bar => "bar" * 1000 + "anne")
        if id % T == 0
          print '.'
          $stdout.flush
        end
      end
      Foo.close_database
    end
  end
end
