require File.dirname(__FILE__) + '/test_helper'

ActiveDocument.default_path = File.dirname(__FILE__) + '/tmp'
ActiveDocument.env_config :cache_size => 1 * 1024 * 1024
ActiveDocument.db_config  :page_size => 512

class Foo < ActiveDocument::Base
  accessor :bar, :id

  primary_key :id
end

class DeadlockTest < Test::Unit::TestCase
  context 'with db path' do
    setup do
      FileUtils.mkdir Foo.path
    end

    teardown do
      FileUtils.rmtree Foo.path
    end

    N = 10000 # total number of records
    R = 20    # number of readers
    W = 20    # number of writers
    T = 20    # reads per transaction
    L = 100   # logging frequency

    should 'detect deadlock' do
      pids = []

      W.times do |n|
        pids << Process.fork(&writer)
      end

      sleep(1)

      R.times do
        pids << Process.fork(&reader)
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
      T.times do
        random_ids.each_slice(T) do |ids|
          Foo.transaction do
            ids.each {|id| Foo.find_by_id(id)}
          end
          log('r')
        end
      end
      Foo.close_database
    end
  end

  def writer
    lambda do
      Foo.open_database
      random_ids.each_with_index do |id, i|
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
      Foo.close_database
    end
  end

  def random_ids
    (1...N).to_a.shuffle
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
