require 'ordered_set'

module Combine
  class Proxy
    def initialize(method)
      @method        = method
      @keys_to_fetch = OrderedSet.new()
      @values_by_key = {}
    end
    
    def new?
      @new
    end

    def <<(keys)
      @new = false
      [*keys].each do |id|
        next if @values_by_key.has_key?(key)
        @keys_to_fetch << key
      end
    end

    def fetch
      @method.call(@keys_to_fetch).each do |key, value|
        (@values_by_key[key] ||= []) << value
      end
    end

    def values(keys)
      queue(keys)
      fetch

      values = {}
      keys.each {|key| values[key] = @values_by_key[key]}
      values
    end
  end

  def combine(method_name, keys)
    proxy = combine_proxy(method_name)
    if proxy.new?
      (class << self; self; end).module_eval do
        define_method("#{method_name}_with_combine") do |keys|
          proxy.values(keys)
        end
        alias_method_chain method_name, :with_combine
      end
    end
    proxy << keys
  end
  
  def combine_proxy(method_name)
    @combine_proxy ||= {}
    @combine_proxy[method_name] ||= Proxy.new( method(method_name) )
  end
end
