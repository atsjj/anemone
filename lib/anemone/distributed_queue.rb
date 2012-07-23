require 'json'
require 'redis'

module Anemone
  class DistributedQueue

    #
    # Create a new DistributedQueue
    #
    def initialize(opts = {})
      @redis = ::Redis.new(opts)
      @queue = opts[:distributed_queue] || 'anemoneq'
      @proc_queue = "bp_#{@queue}"
      @timeout = opts[:distributed_queue_timeout]
      
      unless opts[:preserve_storage_on_start]
        @redis.del @queue
        @redis.del @proc_queue
      end
    end

    def push(obj)
      return unless obj
    
      @redis.lpush @queue, obj.to_json
    end
    
    def pop(non_block=true)
      res = @redis.brpoplpush @queue, @proc_queue, @timeout
      return nil unless res
      
      @redis.lpop @proc_queue
      JSON.parse res
    end

    def length
      @redis.llen @queue
    end
    
    def clear
      @redis.del @queue
    end
    
    def empty?
      length == 0
    end
    
    def num_waiting
      0
    end
    
    alias << push
    alias enq push
    
    alias deq pop
    alias shift pop
    
    alias size length

  end
end
