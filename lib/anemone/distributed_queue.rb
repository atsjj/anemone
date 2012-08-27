require 'json'
require 'redis'

module Anemone
  class DistributedQueue

    @@max_redis_length = 2500
    BATCH_TO_IMPORT = 200

    #
    # Create a new DistributedQueue
    #
    def initialize(storage, opts = {})
      @redis = ::Redis.new(opts)
      @queue = opts[:distributed_queue] || 'anemoneq'
      @proc_queue = "bp_#{@queue}"
      @storage = storage
      
      unless opts[:preserve_storage_on_start]
        @redis.del @queue
        @redis.del @proc_queue
      end
    end

    def push(obj)
      return if length > @@max_redis_length
      
      redis_push(obj)
    end
    
    def pop(non_block=true)      
      if length < (@@max_redis_length/4)
        load_from_mongo
      end
        
      redis_pop
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

  private 

    def redis_push(obj)
      obj = { 'data'=>obj }
      @redis.lpush( @queue, obj.to_json )
    end

    def redis_pop
      res = @redis.brpoplpush @queue, @proc_queue, @timeout
      return nil unless res
      @redis.lpop @proc_queue

      ret = JSON.parse res
      ret ? ret['data'] : nil
    end

    def load_from_mongo
      return unless @storage
      
      links = @storage.load_links BATCH_TO_IMPORT
      return unless links
      links.each { |link| redis_push(link) }
    end

  end
end
