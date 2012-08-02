require 'json'
require 'redis'

module Anemone
  class DistributedQueue

    @@max_redis_length = 2500
    @@batch_to_import = 200

    #
    # Create a new DistributedQueue
    #
    def initialize(mongo_collection, opts = {})
      @redis = ::Redis.new(opts)
      @queue = opts[:distributed_queue] || 'anemoneq'
      @proc_queue = "bp_#{@queue}"
      @mongo_collection = mongo_collection
      
      unless opts[:preserve_storage_on_start]
        @redis.del @queue
        @redis.del @proc_queue
      end
    end

    def push(obj)
      if length > @@max_redis_length
        return
      end
      
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
      return if @mongo_collection.nil?
      
      @mongo_collection.find({"fetched"=>false}, :fields=>['url','referer','depth']).limit(@@batch_to_import).each {|record| 
        redis_push([record['url'], record['referer'], record['depth']])
      }
    end

  end
end
