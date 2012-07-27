$:.unshift(File.dirname(__FILE__))
require 'spec_helper'

module Anemone
  describe DistributedQueue do

    before(:each) do
      @queue = DistributedQueue.new :distributed_queue=>"distributed_test", :distributed_queue_timeout=>5
    end

    it "should enqueue a new object to the distributed queue" do
      @queue << ["a", "b", 107]
      @queue.length.should == 1
    end
    
    it "should enqueue a new object and get it back" do
      @queue << ["a", "b", 107]
      res = @queue.deq
      res.count.should == 3
      res[0].should == "a"
      res[1].should == "b"
      res[2].should == 107
      @queue.empty?.should == true
    end
    
    it "should always return 0 for the num of waiting on the queue" do
      @queue.num_waiting.should == 0
    end
    
    it "should return nil after 5 secs if no data in queue" do
      @queue.clear
      @queue.pop.nil?.should == true
    end
    
  end
end
