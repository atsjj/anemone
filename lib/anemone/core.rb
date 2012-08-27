require 'thread'
require 'robotex'
require 'anemone/tentacle'
require 'anemone/page'
require 'anemone/exceptions'
require 'anemone/page_store'
require 'anemone/storage'
require 'anemone/storage/base'
require 'anemone/distributed_queue'

module Anemone

  VERSION = '0.7.3';

  #
  # Convenience method to start a crawl
  #
  def Anemone.crawl(urls, options = {}, &block)
    if options[:logger]
      logger = options[:logger]
      options.delete :logger
    end
    Core.crawl(urls, options, logger, &block)
  end

  class Core

    # PageStore storing all Page objects encountered during the crawl
    attr_reader :pages
    # Hash of options for the crawl
    attr_reader :opts

    DEFAULT_OPTS = {
      # run 4 Tentacle threads to fetch pages
      :threads => 4,
      # disable verbose output
      :verbose => false,
      # don't throw away the page response body after scanning it for links
      :discard_page_bodies => false,
      # identify self as Anemone/VERSION
      :user_agent => "Anemone/#{Anemone::VERSION}",
      # no delay between requests
      :delay => 0,
      # In case delay is set, force threading (the delay will be per thread, not per process)
      :force_threading => false,
      # don't obey the robots exclusion protocol
      :obey_robots_txt => false,
      # by default, don't limit the depth of the crawl
      :depth_limit => false,
      # number of times HTTP redirects will be followed
      :redirect_limit => 5,
      # storage engine defaults to Hash in +process_options+ if none specified
      :storage => nil,
      # Hash of cookie name => value to send with HTTP requests
      :cookies => nil,
      # accept cookies from the server and send them back?
      :accept_cookies => false,
      # skip any link with a query string? e.g. http://foo.com/?u=user
      :skip_query_strings => false,
      # proxy server hostname 
      :proxy_host => nil,
      # proxy server port number
      :proxy_port => false,
      # HTTP read timeout in seconds
      :read_timeout => nil,
      # In case Anemone runs on a cloud, coordinates the effort through a Redis queue
      :distributed_queue => false,
      # Options for the Redis-Queue (links queue)
      :distributed_links_queue_opts => nil,
      # If working in distributed mode, never stop crawling (just wait for more data to come in)
      :endless_crawling => false,
      # Number of outstanding pages when elaborating from the main queue
      :links_local_buffer=>50,
      
    }

    # Create setter methods for all options to be called from the crawl block
    DEFAULT_OPTS.keys.each do |key|
      define_method "#{key}=" do |value|
        @opts[key.to_sym] = value
      end
      
      define_method "#{key}" do
        @opts[key.to_sym]
      end
    end

    #
    # Initialize the crawl with starting *urls* (single URL or Array of URLs)
    # and optional *block*
    #
    def initialize(urls, opts = {}, logger=nil)
      
      if urls
        @urls = [urls].flatten.map{ |url| url.is_a?(URI) ? url : URI(url) }
        @urls.each{ |url| url.path = '/' if url.path.empty? }
      end

      @tentacles = []
      @on_every_page_blocks = []
      @on_pages_like_blocks = Hash.new { |hash,key| hash[key] = [] }
      @skip_link_patterns = []
      @after_crawl_blocks = []
      @opts = opts
      @logger = logger

      yield self if block_given?
    end

    #
    # Convenience method to start a new crawl
    #
    def self.crawl(urls, opts = {}, logger=nil)
    self.new(urls, opts, logger) do |core|
        yield core if block_given?
        core.run
      end
    end

    #
    # Add a block to be executed on the PageStore after the crawl
    # is finished
    #
    def after_crawl(&block)
      @after_crawl_blocks << block
      self
    end

    #
    # Add one ore more Regex patterns for URLs which should not be
    # followed
    #
    def skip_links_like(*patterns)
      @skip_link_patterns.concat [patterns].flatten.compact
      self
    end

    #
    # Add a block to be executed on every Page as they are encountered
    # during the crawl
    #
    def on_every_page(&block)
      @on_every_page_blocks << block
      self
    end

    #
    # Add a block to be executed on Page objects with a URL matching
    # one or more patterns
    #
    def on_pages_like(*patterns, &block)
      if patterns
        patterns.each do |pattern|
          @on_pages_like_blocks[pattern] << block
        end
      end
      self
    end

    #
    # Specify a block which will select which links to follow on each page.
    # The block should return an Array of URI objects.
    #
    def focus_crawl(&block)
      @focus_crawl_block = block
      self
    end

    #
    # Perform the crawl
    #
    def run
      process_options

      if @urls
        @urls.delete_if { |url| !visit_link?(url) }
        return if @urls.empty?
      end

      if @opts[:distributed_queue]
        storage = (opts[:storage].respond_to? :load_links) ? storage : nil
        distributed_link_queue = DistributedQueue.new(storage, @opts[:distributed_links_queue_opts]) 
      end
      
      link_queue = Queue.new 
      page_queue = Queue.new

      @opts[:threads].times do
        @tentacles << Thread.new { 
          t = Tentacle.new(link_queue, page_queue, @opts)
          t.logger= @logger
          t.run 
        }
      end

      @urls.each{ |url| link_queue.enq(url.to_s) }  if @urls
      
      load_from_distributed_queue distributed_link_queue, link_queue, @opts[:links_local_buffer] if distributed_link_queue
      
      loop do
        page = page_queue.deq
        @pages.touch_key page.url
        @logger.info "Processing page #{page.url} [links: #{link_queue.size}] [pages: #{page_queue.size}]" if @logger
        do_page_blocks page
        page.discard_doc! if @opts[:discard_page_bodies]

        links = links_to_follow page
        links.each do |link|
          destination_queue = (distributed_link_queue) ? distributed_link_queue : link_queue 
          destination_queue << [link.to_s, page.url.dup.to_s, page.depth + 1]
        end
        @pages.touch_keys links

        @pages[page.url] = page
        

        # if we are done with the crawl, tell the threads to end
        if link_queue.empty? and page_queue.empty?
          if distributed_link_queue 
            
            if !@opts[:endless_crawling]
              load_from_distributed_queue(distributed_link_queue, link_queue, @opts[:links_local_buffer]) if distributed_link_queue.size > 0
              break if page_queue.empty? && wait_for_shutdown(link_queue, page_queue)
            else
              load_from_distributed_queue(distributed_link_queue, link_queue, 3 * @opts[:threads])
            end
          else
            #shut down
            break if wait_for_shutdown(link_queue, page_queue)
          end
        end
      end

      @tentacles.each { |thread| thread.join }
      do_after_crawl_blocks
      self
    end

    private
    
    def load_from_distributed_queue(distributed_link_queue, link_queue, n_items)
      found = false
      
      while n_items > 0
        url = distributed_link_queue.pop
        break if url.nil? && found
        next if url.nil?
        
        found = true
        link_queue.enq(url)
        n_items-=1
      end
    end

    def wait_for_shutdown(link_queue, page_queue)
      until link_queue.num_waiting == @tentacles.size
        Thread.pass
      end

      if page_queue.empty?
        @tentacles.size.times { link_queue << :END }
        return true
      end
      
      false
    end

    def process_options
      @opts = DEFAULT_OPTS.merge @opts
      @opts[:threads] = 1 if @opts[:delay] > 0 && !@opts[:force_threading]
      storage = Anemone::Storage::Base.new(@opts[:storage] || Anemone::Storage.Hash)
      @pages = PageStore.new(storage)
      @robots = Robotex.new(@opts[:user_agent]) if @opts[:obey_robots_txt]

      freeze_options
    end

    #
    # Freeze the opts Hash so that no options can be modified
    # once the crawl begins
    #
    def freeze_options
      @opts.freeze
      @opts.each_key { |key| @opts[key].freeze }
      @opts[:cookies].each_key { |key| @opts[:cookies][key].freeze } rescue nil
    end

    #
    # Execute the after_crawl blocks
    #
    def do_after_crawl_blocks
      @after_crawl_blocks.each { |block| block.call(@pages) }
    end

    #
    # Execute the on_every_page blocks for *page*
    #
    def do_page_blocks(page)
      @on_every_page_blocks.each do |block|
        block.call(page)
      end

      @on_pages_like_blocks.each do |pattern, blocks|
        blocks.each { |block| block.call(page) } if page.url.to_s =~ pattern
      end
    end

    #
    # Return an Array of links to follow from the given page.
    # Based on whether or not the link has already been crawled,
    # and the block given to focus_crawl()
    #
    def links_to_follow(page)
      links = @focus_crawl_block ? @focus_crawl_block.call(page) : page.links
      links.select { |link| visit_link?(link, page) }.map { |link| link.dup }
    end

    #
    # Returns +true+ if *link* has not been visited already,
    # and is not excluded by a skip_link pattern...
    # and is not excluded by robots.txt...
    # and is not deeper than the depth limit
    # Returns +false+ otherwise.
    #
    def visit_link?(link, from_page = nil)
      !@pages.has_page?(link) &&
      !skip_link?(link) &&
      !skip_query_string?(link) &&
      allowed(link) &&
      !too_deep?(from_page)
    end

    #
    # Returns +true+ if we are obeying robots.txt and the link
    # is granted access in it. Always returns +true+ when we are
    # not obeying robots.txt.
    #
    def allowed(link)
      @opts[:obey_robots_txt] ? @robots.allowed?(link) : true
    rescue
      false
    end

    #
    # Returns +true+ if we are over the page depth limit.
    # This only works when coming from a page and with the +depth_limit+ option set.
    # When neither is the case, will always return +false+.
    def too_deep?(from_page)
      if from_page && @opts[:depth_limit]
        from_page.depth >= @opts[:depth_limit]
      else
        false
      end
    end
    
    #
    # Returns +true+ if *link* should not be visited because
    # it has a query string and +skip_query_strings+ is true.
    #
    def skip_query_string?(link)
      @opts[:skip_query_strings] && link.query
    end

    #
    # Returns +true+ if *link* should not be visited because
    # its URL matches a skip_link pattern.
    #
    def skip_link?(link)
      @skip_link_patterns.any? { |pattern| link.path =~ pattern }
    end

  end
end
