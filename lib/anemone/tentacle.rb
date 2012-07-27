require 'anemone/http'

module Anemone
  class Tentacle

    #
    # Create a new Tentacle
    #
    def initialize(link_queue, page_queue, opts = {})
      @link_queue = link_queue
      @page_queue = page_queue
      @http = Anemone::HTTP.new(opts)
      @opts = opts
    end

    def logger=(logger)
      @logger=logger
    end

    #
    # Gets links from @link_queue, and returns the fetched
    # Page objects into @page_queue
    #
    def run
      loop do
        link, referer, depth = @link_queue.deq

        break if link == :END

        @logger.info "Downloading page - #{link}" if @logger
        @http.fetch_pages(link, referer, depth).each { |page| @page_queue << page }

        delay
      end
    end

    private

    def delay
      sleep @opts[:delay] if @opts[:delay] > 0
    end

  end
end
