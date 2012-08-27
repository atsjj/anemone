require 'uri'
require 'digest/md5'
require 'yaml'
require 'fileutils'
require 'thread'

module Anemone
  module Storage
    class NFS 

      BINARY_FIELDS = %w(body headers data)
      YML_FILE = 'page.yml'
      UNFETCHED_SIZE = 5000

      def initialize(path, links_path)
        @base_path = path
        @links_path = links_path
        @unfetched = Set.new
        @semaphore = Mutex.new
      end

      def [](url)
        load_content(url.to_s)
      end

      def []=(url, page)
        collected_link(url, page.fetched?)
        insert_content(url,  page.to_hash)
      end

      def delete(url)
        page = self[url]
        remove_content(url)
        page
      end

      def each
        raise "Each method not supported for NFS"
      end

      def merge!(hash)
        hash.each { |key, value| self[key] = value }
        self
      end

      def size
        raise "Size not supported for NFS"
      end

      def keys
        raise "Keys not supported for NFS"
      end

      def has_key?(url)
        content_exists?(url)
      end

      def close; end
      
    private
    
      def content_exists?(url)
        path = url_to_path(url)

        return false unless path
        File.exists? path
      end
    
      def load_content(url)
        path = url_to_path(url)

        return nil unless path
        load_page(url)
      end
      
      def insert_content(url, hash)
        path = url_to_path(url)

        return nil unless path
        write_page(path, hash)
      end
            
      def remove_content(url)i
      
      def load_page(path)
        yml_file = "#{path}/#{YML_FILE}"

        return nil unless File.exists? yml_file
        hash = YAML.load_file(yml_file)
        
        BINARY_FIELDS.each do |field|
          IO.read("#{path}/#{field}")
          hash[field] = hash[field].to_s
        end
        
        Page.from_hash(hash)
      end

      def write_page(path, hash)
        FileUtils.mkdir_p path unless File.exists? path
        
        yaml_hash = {}
        hash.each do |field, value|
          if BINARY_FIELDS.include? field
            IO.write("#{path}/#{field}", value)
          else
            yaml_hash[field] = value
          end
        end
        
        File.open("#{path}/page.yml", "w") {|f| f.write(hash.to_yaml)}
      #rescue TODO
        
      end
    
      def load_links(limit)
        @semaphore.synchronize {
        
          if @unfetched_link.size > 0
            existing_links = @unfetched_link.to_a
            @unfetched_link = Set.new
            return existing_links
          end
        
          random_pagefile = Dir.entries(@links_path).pop
          if File.exists? "#{@links_path}/#{@random_pagefile}"
            links = Marhal.load(IO.read("#{@links_path}/#{@random_pagefile}"))
            File.delete "#{@links_path}/#{@random_pagefile}"
            return links.to_a
          end
  
          nil
        }
        
      #rescue TODO 
        end
    
      def collected_link(link, fetched)
        @semaphore.synchronize {
          if fetched
            @unfetched_link.delete link
            return
          end
        
          @unfetched_link << link
        
          if @unfetched_link.count >= UNFETCHED_SIZE
            IO.write("#{@links_path}/#{Digest::MD5.hexdigest(Time.now.to_i)}" , Marshal.dump(@unfetched_link))
          end
        
          @unfetched_link = Set.new
        }
      end
      
      # Convert an url to a path where to load/store files
      def url_to_path(url_s)
        return nil unless url_s
        
        url = (url_s.class == URI) ? url_s : URI.parse(url_s)
        bucket = url_to_bucket(url)
        
        base_path = (bucket && !bucket.empty?) ? "#{@base_path}/#{bucket}" : @base_path
        "#{base_path}/#{url_to_md5_path(url)}"
      end
    
      # MD5 of the URL path + create new path as block of 4 letters of the MD5
      # e.g. "eb0f/7e9d/1d52/1863/ca6d/afe0/effe/5da3"
      def url_to_md5_path(url)
        url = (url.class == URI) ? url : URI.parse(url)
        md5_path = Digest::MD5.hexdigest url.path
        
        res = []
        i = 0
        8.times { res << md5_path[i..(i+3)]; i+=4 }
        
        full_path = res.join "/"
      end
      
      # If a bucket is defined (e.g. write all the /in/* pages in /in ) use it as a storage prefix
      def url_to_bucket(url)
        return nil unless @buckets

        url = (url.class == URI) ? url : URI.parse(url)
        path = url.path
        @buckets.each { |bucket, bucket_path| return bucket_path if path.match(bucket) }
        
        nil
      end
      
    end
  end
end

