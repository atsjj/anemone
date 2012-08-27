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
      LINK_BATCH_SIZE = 5000

      def initialize(path, buckets=nil)
        @base_path = path
        @unfetched_path = "#{@base_path}/_links"
        @fetched_path = "#{@base_path}/_pages"
        @unfetched_links = Set.new
        @fetched_links = Set.new
        @semaphore = Mutex.new
        @buckets = buckets
        
        [@base_path, @unfetched_path, @fetched_path].each {|directory| FileUtils.mkdir_p directory }
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

      def close
        dump_set_on_fs(@fetched_links, @fetched_path)       if @fetched_links.count > 0
        dump_set_on_fs(@unfetched_links, @unfetched_path)   if @unfetched_links.count > 0
      end
      
      def load_links(limit)
        @semaphore.synchronize {
        
          if @unfetched_links.size > 0
            existing_links = @unfetched_links.to_a
            @unfetched_links.clear
            return existing_links
          end
        
          random_pagefile = Dir.entries(@unfetched_path).pop
          
          return load_set_from_fs("#{@unfetched_path}/#{random_pagefile}") if random_pagefile && (random_pagefile != '.' && random_pagefile != '..')
          
          nil
        }        
      end
      
    private
    
      def content_exists?(url)
        path = url_to_path(url)

        return false unless path
        File.exists? path
      end
    
      def load_content(url)
        path = url_to_path(url)

        return nil unless path
        load_page(path)
      end
      
      def insert_content(url, hash)
        path = url_to_path(url)

        return nil unless path
        write_page(path, hash)
      end
            
      def remove_content(url)
        path = url_to_path(url)

        return unless path
        FileUtils.rm_rf path
      end
      
      def load_page(path)
        yml_file = "#{path}/#{YML_FILE}"

        return nil unless File.exists? yml_file
        hash = YAML.load_file(yml_file)
        
        BINARY_FIELDS.each do |field|
          hash[field] = File.open("#{path}/#{field}", 'r').read().to_s
        end
        
        Page.from_hash(hash)
      end

      def write_page(path, hash)
        FileUtils.mkdir_p path unless File.exists? path
        
        yaml_hash = {}
        hash.each do |field, value|
          if BINARY_FIELDS.include? field
            File.open("#{path}/#{field}", 'w') {|f| f.write(value) }
          else
            yaml_hash[field] = value
          end
        end
        
        File.open("#{path}/page.yml", "w") {|f| f.write(yaml_hash.to_yaml)}        
      end

    
      def collected_link(link, fetched)
        @semaphore.synchronize {
          if fetched
            @unfetched_links.delete link
            @fetched_links << link
          
            dump_set_on_fs(@fetched_links, @fetched_path) if @fetched_links.count >= LINK_BATCH_SIZE
            return
          end
        
          @unfetched_links << link
          dump_set_on_fs(@unfetched_links, @unfetched_path)  if @unfetched_links.count >= LINK_BATCH_SIZE
        }
      end
      
      def load_set_from_fs(file)
        return nil unless File.exists? file
        
        content = File.open(file, "r").read()
        File.delete file
        
        content.split "\n"
      end

      def dump_set_on_fs(set, path)
        links = set.to_a
        file = "#{path}/#{Digest::MD5.hexdigest(Time.now.to_i.to_s)}"
        File.open(file, 'w') {|f| links.each {|link|  f.write("#{link}\n") } }
        set.clear
      end
      
      # Convert an url to a path where to load/store files
      def url_to_path(url_s)
        return nil unless url_s
        
        url = (url_s.class != String) ? url_s : URI.parse(url_s)
        bucket = url_to_bucket(url)
        
        base_path = (bucket && !bucket.empty?) ? "#{@base_path}/#{bucket}" : @base_path
        "#{base_path}/#{url_to_md5_path(url)}"
      end
    
      # MD5 of the URL path + create new path as block of 4 letters of the MD5
      # e.g. "eb0f/7e9d/1d52/1863/ca6d/afe0/effe/5da3"
      def url_to_md5_path(url)
        md5_path = Digest::MD5.hexdigest url.path
        
        res = []
        i = 0
        8.times { res << md5_path[i..(i+3)]; i+=4 }
        
        full_path = res.join "/"
      end
      
      # If a bucket is defined (e.g. write all the /in/* pages in /in ) use it as a storage prefix
      def url_to_bucket(url)
        return nil unless @buckets

        path = url.path
        @buckets.each { |bucket, bucket_path| return bucket_path if path.match(bucket) }
        
        nil
      end
      
    end
  end
end