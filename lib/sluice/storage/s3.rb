# Copyright (c) 2012 SnowPlow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Author::    Alex Dean (mailto:support@snowplowanalytics.com), Michael Tibben
# Copyright:: Copyright (c) 2012 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

require 'fog'
require 'thread'

module Sluice
  module Storage
    module S3

      # TODO: fix new_s3_from so it's freestanding
      # TODO: figure out logging instead of puts

      # Constants
      CONCURRENCY = 10 # Threads
      RETRIES = 3      # Attempts
      RETRY_WAIT = 10  # Seconds

      # TODO: fix this!
      # Helper function to instantiate a new Fog::Storage
      # for S3 based on our config options
      #
      # Parameters:
      # +config+:: the hash of configuration options
      def new_s3_from(config)
        Fog::Storage.new({
          :provider => 'AWS',
          :region => config[:s3][:region],
          :aws_access_key_id => config[:aws][:access_key_id],
          :aws_secret_access_key => config[:aws][:secret_access_key]
        })
      end
      module_function :new_s3_from

      # Delete files from S3 locations concurrently
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +from+:: S3Location to delete files from
      # +match_regex+:: a regex string to match the files to delete
      def delete_files(s3, from_location, match_regex='.+')

        puts "  deleting files from #{from_location}"
        process_files(:delete, s3, from_location, match_regex)
      end
      module_function :delete_files

      # Copies files between S3 locations concurrently
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +from+:: S3Location to copy files from
      # +to+:: S3Location to copy files to
      # +match_regex+:: a regex string to match the files to copy
      # +alter_filename_lambda+:: lambda to alter the written filename
      def copy_files(s3, from_location, to_location, match_regex='.+', alter_filename_lambda=false)

        puts "  copying files from #{from_location} to #{to_location}"
        process_files(:copy, s3, from_location, match_regex, to_location, alter_filename_lambda)
      end
      module_function :copy_files

      # Moves files between S3 locations concurrently
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +from+:: S3Location to move files from
      # +to+:: S3Location to move files to
      # +match_regex+:: a regex string to match the files to move
      # +alter_filename_lambda+:: lambda to alter the written filename
      def move_files(s3, from_location, to_location, match_regex='.+', alter_filename_lambda=false)

        puts "  moving files from #{from_location} to #{to_location}"
        process_files(:move, s3, from_location, match_regex, to_location, alter_filename_lambda)
      end
      module_function :move_files

      private

      # Concurrent file operations between S3 locations. Supports:
      # - Copy
      # - Delete
      # - Move (= Copy + Delete)
      #
      # Parameters:
      # +operation+:: Operation to perform. :copy, :delete, :move supported
      # +s3+:: A Fog::Storage s3 connection
      # +from+:: S3Location to process files from
      # +match_regex+:: a regex string to match the files to process
      # +to+:: S3Location to process files to
      # +alter_filename_lambda+:: lambda to alter the written filename
      def process_files(operation, s3, from_location, match_regex='.+', to_location=nil, alter_filename_lambda=false)

        # Validate that the file operation makes sense
        case operation
        when :copy, :move
          if to_location.nil?
            raise StorageOperationError "File operation %s requires a to_location to be set" % operation
          end
        when :delete
          unless to_location.nil?
            raise StorageOperationError "File operation %s does not support the to_location argument" % operation
          end
          if alter_filename_lambda.class == Proc
            raise StorageOperationError "File operation %s does not support the alter_filename_lambda argument" % operation
          end
        else
          raise StorageOperationError "File operation %s is unsupported. Try :copy, :delete or :move" % operation
        end

        files_to_process = []
        threads = []
        mutex = Mutex.new
        complete = false
        marker_opts = {}

        # If an exception is thrown in a thread that isn't handled, die quickly
        Thread.abort_on_exception = true

        # Create ruby threads to concurrently execute s3 operations
        for i in (0...CONCURRENCY)

          # Each thread pops a file off the files_to_process array, and moves it.
          # We loop until there are no more files
          threads << Thread.new do
            loop do
              file = false
              match = false

              # Critical section:
              # only allow one thread to modify the array at any time
              mutex.synchronize do

                while !complete && !match do
                  if files_to_process.size == 0
                    # s3 batches 1000 files per request
                    # we load up our array with the files to move
                    files_to_process = s3.directories.get(from_location.bucket, :prefix => from_location.dir).files.all(marker_opts)
                    # if we don't have any files after the s3 request, we're complete
                    if files_to_process.size == 0
                      complete = true
                      next
                    else
                      marker_opts['marker'] = files_to_process.last.key

                      # By reversing the array we can use pop and get FIFO behaviour
                      # instead of the performance penalty incurred by unshift
                      files_to_process = files_to_process.reverse
                    end
                  end

                  file = files_to_process.pop
                  match = if match_regex.is_a? NegativeRegex
                            !file.key.match(match_regex.regex)
                          else
                            file.key.match(match_regex)
                          end
                end
              end

              # If we don't have a match, then we must be complete
              break unless match # exit the thread

              # Match the filename, ignoring directory
              file_match = file.key.match('([^/]+)$')

              # Silently skip any sub-directories in the list
              break unless file_match

              if alter_filename_lambda.class == Proc
                filename = alter_filename_lambda.call(file_match[1])
              else
                filename = file_match[1]
              end

              # What are we doing?
              case operation
              when :move
                puts "    MOVE #{from_location.bucket}/#{file.key} -> #{to_location.bucket}/#{to_location.dir_as_path}#{filename}"            
              when :copy
                puts "    COPY #{from_location.bucket}/#{file.key} +-> #{to_location.bucket}/#{to_location.dir_as_path}#{filename}"  
              when :delete
                puts "    DELETE x #{from_location.bucket}/#{file.key}" 
              end

              # A move or copy starts with a copy file
              if [:move, :copy].include? operation
                i = 0
                begin
                  file.copy(to_location.bucket, to_location.dir_as_path + filename)
                  puts "      +-> #{to_location.bucket}/#{to_location.dir_as_path}#{filename}"
                rescue
                  raise unless i < RETRIES
                  puts "Problem copying #{file.key}. Retrying.", $!, $@
                  sleep(RETRY_WAIT)  # give us a bit of time before retrying
                  i += 1
                  retry
                end
              end

              # A move or delete ends with a delete
              if [:move, :delete].include? operation
                i = 0
                begin
                  file.destroy()
                  puts "      x #{from_location.bucket}/#{file.key}"
                rescue
                  raise unless i < RETRIES
                  puts "Problem destroying #{file.key}. Retrying.", $!, $@
                  sleep(RETRY_WAIT) # Give us a bit of time before retrying
                  i += 1
                  retry
                end
              end
            end
          end
        end

        # Wait for threads to finish
        threads.each { |aThread|  aThread.join }

      end
      module_function :process_files

    end
  end
end