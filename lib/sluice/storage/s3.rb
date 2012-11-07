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

# Authors::    Alex Dean (mailto:support@snowplowanalytics.com), Michael Tibben
# Copyright:: Copyright (c) 2012 SnowPlow Analytics Ltd
# License::   Apache License Version 2.0

require 'fog'
require 'thread'

module Sluice
  module Storage
    module S3

      # TODO: figure out logging instead of puts (https://github.com/snowplow/sluice/issues/2)
      # TODO: consider moving to OO structure (https://github.com/snowplow/sluice/issues/3)

      # Constants
      CONCURRENCY = 10 # Threads
      RETRIES = 3      # Attempts
      RETRY_WAIT = 10  # Seconds

      # Class to describe an S3 location
      # TODO: if we are going to impose trailing line-breaks on
      # buckets, maybe we should make that clearer?
      class Location
        attr_reader :bucket, :dir, :s3location

        # Parameters:
        # +s3location+:: the s3 location config string e.g. "bucket/directory"
        def initialize(s3_location)
          @s3_location = s3_location

          s3_location_match = s3_location.match('^s3n?://([^/]+)/?(.*)/$')
          raise ArgumentError, 'Bad S3 location %s' % s3_location unless s3_location_match

          @bucket = s3_location_match[1]
          @dir = s3_location_match[2]
        end

        def dir_as_path
          if @dir.length > 0
            return @dir+'/'
          else
            return ''
          end
        end

        def to_s
          @s3_location
        end
      end

      # Helper function to instantiate a new Fog::Storage
      # for S3 based on our config options
      #
      # Parameters:
      # +region+:: Amazon S3 region we will be working with
      # +access_key_id+:: AWS access key ID
      # +secret_access_key+:: AWS secret access key
      def new_fog_s3_from(region, access_key_id, secret_access_key)
        Fog::Storage.new({
          :provider => 'AWS',
          :region => region,
          :aws_access_key_id => access_key_id,
          :aws_secret_access_key => secret_access_key
        })
      end
      module_function :new_fog_s3_from

      # Determine if a bucket is empty
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +location+:: The location to check
      def is_empty?(s3, location)
        s3.directories.get(location.bucket, :prefix => location.dir).files().length > 1
      end
      module_function :is_empty?

      # Download files from an S3 location to
      # local storage, concurrently
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +from_location+:: S3Location to delete files from      
      # +to_directory+:: Local directory to copy files to
      # +match_regex+:: a regex string to match the files to delete
      def download_files(s3, from_location, to_directory, match_regex='.+')

        puts "  downloading files from #{from_location} to #{to_directory}"
        process_files(:download, s3, from_location, match_regex, to_directory)
      end
      module_function :download_files    

      # Delete files from S3 locations concurrently
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +from_location+:: S3Location to delete files from
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
      # +from_location+:: S3Location to copy files from
      # +to_location+:: S3Location to copy files to
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
      # +from_location+:: S3Location to move files from
      # +to+:: S3Location to move files to
      # +match_regex+:: a regex string to match the files to move
      # +alter_filename_lambda+:: lambda to alter the written filename
      def move_files(s3, from_location, to_location, match_regex='.+', alter_filename_lambda=false)

        puts "  moving files from #{from_location} to #{to_location}"
        process_files(:move, s3, from_location, match_regex, to_location, alter_filename_lambda)
      end
      module_function :move_files

      # Download a single file to the exact path specified.
      # Has no intelligence around filenaming.
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +from_file:: A Fog::File to download
      # +to_file:: A local file path
      def download_file(s3, from_file, to_file)

        puts "We're going to download a file #{from_file.key}"
        puts from_file.body

        # TODO: update this with the file download code

      end
      module_function :download_file

      private

      # Concurrent file operations between S3 locations. Supports:
      # - Copy
      # - Delete
      # - Move (= Copy + Delete)
      #
      # Parameters:
      # +operation+:: Operation to perform. :copy, :delete, :move supported
      # +s3+:: A Fog::Storage s3 connection
      # +from_location+:: S3Location to process files from
      # +match_regex+:: a regex string to match the files to process
      # +to_loc_or_dir+:: S3Location or local directory to process files to
      # +alter_filename_lambda+:: lambda to alter the written filename
      def process_files(operation, s3, from_location, match_regex='.+', to_loc_or_dir=nil, alter_filename_lambda=false)

        # Validate that the file operation makes sense
        case operation
        when :copy, :move, :download
          if to_loc_or_dir.nil?
            raise StorageOperationError "File operation %s requires the to_loc_or_dir to be set" % operation
          end
        when :delete
          unless to_loc_or_dir.nil?
            raise StorageOperationError "File operation %s does not support the to_loc_or_dir argument" % operation
          end
          if alter_filename_lambda.class == Proc
            raise StorageOperationError "File operation %s does not support the alter_filename_lambda argument" % operation
          end
        else
          raise StorageOperationError "File operation %s is unsupported. Try :download, :copy, :delete or :move" % operation
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
                    # S3 batches 1000 files per request.
                    # We load up our array with the files to move
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
              break unless match # Exit the thread

              # Match the filename, ignoring directory
              file_match = file.key.match('([^/]+)$')

              # Silently skip any sub-directories in the list
              break unless file_match

              if alter_filename_lambda.class == Proc
                filename = alter_filename_lambda.call(file_match[1])
              else
                filename = file_match[1]
              end

              # What are we doing? Let's determine source and target
              # Note that target excludes bucket name where relevant
              source = "#{from_location.bucket}/#{file.key}"
              case operation
              when :download

                # TODO: due to nature of S3 & Fog, if there is a sub-path
                # on the from_location bucket, then that sub-path is
                # recreated in the to_loc_or_dir local folder. Maybe we
                # should strip those folders off.
                target = "#{to_loc_or_dir}/#{file.key}"
                puts "    DOWNLOAD #{source} +-> #{target}"
              when :move
                target = name_file(file.key, filename, from_location.dir_as_path, to_loc_or_dir.dir_as_path)
                puts "    MOVE #{source} -> #{to_loc_or_dir.bucket}/#{target}"
              when :copy
                target = name_file(file.key, filename, from_location.dir_as_path, to_loc_or_dir.dir_as_path)
                puts "    COPY #{source} +-> #{to_loc_or_dir.bucket}/#{target}"
              when :delete
                # No target
                puts "    DELETE x #{source}" 
              end

              # Download is a stand-alone operation vs move/copy/delete
              if operation == :download
                download_file(s3, file, target)
                puts "      +/> #{target}"
              end

              # A move or copy starts with a copy file
              if [:move, :copy].include? operation
                i = 0
                begin
                  file.copy(to_loc_or_dir.bucket, target)
                  puts "      +-> #{to_loc_or_dir.bucket}/#{target}"
                rescue
                  raise unless i < RETRIES
                  puts "Problem copying #{file.key}. Retrying.", $!, $@
                  sleep(RETRY_WAIT)  # Give us a bit of time before retrying
                  i += 1
                  retry
                end
              end

              # A move or delete ends with a delete
              if [:move, :delete].include? operation
                i = 0
                begin
                  file.destroy()
                  puts "      x #{source}"
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

      # A helper function to prepare destination
      # filenames and paths. This is a bit weird
      # - it needs to exist because of differences
      # in the way that Amazon S3, Fog and Unix
      # treat filepaths versus keys.
      #
      # Parameter:
      # +filepath+:: Path to file (including old filename)
      # +new_filename+:: Replace the filename in the path with this
      # +remove_path+:: If this is set, strip this from the front of the path
      # +add_path+:: If this is set, add this to the front of the path
      #
      # TODO: this really needs unit tests
      def name_file(filepath, new_filename, remove_path=nil, add_path=nil)

        # First, replace the filename in filepath with new one
        new_filepath = File.dirname(filepath) + '/' + new_filename

        # Nothing more to do
        return new_filepath if remove_path.nil?

        # If we have a 'remove_path', it must be found at
        # the start of the path.
        # If it's not, you're probably using name_file()
        # wrong.
        if !filepath.start_with?(remove_path)
          raise StorageOperationError, "name_file failed. Filepath '#{filepath}' does not start with '#{remove_path}'"
        end

        # Okay, let's remove the filepath
        shortened_filepath = new_filepath[remove_path.length()..-1]

        # Nothing more to do
        return shortened_filepath if add_path.nil?

        # Add the new filepath on to the start and return
        add_path + shortened_filepath
      end
      module_function :name_file

    end
  end
end