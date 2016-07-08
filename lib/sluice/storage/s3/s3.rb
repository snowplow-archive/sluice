# Copyright (c) 2012-2014 Snowplow Analytics Ltd. All rights reserved.
#
# This program is licensed to you under the Apache License Version 2.0,
# and you may not use this file except in compliance with the Apache License Version 2.0.
# You may obtain a copy of the Apache License Version 2.0 at http://www.apache.org/licenses/LICENSE-2.0.
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the Apache License Version 2.0 is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the Apache License Version 2.0 for the specific language governing permissions and limitations there under.

# Authors::   Alex Dean (mailto:support@snowplowanalytics.com), Michael Tibben
# Copyright:: Copyright (c) 2012-2014 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

require 'tmpdir'
require 'fog'
require 'thread'
require 'timeout'

require 'contracts'

module Sluice
  module Storage
    module S3

      include Contracts

      # TODO: figure out logging instead of puts (https://github.com/snowplow/sluice/issues/2)
      # TODO: consider moving to OO structure (https://github.com/snowplow/sluice/issues/3)

      # Constants
      CONCURRENCY = 10 # Threads
      RETRIES = 3      # Attempts
      RETRY_WAIT = 10  # Seconds
      TIMEOUT_WAIT = 1800 # 30 mins should let even large files upload. +1 https://github.com/snowplow/sluice/issues/7 if this is insufficient or excessive

      # Helper function to instantiate a new Fog::Storage
      # for S3 based on our config options
      #
      # Parameters:
      # +region+:: Amazon S3 region we will be working with
      # +access_key_id+:: AWS access key ID
      # +secret_access_key+:: AWS secret access key
      Contract String, String, String => FogStorage
      def self.new_fog_s3_from(region, access_key_id, secret_access_key)
        fog = Fog::Storage.new({
          :provider => 'AWS',
          :region => region,
          :aws_access_key_id => access_key_id,
          :aws_secret_access_key => secret_access_key
        })
        fog.sync_clock
        fog
      end

      # Return an array of all Fog::Storage::AWS::File's
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +location+:: The location to return files from
      #
      # Returns array of Fog::Storage::AWS::File's
      Contract FogStorage, Location => ArrayOf[FogFile]
      def self.list_files(s3, location)
        files_and_dirs = s3.directories.get(location.bucket, prefix: location.dir_as_path).files

        files = [] # Can't use a .select because of Ruby deep copy issues (array of non-POROs)
        files_and_dirs.each { |f|
          if is_file?(f.key)
            files << f.dup
          end
        }
        files
      end

      # Whether the given path is a directory or not
      #
      # Parameters:
      # +path+:: S3 path in String form
      #
      # Returns boolean
      Contract String => Bool
      def self.is_folder?(path)
        (path.end_with?('_$folder$') || # EMR-created
          path.end_with?('/'))
      end

      # Whether the given path is a file or not
      #
      # Parameters:
      # +path+:: S3 path in String form
      #
      # Returns boolean
      Contract String => Bool
      def self.is_file?(path)
        !is_folder?(path)
      end

      # Returns the basename for the given path
      #
      # Parameters:
      # +path+:: S3 path in String form
      #
      # Returns the basename, or nil if the
      # path is to a folder
      Contract String => Maybe[String]
      def self.get_basename(path)
        if is_folder?(path)
          nil
        else
          match = path.match('([^/]+)$')
          if match
            match[1]
          else
            nil
          end
        end
      end

      # Determine if a bucket is empty
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +location+:: The location to check
      Contract FogStorage, Location => Bool
      def self.is_empty?(s3, location)
        list_files(s3, location).length == 0
      end

      # Download files from an S3 location to
      # local storage, concurrently
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +from_files_or_loc+:: Array of filepaths or Fog::Storage::AWS::File objects, or S3Location to download files from      
      # +to_directory+:: Local directory to copy files to
      # +match_regex+:: a regex string to match the files to delete
      def self.download_files(s3, from_files_or_loc, to_directory, match_regex='.+')

        puts "  downloading #{describe_from(from_files_or_loc)} to #{to_directory}"
        process_files(:download, s3, from_files_or_loc, [], match_regex, to_directory)
      end

      # Delete files from S3 locations concurrently
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +from_files_or_loc+:: Array of filepaths or Fog::Storage::AWS::File objects, or S3Location to delete files from
      # +match_regex+:: a regex string to match the files to delete
      def self.delete_files(s3, from_files_or_loc, match_regex='.+')

        puts "  deleting #{describe_from(from_files_or_loc)}"
        process_files(:delete, s3, from_files_or_loc, [], match_regex)
      end

      # Copies files between S3 locations in two different accounts
      #
      # Implementation is as follows:
      # 1. Concurrent download of all files from S3 source to local tmpdir
      # 2. Concurrent upload of all files from local tmpdir to S3 target
      #
      # In other words, the download and upload are not interleaved (which is
      # inefficient because upload speeds are much lower than download speeds)
      #
      # In other words, the download and upload are not interleaved (which
      # is inefficient because upload speeds are much lower than download speeds)
      #
      # +from_s3+:: A Fog::Storage s3 connection for accessing the from S3Location
      # +to_s3+:: A Fog::Storage s3 connection for accessing the to S3Location
      # +from_location+:: S3Location to copy files from
      # +to_location+:: S3Location to copy files to
      # +match_regex+:: a regex string to match the files to move
      # +alter_filename_lambda+:: lambda to alter the written filename
      # +flatten+:: strips off any sub-folders below the from_location
      def self.copy_files_inter(from_s3, to_s3, from_location, to_location, match_regex='.+', alter_filename_lambda=false, flatten=false)

        puts "  copying inter-account #{describe_from(from_location)} to #{to_location}"
        processed = []
        Dir.mktmpdir do |t|
          tmp = Sluice::Storage.trail_slash(t)
          processed = download_files(from_s3, from_location, tmp, match_regex)
          upload_files(to_s3, tmp, to_location, '**/*') # Upload all files we downloaded
        end

        processed
      end

      # Copies files between S3 locations concurrently
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +from_files_or_loc+:: Array of filepaths or Fog::Storage::AWS::File objects, or S3Location to copy files from
      # +to_location+:: S3Location to copy files to
      # +match_regex+:: a regex string to match the files to copy
      # +alter_filename_lambda+:: lambda to alter the written filename
      # +flatten+:: strips off any sub-folders below the from_location
      def self.copy_files(s3, from_files_or_loc, to_location, match_regex='.+', alter_filename_lambda=false, flatten=false)

        puts "  copying #{describe_from(from_files_or_loc)} to #{to_location}"
        process_files(:copy, s3, from_files_or_loc, [], match_regex, to_location, alter_filename_lambda, flatten)
      end

      # Copies files between S3 locations maintaining a manifest to
      # avoid copying a file which was copied previously.
      #
      # Useful in scenarios such as:
      # 1. You would like to do a move but only have read permission
      #    on the source bucket
      # 2. You would like to do a move but some other process needs
      #    to use the files after you
      #
      # +s3+:: A Fog::Storage s3 connection
      # +manifest+:: A Sluice::Storage::S3::Manifest object
      # +from_files_or_loc+:: Array of filepaths or Fog::Storage::AWS::File objects, or S3Location to copy files from
      # +to_location+:: S3Location to copy files to
      # +match_regex+:: a regex string to match the files to copy
      # +alter_filename_lambda+:: lambda to alter the written filename
      # +flatten+:: strips off any sub-folders below the from_location
      def self.copy_files_manifest(s3, manifest, from_files_or_loc, to_location, match_regex='.+', alter_filename_lambda=false, flatten=false)

        puts "  copying with manifest #{describe_from(from_files_or_loc)} to #{to_location}"
        ignore = manifest.get_entries(s3) # Files to leave untouched
        processed = process_files(:copy, s3, from_files_or_loc, ignore, match_regex, to_location, alter_filename_lambda, flatten)
        manifest.add_entries(s3, processed)

        processed
      end

      # Moves files between S3 locations in two different accounts
      #
      # Implementation is as follows:
      # 1. Concurrent download of all files from S3 source to local tmpdir
      # 2. Concurrent upload of all files from local tmpdir to S3 target
      # 3. Concurrent deletion of all files from S3 source
      #
      # In other words, the three operations are not interleaved (which is
      # inefficient because upload speeds are much lower than download speeds)
      #
      # +from_s3+:: A Fog::Storage s3 connection for accessing the from S3Location
      # +to_s3+:: A Fog::Storage s3 connection for accessing the to S3Location      
      # +from_location+:: S3Location to move files from
      # +to_location+:: S3Location to move files to
      # +match_regex+:: a regex string to match the files to move
      # +alter_filename_lambda+:: lambda to alter the written filename
      # +flatten+:: strips off any sub-folders below the from_location
      def self.move_files_inter(from_s3, to_s3, from_location, to_location, match_regex='.+', alter_filename_lambda=false, flatten=false)

        puts "  moving inter-account #{describe_from(from_location)} to #{to_location}"
        processed = []
        Dir.mktmpdir do |t|
          tmp = Sluice::Storage.trail_slash(t)
          processed = download_files(from_s3, from_location, tmp, match_regex)
          upload_files(to_s3, tmp, to_location, '**/*') # Upload all files we downloaded
          delete_files(from_s3, from_location, '.+') # Delete all files we downloaded
        end

        processed
      end

      # Moves files between S3 locations concurrently
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +from_files_or_loc+:: Array of filepaths or Fog::Storage::AWS::File objects, or S3Location to move files from
      # +to_location+:: S3Location to move files to
      # +match_regex+:: a regex string to match the files to move
      # +alter_filename_lambda+:: lambda to alter the written filename
      # +flatten+:: strips off any sub-folders below the from_location
      def self.move_files(s3, from_files_or_loc, to_location, match_regex='.+', alter_filename_lambda=false, flatten=false)

        puts "  moving #{describe_from(from_files_or_loc)} to #{to_location}"
        process_files(:move, s3, from_files_or_loc, [], match_regex, to_location, alter_filename_lambda, flatten)
      end

      # Uploads files to S3 locations concurrently
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +from_files_or_dir+:: Local array of files or local directory to upload files from
      # +to_location+:: S3Location to upload files to
      # +match_glob+:: a filesystem glob to match the files to upload
      def self.upload_files(s3, from_files_or_dir, to_location, match_glob='*')

        puts "  uploading #{describe_from(from_files_or_dir)} to #{to_location}"
        process_files(:upload, s3, from_files_or_dir, [], match_glob, to_location)
      end

      # Upload a single file to the exact location specified
      # Has no intelligence around filenaming.
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +from_file:: A local file path
      # +to_bucket:: The Fog::Directory to upload to
      # +to_file:: The file path to upload to
      def self.upload_file(s3, from_file, to_bucket, to_file)

        local_file = File.open(from_file)

        dir = s3.directories.new(:key => to_bucket) # No request made
        file = dir.files.create(
          :key    => to_file,
          :body   => local_file
        )

        local_file.close
      end

      # Download a single file to the exact path specified
      # Has no intelligence around filenaming.
      # Makes sure to create the path as needed.
      #
      # Parameters:
      # +s3+:: A Fog::Storage s3 connection
      # +from_file:: A Fog::Storage::AWS::File to download
      # +to_file:: A local file path
      def self.download_file(s3, from_file, to_file)

        FileUtils.mkdir_p(File.dirname(to_file))

        # TODO: deal with bug where Fog hangs indefinitely if network connection dies during download

        local_file = File.open(to_file, "w")
        local_file.write(from_file.body)
        local_file.close
      end

      private

      # Provides string describing from_files_or_dir_or_loc
      # for logging purposes.
      #
      # Parameters:
      # +from_files_or_dir_or_loc+:: Array of filepaths or Fog::Storage::AWS::File objects, local directory or S3Location to process files from
      #
      # Returns a log-friendly string
      def self.describe_from(from_files_or_dir_or_loc)
        if from_files_or_dir_or_loc.is_a?(Array)
          "#{from_files_or_dir_or_loc.length} file(s)"
        else
          "files from #{from_files_or_dir_or_loc}"
        end
      end

      # Concurrent file operations between S3 locations. Supports:
      # - Download
      # - Upload
      # - Copy
      # - Delete
      # - Move (= Copy + Delete)
      #
      # Parameters:
      # +operation+:: Operation to perform. :download, :upload, :copy, :delete, :move supported
      # +ignore+:: Array of filenames to ignore (used by manifest code)
      # +s3+:: A Fog::Storage s3 connection
      # +from_files_or_dir_or_loc+:: Array of filepaths or Fog::Storage::AWS::File objects, local directory or S3Location to process files from
      # +match_regex_or_glob+:: a regex or glob string to match the files to process
      # +to_loc_or_dir+:: S3Location or local directory to process files to
      # +alter_filename_lambda+:: lambda to alter the written filename
      # +flatten+:: strips off any sub-folders below the from_loc_or_dir
      def self.process_files(operation, s3, from_files_or_dir_or_loc, ignore=[], match_regex_or_glob='.+', to_loc_or_dir=nil, alter_filename_lambda=false, flatten=false)

        # Validate that the file operation makes sense
        case operation
        when :copy, :move, :download, :upload
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
          raise StorageOperationError "File operation %s is unsupported. Try :download, :upload, :copy, :delete or :move" % operation
        end

        # If we have an array of files, no additional globbing required
        if from_files_or_dir_or_loc.is_a?(Array)
          files_to_process = from_files_or_dir_or_loc # Could be filepaths or Fog::Storage::AWS::File's
          globbed = true
        # Otherwise if it's an upload, we can glob now
        elsif operation == :upload
          files_to_process = glob_files(from_files_or_dir_or_loc, match_regex_or_glob)
          globbed = true
        # Otherwise we'll do threaded globbing later...
        else
          files_to_process = []
          from_loc = from_files_or_dir_or_loc # Alias
          globbed = false
        end

        threads = []
        mutex = Mutex.new
        complete = false
        marker_opts = {}
        processed_files = [] # For manifest updating, determining if any files were moved etc

        # If an exception is thrown in a thread that isn't handled, die quickly
        Thread.abort_on_exception = true

        # Create Ruby threads to concurrently execute s3 operations
        for i in (0...CONCURRENCY)

          # Each thread pops a file off the files_to_process array, and moves it.
          # We loop until there are no more files
          threads << Thread.new(i) do |thread_idx|

            loop do
              file = false
              filepath = false
              from_bucket = false
              from_path = false
              match = false

              # First critical section:
              # only allow one thread to modify the array at any time
              mutex.synchronize do

                # No need to do further globbing 
                if globbed
                  if files_to_process.size == 0
                    complete = true
                    next
                  end

                  file = files_to_process.pop
                  # Support raw filenames and also Fog::Storage::AWS::File's
                  if (file.is_a?(Fog::Storage::AWS::File))
                    from_bucket = file.directory.key # Bucket
                    from_path = Sluice::Storage.trail_slash(File.dirname(file.key))
                    filepath = file.key
                  else
                    from_bucket = nil # Not used
                    if from_files_or_dir_or_loc.is_a?(Array)
                      from_path = Sluice::Storage.trail_slash(File.dirname(file))
                    else
                      from_path = from_files_or_dir_or_loc # The root dir
                    end
                    filepath = file
                  end

                  match = true # Match is implicit in the glob
                else

                  while !complete && !match do
                    if files_to_process.size == 0
                      # S3 batches 1000 files per request.
                      # We load up our array with the files to move
                      files_to_process = s3.directories.get(from_loc.bucket, :prefix => from_loc.dir).files.all(marker_opts).to_a
                      # If we don't have any files after the S3 request, we're complete
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
                    from_bucket = from_loc.bucket
                    from_path = from_loc.dir_as_path
                    filepath = file.key

                    # TODO: clean up following https://github.com/snowplow/sluice/issues/25
                    match = if match_regex_or_glob.is_a? NegativeRegex
                              !filepath.match(match_regex_or_glob.regex)
                            else
                              filepath.match(match_regex_or_glob)
                            end

                  end
                end
              end
              # End of mutex.synchronize

              # Kill this thread's loop (and thus this thread) if we are complete
              break if complete

              # Skip processing for a folder or file which doesn't match our regexp or glob
              next if is_folder?(filepath) or not match

              # Name file
              basename = get_basename(filepath)
              next if ignore.include?(basename) # Don't process if in our leave list

              filename = rename_file(filepath, basename, alter_filename_lambda)

              # What are we doing? Let's determine source and target
              # Note that target excludes bucket name where relevant
              case operation
              when :upload
                source = "#{filepath}"
                target = name_file(filepath, filename, from_path, to_loc_or_dir.dir_as_path, flatten)
                puts "(t#{thread_idx})    UPLOAD #{source} +-> #{to_loc_or_dir.bucket}/#{target}"                
              when :download
                source = "#{from_bucket}/#{filepath}"
                target = name_file(filepath, filename, from_path, to_loc_or_dir, flatten)
                puts "(t#{thread_idx})    DOWNLOAD #{source} +-> #{target}"
              when :move
                source = "#{from_bucket}/#{filepath}"
                target = name_file(filepath, filename, from_path, to_loc_or_dir.dir_as_path, flatten)
                puts "(t#{thread_idx})    MOVE #{source} -> #{to_loc_or_dir.bucket}/#{target}"
              when :copy
                source = "#{from_bucket}/#{filepath}"
                target = name_file(filepath, filename, from_path, to_loc_or_dir.dir_as_path, flatten)
                puts "(t#{thread_idx})    COPY #{source} +-> #{to_loc_or_dir.bucket}/#{target}"
              when :delete
                source = "#{from_bucket}/#{filepath}"
                # No target
                puts "(t#{thread_idx})    DELETE x #{source}" 
              end

              # Upload is a standalone operation vs move/copy/delete
              if operation == :upload
                retry_x(
                  Sluice::Storage::S3,
                  [:upload_file, s3, filepath, to_loc_or_dir.bucket, target],
                  RETRIES,
                  "      +/> #{target}",
                  "Problem uploading #{filepath}. Retrying.")
              end                

              # Download is a standalone operation vs move/copy/delete
              if operation == :download
                retry_x(
                  Sluice::Storage::S3,
                  [:download_file, s3, file, target],
                  RETRIES,
                  "      +/> #{target}",
                  "Problem downloading #{filepath}. Retrying.")
              end

              # A move or copy starts with a copy file
              if [:move, :copy].include? operation
                retry_x(
                  file,
                  [:copy, to_loc_or_dir.bucket, target],
                  RETRIES,
                  "      +-> #{to_loc_or_dir.bucket}/#{target}",
                  "Problem copying #{filepath}. Retrying.")
              end

              # A move or delete ends with a delete
              if [:move, :delete].include? operation
                retry_x(
                  file,
                  [:destroy],
                  RETRIES,
                  "      x #{source}",
                  "Problem destroying #{filepath}. Retrying.")
              end

              # Second critical section: we need to update
              # processed_files in a thread-safe way
              mutex.synchronize do
                processed_files << filepath
              end
            end
          end
        end

        # Wait for threads to finish
        threads.each { |aThread|  aThread.join }

        processed_files # Return the processed files
      end

      # A helper function to rename a file
      # TODO: fixup lambda to be Maybe[Proc]
      def self.rename_file(filepath, basename, rename_lambda=false)

        if rename_lambda.class == Proc
          case rename_lambda.arity
          when 2
            rename_lambda.call(basename, filepath)
          when 1
            rename_lambda.call(basename)
          when 0
            rename_lambda.call()
          else
            raise StorageOperationError "Expect arity of 0, 1 or 2 for rename_lambda, not #{rename_lambda.arity}"
          end
        else
          basename
        end
      end

      # A helper function to list all files
      # recursively in a folder
      #
      # Parameters:
      # +dir+:: Directory to list files recursively
      # +match_regex+:: a regex string to match the files to copy      
      #
      # Returns array of files (no sub-directories)
      def self.glob_files(dir, glob)
        Dir.glob(File.join(dir, glob)).select { |f|
          File.file?(f) # Drop sub-directories
        }
      end

      # A helper function to attempt to run a
      # function retries times
      #
      # Parameters:
      # +object+:: Object to send our function to
      # +send_args+:: Function plus arguments
      # +retries+:: Number of retries to attempt
      # +attempt_msg+:: Message to puts on each attempt
      # +failure_msg+:: Message to puts on each failure
      def self.retry_x(object, send_args, retries, attempt_msg, failure_msg)
        i = 0
        begin
          Timeout::timeout(TIMEOUT_WAIT) do # In case our operation times out
            object.send(*send_args)
            puts attempt_msg
          end
        rescue
          raise unless i < retries
          puts failure_msg
          sleep(RETRY_WAIT)  # Give us a bit of time before retrying
          i += 1
          retry
        end
      end

      # A helper function to prepare destination
      # filenames and paths. This is a bit weird
      # - it needs to exist because of differences
      # in the way that Amazon S3, Fog and Unix
      # treat filepaths versus keys.
      #
      # Parameters:
      # +filepath+:: Path to file (including old filename)
      # +new_filename+:: Replace the filename in the path with this
      # +remove_path+:: If this is set, strip this from the front of the path
      # +add_path+:: If this is set, add this to the front of the path
      # +flatten+:: strips off any sub-folders below the from_location
      #
      # TODO: this badly needs unit tests
      def self.name_file(filepath, new_filename, remove_path=nil, add_path=nil, flatten=false)

        # First, replace the filename in filepath with new one
        dirname = File.dirname(filepath)
        new_filepath = (dirname == '.') ? new_filename : dirname + '/' + new_filename

        # Nothing more to do
        return new_filepath if remove_path.nil? and add_path.nil? and not flatten

        shortened_filepath =  if flatten
                                # Let's revert to just the filename
                                new_filename
                              else
                                # If we have a 'remove_path', it must be found at
                                # the start of the path.
                                # If it's not, you're probably using name_file()
                                # wrong.
                                if !filepath.start_with?(remove_path)
                                  raise StorageOperationError, "name_file failed. Filepath '#{filepath}' does not start with '#{remove_path}'"
                                end

                                # Okay, let's remove the filepath
                                new_filepath[remove_path.length()..-1]
                              end

        # Nothing more to do
        return shortened_filepath if add_path.nil?

        # Add the new filepath on to the start and return
        return add_path + shortened_filepath
      end

    end
  end
end