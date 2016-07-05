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

require 'set'

require 'contracts'

module Sluice
  module Storage
    module S3

      # Legitimate manifest scopes:
      # 1. :filename - store only the filename
      #                in the manifest
      # 2. :relpath  - store the relative path
      #                to the file in the manifest
      # 3. :abspath  - store the absolute path
      #                to the file in the manifest
      # 4. :bucket   - store bucket PLUS absolute
      #                path to the file in the manifest
      #
      # TODO: add support for 2-4. Currently only 1 supported 
      class ManifestScope

        @@scopes = Set::[](:filename) # TODO add :relpath, :abspath, :bucket

        def self.valid?(val)
          val.is_a?(Symbol) &&
            @@scopes.include?(val)
        end
      end

      # Class to read and maintain a manifest.
      class Manifest

        include Contracts

        attr_reader :s3_location, :scope, :manifest_file

        # Manifest constructor
        #
        # Parameters:
        # +path+:: full path to the manifest file
        # +scope+:: whether file entries in the
        #           manifest should be scoped to
        #           filename, relative path, absolute
        #           path, or absolute path and bucket
        Contract Location, ManifestScope => nil
        def initialize(s3_location, scope)
          @s3_location = s3_location
          @scope = scope
          @manifest_file = "%ssluice-%s-manifest" % [s3_location.dir_as_path, scope.to_s]
          nil
        end

        # Get the current file entries in the manifest
        #
        # Parameters:
        # +s3+:: A Fog::Storage s3 connection
        #
        # Returns an Array of filenames as Strings
        Contract FogStorage => ArrayOf[String]
        def get_entries(s3)

          manifest = self.class.get_manifest(s3, @s3_location, @manifest_file)
          if manifest.nil?
            return []
          end

          manifest.body.split("\n").reject(&:empty?)
        end

        # Add (i.e. append) the following file entries
        # to the manifest
        # Files listed previously in the manifest will
        # be kept in the new manifest file.
        #
        # Parameters:
        # +s3+:: A Fog::Storage s3 connection
        # +entries+:: an Array of filenames as Strings
        #
        # Returns all entries now in the manifest
        Contract FogStorage, ArrayOf[String] => ArrayOf[String]
        def add_entries(s3, entries)

          existing = get_entries(s3)
          filenames = entries.map { |filepath|
            File.basename(filepath)
          } # TODO: update when non-filename-based manifests supported
          all = (existing + filenames)

          manifest = self.class.get_manifest(s3, @s3_location, @manifest_file)
          body = all.join("\n")
          if manifest.nil?
            bucket = s3.directories.get(s3_location.bucket).files.create(
              :key  => @manifest_file,
              :body => body
            )
          else
            manifest.body = body
            manifest.save
          end

          all
        end

        private

        # Helper to get the manifest file
        Contract FogStorage, Location, String => Maybe[FogFile]
        def self.get_manifest(s3, s3_location, filename)
          s3.directories.get(s3_location.bucket, prefix: s3_location.dir).files.get(filename) # TODO: break out into new generic get_file() procedure
        end

      end

    end
  end
end
