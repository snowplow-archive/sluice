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

require 'contracts'
include Contracts

module Sluice
  module Storage
    module S3

      # Class to describe an S3 location
      # TODO: if we are going to require trailing line-breaks on
      # buckets, maybe we should make that clearer?
      class Location
        
        attr_reader :bucket, :dir

        # Location constructor
        #
        # Parameters:
        # +s3location+:: the s3 location config string e.g. "bucket/directory"
        Contract String => Location
        def initialize(s3_location)
          @s3_location = s3_location

          s3_location_match = s3_location.match('^s3n?://([^/]+)/?(.*)/$')
          raise ArgumentError, 'Bad S3 location %s' % s3_location unless s3_location_match

          @bucket = s3_location_match[1]
          @dir = s3_location_match[2]
          self
        end

        Contract nil => String
        def dir_as_path
          if @dir.length > 0
            return @dir+'/'
          else
            return ''
          end
        end

        Contract nil => String
        def to_s
          @s3_location
        end
      end

    end
  end
end
