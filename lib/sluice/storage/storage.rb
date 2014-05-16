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

# Author::    Alex Dean (mailto:support@snowplowanalytics.com), Michael Tibben
# Copyright:: Copyright (c) 2012-2014 Snowplow Analytics Ltd
# License::   Apache License Version 2.0

module Sluice
  module Storage

    # To handle negative file matching
    NegativeRegex = Struct.new(:regex)  

    # Find files within the given date range
    # (inclusive).
    #
    # Parameters:
    # +start_date+:: start date
    # +end_date+:: end date
    # +date_format:: format of date in filenames
    # +file_ext:: extension on files (if any)   
    def files_between(start_date, end_date, date_format, file_ext=nil)

      dates = []
      Date.parse(start_date).upto(Date.parse(end_date)) do |day|
        dates << day.strftime(date_format)
      end

      '(' + dates.join('|') + ')[^/]+%s$' % regexify(file_ext)
    end
    module_function :files_between

    # Add a trailing slash to a path if missing
    #
    # Parameters:
    # +path+:: path to add a trailing slash to
    def trail_slash(path)

      path[-1].chr != '/' ? path << '/' : path
    end
    module_function :trail_slash

    # Find files up to (and including) the given date.
    #
    # Returns a regex in a NegativeRegex so that the
    # matcher can negate the match.
    #
    # Parameters:
    # +end_date+:: end date
    # +date_format:: format of date in filenames
    # +file_ext:: extension on files (if any)  
    def files_up_to(end_date, date_format, file_ext=nil)

      # Let's create a black list from the day
      # after the end_date up to today
      day_after = Date.parse(end_date) + 1
      today = Date.today

      dates = []
      day_after.upto(today) do |day|
        dates << day.strftime(date_format) # Black list
      end

      NegativeRegex.new('(' + dates.join('|') + ')[^/]+%s$' % regexify(file_ext))
    end
    module_function :files_up_to

    # Find files starting from the given date.
    #
    # Parameters:
    # +start_date+:: start date
    # +date_format:: format of date in filenames
    # +file_ext:: extension on files (if any); include period    
    def files_from(start_date, date_format, file_ext=nil)

      # Let's create a white list from the start_date to today
      today = Date.today

      dates = []
      Date.parse(start_date).upto(today) do |day|
        dates << day.strftime(date_format)
      end

      '(' + dates.join('|') + ')[^/]+%s$' % regexify(file_ext)
    end
    module_function :files_from

    private

    # Make a file extension regular expression friendly,
    # adding a starting period (.) if missing 
    #
    # Parameters:
    # +file_ext:: the file extension to make regexp friendly
    def regexify(file_ext)
      file_ext.nil? ? nil : file_ext[0].chr != '.' ? '\\.' << file_ext : '\\' << file_ext
    end
    module_function :regexify

  end
end