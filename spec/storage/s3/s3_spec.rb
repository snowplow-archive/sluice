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

require 'spec_helper'

S3 = Sluice::Storage::S3

describe S3 do

  it 'should allow filenames to be renamed' do

    concat_subdir = lambda { |basename, filepath|
      if m = filepath.match('([^/]+)/[^/]+$')
        return m[1] + '-' + basename
      else
        return basename
      end
    }

    foobar = lambda {
      'foobar'
    }

    S3.rename_file('/dir/subdir/file', 'file', lambda=nil).should eql 'file'
    S3.rename_file('/dir/subdir/file', nil, foobar).should eql 'foobar'
    S3.rename_file('resources/environments/logs/publish/e-bgp9nsynv7/i-f2b831bd/_var_log_tomcat7_localhost_access_log.txt-1391958061.gz', '_var_log_tomcat7_localhost_access_log.txt-1391958061.gz', concat_subdir).should eql 'i-f2b831bd-_var_log_tomcat7_localhost_access_log.txt-1391958061.gz'

  end

end