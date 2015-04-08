# Sluice
[![Gem Version](https://badge.fury.io/rb/sluice.svg)](http://badge.fury.io/rb/sluice)
[![Build Status](https://travis-ci.org/snowplow/sluice.png)](https://travis-ci.org/snowplow/sluice)
[![Code Climate](https://codeclimate.com/github/snowplow/sluice.png)](https://codeclimate.com/github/snowplow/sluice)
[![Coverage Status](https://coveralls.io/repos/snowplow/sluice/badge.png?branch=master)](https://coveralls.io/r/snowplow/sluice?branch=master)
[![License][license-image]][license]

Sluice is a Ruby gem (built with [Bundler] [bundler]) to help you build cloud-friendly ETL (extract, transform, load) processes.

Currently Sluice provides the following very robust, very parallel S3 operations:

* File upload to S3
* File download from S3
* File delete from S3
* File move within S3 (from/to the same or different AWS accounts)
* File copy within S3 (from/to the same or different AWS accounts; optionally using a manifest)

Sluice has been extracted from a pair of Ruby ETL applications built by the [Snowplow Analytics] [snowplow-analytics] team, specifically:

1. [EmrEtlRunner] [emr-etl-runner], a Ruby application to run the Snowplow ETL process on Elastic MapReduce
2. [StorageLoader] [storage-loader], a Ruby application to load Snowplow event files from Amazon S3 into databases such as Redshift and Postgres

## Installation 

    $ gem install sluice

Or in your Gemfile:

    gem 'sluice', '~> 0.2.2'

## Usage

Rubydoc and usage examples to come.

## Developer quickstart

Assuming git, **[Vagrant] [vagrant-install]** and **[VirtualBox] [virtualbox-install]** installed:

```bash
 host$ git clone https://github.com/snowplow/sluice.git
 host$ cd sluice
 host$ vagrant up && vagrant ssh
guest$ cd /vagrant
guest$ gem install bundler
guest$ rspec
```

## Publishing

```bash
 host$ vagrant push
```

## Copyright and license

Sluice is copyright 2012-2015 Snowplow Analytics Ltd.

Licensed under the [Apache License, Version 2.0] [license] (the "License");
you may not use this software except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

[bundler]: http://gembundler.com/

[license-image]: http://img.shields.io/badge/license-Apache--2-blue.svg?style=flat
[license]: http://www.apache.org/licenses/LICENSE-2.0

[snowplow-analytics]: http://snowplowanalytics.com
[alexanderdean]: https://github.com/alexanderdean
[mtibben]: https://github.com/mtibben
[99designs]: http://99designs.com

[emr-etl-runner]: https://github.com/snowplow/snowplow/tree/master/3-enrich/emr-etl-runner
[storage-loader]: https://github.com/snowplow/snowplow/tree/master/4-storage/storage-loader

[vagrant-install]: http://docs.vagrantup.com/v2/installation/index.html
[virtualbox-install]: https://www.virtualbox.org/wiki/Downloads

[license]: http://www.apache.org/licenses/LICENSE-2.0
