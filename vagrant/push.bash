#!/bin/bash
set -e

# Constants
rubygems_user=snowplow
rubygems_gem=sluice

# Check if our Vagrant box is running. Expects `vagrant status` to look like:
#
# > Current machine states:
# >
# > default                   poweroff (virtualbox)
# >
# > The VM is powered off. To restart the VM, simply run `vagrant up`
function running {
    set +e
    vagrant status | sed -n 3p | grep -q "^default\s*running (virtualbox)$"
    local is_running=$?
    set -e
    echo $is_running
}

# Reads the version out of the version.rb
function parse_version {
    cat "lib/${rubygems_gem}/version.rb" | awk '/  VERSION =/ {v=$3; gsub(/\047/, "", v)} END {print v}'
}

# Installs RubyGems.org credentials in our guest
#
# Parameters:
# 1. rubygems_password
function install_creds {
    vagrant ssh -c "curl -u ${rubygems_user}:${1} https://rubygems.org/api/v1/api_key.yaml > ~/.gem/credentials"
    vagrant ssh -c "chmod 0600 ~/.gem/credentials"
}

# Builds our gem
function build_gem {
    vagrant ssh -c "cd /vagrant && gem build ${rubygems_gem}.gemspec"
}

# Installs our gem
#
# Parameters:
# 1. rubygems_version
function install_gem {
    vagrant ssh -c "cd /vagrant && sudo gem install ./${rubygems_gem}-${1}.gem"
}

# Pushes our gem to RubyGems.org
#
# Parameters:
# 1. rubygems_version
function push_gem {
    vagrant ssh -c "cd /vagrant && gem push ./${rubygems_gem}-${1}.gem"
}


# Move to the parent directory of this script
source="${BASH_SOURCE[0]}"
while [ -h "${source}" ] ; do source="$(readlink "${source}")"; done
dir="$( cd -P "$( dirname "${source}" )/.." && pwd )"
cd ${dir}

# Precondition for running
if [ $(running) != "0" ]; then
    echo "Vagrant guest must be running to push"
    exit 1
fi

# Can't pass args thru vagrant push so have to prompt
read -e -p "Please enter password for RubyGems.org user ${rubygems_user}: " rubygems_password

# Build, install & push
build_gem
rubygems_version=$(parse_version)
install_gem ${rubygems_version}
install_creds ${rubygems_password}
push_gem ${rubygems_version}
