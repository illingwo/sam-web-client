#! /bin/bash

version=${1:?Need to specify release version}

# check upd is available

type -t upd > /dev/null || { echo "upd is not available"; exit 1; }

git tag -a -m "Release $version" $version || exit 1
make dist || exit 1

upd addproduct sam_web_client -T dist.tar.gz -m ups/sam_web_client.table -0 $version

# Try to push tags to main repository
git push --tags
