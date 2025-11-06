#!/usr/bin/env bash

set -e

declare -r samples_sources="${PWD}/samples"
declare -r akka_docs_attachments="${PWD}/akka-docs/src/main/paradox/attachments"
declare -r target_temporal_attachments="${PWD}/target/akka-docs/_attachments"

declare -r temporal_folder="${PWD}/target/zips"

function sed_command() {
   local platform="$(uname -s | tr '[:upper:]' '[:lower:]')"

   if [ "${platform}" != "darwin" ]; then
      echo "sed"
   else
      # using gnu-sed on Mac
      echo "gsed"
   fi
}

## Remove the tags used by Paradox snippets from the codebase in the current folder
function removeTags() {
   ## remove tags from code
   find . -type f -print0 | xargs -0 $(sed_command) -i "s/\/\/ #.*//g"
}


## Cleanup the temporal folder from previous executions
function prepareTemporalFolder() {
   rm -rf ${temporal_folder}
   mkdir -p ${temporal_folder}
}

## Copy a folder with some code into the temporal folder. The 
## copied folder will be renamed to the folder name we want the 
## user to see when unzipping the file.
##   source_name -> folder in `examples`
##   target_name ->  folder name the user should see (must not use a numeric prefix of a laguage suffix)
function fetchProject() {
   source_name=$1
   target_name=$2
   echo "Fetching content from [$1] to [$2]"
   cp -a ${source_name} ${temporal_folder}/${target_name}
   rm -rf ${temporal_folder}/${target_name}/target
}

## Zip the contents in $temporal_folder and create the 
## attachment file (aka, the zip file on the appropriate location)
function zipAndAttach() {
   zip_name=$1
   temporal_attachments=$2
   echo "Preparing zip $1"
   pushd ${temporal_folder}
   removeTags
   zip --quiet -r ${zip_name} *
   cp ${zip_name} ${temporal_attachments}
   echo "Prepared attachment at ${zip_name}"
   popd
}

mkdir -p ${akka_docs_attachments}
mkdir -p ${target_temporal_attachments}

## akka-quickstart-java zip file
prepareTemporalFolder
fetchProject ${samples_sources}/akka-quickstart-java akka-quickstart
zipAndAttach ${akka_docs_attachments}/akka-quickstart-java.zip ${target_temporal_attachments}

## akka-quickstart-scala zip file
prepareTemporalFolder
fetchProject ${samples_sources}/akka-quickstart-scala akka-quickstart
zipAndAttach ${akka_docs_attachments}/akka-quickstart-scala.zip ${target_temporal_attachments}

# akka-sample-cluster-java
prepareTemporalFolder
fetchProject ${samples_sources}/akka-sample-cluster-java akka-sample-cluster
zipAndAttach ${akka_docs_attachments}/akka-sample-cluster-java.zip ${target_temporal_attachments}

# akka-sample-cluster-scala
prepareTemporalFolder
fetchProject ${samples_sources}/akka-sample-cluster-scala akka-sample-cluster
zipAndAttach ${akka_docs_attachments}/akka-sample-cluster-scala.zip ${target_temporal_attachments}

# akka-sample-distributed-data-java
prepareTemporalFolder
fetchProject ${samples_sources}/akka-sample-distributed-data-java akka-sample-distributed-data
zipAndAttach ${akka_docs_attachments}/akka-sample-distributed-data-java.zip ${target_temporal_attachments}

# akka-sample-distributed-data-scala
prepareTemporalFolder
fetchProject ${samples_sources}/akka-sample-distributed-data-scala akka-sample-distributed-data
zipAndAttach ${akka_docs_attachments}/akka-sample-distributed-data-scala.zip ${target_temporal_attachments}

# akka-sample-fsm-java
prepareTemporalFolder
fetchProject ${samples_sources}/akka-sample-fsm-java akka-sample-fsm
zipAndAttach ${akka_docs_attachments}/akka-sample-fsm-java.zip ${target_temporal_attachments}

# akka-sample-fsm-scala
prepareTemporalFolder
fetchProject ${samples_sources}/akka-sample-fsm-scala akka-sample-fsm
zipAndAttach ${akka_docs_attachments}/akka-sample-fsm-scala.zip ${target_temporal_attachments}

# akka-sample-sharding-java
prepareTemporalFolder
fetchProject ${samples_sources}/akka-sample-sharding-java akka-sample-sharding
zipAndAttach ${akka_docs_attachments}/akka-sample-sharding-java.zip ${target_temporal_attachments}

# akka-sample-sharding-scala
prepareTemporalFolder
fetchProject ${samples_sources}/akka-sample-sharding-scala akka-sample-sharding
zipAndAttach ${akka_docs_attachments}/akka-sample-sharding-scala.zip ${target_temporal_attachments}

# akka-sample-kafka-to-sharding-scala
prepareTemporalFolder
fetchProject ${samples_sources}/akka-sample-kafka-to-sharding-scala akka-sample-kafka-to-sharding
zipAndAttach ${akka_docs_attachments}/akka-sample-kafka-to-sharding-scala.zip ${target_temporal_attachments}
