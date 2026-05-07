#!/usr/bin/env bash
#
# Generates statistics for release notes
#

# get the source location for this script; handles symlinks
function get_script_path {
  local source="${BASH_SOURCE[0]}"
  while [ -h "${source}" ] ; do
    source="$(readlink "${source}")";
  done
  echo ${source}
}

# path, name, and dir for this script
declare -r script_path=$(get_script_path)
declare -r script_name=$(basename "${script_path}")
declare -r script_dir="$(cd -P "$(dirname "${script_path}")" && pwd)"

# print usage info
function usage {
  echo "Usage: ${script_name} <tag-from> <tag-to> <milestone-name>"
  echo "Example: ${script_name} v2.3.2 v2.3.3 2.3.3"
}

declare -r tag1=$1
declare -r tag2=$2
declare -r milestone_name=$3

declare -r tag_range="$tag1..$tag2"
declare authors=$($script_dir/authors.pl $tag_range)
declare author_count=$(echo "$authors" | wc -l | grep -o '[1-9].*')
declare diff_short=$(git diff --shortstat $tag_range | grep -o '[1-9].*')

declare script_user_agent="User-Agent: Akka-Stats-Script"
declare open_milestones=$(curl -s -H "$script_user_agent" "https://api.github.com/repos/akka/akka-core/milestones?state=open")
declare closed_milestones=$(curl -s -H "$script_user_agent" "https://api.github.com/repos/akka/akka-core/milestones?state=closed")
declare milestone_id=$(echo "$open_milestones$closed_milestones" | sed 's/"description"/\n/g' | perl -ne 'm/number":([0-9]+),"title":"(.+?)",/ && print "$1,$2\n"' | grep "$milestone_name" | cut -d"," -f 1)
declare tickets=$(curl -s -H "$script_user_agent" "https://api.github.com/repos/akka/akka-core/issues?milestone=$milestone_id&state=all&per_page=100" | sed 's/"comments"/\n/g' | perl -ne 'm/number":([0-9]+),"title":"(.+?)",/ && print " - *$1* $2\n"' | sort -n)
declare ticket_count=$(echo "$tickets" | wc -l | grep -o '[1-9].*')

echo "$tag1 compared to Akka $tag2":

echo "* $ticket_count tickets closed"

echo "* $diff_short"

echo "* X pages of docs vs Y in $tag1"

echo "* … and a total of $author_count committers!"

echo ""
echo "Fixed Tickets:"
echo "$tickets"

echo ""
echo "Credits:"
echo "commits added removed"
echo "$authors"


