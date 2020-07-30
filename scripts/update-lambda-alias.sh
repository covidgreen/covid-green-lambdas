#!/bin/bash
# Assumes AWS creds are configured and jq is available (Is on the default Ubunutu instance for GitHub Actions)
# Asumes the AWS creds have privs to make the update
# Ideally this would be extracted into a GitHub action
# 	Seen a similar request in the action we use at https://github.com/appleboy/lambda-action/issues/6
# Note: Since the AWS CLI is an older version we need to add the --region parameter to all the calls
#
set -eou pipefail
: ${1?'Function name required'}

function_name=${1}
function_alias_suffix=${2:-'-live'}
function_alias_name=${function_name}${function_alias_suffix}

# Check that the function alias exists, if so update the alias
if [[ ! -z $(aws lambda list-aliases --region ${AWS_REGION} --function-name ${function_name} | jq -r '.Aliases[] | select(.Name == "'$function_alias_name'") | .Name') ]]; then
	printf "Updating alias ${function_alias_name} to v"

	# Get latest version - concern is paging output
	# See https://stackoverflow.com/a/61112105  - Had to remove the --no-paginate going against their advice
	# See https://github.com/aws/aws-cli/issues/4051
	version=$(aws lambda list-versions-by-function --region ${AWS_REGION} --function-name ${function_name} --query "max_by(Versions, &to_number(to_number(Version) || '0'))" | jq -r .Version)
	printf "${version}\n"

	# Update alias to use latest version
	aws lambda update-alias --region ${AWS_REGION} --function-name ${function_name} --name ${function_alias_name} --function-version ${version}
else
	echo "No function alias to update for ${function_name}"
fi
