# mergestat-sync

The purpose of this is to recreate, in a more simplistic form, the ability to sync git data (starting with gitlab) and mimic mergestat's functionality.

## Gitlab API

We need to pull the following data that mirror these commands from glab-cli:

```bash
# using a group access token
export GITLAB_TOKEN=<token>

# retrieve all groups
glab api groups --paginate
# retrieve all repos
glab repo ls -g minted --include-subgroups --output json
# use groups to get mr lists
glab mr list --output json

```
