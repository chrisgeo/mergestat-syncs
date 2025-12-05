# Git Metrics

Using Mergestat's database schema, this set of syncs will use any git repository locally, or from gitlab (github ones already exist) to allow you to more quickly add data without the complicated setup that mergestat and entails.

## Why?
Mostly because using mergestat's syncs are great but take a lot of time to understand. The goal of this was for a personal project to understand how my teams are doing, and with a limited budget.

## Gitlab API

We need to pull the following data that mirror these commands from glab-cli:

```bash
# using a group access token
export GITLAB_TOKEN=<token>

# retrieve all groups
glab api groups --paginate
# retrieve all repos
glab repo ls -g  <group> --include-subgroups --output json
# use groups to get mr lists
glab mr list --output json

```
