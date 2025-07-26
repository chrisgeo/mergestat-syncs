# mergestat-sync

The purpose of this is to recreate, in a more simplistic form, the ability to sync git data (starting with gitlab) and mimic mergestat's functionality.

# Git Metrics

Using Mergestat's database format, this set of syncs will use any git repository locally, or from gitlab (github ones already exist) too allow you to more quickly add data without the complicated setup that mergestate and entail.

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
glab repo ls -g minted --include-subgroups --output json
# use groups to get mr lists
glab mr list --output json

```
