#!/usr/bin/env bash

set -e

trap 'echo "error $? in $0 line $LINENO"' ERR

# Copy contents
mkdir gh-pages
cp -r ./docs/_build/dirhtml/. gh-pages

# Create gh-pages branch
cd gh-pages
git init
git config --local user.email "action@scylladb.com"
git config --local user.name "GitHub Action"
git remote add origin "https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPOSITORY}.git"
git checkout -b gh-pages

# Deploy
git add .
git commit -m "Publish docs" || true
git push origin gh-pages --force
