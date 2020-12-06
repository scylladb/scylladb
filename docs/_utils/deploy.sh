#!/usr/bin/env bash

# Copy contents
mkdir gh-pages
cp -r ./docs/_build/dirhtml/* gh-pages
./docs/_utils/redirect.sh > gh-pages/index.html

# Create gh-pages branch
cd gh-pages
touch .nojekyll
git init
git config --local user.email "action@scylladb.com"
git config --local user.name "GitHub Action"
git remote add origin "https://x-access-token:${GITHUB_TOKEN}@github.com/${GITHUB_REPOSITORY}.git"
git checkout -b gh-pages

# Deploy
git add .
git commit -m "Publish docs" || true
git push origin gh-pages --force
