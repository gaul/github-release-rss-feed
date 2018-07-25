# GitHubWatch

Watch GitHub repositories for new releases.

## Usage

```
mkvirtualenv --python=`which python3.6` githubwatch
workon githubwatch
pip install --requirement requirements.txt
 export GITHUB_ACCESS_TOKEN=...
./githubwatch.py COMMAND [USER]
```

## TODO

* automatically fetch user stars
* run as web service
  - landing page shows latest releases for a given user in table
  - some way to detect GitHub user name?
* provide RSS feed
* email user updates
  - use github email
* periodically refresh `user_stars` and `versions` tables
  - initial experience poor with nothing refreshed until interval
* rate limiting
  - limited to 5,000 requests per hour
    + how many requests per repo?
  - conditional GET?
  - https://developer.github.com/v3/#rate-limiting
* tests

## Requirements

TODO:

* Tested with Python 3.6 on Ubuntu 14.04

## License

Copyright (C) 2018 Andrew Gaul

Licensed under the Apache License, Version 2.0

TODO: Apache or MIT?  AGPL?
