# GitHub Version RSS Feed

Watch starred GitHub repositories for new releases via RSS.  Packages as a
Flask web application which can be deployed in AWS via Zappa.

## Usage

```
mkvirtualenv --python=`which python3.6` github_version_rss
workon github_version_rss
pip install --requirement requirements-dev.txt
 export GITHUB_ACCESS_TOKEN=...
./github_version_rss.py
```

## Requirements

* Tested with Python 3.6 on Ubuntu 14.04

## License

Copyright (C) 2018 Andrew Gaul

Licensed under the Apache License, Version 2.0
