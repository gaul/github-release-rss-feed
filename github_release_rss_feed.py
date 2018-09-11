#!/usr/bin/env python3

import os
import sys
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import PyRSS2Gen

import dateutil.parser

from flask import Flask, Response, abort, request

import requests

import tabulate


app = Flask(__name__)

INDEX_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <title>GitHub Release RSS feed</title>
</head>

<body>

<h1>GitHub Release RSS feed</h1>

<p>Watch starred GitHub repositories for new releases via RSS.</p>

<form action="{}/rss" method="GET">
    <input type="text" name="user" />
    <input type="submit" value="Create feed" />
</form>

<p><a href="https://github.com/gaul/github-release-rss-feed">Source code</a></p>

</body>
</html>
"""

QUERY_STARS = """\
query {{
    rateLimit {{
        limit
        cost
        remaining
        resetAt
    }}
    user(login: {}) {{
        login
        starredRepositories(first: 100, after: {}) {{
            edges {{
                node {{
                    nameWithOwner
                    releases(last: 1) {{
                        nodes {{
                            tag {{
                                name
                            }}
                            publishedAt
                            url
                        }}
                    }}
                    tags: refs(refPrefix: "refs/tags/", last: 1) {{
                        edges {{
                            tag: node {{
                                name
                                target {{
                                    ... on Commit {{
                                        committer {{
                                            date
                                        }}
                                    }}
                                    ... on Tag {{
                                        tagger {{
                                            date
                                        }}
                                    }}
                                }}
                            }}
                        }}
                    }}
                }}
            }}
            pageInfo {{
                endCursor
                hasNextPage
            }}
        }}
    }}
}}
"""


class Release:
    def __init__(self, repo: str, name: str, date: datetime, *, url: Optional[str]=None) -> None:
        self.repo = repo
        self.name = name
        self.date = date
        self.url = url


def run_query(query: str, headers: Dict[str, str]) -> Dict[str, Any]:
    request = requests.post("https://api.github.com/graphql", json={"query": query}, headers=headers)
    if request.status_code == 200:
        return request.json()  # type: ignore
    else:
        raise Exception("Query failed to run by returning code of {}. {}".format(request.status_code, query))


def fetch_releases(user: str) -> List[Release]:
    access_token = os.environ["GITHUB_ACCESS_TOKEN"]
    headers = {"Authorization": "token {}".format(access_token)}

    releases: List[Release] = []
    cursor = "null"
    while True:
        result = run_query(QUERY_STARS.format(user, cursor), headers)
        for edge in result["data"]["user"]["starredRepositories"]["edges"]:
            node = edge["node"]
            repo = node["nameWithOwner"]
            nodes = node["releases"]["nodes"]
            tags = node["tags"]["edges"]

            if len(nodes) > 0 and nodes[0] is not None:
                release = nodes[0]
                date = release["publishedAt"]
                releases += [Release(repo, release["tag"]["name"], dateutil.parser.parse(date), url=release["url"])]
            elif len(tags) > 0:
                tag = tags[0]["tag"]
                target = tag["target"]
                if "tagger" in target:
                    # heavyweight tags have tagger
                    date = target["tagger"]["date"]
                else:
                    # lightweight tags lack tagger
                    date = target["committer"]["date"]
                releases += [Release(repo, tag["name"], dateutil.parser.parse(date))]

        page_info = result["data"]["user"]["starredRepositories"]["pageInfo"]
        if not page_info["hasNextPage"]:
            break
        cursor = "\"{}\"".format(page_info["endCursor"])

    return releases


@app.route("/")  # type: ignore
def root() -> str:
    user = request.args.get("user")
    if user is None:
        return INDEX_HTML.format(request.base_url)

    releases = fetch_releases(user)
    releases.sort(key=lambda x: x.date, reverse=True)

    title = "<title>GitHub Release feed for {}</title>".format(user)
    headers = ["Repository", "Release", "Date"]
    body = tabulate.tabulate([(v.repo, v.name, v.date.strftime("%Y-%m-%d")) for v in releases], headers, tablefmt="html")
    link = "<p><a href=\"{}rss?user={}\">link to RSS</a></p>".format(request.base_url, user)
    return "\n".join([title, body, link])


@app.route("/rss")  # type: ignore
def rss() -> str:
    user = request.args.get("user")
    if user is None:
        abort(Response("Must provide HTTP query parameter \"user\".", 400))

    releases = fetch_releases(user)
    releases.sort(key=lambda x: x.date, reverse=True)

    def create_description(release: Release) -> Optional[str]:
        if release.url is not None:
            return "<a href=\"{}\">Release notes</a>".format(release.url)
        return None

    rss = PyRSS2Gen.RSS2(
        title="GitHub Release RSS feed for {}".format(user),
        description="Newest releases of all starred repositories",
        link=request.base_url,
        lastBuildDate=datetime.now(),
        items=[PyRSS2Gen.RSSItem(
            title="{} {} released".format(release.repo, release.name),
            link=release.url,
            description=create_description(release),
            # TODO: guid
            pubDate=release.date)
            for release in releases]
    )

    return rss.to_xml()  # type: ignore


@app.after_request  # type: ignore
def add_header(response: Response) -> Response:
    response.cache_control.max_age = 60 * 60
    return response


def main() -> None:
    if len(sys.argv) != 2:
        sys.exit("Must provide GitHub user")
    releases = fetch_releases(sys.argv[1])
    print(type(releases[0]))
    print(releases[0])
    releases.sort(key=lambda x: x.date)
    headers = ["Repository", "Release", "Date"]
    print(tabulate.tabulate([(v.repo, v.name, v.date.strftime("%Y-%m-%d")) for v in releases], headers, tablefmt="simple"))


if __name__ == "__main__":
    if len(sys.argv) == 1:
        app.run(debug=True)
    else:
        main()
