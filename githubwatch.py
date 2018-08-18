#!/usr/bin/env python3

import asyncio
import datetime
import logging
import os
import sqlite3
import sys
import time
from typing import Any, Awaitable, Coroutine, List, Tuple

import aioboto3

import boto3
from boto3.dynamodb.conditions import Key
from boto3.resources.base import ServiceResource

from flask import Flask, request

import github

import tabulate


# TODO: abstract class

class GitHubWatcherSqlite():
    def __init__(self, g: github.MainClass.Github) -> None:
        self.g = g
        self.conn = sqlite3.connect("versions.db")

    def close(self) -> None:
        self.conn.close()

    def createdb(self) -> None:
        c = self.conn.cursor()
        c.execute("CREATE TABLE versions ("
                  " repo TEXT NOT NULL,"
                  " version TEXT NOT NULL,"
                  " created_at TIMESTAMP NOT NULL,"
                  " PRIMARY KEY (repo, version))")

        c.execute("CREATE TABLE user_stars ("
                  " user TEXT NOT NULL,"
                  " repo TEXT NOT NULL,"
                  " PRIMARY KEY (user, repo))")

    def deletedb(self) -> None:
        # TODO:
        pass

    def fetch_user(self, user: str) -> None:
        u = self.g.get_user(user)
        c = self.conn.cursor()
        c.execute("SELECT repo FROM user_stars WHERE user = ?",
                  (sys.argv[2],))
        repos_db = frozenset(t[0] for t in c.fetchall())
        repos_gh = frozenset(repo.full_name for repo in u.get_starred())
        c.executemany("DELETE FROM user_stars WHERE user = ? AND repo = ?",
                      [(sys.argv[2], repo) for repo in repos_db - repos_gh])
        c.executemany("INSERT INTO user_stars VALUES (?, ?)",
                      [(sys.argv[2], repo) for repo in repos_gh - repos_db])
        self.conn.commit()

    def fetch_releases(self, repo: github.Repository) -> None:
        c = self.conn.cursor()
        c.execute("SELECT version FROM versions WHERE repo = ?", (repo.full_name,))
        versions = frozenset(t[0] for t in c.fetchall())
        try:
            # TODO: store all releases for symmetry with tags?
            release = repo.get_latest_release()
            if release.tag_name not in versions:
                c.execute("INSERT INTO versions VALUES (?, ?, ?)",
                          (repo.full_name, release.tag_name, release.created_at))
        except github.UnknownObjectException as e:
            # Fall back to tags when a repo does not use releases.
            # GitHub returns some useful sort order, but also demonstrates
            # weirdness like flake8_tuple tag "add" before 0.2.13.
            params: List[Tuple[str, str, datetime.datetime]] = []
            for tag in repo.get_tags():
                # TODO limit?
                if tag.name in versions:
                    continue
                date = tag.commit.commit.committer.date
                params += [(repo.full_name, tag.name, date)]
            c.executemany("INSERT INTO versions VALUES (?, ?, ?)", params)

        self.conn.commit()

    def query_all_repos(self) -> List[str]:
        c = self.conn.cursor()
        c.execute("SELECT repo FROM user_stars")
        return [repo for repo, in c.fetchall()]

    def query_stars(self, user: str) -> List[Tuple[str, str, str]]:
        c = self.conn.cursor()
        versions : List[Tuple[str, str, str]] = []
        c.execute("SELECT repo FROM user_stars WHERE user = ?", (user,))
        for repo, in c.fetchall():
            c.execute("SELECT repo, version, created_at"
                      " FROM versions WHERE repo = ?"
                      " ORDER BY created_at DESC LIMIT 1", (repo,))
            result = c.fetchone()
            if result is not None:
                versions += [result]
        return versions


class GitHubWatcherDynamodb:
    def __init__(self, g: github.MainClass.Github, loop: asyncio.AbstractEventLoop) -> None:
        self.g = g
        self.loop = loop
        self.dynamodb = boto3.resource("dynamodb")
        self.aiodynamodb = aioboto3.resource("dynamodb", loop=loop)

    def close(self) -> None:
        self.loop.run_until_complete(self.aiodynamodb.close())

    def createdb(self) -> None:
        self.dynamodb.create_table(
            TableName="versions",
            KeySchema=[
                {
                    "AttributeName": "repo",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "version",
                    "KeyType": "RANGE"
                }
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "repo",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "version",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "created_at",
                    "AttributeType": "S"
                }
            ],
            LocalSecondaryIndexes=[
                {
                    "IndexName": "created_at_index",
                    "KeySchema": [
                        {
                            "AttributeName": "repo",
                            "KeyType": "HASH"
                        },
                        {
                            "AttributeName": "created_at",
                            "KeyType": "RANGE"
                        }
                    ],
                    "Projection": {
                        "ProjectionType": "INCLUDE",
                        "NonKeyAttributes": [
                            "created_at"
                        ]
                    }
                },
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 1
            }
        )

        self.dynamodb.create_table(
            TableName="user_stars",
            KeySchema=[
                {
                    "AttributeName": "user",
                    "KeyType": "HASH"
                },
                {
                    "AttributeName": "repo",
                    "KeyType": "RANGE"
                }
            ],
            AttributeDefinitions=[
                {
                    "AttributeName": "user",
                    "AttributeType": "S"
                },
                {
                    "AttributeName": "repo",
                    "AttributeType": "S"
                },
            ],
            ProvisionedThroughput={
                "ReadCapacityUnits": 5,
                "WriteCapacityUnits": 1
            }
        )

        client = boto3.client("dynamodb")
        client.get_waiter("table_exists").wait(TableName="versions")
        client.get_waiter("table_exists").wait(TableName="user_stars")

    def deletedb(self) -> None:
        client = boto3.client("dynamodb")
        client.delete_table(TableName="versions")
        client.delete_table(TableName="user_stars")

        client.get_waiter("table_not_exists").wait(TableName="versions")
        client.get_waiter("table_not_exists").wait(TableName="user_stars")

    def fetch_user(self, user: str) -> None:
        u = self.g.get_user(user)
        table = self.dynamodb.Table("user_stars")
        response = table.query(
            KeyConditionExpression=Key("user").eq(sys.argv[2])
        )
        repos_db = frozenset(item["repo"] for item in response["Items"])
        repos_gh = frozenset(repo.full_name for repo in u.get_starred())
        with table.batch_writer() as batch:
            for repo in repos_db - repos_gh:
                batch.delete_item(
                    Item={
                        "user": sys.argv[2],
                        "repo": repo
                    }
                )
            for repo in repos_gh - repos_db:
                batch.put_item(
                    Item={
                        "user": sys.argv[2],
                        "repo": repo
                    }
                )

    def fetch_releases(self, repo: github.Repository) -> None:
        logging.debug("Fetching: %s", repo.full_name)

        table = self.dynamodb.Table("versions")
        response = table.query(
            KeyConditionExpression=Key("repo").eq(repo.full_name),
        )
        versions = frozenset(item["version"] for item in response["Items"])
        try:
            # TODO: store all releases for symmetry with tags?
            release = repo.get_latest_release()
            if release.tag_name not in versions:
                table.put_item(
                    Item={
                        "repo": repo.full_name,
                        "version": release.tag_name,
                        "created_at": str(release.created_at)
                    }
                )
        except github.UnknownObjectException as e:
            # Fall back to tags when a repo does not use releases.
            # GitHub returns some useful sort order, but also demonstrates
            # weirdness like flake8_tuple tag "add" before 0.2.13.
            with table.batch_writer() as batch:
                count = 0
                for tag in repo.get_tags():
                    if tag.name in versions:
                        continue
                    if count == 3:
                        # Limit repo tags to avoid downloading the entire
                        # history synchronously.  Subsequent runs can fetch
                        # this information.
                        break
                    count += 1
                    logging.debug("Tagging: %s", tag.name)
                    date = tag.commit.commit.committer.date
                    logging.debug("Tag complete: %s", tag.name)
                    batch.put_item(
                        Item={
                            "repo": repo.full_name,
                            "version": tag.name,
                            "created_at": str(date)
                        }
                    )

        logging.debug("Fetch complete: %s", repo.full_name)

    def query_all_repos(self) -> List[str]:
        table = self.dynamodb.Table("user_stars")
        response = table.scan()
        return [item["repo"] for item in response["Items"]]

    def query_stars(self, user: str) -> List[Tuple[str, str, str]]:
        versions: List[Tuple[str, str, str]] = []
        table = self.dynamodb.Table("user_stars")
        response = table.query(
            KeyConditionExpression=Key("user").eq(user)
        )

        # DynamoDB batch query API only allows exact hash lookups, not nearest
        # neighbor range queries.  Instead issue multiple asynchronous queries.
        table = self.aiodynamodb.Table("versions")
        futures: List[Awaitable[Any]] = []
        for item in response["Items"]:
            futures += [table.query(
                IndexName="created_at_index",
                KeyConditionExpression=Key("repo").eq(item["repo"]),
                Limit=1,
                ScanIndexForward=False
            )]
        responses = self.loop.run_until_complete(asyncio.gather(*futures, loop=loop))
        for response in responses:
            for item in response["Items"]:
                versions += [(item["repo"], item["version"], item["created_at"])]

        return versions


loop = asyncio.new_event_loop()
asyncio.set_event_loop(loop)
app = Flask(__name__)

@app.route("/")  # type: ignore
def root() -> str:
    user = request.args.get("user")
    if user is None:
        # TODO: serve help page
        return "Must provide HTTP query parameter \"user\"."

    access_token = os.environ["GITHUB_ACCESS_TOKEN"]
    g = github.Github(access_token, per_page=100)
    # TODO: backend flag
    #ghw = GitHubWatcherSqlite(g)
    ghw = GitHubWatcherDynamodb(g, loop)
    try:
        versions = ghw.query_stars(user)
    finally:
        ghw.close()

    versions.sort(key=lambda x: x[2])
    headers = ["Repository", "Version", "Date"]
    return tabulate.tabulate(versions, headers, tablefmt="html")


def main() -> None:
    access_token = os.environ["GITHUB_ACCESS_TOKEN"]
    g = github.Github(access_token, per_page=100)
    # TODO: backend flag
    #ghw = GitHubWatcherSqlite(g)
    ghw = GitHubWatcherDynamodb(g, loop)

    try:
        # TODO: more robust parsing
        if sys.argv[1] == "createdb":
            ghw.createdb()

        elif sys.argv[1] == "deletedb":
            ghw.deletedb()

        elif sys.argv[1] == "ratelimit":
            print("Limit: {}".format(g.get_rate_limit().rate.limit))
            print("Remaining: {}".format(g.get_rate_limit().rate.remaining))
            print("Reset time: {}".format(g.rate_limiting_resettime))

        elif sys.argv[1] == "fetch":
            for repo in ghw.query_all_repos():
                while g.get_rate_limit().rate.remaining < 1000:
                    print("Sleeping for {}".format(60))
                    time.sleep(60)
                # TODO: handle github.GithubException.RateLimitExceededException
                ghw.fetch_releases(g.get_repo(repo))

        elif sys.argv[1] == "fetchuser":
            ghw.fetch_user(sys.argv[2])

        elif sys.argv[1] == "querystars":
            versions = loop.run_until_complete(ghw.query_stars(sys.argv[2]))

            versions.sort(key=lambda x: x[2])
            headers = ["Repository", "Version", "Date"]
            print(tabulate.tabulate(versions, headers, tablefmt="simple"))

        else:
            raise NotImplementedError()
    finally:
        loop.run_until_complete(ghw.close())


if __name__ == "__main__":
    # TODO: sink app initialization into main command
    #main()
    app.run(debug=True)
