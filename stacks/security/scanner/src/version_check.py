from __future__ import annotations

import logging
import os
import re

import requests

logger = logging.getLogger(__name__)

_GITHUB_TOKEN = os.environ.get("GITHUB_TOKEN", "")
_SESSION = requests.Session()
_SESSION.headers["User-Agent"] = "ppd-security-scanner/1.0"

# Maps image name → Docker Hub repository path (library/ prefix = official images).
# Extend this dict as new services are added to the stack.
_DOCKERHUB_REPOS: dict[str, str] = {
    "postgres":              "library/postgres",
    "pgvector/pgvector":     "pgvector/pgvector",
    "redis":                 "library/redis",
    "minio/minio":           "minio/minio",
    "metabase/metabase":     "metabase/metabase",
    "qdrant/qdrant":         "qdrant/qdrant",
    "provectuslabs/kafka-ui":"provectuslabs/kafka-ui",
    "bitnami/kafka":         "bitnami/kafka",
    "nginx":                 "library/nginx",
}

# Maps image name → GitHub org/repo for services not on Docker Hub version tags.
_GITHUB_REPOS: dict[str, str] = {
    "apache/airflow": "apache/airflow",
    "dagster":        "dagster-io/dagster",
    "apache/superset":"apache/superset",
}


def check_image_version(image_name: str, running_tag: str) -> dict | None:
    """
    Return drift info if a newer version exists, or None if up-to-date / unknown.
    Tries Docker Hub first, then GitHub releases.
    """
    try:
        if image_name in _DOCKERHUB_REPOS:
            return _check_dockerhub(image_name, running_tag)
        if image_name in _GITHUB_REPOS:
            return _check_github(image_name, running_tag)
    except Exception as exc:
        logger.debug("Version check failed for %s: %s", image_name, exc)
    return None


def _check_dockerhub(image_name: str, running_tag: str) -> dict | None:
    repo = _DOCKERHUB_REPOS[image_name]
    url = f"https://hub.docker.com/v2/repositories/{repo}/tags?page_size=25&ordering=last_updated"
    resp = _SESSION.get(url, timeout=10)
    if resp.status_code != 200:
        return None

    tags = [t["name"] for t in resp.json().get("results", [])]
    # Only consider semver-ish tags (e.g. "16", "16.3", "3.7.2") — skip "latest", "alpine", etc.
    version_tags = [t for t in tags if re.match(r"^\d+(\.\d+)*$", t)]
    if not version_tags:
        return None

    latest = version_tags[0]
    if latest == running_tag:
        return None

    return {
        "latest_tag": latest,
        "versions_behind": _semver_distance(running_tag, latest),
        "release_notes_url": f"https://hub.docker.com/r/{repo}/tags",
    }


def _check_github(image_name: str, running_tag: str) -> dict | None:
    gh_repo = _GITHUB_REPOS[image_name]
    headers = {"Authorization": f"Bearer {_GITHUB_TOKEN}"} if _GITHUB_TOKEN else {}
    resp = _SESSION.get(
        f"https://api.github.com/repos/{gh_repo}/releases/latest",
        headers=headers,
        timeout=10,
    )
    if resp.status_code != 200:
        return None

    latest_tag = resp.json().get("tag_name", "").lstrip("v")
    running_clean = running_tag.lstrip("v")
    if latest_tag == running_clean:
        return None

    return {
        "latest_tag": latest_tag,
        "versions_behind": _semver_distance(running_clean, latest_tag),
        "release_notes_url": resp.json().get("html_url"),
    }


def _semver_distance(running: str, latest: str) -> int:
    """Rough version distance — just compares major version numbers."""
    try:
        r_major = int(running.split(".")[0])
        l_major = int(latest.split(".")[0])
        return max(1, l_major - r_major)
    except (ValueError, IndexError):
        return 1
