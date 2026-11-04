"""Tests of the topic definitions."""

from pathlib import Path

import yaml

from producers.create_topics import TOPICS_PATH, build_topics
from rtap.config import load_config


def definition() -> dict:
    return yaml.safe_load(Path(TOPICS_PATH).read_text(encoding="utf-8"))


def test_every_configured_topic_is_defined() -> None:
    topics = load_config(env={}).kafka.topics
    names = {topic["name"] for topic in definition()["topics"]}

    assert names == {topics.raw, topics.valid, topics.invalid, topics.transactions}


def test_defaults_and_overrides_are_applied() -> None:
    topics = {topic.topic: topic for topic in build_topics(definition())}

    assert topics["ecommerce-events"].num_partitions == 3
    assert topics["ecommerce-events"].replication_factor == 1
    assert topics["ecommerce-events"].config["retention.ms"] == "86400000"
    assert topics["invalid-events"].config["retention.ms"] == "604800000"
