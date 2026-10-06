"""Self-tests for the compacted-layer expectations of the fixture (no S3 needed)."""
import fixture


class FakeFS:
    def __init__(self, keys):
        self.keys = keys

    def find(self, bucket):
        return [k for k in self.keys if k.startswith(bucket + "/")]


def test_lone_partitions_are_not_mergeable_and_pairs_are():
    fs = FakeFS([
        "obs-raw/1001/0/logs/dt=2026-10-05/hour=01/a.parquet",
        "obs-raw/1001/0/logs/dt=2026-10-05/hour=02/b.parquet",
        "obs-raw/4401/1/logs/dt=2026-10-05/hour=01/c.parquet",
        "obs-raw/4401/1/logs/dt=2026-10-05/hour=01/d.parquet",
        "obs-raw/4401/1/logs/dt=2026-10-05/hour=02/e.parquet",
        "obs-raw/4401/1/logs/_meta/x.json",
    ])
    parts = fixture.raw_partitions(fs)
    assert parts[("1001/0", "logs")] == {
        "dt=2026-10-05/hour=01": ["1001/0/logs/dt=2026-10-05/hour=01/a.parquet"],
        "dt=2026-10-05/hour=02": ["1001/0/logs/dt=2026-10-05/hour=02/b.parquet"],
    }
    assert len(parts[("4401/1", "logs")]["dt=2026-10-05/hour=01"]) == 2
    assert fixture.mergeable_groups(fs) == {("4401/1", "logs")}


def test_no_raw_objects_means_nothing_mergeable():
    assert fixture.mergeable_groups(FakeFS([])) == set()
