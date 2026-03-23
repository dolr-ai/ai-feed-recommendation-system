import pytest


@pytest.mark.asyncio
async def test_repository_roundtrip(repo, sample_feed_rows):
    influencers, scores = sample_feed_rows

    await repo.write_ranked_feed(scores, influencers)
    await repo.write_stats_snapshot(influencers)

    blobs, total_count, feed_generated_at = await repo.get_feed(0, 10)
    assert total_count == 2
    assert feed_generated_at is not None
    assert [blob["id"] for blob in blobs] == ["i1", "i2"]

    snapshot = await repo.load_stats_snapshot()
    assert set(snapshot.keys()) == {"i1", "i2"}

    await repo.increment_serve_counts(["i1", "i2"], offset=0)
    counts = await repo.get_all_serve_counts(["i1", "i2"])
    assert counts["i1"] == 1.0
    assert counts["i2"] == 1.0
