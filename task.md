High level task:
    => if a video is reported and is also NSFW as per our definition, we don't wanna show this post
    => don't pick these videos ever
    => if we say that the video is not clean (nsfw + grey area) && the video is reported => ban this video


potential proposition
=> The feed server fetches the last updated timestamp for the set to exclude every 5 minute. Runs the query and also stores this result in kvrocks if required.
=> The feed server while returning, makes sure in the lua script itself that the given video_id is not in the exclude set

=> there is also an airflow job that runs every 5 minutes, takes the last updated timestmap and maintains a set of video ids that are to be excluded (reported + not clean as per data science)




 NSFW Check Benchmark

  KVRocks

  Key: offchain:video_nsfw:{video_id}

  Query:
  from src.kvrocks_connect import KVRocksClient

  kv = KVRocksClient()
  client = kv._client

  video_id = "your_video_id"
  key = f"offchain:video_nsfw:{video_id}"
  data = {k.decode(): v.decode() for k, v in client.hgetall(key).items()}

  # Check NSFW
  is_nsfw = data.get("is_nsfw") == "true"
  probability = float(data.get("probability", 0))
  nsfw_ec = data.get("nsfw_ec")

  ---
  BigQuery

  Table: yral_ds.video_nsfw_agg

  Query:
  SELECT video_id, is_nsfw, probability, nsfw_ec, nsfw_gore
  FROM yral_ds.video_nsfw_agg
  WHERE video_id = 'your_video_id'

  ---
  Thresholds (from yral-ml-feed-server)
  ┌─────────────────────────────────────────────────────────┬───────────────────┐
  │                        Condition                        │  Classification   │
  ├─────────────────────────────────────────────────────────┼───────────────────┤
  │ probability < 0.4 AND nsfw_ec = 'neutral'               │ Clean             │
  ├─────────────────────────────────────────────────────────┼───────────────────┤
  │ probability > 0.4                                       │ is_nsfw = True    │
  ├─────────────────────────────────────────────────────────┼───────────────────┤
  │ probability > 0.7 AND nsfw_ec IN ('nudity', 'explicit') │ NSFW feed content │
  └─────────────────────────────────────────────────────────┴───────────────────┘
  ---
  Quick Decision

  if probability < 0.4 and nsfw_ec == 'neutral':
      # Safe for clean feed
  elif probability > 0.7 and nsfw_ec in ('nudity', 'explicit'):
      # Show in NSFW feed only
  else:
      # Gray zone (0.4-0.7) - excluded from both feeds
