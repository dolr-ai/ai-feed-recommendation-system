"""
Test script: fetch influencers + enrich with canister data.

Steps:
  1. GET /api/v1/influencers → fetch first 100 records
  2. For each influencer call two IC canister endpoints:
       a. get_user_profile_details_v7(principal)
          Canister: ivkka-7qaaa-aaaas-qbg3q-cai
          → followers_count, following_count, is_ai_influencer, account_type
       b. get_posts_of_this_user_profile_with_pagination_cursor(principal, 0, 10)
          Canister: gxhc3-pqaaa-aaaas-qbh3q-cai
          → last 10 posts: views, shares, last_post_at (ReadyToView only)
  3. Save enriched records to influencers_enriched.json

Notes:
  - ic-py's query_raw() returns already-decoded Python objects, NOT raw bytes.
    Do NOT call decode() on the result.
  - Fields use Candid hash IDs. Key mappings (candid_hash fn: h=h*223+ord(c)):
      _17724         = Ok
      _3456837       = Err
      _3633470565    = followers_count
      _1284286177    = following_count
      _633437119     = is_ai_influencer
      _4140564172    = account_type
      _1667647878    = BotAccount
      _645524820     = MainAccount
      _947296307     = owner (in BotAccount)
      _100394802     = status (post field)
      _3389994531    = ReadyToView (status value)
      _375185967     = share_count
      _1779848746    = created_at
      _2698548359    = secs_since_epoch
      _2360035717    = view_stats
      _1639276560    = total_view_count
      _3375235756    = average_watch_percentage
  - ic-py has a Python 2-style bug in Principal.from_str — patched below.
"""

import json
import time
import base64
import math
import requests

# ── Monkey-patch ic-py Principal.from_str (Python 2-style raise bug) ──────────
from ic.principal import Principal, CRC_LENGTH_IN_BYTES

def _fixed_from_str(s: str):
    s1 = s.replace('-', '')
    pad_len = math.ceil(len(s1) / 8) * 8 - len(s1)
    b = base64.b32decode(s1.upper().encode() + b'=' * pad_len)
    if len(b) < CRC_LENGTH_IN_BYTES:
        raise ValueError("principal length error")
    return Principal(bytes=b[CRC_LENGTH_IN_BYTES:])

Principal.from_str = staticmethod(_fixed_from_str)
# ─────────────────────────────────────────────────────────────────────────────

from ic.client import Client
from ic.agent import Agent
from ic.identity import Identity
from ic.candid import encode, Types

# ── Config ────────────────────────────────────────────────────────────────────
INFLUENCERS_URL  = "https://chat.yral.com/api/v1/influencers"
IC_URL           = "https://ic0.app"
PROFILE_CANISTER = "ivkka-7qaaa-aaaas-qbg3q-cai"
POSTS_CANISTER   = "gxhc3-pqaaa-aaaas-qbh3q-cai"

FETCH_LIMIT      = 100      # Only first 100 influencers for this test
POSTS_LIMIT      = 10       # Most recent posts per influencer
RATE_LIMIT_DELAY = 0.2      # 200ms between calls ≈ 5 RPS
OUTPUT_FILE      = "influencers_enriched.json"

# Candid field hash constants (computed via h = h*223 + ord(c), mod 2^32)
F_OK               = "_17724"
F_ERR              = "_3456837"
F_FOLLOWERS        = "_3633470565"
F_FOLLOWING        = "_1284286177"
F_IS_AI            = "_633437119"
F_ACCOUNT_TYPE     = "_4140564172"
F_STATUS           = "_100394802"
F_READY_TO_VIEW    = "_3389994531"
F_SHARE_COUNT      = "_375185967"
F_CREATED_AT       = "_1779848746"
F_SECS_EPOCH       = "_2698548359"
F_VIEW_STATS       = "_2360035717"
F_TOTAL_VIEWS      = "_1639276560"
F_AVG_WATCH        = "_3375235756"

# ── IC Agent (anonymous read-only) ────────────────────────────────────────────
agent = Agent(Identity(), Client(url=IC_URL))


# ── Step 1: Fetch first 100 influencers ──────────────────────────────────────
print("=" * 60)
print("STEP 1 — Fetching first 100 influencers from listing API")
print("=" * 60)

resp = requests.get(
    INFLUENCERS_URL,
    headers={"accept": "application/json"},
    params={"limit": FETCH_LIMIT, "offset": 0},
)
resp.raise_for_status()
data        = resp.json()
influencers = data.get("influencers", [])
api_total   = data.get("total", "?")

print(f"  API total influencers : {api_total}")
print(f"  Fetched for this test : {len(influencers)}\n")


# ── Canister helpers ──────────────────────────────────────────────────────────

def call_profile(principal_id: str) -> dict:
    """
    Call get_user_profile_details_v7 on the profile canister.
    Returns extracted fields or error info.
    """
    try:
        raw = agent.query_raw(
            PROFILE_CANISTER,
            "get_user_profile_details_v7",
            encode([{"type": Types.Principal, "value": principal_id}]),
        )
        # raw is already a decoded Python list — do NOT call decode()
        if not raw:
            return {"ok": False, "error": "empty response"}

        variant = raw[0].get("value", {})

        if F_OK in variant:
            ok = variant[F_OK]
            return {
                "ok": True,
                "followers_count":  ok.get(F_FOLLOWERS, 0),
                "following_count":  ok.get(F_FOLLOWING, 0),
                "is_ai_influencer": ok.get(F_IS_AI, False),
                "account_type":     ok.get(F_ACCOUNT_TYPE, {}),
            }
        elif F_ERR in variant:
            return {"ok": False, "error": variant[F_ERR]}
        else:
            return {"ok": False, "error": f"unknown variant keys: {list(variant.keys())}"}

    except Exception as e:
        return {"ok": False, "error": str(e)}


def call_posts(principal_id: str) -> dict:
    """
    Call get_posts_of_this_user_profile_with_pagination_cursor.
    Returns aggregated stats from the last POSTS_LIMIT posts.
    """
    try:
        raw = agent.query_raw(
            POSTS_CANISTER,
            "get_posts_of_this_user_profile_with_pagination_cursor",
            encode([
                {"type": Types.Principal, "value": principal_id},
                {"type": Types.Nat64,     "value": 0},            # cursor
                {"type": Types.Nat64,     "value": POSTS_LIMIT},  # limit
            ]),
        )
        # raw is already decoded — first element's value is the vec of posts
        posts = raw[0].get("value", []) if raw else []

        # Keep only ReadyToView posts
        ready = [
            p for p in posts
            if isinstance(p, dict) and F_READY_TO_VIEW in p.get(F_STATUS, {})
        ]

        total_views  = sum(p.get(F_VIEW_STATS, {}).get(F_TOTAL_VIEWS, 0) for p in ready)
        total_shares = sum(p.get(F_SHARE_COUNT, 0) for p in ready)
        last_post_ts = max(
            (p.get(F_CREATED_AT, {}).get(F_SECS_EPOCH, 0) for p in ready),
            default=None,
        )

        return {
            "ok": True,
            "total_posts_fetched": len(posts),
            "ready_to_view_count": len(ready),
            "total_video_views":   total_views,
            "share_count_total":   total_shares,
            "last_post_secs_epoch": last_post_ts,
        }

    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── Step 2: Enrich each influencer ───────────────────────────────────────────
print("=" * 60)
print("STEP 2 — Calling canister endpoints (profile + posts)")
print("=" * 60)

enriched_records = []
profile_ok = posts_ok = profile_err = posts_err = 0

for i, inf in enumerate(influencers, 1):
    iid = inf.get("id", "")
    name = inf.get("display_name") or inf.get("name") or iid[:20]
    print(f"  [{i:3d}/{len(influencers)}] {name:<25s}", end="  ")

    time.sleep(RATE_LIMIT_DELAY)
    profile = call_profile(iid)

    time.sleep(RATE_LIMIT_DELAY)
    posts = call_posts(iid)

    profile_ok  += profile["ok"]
    profile_err += not profile["ok"]
    posts_ok    += posts["ok"]
    posts_err   += not posts["ok"]

    p_status = "✓" if profile["ok"] else f"✗ {profile.get('error','')[:30]}"
    q_status = "✓" if posts["ok"]   else f"✗ {posts.get('error','')[:30]}"
    print(f"profile={p_status}  posts={q_status}")

    record = dict(inf)
    record["canister_profile"] = {k: v for k, v in profile.items() if k != "ok"}
    record["canister_posts"]   = {k: v for k, v in posts.items()   if k != "ok"}
    enriched_records.append(record)


# ── Step 3: Save ─────────────────────────────────────────────────────────────
with open(OUTPUT_FILE, "w") as f:
    json.dump(enriched_records, f, indent=2, default=str)

print()
print("=" * 60)
print("SUMMARY")
print("=" * 60)
print(f"  Influencers processed : {len(enriched_records)}")
print(f"  Profile calls OK/ERR  : {profile_ok} / {profile_err}")
print(f"  Posts   calls OK/ERR  : {posts_ok} / {posts_err}")
print(f"  Output saved to       : {OUTPUT_FILE}")
print()

# ── Sample output ─────────────────────────────────────────────────────────────
if enriched_records:
    sample = enriched_records[0]
    print("Sample enriched record (first entry):")
    print(json.dumps({
        "id":              sample.get("id"),
        "display_name":    sample.get("display_name"),
        "category":        sample.get("category"),
        "canister_profile": sample.get("canister_profile"),
        "canister_posts":   sample.get("canister_posts"),
    }, indent=2, default=str))
