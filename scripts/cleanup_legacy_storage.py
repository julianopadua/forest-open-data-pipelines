from __future__ import annotations

import argparse
import os
from pathlib import Path

import httpx
from dotenv import load_dotenv
from supabase import create_client

from forest_pipelines.storage.legacy_cleanup import (
    audit_references,
    is_legacy_object,
    list_objects,
)


def _describe_error(exc: Exception) -> str:
    current: BaseException | None = exc
    while current is not None:
        if isinstance(current, httpx.HTTPStatusError):
            return f"HTTP {current.response.status_code} from Supabase Storage"
        current = current.__context__ or current.__cause__
    return f"{type(exc).__name__}: {exc}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Inventory and optionally remove legacy dataset payloads from Supabase Storage."
    )
    parser.add_argument("--apply", action="store_true", help="Delete listed objects with the Storage API.")
    parser.add_argument(
        "--expected-count", type=int, help="Required with --apply; must match the dry-run count."
    )
    args = parser.parse_args()
    if args.apply and (args.expected_count is None or args.expected_count < 1):
        parser.error("--apply requires --expected-count from a previous dry run")

    root = Path(__file__).resolve().parents[1]
    load_dotenv(root / ".env")
    url = os.getenv("SUPABASE_URL", "").strip()
    key = os.getenv("SUPABASE_SERVICE_ROLE_KEY", "").strip()
    bucket = os.getenv("SUPABASE_BUCKET_OPEN_DATA", "open-data").strip()
    if not url or not key or not bucket:
        parser.error("SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY and bucket are required")

    bucket_client = create_client(url, key).storage.from_(bucket)
    try:
        objects = list_objects(bucket_client)
        candidates = [obj for obj in objects if is_legacy_object(obj.path)]
        references = audit_references(bucket_client, bucket, objects)
    except Exception as exc:
        print(f"Storage audit failed: {_describe_error(exc)}")
        return 1

    for obj in candidates:
        print(f"{obj.size_bytes:>12}  {obj.path}")
    total = sum(obj.size_bytes for obj in candidates)
    print(f"Legacy objects: {len(candidates)}, total MiB: {total / 1048576:.2f}")
    if references:
        print("Deletion blocked because published JSON still references these objects:")
        for path in sorted(references):
            print(path)
        return 1
    if not args.apply:
        return 0
    if len(candidates) != args.expected_count:
        print("Deletion blocked because the candidate count changed since the dry run.")
        return 1

    paths = [obj.path for obj in candidates]
    try:
        for start in range(0, len(paths), 100):
            batch = paths[start:start + 100]
            bucket_client.remove(batch)
            print(f"Removed {len(batch)} objects")
        remaining = [obj.path for obj in list_objects(bucket_client) if is_legacy_object(obj.path)]
    except Exception as exc:
        print(f"Storage removal failed: {_describe_error(exc)}")
        return 1
    if remaining:
        print(f"Verification failed: {len(remaining)} legacy objects remain.")
        return 1
    print(f"Verified removal of {len(paths)} legacy objects.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
