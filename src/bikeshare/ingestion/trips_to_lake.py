"""Download monthly Citi Bike trip archives and land the raw CSVs in the lake.

    s3://tripdata/<JC->YYYYMM-citibike-tripdata(.csv).zip
        -> s3://bikeshare-lake/raw/tripdata/city=<CITY>/month=<YYYY-MM>/<file>.csv
        -> .../_SUCCESS   (marker, makes re-runs skip finished months)

Usage:
    python -m bikeshare.ingestion.trips_to_lake --city JC --start-month 2025-01 --end-month 2025-03
"""

from __future__ import annotations

import argparse
import logging
import os
import tempfile
import xml.etree.ElementTree as ET
import zipfile

import requests

from bikeshare.config import TRIPDATA_URL, city, lake_bucket, month_range
from bikeshare.lake import delete_prefix, object_exists, s3_client

log = logging.getLogger("trips_to_lake")
S3_NS = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}


class MonthNotPublished(Exception):
    pass


def lake_prefix(city_code: str, month: str) -> str:
    return f"raw/tripdata/city={city_code}/month={month}/"


def find_archive(city_code: str, month: str) -> tuple[str, list[str] | None]:
    """Return (archive key, member filter). Monthly archives are preferred; older
    NYC years are only published as one yearly zip, filtered to the month."""
    c = city(city_code)
    yyyymm = month.replace("-", "")
    candidates = [(f"{c.trip_file_prefix}{yyyymm}-citibike-tripdata", None)]
    if not c.trip_file_prefix:
        candidates.append((f"{month[:4]}-citibike-tripdata", [yyyymm]))

    for prefix, member_filter in candidates:
        resp = requests.get(TRIPDATA_URL, params={"list-type": "2", "prefix": prefix}, timeout=30)
        resp.raise_for_status()
        keys = [k.text for k in ET.fromstring(resp.content).findall(".//s3:Key", S3_NS)]
        zips = [k for k in keys if k.endswith(".zip")]
        if zips:
            return zips[0], member_filter
    raise MonthNotPublished(f"No trip archive published for {city_code} {month}")


def download(key: str, dest: str) -> None:
    url = f"{TRIPDATA_URL}/{key}"
    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("Content-Length", 0))
        done = 0
        with open(dest, "wb") as fh:
            for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                fh.write(chunk)
                done += len(chunk)
                if total and done % (128 * 1024 * 1024) < len(chunk):
                    log.info("  downloaded %.0f / %.0f MB", done / 1e6, total / 1e6)


def iter_csv_members(zf: zipfile.ZipFile, workdir: str, member_filter: list[str] | None):
    """Yield (name, file object) for every CSV, descending into nested zips."""
    for info in zf.infolist():
        name = info.filename
        base = os.path.basename(name)
        if info.is_dir() or name.startswith("__MACOSX") or base.startswith("._"):
            continue
        if name.endswith(".zip"):
            if member_filter and not any(f in base for f in member_filter):
                continue
            nested_path = zf.extract(info, workdir)
            with zipfile.ZipFile(nested_path) as nested:
                yield from iter_csv_members(nested, workdir, None)
            os.remove(nested_path)
        elif name.endswith(".csv"):
            if member_filter and not any(f in base for f in member_filter):
                continue
            with zf.open(info) as fh:
                yield base, fh


def ingest_month(city_code: str, month: str, force: bool = False) -> int:
    bucket = lake_bucket()
    prefix = lake_prefix(city_code, month)
    marker = prefix + "_SUCCESS"
    if not force and object_exists(marker):
        log.info("%s %s already in lake, skipping (use --force to reload)", city_code, month)
        return 0

    key, member_filter = find_archive(city_code, month)
    log.info("%s %s <- %s/%s", city_code, month, TRIPDATA_URL, key)
    removed = delete_prefix(prefix)
    if removed:
        log.info("  removed %d stale objects under %s", removed, prefix)

    client = s3_client()
    uploaded = 0
    with tempfile.TemporaryDirectory() as tmp:
        archive = os.path.join(tmp, "archive.zip")
        download(key, archive)
        with zipfile.ZipFile(archive) as zf:
            for name, fh in iter_csv_members(zf, tmp, member_filter):
                client.upload_fileobj(fh, bucket, prefix + name)
                uploaded += 1
                log.info("  uploaded s3://%s/%s%s", bucket, prefix, name)

    if uploaded == 0:
        raise RuntimeError(f"Archive {key} contained no CSV files for {month}")
    client.put_object(Bucket=bucket, Key=marker, Body=b"")
    return uploaded


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--city", default=os.getenv("DEFAULT_CITY", "JC"))
    parser.add_argument("--start-month", required=True)
    parser.add_argument("--end-month", required=True)
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--skip-missing", action="store_true", help="don't fail on months not yet published")
    args = parser.parse_args()

    for month in month_range(args.start_month, args.end_month):
        try:
            ingest_month(args.city.upper(), month, args.force)
        except MonthNotPublished as exc:
            if not args.skip_missing:
                raise
            log.warning("%s", exc)


if __name__ == "__main__":
    main()
