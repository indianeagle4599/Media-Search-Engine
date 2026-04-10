"""
date.py

Date parsing, normalization, and filter utilities.
"""

import os, platform, re, calendar
from datetime import datetime, timedelta, timezone
from warnings import warn
from zoneinfo import ZoneInfo

try:
    from timezonefinder import TimezoneFinder
except ImportError:
    TimezoneFinder = None
    warn(
        "timezonefinder package not found. GPS-based local time conversion will be unavailable."
    )


TF = TimezoneFinder() if TimezoneFinder else None

DATE_KEYS = (
    "creation_date",
    "modification_date",
    "index_date",
    "DateTime",
    "DateTimeOriginal",
    "DateTimeDigitized",
    "GPSDateStamp",
)

DATE_MASK_RE = (
    r"[0-9Xx]{4}-[0-9Xx]{2}-[0-9Xx]{2}" r"[Tt][0-9Xx]{2}:[0-9Xx]{2}:[0-9Xx]{2}"
)
DATE_MASK_PATTERN = re.compile(f"({DATE_MASK_RE})")
DATE_RANGE_PATTERN = re.compile(
    rf"({DATE_MASK_RE})\s*(?:\bto\b|\bthrough\b|\buntil\b|-)\s*({DATE_MASK_RE})",
    re.IGNORECASE,
)
SEMANTIC_DATE_MASKS = {
    # Existing Times of Day
    "midnight": ("XXXX-XX-XXT00:00:00", "XXXX-XX-XXT00:59:59"),
    "dawn": ("XXXX-XX-XXT05:00:00", "XXXX-XX-XXT06:59:59"),
    "sunrise": ("XXXX-XX-XXT06:00:00", "XXXX-XX-XXT07:59:59"),
    "morning": ("XXXX-XX-XXT06:00:00", "XXXX-XX-XXT11:59:59"),
    "noon": ("XXXX-XX-XXT12:00:00", "XXXX-XX-XXT12:59:59"),
    "afternoon": ("XXXX-XX-XXT13:00:00", "XXXX-XX-XXT16:59:59"),
    "golden hour": ("XXXX-XX-XXT17:00:00", "XXXX-XX-XXT18:59:59"),
    "sunset": ("XXXX-XX-XXT18:00:00", "XXXX-XX-XXT18:59:59"),
    "twilight": ("XXXX-XX-XXT18:00:00", "XXXX-XX-XXT19:59:59"),
    "evening": ("XXXX-XX-XXT17:00:00", "XXXX-XX-XXT20:59:59"),
    "night": ("XXXX-XX-XXT20:00:00", "XXXX-XX-XXT23:59:59"),
    # Business / Extended Times
    "business hours": ("XXXX-XX-XXT09:00:00", "XXXX-XX-XXT17:59:59"),
    "working hours": ("XXXX-XX-XXT09:00:00", "XXXX-XX-XXT17:59:59"),
    "lunch": ("XXXX-XX-XXT12:00:00", "XXXX-XX-XXT13:59:59"),
    "lunchtime": ("XXXX-XX-XXT12:00:00", "XXXX-XX-XXT13:59:59"),
    # Decades & Centuries
    "70s": ("197X-XX-XXTXX:XX:XX", "197X-XX-XXTXX:XX:XX"),
    "1970s": ("197X-XX-XXTXX:XX:XX", "197X-XX-XXTXX:XX:XX"),
    "80s": ("198X-XX-XXTXX:XX:XX", "198X-XX-XXTXX:XX:XX"),
    "1980s": ("198X-XX-XXTXX:XX:XX", "198X-XX-XXTXX:XX:XX"),
    "90s": ("199X-XX-XXTXX:XX:XX", "199X-XX-XXTXX:XX:XX"),
    "1990s": ("199X-XX-XXTXX:XX:XX", "199X-XX-XXTXX:XX:XX"),
    "2000s": ("200X-XX-XXTXX:XX:XX", "200X-XX-XXTXX:XX:XX"),
    "2010s": ("201X-XX-XXTXX:XX:XX", "201X-XX-XXTXX:XX:XX"),
    "2020s": ("202X-XX-XXTXX:XX:XX", "202X-XX-XXTXX:XX:XX"),
    "20th century": ("19XX-XX-XXTXX:XX:XX", "19XX-XX-XXTXX:XX:XX"),
    "21st century": ("20XX-XX-XXTXX:XX:XX", "20XX-XX-XXTXX:XX:XX"),
    # Day-Level Month Segments
    "start of month": ("XXXX-XX-01TXX:XX:XX", "XXXX-XX-10TXX:XX:XX"),
    "early month": ("XXXX-XX-01TXX:XX:XX", "XXXX-XX-10TXX:XX:XX"),
    "mid month": ("XXXX-XX-11TXX:XX:XX", "XXXX-XX-20TXX:XX:XX"),
    "middle of month": ("XXXX-XX-11TXX:XX:XX", "XXXX-XX-20TXX:XX:XX"),
    "end of month": ("XXXX-XX-21TXX:XX:XX", "XXXX-XX-31TXX:XX:XX"),
    "late month": ("XXXX-XX-21TXX:XX:XX", "XXXX-XX-31TXX:XX:XX"),
    # Seasons (Non-Wrapping)
    "spring": ("XXXX-03-XXTXX:XX:XX", "XXXX-05-XXTXX:XX:XX"),
    "summer": ("XXXX-06-XXTXX:XX:XX", "XXXX-08-XXTXX:XX:XX"),
    "autumn": ("XXXX-09-XXTXX:XX:XX", "XXXX-11-XXTXX:XX:XX"),
    "fall": ("XXXX-09-XXTXX:XX:XX", "XXXX-11-XXTXX:XX:XX"),
    # Holidays & Notable Periods
    "holiday season": ("XXXX-11-20TXX:XX:XX", "XXXX-12-31TXX:XX:XX"),
    "christmas": ("XXXX-12-24TXX:XX:XX", "XXXX-12-26TXX:XX:XX"),
    "new year's eve": ("XXXX-12-31TXX:XX:XX", "XXXX-12-31TXX:XX:XX"),
    "new year's day": ("XXXX-01-01TXX:XX:XX", "XXXX-01-01TXX:XX:XX"),
}


# === IO Date Normalization ===


def format_datetime(value):
    def clean(dt):
        if not dt:
            return None
        dt = dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        now = datetime.now(timezone.utc) + timedelta(days=1)
        return dt.isoformat() if 1980 <= dt.year and dt <= now else None

    if isinstance(value, (float, int)):
        return clean(datetime.fromtimestamp(value, tz=timezone.utc))

    if isinstance(value, (list, tuple)):
        return type(value)(format_datetime(v) for v in value)

    if isinstance(value, str):
        try:
            return clean(datetime.fromisoformat(value))
        except ValueError:
            return None

    return None


def _resolve_date_values(vals: dict):
    flags = []

    if (
        vals["creation_date"]
        and vals["modification_date"]
        and vals["creation_date"] > vals["modification_date"]
    ):
        vals["creation_date"] = vals["modification_date"]
        flags.append("os_creation_after_modification_corrected")

    created = (
        vals["DateTimeOriginal"]
        or vals["DateTimeDigitized"]
        or vals["GPSDateStamp"]
        or vals["creation_date"]
    )
    modified = vals["DateTime"] or vals["modification_date"]

    if created and modified and modified < created:
        flags.append("modification_before_creation")
        if created == vals["creation_date"] and vals["DateTime"]:
            created = modified
            flags.append("os_creation_downgraded_to_modification")

    return created, modified, flags


def resolve_dates(dates: dict) -> dict:
    vals = {k: dates.get(k) for k in DATE_KEYS}
    created, modified, flags = _resolve_date_values(vals)

    return {
        "master_date": created or modified,
        "true_creation_date": created,
        "true_modification_date": modified,
        "index_date": vals["index_date"],
        "creation_date": vals["creation_date"],
        "modification_date": vals["modification_date"],
        "date_reliability": (
            "invalid"
            if not (created or modified)
            else (
                "high"
                if vals["DateTimeOriginal"]
                and "modification_before_creation" not in flags
                else (
                    "medium"
                    if (vals["DateTimeDigitized"] or vals["GPSDateStamp"])
                    and "modification_before_creation" not in flags
                    else "low"
                )
            )
        ),
        "flags": flags,
    }


# === IO File Date Sources ===


def get_windows_times(file_path: str):
    now = datetime.now().timestamp()
    try:
        ctime, mtime = os.path.getctime(file_path), os.path.getmtime(file_path)
        return ctime, mtime, now
    except (FileNotFoundError, PermissionError) as e:
        warn(f"Error getting file times for {file_path}: {e}")
    return now, now, now


def get_unix_times(file_path: str):
    now = datetime.now().timestamp()
    try:
        stat = os.stat(file_path)
        try:
            ctime = stat.st_birthtime
        except AttributeError:
            ctime = min(stat.st_mtime, stat.st_ctime)
        mtime = os.path.getmtime(file_path)
        return ctime, mtime, now
    except (FileNotFoundError, PermissionError) as e:
        warn(f"Error getting file times for {file_path}: {e}")
    return now, now, now


def get_time_function():
    support_dict = {
        "Windows": get_windows_times,
        "Linux": get_unix_times,
        "Darwin": get_unix_times,
    }
    curr_os = platform.system()
    return support_dict.get(curr_os, get_unix_times)


def get_os_dates(file_path: str):
    creation_date, modification_date, index_date = get_time_function()(file_path)
    return {
        "creation_date": format_datetime(creation_date),
        "modification_date": format_datetime(modification_date),
        "index_date": datetime.fromtimestamp(index_date)
        .astimezone()
        .isoformat(),  # Use local timezone for index date
    }


def resolve_file_dates(
    file_path: str, extracted_date_items: dict | None = None
) -> dict:
    dates = get_os_dates(file_path)
    dates.update(extracted_date_items or {})
    return resolve_dates(dates)


# === IO EXIF Date Extraction ===


def get_local_gps_time(gps_info: dict, utc_dt: datetime) -> str:
    """Finds timezone from coordinates and shifts UTC time to Local time."""
    lat, lat_ref = gps_info.get("GPSLatitude"), gps_info.get("GPSLatitudeRef")
    lon, lon_ref = gps_info.get("GPSLongitude"), gps_info.get("GPSLongitudeRef")

    if TF and lat and lat_ref and lon and lon_ref:
        try:
            lat_dec = float(lat[0]) + float(lat[1]) / 60 + float(lat[2]) / 3600
            if lat_ref.upper() == "S":
                lat_dec = -lat_dec

            lon_dec = float(lon[0]) + float(lon[1]) / 60 + float(lon[2]) / 3600
            if lon_ref.upper() == "W":
                lon_dec = -lon_dec

            tz_str = TF.timezone_at(lng=lon_dec, lat=lat_dec)
            if tz_str:
                return utc_dt.astimezone(ZoneInfo(tz_str)).isoformat()
        except Exception as e:
            warn(f"Error converting GPS time to local: {e}")
    return utc_dt.isoformat()


def extract_ifd_date_items(ifd_name: str, ifd_data: dict, date_items: dict):
    if ifd_name == "Exif":
        temp_date_items = {}
        dt_keys = {
            "DateTime": ["SubsecTime", "OffsetTime"],
            "DateTimeOriginal": ["SubsecTimeOriginal", "OffsetTimeOriginal"],
            "DateTimeDigitized": ["SubsecTimeDigitized", "OffsetTimeDigitized"],
        }
        for base_key, (ss_key, ofs_key) in dt_keys.items():
            if base_key not in ifd_data:
                continue

            raw_date = str(ifd_data.get(base_key, "")).strip()
            if not raw_date:
                continue

            base_date = raw_date.replace(":", "-", 2).replace(" ", "T", 1)
            subsec = str(ifd_data.get(ss_key, "000")).strip()
            offset = str(ifd_data.get(ofs_key, "+00:00")).strip()

            temp_date_items[base_key] = format_datetime(f"{base_date}.{subsec}{offset}")
        date_items.update(temp_date_items)

    elif ifd_name == "GPSInfo":
        raw_date = ifd_data.get("GPSDateStamp", None)
        if not raw_date:
            return date_items

        gps_date = str(raw_date).replace(":", "-", 2)
        h_raw, m_raw, s_raw = ifd_data.get("GPSTimeStamp", [0.0, 0.0, 0.0])
        h, m, s_float, s_int = int(h_raw), int(m_raw), float(s_raw), int(s_raw)
        ss = int(round((s_float % 1) * 1_000_000))

        base_utc_str = f"{gps_date}T{h:02d}:{m:02d}:{s_int:02d}.{ss:06d}+00:00".strip()
        try:
            base_utc_date = datetime.fromisoformat(base_utc_str)
            date_items["GPSDateStamp"] = format_datetime(
                get_local_gps_time(ifd_data, base_utc_date)
            )
        except Exception as e:
            warn(f"Error parsing GPS datetime: {e}")
    return date_items


# === Retrieval Date Parser ===


def extract_date_filter_from_query(query_text: str):
    q = query_text.strip()
    clean = q
    date_filters = []
    seen_filters = set()

    def add_date_filter(
        start_mask: str,
        end_mask: str,
        source: str,
        matched_text: str,
        matched_semantic_key: str | None = None,
    ):
        key = (source, start_mask.upper(), end_mask.upper(), matched_semantic_key)
        if key in seen_filters:
            return
        seen_filters.add(key)
        date_filter = {
            "start_mask": start_mask.upper(),
            "end_mask": end_mask.upper(),
            "source": source,
            "matched_text": matched_text,
        }
        if matched_semantic_key:
            date_filter["matched_semantic_key"] = matched_semantic_key
        date_filters.append(date_filter)

    for match in DATE_RANGE_PATTERN.finditer(q):
        add_date_filter(
            match.group(1),
            match.group(2),
            "explicit_range",
            match.group(0),
        )
    clean = DATE_RANGE_PATTERN.sub(" ", clean)

    for match in DATE_MASK_PATTERN.finditer(clean):
        mask = match.group(1)
        add_date_filter(mask, mask, "single_mask", match.group(0))
    clean = DATE_MASK_PATTERN.sub(" ", clean)
    clean = re.sub(r"\s+", " ", clean).strip()

    for key in sorted(SEMANTIC_DATE_MASKS, key=len, reverse=True):
        match = re.search(rf"(?<!\w){re.escape(key)}(?!\w)", clean, re.IGNORECASE)
        if match:
            add_date_filter(
                SEMANTIC_DATE_MASKS[key][0],
                SEMANTIC_DATE_MASKS[key][1],
                "semantic",
                match.group(0),
                key,
            )

    return {
        "date_filters": date_filters,
        "clean_query_text": clean.lower(),
    }


# === Chroma Date Filter Builders ===


def split_date(date_str: str):
    try:
        dt = datetime.fromisoformat(date_str)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return {
            "year": dt.year,
            "month": dt.month,
            "day": dt.day,
            "hour": dt.hour,
            "minute": dt.minute,
            "second": dt.second,
            "microsecond": dt.microsecond,
            "ts": float(dt.timestamp()),
            "tz_offset_minutes": (
                int(dt.utcoffset().total_seconds() // 60) if dt.utcoffset() else 0
            ),
        }
    except Exception as e:
        print(f"Error splitting date string '{date_str}' to smaller parts:", e)
        return None


def date_dict_to_ts(date_dict: dict):
    ts_dict = {}
    for key, date_str in date_dict.items():
        if not date_str:
            continue

        date_parts = split_date(date_str)
        if not date_parts:
            continue

        for date_part in date_parts:
            date_item_key = key.replace("date", date_part)
            ts_dict[date_item_key] = date_parts[date_part]
    return ts_dict


def count_mask_specificity(start_mask: str, end_mask: str):
    if not start_mask or not end_mask:
        return 0

    s_parts = re.fullmatch(
        r"([0-9X]{4})-([0-9X]{2})-([0-9X]{2})T([0-9X]{2}):([0-9X]{2}):([0-9X]{2})",
        start_mask.upper(),
    )
    e_parts = re.fullmatch(
        r"([0-9X]{4})-([0-9X]{2})-([0-9X]{2})T([0-9X]{2}):([0-9X]{2}):([0-9X]{2})",
        end_mask.upper(),
    )
    if not s_parts or not e_parts:
        return 0

    score = 0
    for s, e in zip(s_parts.groups(), e_parts.groups()):
        s_all_x = set(s) == {"X"}
        e_all_x = set(e) == {"X"}

        if s_all_x and e_all_x:
            continue
        if "X" not in s and "X" not in e and s == e:
            score += 2
        else:
            score += 1
    return score


def _parse_mask_groups(date_mask: str):
    m = re.fullmatch(
        r"([0-9X]{4})-([0-9X]{2})-([0-9X]{2})T([0-9X]{2}):([0-9X]{2}):([0-9X]{2})",
        date_mask.upper(),
    )
    if not m:
        raise ValueError(
            "date_mask must match YYYY-MM-DDTHH:MM:SS using digits and/or X"
        )
    return m.groups()


def _masked_int_bounds(mask: str, lo_default: int, hi_default: int):
    mask = mask.upper()
    if set(mask) == {"X"}:
        return lo_default, hi_default
    lo = int(mask.replace("X", "0"))
    hi = int(mask.replace("X", "9"))
    return lo, hi


def mask_to_datetime_bounds(date_mask: str):
    year_s, month_s, day_s, hour_s, minute_s, second_s = _parse_mask_groups(date_mask)

    year_lo, year_hi = _masked_int_bounds(year_s, 1, 9999)
    month_lo, month_hi = _masked_int_bounds(month_s, 1, 12)
    hour_lo, hour_hi = _masked_int_bounds(hour_s, 0, 23)
    minute_lo, minute_hi = _masked_int_bounds(minute_s, 0, 59)
    second_lo, second_hi = _masked_int_bounds(second_s, 0, 59)

    year_lo = max(1, min(year_lo, 9999))
    year_hi = max(1, min(year_hi, 9999))
    month_lo = max(1, min(month_lo, 12))
    month_hi = max(1, min(month_hi, 12))

    max_day_lo = calendar.monthrange(year_lo, month_lo)[1]
    max_day_hi = calendar.monthrange(year_hi, month_hi)[1]

    if set(day_s.upper()) == {"X"}:
        day_lo, day_hi = 1, max_day_hi
    else:
        day_lo = int(day_s.upper().replace("X", "0"))
        day_hi = int(day_s.upper().replace("X", "9"))
        day_lo = max(1, min(day_lo, max_day_lo))
        day_hi = max(1, min(day_hi, max_day_hi))

    start_dt = datetime(
        year_lo,
        month_lo,
        day_lo,
        hour_lo,
        minute_lo,
        second_lo,
        tzinfo=timezone.utc,
    )
    end_dt = datetime(
        year_hi,
        month_hi,
        day_hi,
        hour_hi,
        minute_hi,
        second_hi,
        tzinfo=timezone.utc,
    )

    if start_dt > end_dt:
        raise ValueError(f"Invalid masked datetime range derived from {date_mask}")

    return start_dt, end_dt


def mask_to_ts_bounds(date_mask: str):
    start_dt, end_dt = mask_to_datetime_bounds(date_mask)
    return float(start_dt.timestamp()), float(end_dt.timestamp())


def combine_where_clauses(*clauses):
    flat = []
    for clause in clauses:
        if not clause:
            continue
        if isinstance(clause, dict) and "$and" in clause and len(clause) == 1:
            flat.extend(clause["$and"])
        else:
            flat.append(clause)

    if not flat:
        return None
    if len(flat) == 1:
        return flat[0]
    return {"$and": flat}


def build_timestamp_where_clause(date_field: str, start_mask: str, end_mask: str):
    valid_fields = {
        "estimated_date": "estimated_ts",
        "creation_date": "creation_ts",
        "modification_date": "modification_ts",
        "master_date": "master_ts",
    }
    if date_field not in valid_fields:
        raise ValueError(f"Invalid date_field: {date_field}")

    ts_field = valid_fields[date_field]
    start_ts, _ = mask_to_ts_bounds(start_mask)
    _, end_ts = mask_to_ts_bounds(end_mask)

    if start_ts > end_ts:
        raise ValueError(
            f"Invalid timestamp range: start_mask={start_mask}, end_mask={end_mask}"
        )

    return {
        "$and": [
            {ts_field: {"$gte": start_ts}},
            {ts_field: {"$lte": end_ts}},
        ]
    }


def build_date_where_clause(date_field: str, date_filter: dict):
    start_mask = date_filter.get("start_mask")
    end_mask = date_filter.get("end_mask")
    if not start_mask or not end_mask:
        return None

    use_recurring_filter = date_filter.get("source") == "semantic" or (
        start_mask[:4].upper() == "XXXX" and end_mask[:4].upper() == "XXXX"
    )
    if use_recurring_filter:
        return build_recurring_where_clause(date_field, start_mask, end_mask)
    return build_timestamp_where_clause(date_field, start_mask, end_mask)


def _range_clause(field: str, lo: int, hi: int):
    return combine_where_clauses({field: {"$gte": lo}}, {field: {"$lte": hi}})


def _lexicographic_bound_clause(parts: list[tuple[str, int, int]], bound: str):
    clauses = []
    is_lower = bound == "lower"
    eq_idx = 1 if is_lower else 2
    for i, (field, lo, hi) in enumerate(parts):
        value = lo if is_lower else hi
        op = "$gte" if i == len(parts) - 1 and is_lower else "$gt"
        op = "$lte" if i == len(parts) - 1 and not is_lower else op
        op = "$lt" if i < len(parts) - 1 and not is_lower else op
        prefix = [{prev[0]: {"$eq": prev[eq_idx]}} for prev in parts[:i]]
        clauses.append(combine_where_clauses(*prefix, {field: {op: value}}))
    return clauses[0] if len(clauses) == 1 else {"$or": clauses}


def build_recurring_where_clause(date_field: str, start_mask: str, end_mask: str):
    date_prefix = date_field.replace("_date", "")
    part_specs = [
        ("year", 1, 9999),
        ("month", 1, 12),
        ("day", 1, 31),
        ("hour", 0, 23),
        ("minute", 0, 59),
        ("second", 0, 59),
    ]
    parts = []
    for (part_name, min_value, max_value), start_part, end_part in zip(
        part_specs, _parse_mask_groups(start_mask), _parse_mask_groups(end_mask)
    ):
        if set(start_part.upper()) == {"X"} and set(end_part.upper()) == {"X"}:
            continue
        lo, _ = _masked_int_bounds(start_part, min_value, max_value)
        _, hi = _masked_int_bounds(end_part, min_value, max_value)
        lo = max(min_value, min(lo, max_value))
        hi = max(min_value, min(hi, max_value))
        parts.append((f"{date_prefix}_{part_name}", lo, hi))

    if not parts:
        return None
    if len(parts) == 1:
        return _range_clause(parts[0][0], parts[0][1], parts[0][2])
    return combine_where_clauses(
        _lexicographic_bound_clause(parts, "lower"),
        _lexicographic_bound_clause(parts, "upper"),
    )
