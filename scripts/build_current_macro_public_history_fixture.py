#!/usr/bin/env python3
"""Build a current U.S. macro public-history workspace from public sources."""

from __future__ import annotations

import argparse
import csv
import html
import json
import math
import re
import unicodedata
from dataclasses import dataclass
from datetime import UTC, date, datetime
from html.parser import HTMLParser
from io import StringIO
from pathlib import Path
from typing import Any, Iterable, TypeVar
from urllib.parse import urlencode, urljoin
from zoneinfo import ZoneInfo

import requests
import defusedxml.ElementTree as ET

from vei.context.api import (
    ContextSnapshot,
    ContextSourceResult,
    write_canonical_history_sidecars,
)

DEFAULT_WORKSPACE = Path("docs/examples/current-public-history-macro/workspace")
DEFAULT_START_DATE = "2024-01-01"
DEFAULT_TIMEOUT_S = 30
SOURCE_ID = "current_macro_public_history_world"
LAUNCH_COMMAND = (
    "vei ui serve --root docs/examples/current-public-history-macro/workspace "
    "--host 127.0.0.1 --port 3058"
)
JEPA_CHECKPOINT_PATH = "../../news-public-history-demo/workspace/jepa_model.pt"
USER_AGENT = "vei-current-macro-public-history/1.0 contact=https://strangelab.ai"
T = TypeVar("T")

FRED_CSV_URL = "https://fred.stlouisfed.org/graph/fredgraph.csv"
BLS_PUBLIC_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BEA_CURRENT_RELEASES_URL = "https://www.bea.gov/news/current-releases"
FED_MONETARY_POLICY_URL = "https://www.federalreserve.gov/monetarypolicy.htm"
TREASURY_YIELD_XML_URL = (
    "https://home.treasury.gov/resource-center/data-chart-center/"
    "interest-rates/pages/xml"
)
FEDERAL_REGISTER_API_URL = "https://www.federalregister.gov/api/v1/documents.json"
GDELT_DOC_API_URL = "https://api.gdeltproject.org/api/v2/doc/doc"
GDELT_MACRO_QUERY = (
    '(inflation OR "Federal Reserve" OR "interest rates" OR jobs OR '
    "unemployment OR GDP OR Treasury OR tariffs OR recession) sourceCountry:US"
)
FEDERAL_REGISTER_MACRO_QUERY = (
    "inflation energy interest rates labor housing banking financial stability"
)


@dataclass(frozen=True)
class FredSeriesSpec:
    series_id: str
    title: str
    units: str
    topic: str


@dataclass(frozen=True)
class BlsSeriesSpec:
    series_id: str
    title: str
    units: str
    topic: str


@dataclass(frozen=True)
class MacroWindow:
    window_id: str
    label: str
    start_date: str
    end_date: str
    role: str


@dataclass(frozen=True)
class SourceDocument:
    doc_id: str
    title: str
    body: str
    created_time: str
    owner: str
    url: str
    source_family: str
    topic: str
    window_id: str = ""
    window_label: str = ""
    window_role: str = ""


@dataclass(frozen=True)
class SourceFetchRecord:
    source_family: str
    url: str
    status: str
    record_count: int = 0
    detail: str = ""


class _ParagraphParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self._capture_stack: list[str] = []
        self._current: list[str] = []
        self.paragraphs: list[str] = []
        self.links: list[tuple[str, str]] = []
        self._link_href = ""
        self._link_text: list[str] = []

    def handle_starttag(
        self,
        tag: str,
        attrs: list[tuple[str, str | None]],
    ) -> None:
        if tag in {"p", "h1", "h2", "h3", "td", "time"}:
            self._capture_stack.append(tag)
            self._current = []
        if tag == "a":
            attrs_map = {key: value or "" for key, value in attrs}
            self._link_href = attrs_map.get("href", "")
            self._link_text = []

    def handle_endtag(self, tag: str) -> None:
        if tag in {"p", "h1", "h2", "h3", "td", "time"} and self._capture_stack:
            text = _clean_text(" ".join(self._current), max_chars=2000)
            if text:
                self.paragraphs.append(text)
            self._capture_stack.pop()
            self._current = []
        if tag == "a" and self._link_href:
            text = _clean_text(" ".join(self._link_text), max_chars=240)
            if text:
                self.links.append((self._link_href, text))
            self._link_href = ""
            self._link_text = []

    def handle_data(self, data: str) -> None:
        if self._capture_stack:
            self._current.append(data)
        if self._link_href:
            self._link_text.append(data)


FRED_SERIES: tuple[FredSeriesSpec, ...] = (
    FredSeriesSpec(
        "CPIAUCSL",
        "Consumer Price Index",
        "index 1982-84=100, seasonally adjusted",
        "banking_markets",
    ),
    FredSeriesSpec(
        "CPILFESL",
        "Core CPI",
        "index 1982-84=100, seasonally adjusted",
        "banking_markets",
    ),
    FredSeriesSpec(
        "PCEPI",
        "PCE Price Index",
        "index 2017=100, seasonally adjusted",
        "banking_markets",
    ),
    FredSeriesSpec(
        "PCEPILFE",
        "Core PCE Price Index",
        "index 2017=100, seasonally adjusted",
        "banking_markets",
    ),
    FredSeriesSpec(
        "UNRATE",
        "Unemployment Rate",
        "percent",
        "labor_work",
    ),
    FredSeriesSpec(
        "PAYEMS",
        "Total Nonfarm Payrolls",
        "thousands of persons",
        "labor_work",
    ),
    FredSeriesSpec(
        "FEDFUNDS",
        "Effective Federal Funds Rate",
        "percent",
        "government_policy",
    ),
    FredSeriesSpec("DGS2", "2-Year Treasury Yield", "percent", "banking_markets"),
    FredSeriesSpec("DGS10", "10-Year Treasury Yield", "percent", "banking_markets"),
    FredSeriesSpec(
        "T10Y2Y",
        "10-Year Minus 2-Year Treasury Spread",
        "percentage points",
        "banking_markets",
    ),
    FredSeriesSpec(
        "RSAFS",
        "Retail and Food Services Sales",
        "millions of dollars",
        "banking_markets",
    ),
    FredSeriesSpec(
        "INDPRO",
        "Industrial Production Index",
        "index 2017=100",
        "banking_markets",
    ),
    FredSeriesSpec(
        "GDPC1",
        "Real Gross Domestic Product",
        "billions of chained 2017 dollars",
        "government_policy",
    ),
    FredSeriesSpec("DGS30", "30-Year Treasury Yield", "percent", "banking_markets"),
    FredSeriesSpec(
        "T10YIE",
        "10-Year Breakeven Inflation Rate",
        "percent",
        "banking_markets",
    ),
    FredSeriesSpec("DFII10", "10-Year Real Treasury Rate", "percent", "banking_markets"),
    FredSeriesSpec(
        "VIXCLS",
        "CBOE Volatility Index",
        "index",
        "banking_markets",
    ),
    FredSeriesSpec(
        "DCOILWTICO",
        "WTI Crude Oil Price",
        "dollars per barrel",
        "banking_markets",
    ),
    FredSeriesSpec(
        "GASREGW",
        "Regular Gasoline Price",
        "dollars per gallon",
        "banking_markets",
    ),
    FredSeriesSpec(
        "MORTGAGE30US",
        "30-Year Fixed Mortgage Rate",
        "percent",
        "banking_markets",
    ),
    FredSeriesSpec(
        "UMCSENT",
        "University of Michigan Consumer Sentiment",
        "index 1966:Q1=100",
        "banking_markets",
    ),
    FredSeriesSpec("HOUST", "Housing Starts", "thousands of units", "banking_markets"),
    FredSeriesSpec(
        "PERMIT",
        "Building Permits",
        "thousands of units",
        "banking_markets",
    ),
    FredSeriesSpec(
        "ICSA",
        "Initial Unemployment Claims",
        "number",
        "labor_work",
    ),
    FredSeriesSpec(
        "JTSJOL",
        "Job Openings",
        "thousands",
        "labor_work",
    ),
    FredSeriesSpec(
        "PPIACO",
        "Producer Price Index: All Commodities",
        "index 1982=100",
        "banking_markets",
    ),
    FredSeriesSpec(
        "NFCI",
        "Chicago Fed National Financial Conditions Index",
        "index",
        "banking_markets",
    ),
    FredSeriesSpec(
        "WALCL",
        "Federal Reserve Total Assets",
        "millions of dollars",
        "government_policy",
    ),
    FredSeriesSpec("BAA", "Moody's Baa Corporate Bond Yield", "percent", "banking_markets"),
    FredSeriesSpec("AAA", "Moody's Aaa Corporate Bond Yield", "percent", "banking_markets"),
    FredSeriesSpec(
        "DTWEXBGS",
        "Nominal Broad U.S. Dollar Index",
        "index Jan 2006=100",
        "banking_markets",
    ),
)


BLS_SERIES: tuple[BlsSeriesSpec, ...] = (
    BlsSeriesSpec(
        "CUUR0000SA0",
        "Consumer Price Index for All Urban Consumers",
        "index 1982-84=100",
        "banking_markets",
    ),
    BlsSeriesSpec(
        "LNS14000000",
        "Unemployment Rate",
        "percent",
        "labor_work",
    ),
    BlsSeriesSpec(
        "CES0000000001",
        "Total Nonfarm Employment",
        "thousands of persons",
        "labor_work",
    ),
    BlsSeriesSpec(
        "WPUFD4",
        "Producer Price Index: Final Demand",
        "index Nov 2009=100",
        "banking_markets",
    ),
    BlsSeriesSpec(
        "JTS000000000000000JOL",
        "Job Openings: Total Nonfarm",
        "thousands",
        "labor_work",
    ),
)


ANALOG_WINDOWS: tuple[MacroWindow, ...] = (
    MacroWindow(
        "volcker_inflation_1979_1982",
        "Volcker inflation fight",
        "1979-01-01",
        "1982-12-31",
        "similar: inflation, energy, rates, recession pressure; different: policy regime and data environment",
    ),
    MacroWindow(
        "soft_landing_1994_1995",
        "1994-95 tightening and soft landing",
        "1994-01-01",
        "1995-12-31",
        "different: tightening cycle with a less severe macro break",
    ),
    MacroWindow(
        "dotcom_911_2000_2002",
        "Dot-com and post-9/11 slowdown",
        "2000-03-01",
        "2002-12-31",
        "different: market crash, security shock, and mild recession",
    ),
    MacroWindow(
        "global_financial_crisis_2007_2009",
        "Global financial crisis",
        "2007-07-01",
        "2009-06-30",
        "similar: bank stress, Treasury/Fed action, credit-market confidence",
    ),
    MacroWindow(
        "covid_policy_shock_2020_2021",
        "COVID policy shock",
        "2020-02-01",
        "2021-06-30",
        "different: public-health shock with extraordinary fiscal and monetary response",
    ),
    MacroWindow(
        "inflation_hiking_cycle_2021_2023",
        "Inflation and hiking cycle",
        "2021-07-01",
        "2023-12-31",
        "similar: inflation, labor tightness, energy pressure, rapid rate hikes",
    ),
)


def main() -> None:
    args = _parse_args()
    as_of = args.as_of or _today_los_angeles().isoformat()
    output_root = args.workspace.expanduser().resolve()
    output_root.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT})
    documents: list[SourceDocument] = []
    fetch_records: list[SourceFetchRecord] = []
    windows = _macro_windows(
        as_of=as_of,
        current_start_date=args.start_date,
        include_analogs=not args.current_only,
    )

    fred_documents, fred_records = _fetch_fred_documents(
        session=session,
        as_of=as_of,
        windows=windows,
        max_observations_per_series=args.max_observations_per_series,
        timeout_s=args.timeout_s,
    )
    documents.extend(fred_documents)
    fetch_records.extend(fred_records)

    treasury_documents, treasury_record = _fetch_treasury_yield_documents(
        session=session,
        as_of=as_of,
        windows=windows,
        max_rows_per_month=args.max_treasury_rows_per_month,
        timeout_s=args.timeout_s,
    )
    documents.extend(treasury_documents)
    fetch_records.extend(treasury_record)

    bls_documents, bls_records = _fetch_bls_documents(
        session=session,
        windows=windows,
        max_observations_per_series=args.max_bls_observations_per_series,
        timeout_s=args.timeout_s,
    )
    documents.extend(bls_documents)
    fetch_records.extend(bls_records)

    federal_register_documents, federal_register_records = (
        _fetch_federal_register_documents(
            session=session,
            windows=windows,
            max_records_per_window=args.max_federal_register_records,
            timeout_s=args.timeout_s,
        )
    )
    documents.extend(federal_register_documents)
    fetch_records.extend(federal_register_records)

    release_documents, release_records = _fetch_official_release_documents(
        session=session,
        as_of=as_of,
        max_releases=args.max_official_releases,
        timeout_s=args.timeout_s,
    )
    documents.extend(release_documents)
    fetch_records.extend(release_records)

    if args.include_gdelt_headlines and not args.skip_gdelt:
        gdelt_documents, gdelt_record = _fetch_gdelt_headline_documents(
            session=session,
            as_of=as_of,
            max_records=args.max_gdelt_records,
            timeout_s=args.timeout_s,
        )
        documents.extend(gdelt_documents)
        fetch_records.append(gdelt_record)

    documents = _dedupe_documents(documents)
    documents.sort(key=lambda item: (item.created_time, item.doc_id))

    snapshot_path = output_root / "context_snapshot.json"
    _write_context_snapshot(
        documents=documents,
        fetch_records=fetch_records,
        as_of=as_of,
        start_date=windows[0].start_date if windows else args.start_date,
        windows=windows,
        output_path=snapshot_path,
    )
    manifest = _build_public_manifest(
        documents=documents,
        as_of=as_of,
        start_date=windows[0].start_date if windows else args.start_date,
        windows=windows,
    )
    _write_json(output_root / "public_demo_manifest.json", manifest)
    _write_json(output_root / "vei_project.json", _build_project_manifest(manifest))
    _write_json(
        output_root / "source_manifest.json",
        {
            "version": "1",
            "generated_at": _iso_now(),
            "as_of": as_of,
            "comparison_windows": [
                {
                    "window_id": window.window_id,
                    "label": window.label,
                    "start_date": window.start_date,
                    "end_date": window.end_date,
                    "role": window.role,
                }
                for window in windows
            ],
            "source_records": [
                {
                    "source_family": record.source_family,
                    "url": record.url,
                    "status": record.status,
                    "record_count": record.record_count,
                    "detail": record.detail,
                }
                for record in fetch_records
            ],
        },
    )
    print(
        json.dumps(
            {
                "workspace": str(output_root),
                "context_snapshot": str(snapshot_path),
                "documents": len(documents),
                "date_range": manifest["date_range"],
                "as_of": as_of,
            },
            indent=2,
        )
    )


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--workspace", type=Path, default=DEFAULT_WORKSPACE)
    parser.add_argument("--start-date", default=DEFAULT_START_DATE)
    parser.add_argument("--as-of", default="")
    parser.add_argument("--timeout-s", type=int, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--max-observations-per-series", type=int, default=24)
    parser.add_argument("--max-bls-observations-per-series", type=int, default=18)
    parser.add_argument("--max-treasury-rows-per-month", type=int, default=8)
    parser.add_argument("--max-federal-register-records", type=int, default=16)
    parser.add_argument("--max-official-releases", type=int, default=6)
    parser.add_argument("--max-gdelt-records", type=int, default=75)
    parser.add_argument(
        "--current-only",
        action="store_true",
        help="Use only the current macro window instead of the mixed analog corpus.",
    )
    parser.add_argument(
        "--include-gdelt-headlines",
        action="store_true",
        help=(
            "Also query GDELT ArticleList headline metadata. This is optional "
            "because the public endpoint can rate-limit."
        ),
    )
    parser.add_argument(
        "--skip-gdelt",
        action="store_true",
        help="Use only official/FRED/Treasury sources, with no GDELT headlines.",
    )
    return parser.parse_args()


def _macro_windows(
    *,
    as_of: str,
    current_start_date: str,
    include_analogs: bool,
) -> list[MacroWindow]:
    current = MacroWindow(
        "current_2024_now",
        "Current higher-rate macro regime",
        current_start_date,
        as_of,
        "current anchor: inflation, labor, Treasury yields, Fed communication, and policy uncertainty",
    )
    if not include_analogs:
        return [current]
    return [*ANALOG_WINDOWS, current]


def _fetch_fred_documents(
    *,
    session: requests.Session,
    as_of: str,
    windows: list[MacroWindow],
    max_observations_per_series: int,
    timeout_s: int,
) -> tuple[list[SourceDocument], list[SourceFetchRecord]]:
    documents: list[SourceDocument] = []
    records: list[SourceFetchRecord] = []
    start_date = min(window.start_date for window in windows)
    for spec in FRED_SERIES:
        params = {"id": spec.series_id, "cosd": start_date, "coed": as_of}
        url = f"{FRED_CSV_URL}?{urlencode(params)}"
        try:
            response = session.get(url, timeout=timeout_s)
            response.raise_for_status()
            observations = _parse_fred_csv(response.text, spec.series_id)
        except requests.RequestException as exc:
            records.append(
                SourceFetchRecord(
                    source_family="fred",
                    url=url,
                    status="error",
                    detail=str(exc),
                )
            )
            continue
        selected_observations = _sample_windowed_observations(
            observations=observations,
            windows=windows,
            max_observations_per_window=max_observations_per_series,
        )
        for index, (observation_date, value, window) in enumerate(selected_observations):
            previous = (
                (selected_observations[index - 1][0], selected_observations[index - 1][1])
                if index
                else None
            )
            documents.append(
                _fred_document(
                    spec=spec,
                    observation_date=observation_date,
                    value=value,
                    previous=previous,
                    source_url=f"https://fred.stlouisfed.org/series/{spec.series_id}",
                    as_of=as_of,
                    window=window,
                )
            )
        records.append(
            SourceFetchRecord(
                source_family="fred",
                url=url,
                status="ok",
                record_count=len(selected_observations),
                detail=(
                    f"{spec.series_id}; windows={len(windows)}; "
                    f"max_per_window={max_observations_per_series}"
                ),
            )
        )
    return documents, records


def _sample_windowed_observations(
    *,
    observations: list[tuple[str, str]],
    windows: list[MacroWindow],
    max_observations_per_window: int,
) -> list[tuple[str, str, MacroWindow]]:
    selected: list[tuple[str, str, MacroWindow]] = []
    seen_dates: set[str] = set()
    for window in windows:
        valid_observations = [
            item
            for item in observations
            if window.start_date <= item[0] <= window.end_date and _is_float(item[1])
        ]
        for observation_date, value in _evenly_sample(
            valid_observations,
            max_items=max_observations_per_window,
        ):
            if observation_date in seen_dates:
                continue
            seen_dates.add(observation_date)
            selected.append((observation_date, value, window))
    selected.sort(key=lambda item: item[0])
    return selected


def _evenly_sample(items: list[T], *, max_items: int) -> list[T]:
    if max_items <= 0 or len(items) <= max_items:
        return list(items)
    if max_items == 1:
        return [items[-1]]
    indexes = {
        min(len(items) - 1, round(index * (len(items) - 1) / (max_items - 1)))
        for index in range(max_items)
    }
    return [items[index] for index in sorted(indexes)]


def _parse_fred_csv(text: str, series_id: str) -> list[tuple[str, str]]:
    reader = csv.DictReader(StringIO(text))
    rows = []
    for row in reader:
        rows.append(
            (str(row.get("observation_date") or ""), str(row.get(series_id) or ""))
        )
    return rows


def _fred_document(
    *,
    spec: FredSeriesSpec,
    observation_date: str,
    value: str,
    previous: tuple[str, str] | None,
    source_url: str,
    as_of: str,
    window: MacroWindow,
) -> SourceDocument:
    previous_text = "No prior kept observation is included in this fixture."
    if previous is not None and _is_float(previous[1]):
        delta = float(value) - float(previous[1])
        previous_text = (
            f"Previous kept observation on {previous[0]} was {previous[1]}; "
            f"change is {delta:+.3f}."
        )
    title = f"{spec.title}: {value} on {observation_date}"
    body = (
        f"Topic: {_topic_label(spec.topic)}. Date: {observation_date}. "
        f"Comparison window: {window.label} ({window.role}). "
        f"Source: FRED series {spec.series_id}. Public macro observation as of "
        f"{as_of}: {spec.title} was {value} {spec.units}. {previous_text} "
        f"Source URL: {source_url}."
    )
    return SourceDocument(
        doc_id=_doc_id("fred", spec.series_id, observation_date),
        title=title,
        body=body,
        created_time=_date_to_timestamp(observation_date),
        owner="fred@stlouisfed.org",
        url=source_url,
        source_family="fred",
        topic=spec.topic,
        window_id=window.window_id,
        window_label=window.label,
        window_role=window.role,
    )


def _fetch_treasury_yield_documents(
    *,
    session: requests.Session,
    as_of: str,
    windows: list[MacroWindow],
    max_rows_per_month: int,
    timeout_s: int,
) -> tuple[list[SourceDocument], list[SourceFetchRecord]]:
    documents: list[SourceDocument] = []
    records: list[SourceFetchRecord] = []
    for window in windows:
        for month in _representative_months(window):
            params = {
                "data": "daily_treasury_yield_curve",
                "field_tdr_date_value_month": month.replace("-", ""),
            }
            url = f"{TREASURY_YIELD_XML_URL}?{urlencode(params)}"
            try:
                response = session.get(url, timeout=timeout_s)
                response.raise_for_status()
                month_documents = _parse_treasury_yield_xml(
                    response.text,
                    source_url=url,
                    window=window,
                )
                month_documents = [
                    document
                    for document in month_documents
                    if window.start_date <= document.created_time[:10] <= window.end_date
                    and document.created_time[:10] <= as_of
                ]
                month_documents = _evenly_sample(
                    month_documents,
                    max_items=max_rows_per_month,
                )
                documents.extend(month_documents)
                records.append(
                    SourceFetchRecord(
                        source_family="treasury_yield_curve",
                        url=url,
                        status="ok",
                        record_count=len(month_documents),
                        detail=f"window={window.window_id}; month={month}",
                    )
                )
            except (requests.RequestException, ET.ParseError) as exc:
                records.append(
                    SourceFetchRecord(
                        source_family="treasury_yield_curve",
                        url=url,
                        status="error",
                        detail=f"window={window.window_id}; {exc}",
                    )
                )
    return documents, records


def _representative_months(window: MacroWindow) -> list[str]:
    start_month = window.start_date[:7]
    end_month = window.end_date[:7]
    months = [start_month, end_month]
    start = datetime.strptime(window.start_date[:10], "%Y-%m-%d").date()
    end = datetime.strptime(window.end_date[:10], "%Y-%m-%d").date()
    midpoint = start.toordinal() + math.floor((end.toordinal() - start.toordinal()) / 2)
    midpoint_month = date.fromordinal(midpoint).isoformat()[:7]
    months.append(midpoint_month)
    return sorted(set(months))


def _parse_treasury_yield_xml(
    text: str,
    *,
    source_url: str,
    window: MacroWindow,
) -> list[SourceDocument]:
    namespaces = {
        "atom": "http://www.w3.org/2005/Atom",
        "m": "http://schemas.microsoft.com/ado/2007/08/dataservices/metadata",
        "d": "http://schemas.microsoft.com/ado/2007/08/dataservices",
    }
    root = ET.fromstring(text)
    documents: list[SourceDocument] = []
    for entry in root.findall("atom:entry", namespaces):
        properties = entry.find("atom:content/m:properties", namespaces)
        if properties is None:
            continue
        values = {
            child.tag.rsplit("}", 1)[-1]: (child.text or "")
            for child in list(properties)
        }
        raw_date = str(values.get("NEW_DATE") or "")[:10]
        if not raw_date:
            continue
        ten_year = values.get("BC_10YEAR", "")
        two_year = values.get("BC_2YEAR", "")
        thirty_year = values.get("BC_30YEAR", "")
        one_month = values.get("BC_1MONTH", "")
        title = f"Treasury yield curve: 10Y {ten_year}% on {raw_date}"
        body = (
            f"Topic: Banking Markets. Date: {raw_date}. Comparison window: "
            f"{window.label} ({window.role}). Source: U.S. Treasury "
            f"daily yield curve. Public market observation: 1M {one_month}%, "
            f"2Y {two_year}%, 10Y {ten_year}%, 30Y {thirty_year}%. "
            f"Source URL: {source_url}."
        )
        documents.append(
            SourceDocument(
                doc_id=_doc_id("treasury-yield", "curve", raw_date),
                title=title,
                body=body,
                created_time=_date_to_timestamp(raw_date),
                owner="treasury@public.gov",
                url=source_url,
                source_family="treasury_yield_curve",
                topic="banking_markets",
                window_id=window.window_id,
                window_label=window.label,
                window_role=window.role,
            )
        )
    return documents


def _fetch_bls_documents(
    *,
    session: requests.Session,
    windows: list[MacroWindow],
    max_observations_per_series: int,
    timeout_s: int,
) -> tuple[list[SourceDocument], list[SourceFetchRecord]]:
    documents: list[SourceDocument] = []
    records: list[SourceFetchRecord] = []
    specs_by_id = {spec.series_id: spec for spec in BLS_SERIES}
    for window in windows:
        payload = {
            "seriesid": [spec.series_id for spec in BLS_SERIES],
            "startyear": window.start_date[:4],
            "endyear": window.end_date[:4],
        }
        try:
            response = session.post(
                BLS_PUBLIC_API_URL,
                json=payload,
                timeout=timeout_s,
                headers={"Content-Type": "application/json"},
            )
            response.raise_for_status()
            payload_json = response.json()
        except (requests.RequestException, json.JSONDecodeError) as exc:
            records.append(
                SourceFetchRecord(
                    source_family="bls_public_api",
                    url=BLS_PUBLIC_API_URL,
                    status="error",
                    detail=f"window={window.window_id}; {exc}",
                )
            )
            continue
        count = 0
        for series in payload_json.get("Results", {}).get("series", []):
            if not isinstance(series, dict):
                continue
            spec = specs_by_id.get(str(series.get("seriesID") or ""))
            if spec is None:
                continue
            observations = []
            for row in series.get("data") or []:
                if not isinstance(row, dict):
                    continue
                observation_date = _bls_period_date(row)
                value = str(row.get("value") or "")
                if (
                    observation_date
                    and window.start_date <= observation_date <= window.end_date
                    and _is_float(value)
                ):
                    observations.append((observation_date, value))
            observations.sort(key=lambda item: item[0])
            sampled_observations = _evenly_sample(
                observations,
                max_items=max_observations_per_series,
            )
            for index, (observation_date, value) in enumerate(sampled_observations):
                previous = sampled_observations[index - 1] if index else None
                documents.append(
                    _bls_document(
                        spec=spec,
                        observation_date=observation_date,
                        value=value,
                        previous=previous,
                        window=window,
                    )
                )
                count += 1
        records.append(
            SourceFetchRecord(
                source_family="bls_public_api",
                url=BLS_PUBLIC_API_URL,
                status=str(payload_json.get("status") or "ok").lower(),
                record_count=count,
                detail=f"window={window.window_id}; series={len(BLS_SERIES)}",
            )
        )
    return documents, records


def _bls_period_date(row: dict[str, Any]) -> str:
    year = str(row.get("year") or "")
    period = str(row.get("period") or "")
    if not re.match(r"^\d{4}$", year) or not re.match(r"^M\d{2}$", period):
        return ""
    month = period[1:]
    if not 1 <= int(month) <= 12:
        return ""
    return f"{year}-{month}-01"


def _bls_document(
    *,
    spec: BlsSeriesSpec,
    observation_date: str,
    value: str,
    previous: tuple[str, str] | None,
    window: MacroWindow,
) -> SourceDocument:
    previous_text = "No prior kept BLS observation is included in this fixture."
    if previous is not None and _is_float(previous[1]):
        delta = float(value) - float(previous[1])
        previous_text = (
            f"Previous kept observation on {previous[0]} was {previous[1]}; "
            f"change is {delta:+.3f}."
        )
    title = f"BLS {spec.title}: {value} on {observation_date}"
    body = (
        f"Topic: {_topic_label(spec.topic)}. Date: {observation_date}. "
        f"Comparison window: {window.label} ({window.role}). Source: BLS Public "
        f"Data API series {spec.series_id}. {spec.title} was {value} {spec.units}. "
        f"{previous_text} Source URL: {BLS_PUBLIC_API_URL}."
    )
    return SourceDocument(
        doc_id=_doc_id("bls", spec.series_id, observation_date),
        title=title,
        body=body,
        created_time=_date_to_timestamp(observation_date),
        owner="bls@public.gov",
        url=BLS_PUBLIC_API_URL,
        source_family="bls_public_api",
        topic=spec.topic,
        window_id=window.window_id,
        window_label=window.label,
        window_role=window.role,
    )


def _fetch_federal_register_documents(
    *,
    session: requests.Session,
    windows: list[MacroWindow],
    max_records_per_window: int,
    timeout_s: int,
) -> tuple[list[SourceDocument], list[SourceFetchRecord]]:
    documents: list[SourceDocument] = []
    records: list[SourceFetchRecord] = []
    for window in windows:
        params = {
            "conditions[publication_date][gte]": window.start_date,
            "conditions[publication_date][lte]": window.end_date,
            "conditions[term]": FEDERAL_REGISTER_MACRO_QUERY,
            "order": "newest",
            "per_page": str(max_records_per_window),
        }
        url = f"{FEDERAL_REGISTER_API_URL}?{urlencode(params)}"
        try:
            response = session.get(url, timeout=timeout_s)
            response.raise_for_status()
            payload = response.json()
        except (requests.RequestException, json.JSONDecodeError) as exc:
            records.append(
                SourceFetchRecord(
                    source_family="federal_register",
                    url=url,
                    status="error",
                    detail=f"window={window.window_id}; {exc}",
                )
            )
            continue
        count = 0
        for item in payload.get("results") or []:
            if not isinstance(item, dict):
                continue
            document = _federal_register_document(item=item, window=window)
            if document is None:
                continue
            documents.append(document)
            count += 1
        records.append(
            SourceFetchRecord(
                source_family="federal_register",
                url=url,
                status="ok",
                record_count=count,
                detail=f"window={window.window_id}; count={payload.get('count', 0)}",
            )
        )
    return documents, records


def _federal_register_document(
    *,
    item: dict[str, Any],
    window: MacroWindow,
) -> SourceDocument | None:
    publication_date = str(item.get("publication_date") or "")[:10]
    title = _clean_text(str(item.get("title") or ""), max_chars=220)
    if not publication_date or not title:
        return None
    abstract = _clean_text(str(item.get("abstract") or ""), max_chars=900)
    document_type = _clean_text(str(item.get("type") or ""), max_chars=80)
    html_url = _clean_text(str(item.get("html_url") or ""), max_chars=500)
    agency_names = ", ".join(
        _clean_text(str(agency.get("name") or ""), max_chars=100)
        for agency in item.get("agencies") or []
        if isinstance(agency, dict) and agency.get("name")
    )
    topic = _topic_for_text(f"{title} {abstract} {agency_names}")
    body = (
        f"Topic: {_topic_label(topic)}. Date: {publication_date}. Comparison "
        f"window: {window.label} ({window.role}). Source: Federal Register "
        f"{document_type or 'document'} from {agency_names or 'listed agency'}. "
        f"{abstract or title} Source URL: {html_url or FEDERAL_REGISTER_API_URL}."
    )
    return SourceDocument(
        doc_id=_doc_id("federal-register", title, publication_date),
        title=title,
        body=body,
        created_time=_date_to_timestamp(publication_date),
        owner="federalregister@public.gov",
        url=html_url or FEDERAL_REGISTER_API_URL,
        source_family="federal_register",
        topic=topic,
        window_id=window.window_id,
        window_label=window.label,
        window_role=window.role,
    )


def _fetch_official_release_documents(
    *,
    session: requests.Session,
    as_of: str,
    max_releases: int,
    timeout_s: int,
) -> tuple[list[SourceDocument], list[SourceFetchRecord]]:
    documents: list[SourceDocument] = []
    records: list[SourceFetchRecord] = []
    bea_docs, bea_record = _fetch_bea_current_releases(
        session=session,
        as_of=as_of,
        max_releases=max_releases,
        timeout_s=timeout_s,
    )
    documents.extend(bea_docs)
    records.append(bea_record)
    fed_doc, fed_record = _fetch_latest_fomc_statement(
        session=session,
        as_of=as_of,
        timeout_s=timeout_s,
    )
    if fed_doc is not None:
        documents.append(fed_doc)
    records.append(fed_record)
    return documents, records


def _fetch_bea_current_releases(
    *,
    session: requests.Session,
    as_of: str,
    max_releases: int,
    timeout_s: int,
) -> tuple[list[SourceDocument], SourceFetchRecord]:
    try:
        response = session.get(BEA_CURRENT_RELEASES_URL, timeout=timeout_s)
        response.raise_for_status()
    except requests.RequestException as exc:
        return [], SourceFetchRecord(
            source_family="bea_current_releases",
            url=BEA_CURRENT_RELEASES_URL,
            status="error",
            detail=str(exc),
        )
    release_refs = _extract_bea_release_refs(
        _decode_response_text(response),
        max_releases=max_releases,
    )
    documents: list[SourceDocument] = []
    for href, title, release_date in release_refs:
        if release_date > as_of:
            continue
        url = urljoin(BEA_CURRENT_RELEASES_URL, href)
        try:
            detail_response = session.get(url, timeout=timeout_s)
            detail_response.raise_for_status()
        except requests.RequestException:
            excerpt = title
        else:
            excerpt = _article_excerpt(
                _decode_response_text(detail_response), title=title
            )
        topic = _topic_for_text(f"{title} {excerpt}")
        body = (
            f"Topic: {_topic_label(topic)}. Date: {release_date}. Source: U.S. "
            f"Bureau of Economic Analysis news release. {excerpt} Source URL: {url}."
        )
        documents.append(
            SourceDocument(
                doc_id=_doc_id("bea", title, release_date),
                title=title,
                body=body,
                created_time=_date_to_timestamp(release_date),
                owner="bea@public.gov",
                url=url,
                source_family="bea_current_releases",
                topic=topic,
            )
        )
    return documents, SourceFetchRecord(
        source_family="bea_current_releases",
        url=BEA_CURRENT_RELEASES_URL,
        status="ok",
        record_count=len(documents),
        detail=f"max_releases={max_releases}",
    )


def _extract_bea_release_refs(
    html_text: str,
    *,
    max_releases: int,
) -> list[tuple[str, str, str]]:
    matches = re.findall(
        r'<a href="(/news/[^"]+)"[^>]*>(.*?)</a>.*?' r'<time datetime="([^"]+)"',
        html_text,
        flags=re.DOTALL,
    )
    refs = []
    for href, raw_title, raw_datetime in matches:
        title = _clean_text(_strip_tags(raw_title), max_chars=160)
        release_date = raw_datetime[:10]
        if title and release_date:
            refs.append((href, title, release_date))
        if len(refs) >= max_releases:
            break
    return refs


def _fetch_latest_fomc_statement(
    *,
    session: requests.Session,
    as_of: str,
    timeout_s: int,
) -> tuple[SourceDocument | None, SourceFetchRecord]:
    try:
        response = session.get(FED_MONETARY_POLICY_URL, timeout=timeout_s)
        response.raise_for_status()
        page_text = _decode_response_text(response)
        href_match = re.search(
            r"FOMC Statement:.*?href=\"([^\"]+)\"[^>]*>HTML",
            page_text,
            flags=re.DOTALL,
        )
        if href_match is None:
            raise ValueError("latest FOMC statement HTML link not found")
        url = urljoin(FED_MONETARY_POLICY_URL, href_match.group(1))
        detail_response = session.get(url, timeout=timeout_s)
        detail_response.raise_for_status()
    except (requests.RequestException, ValueError) as exc:
        return None, SourceFetchRecord(
            source_family="federal_reserve_fomc",
            url=FED_MONETARY_POLICY_URL,
            status="error",
            detail=str(exc),
        )
    detail_text = _decode_response_text(detail_response)
    release_date = _extract_page_date(detail_text) or as_of
    title = "Federal Reserve issues FOMC statement"
    excerpt = _article_excerpt(detail_text, title=title)
    body = (
        f"Topic: Government Policy. Date: {release_date}. Source: Federal Reserve "
        f"FOMC statement. {excerpt} Source URL: {url}."
    )
    document = SourceDocument(
        doc_id=_doc_id("fomc", "statement", release_date),
        title=title,
        body=body,
        created_time=_date_to_timestamp(release_date),
        owner="federalreserve@public.gov",
        url=url,
        source_family="federal_reserve_fomc",
        topic="government_policy",
    )
    return document, SourceFetchRecord(
        source_family="federal_reserve_fomc",
        url=url,
        status="ok",
        record_count=1,
        detail=f"as_of={as_of}",
    )


def _fetch_gdelt_headline_documents(
    *,
    session: requests.Session,
    as_of: str,
    max_records: int,
    timeout_s: int,
) -> tuple[list[SourceDocument], SourceFetchRecord]:
    params = {
        "query": GDELT_MACRO_QUERY,
        "mode": "ArtList",
        "format": "json",
        "sort": "HybridRel",
        "maxrecords": str(max_records),
        "timespan": "3months",
    }
    url = f"{GDELT_DOC_API_URL}?{urlencode(params)}"
    try:
        response = session.get(url, timeout=timeout_s)
        response.raise_for_status()
        payload = response.json()
    except (requests.RequestException, json.JSONDecodeError) as exc:
        return [], SourceFetchRecord(
            source_family="gdelt_headlines",
            url=url,
            status="error",
            detail=str(exc),
        )
    documents: list[SourceDocument] = []
    for article in payload.get("articles") or []:
        if not isinstance(article, dict):
            continue
        raw_seen = str(article.get("seendate") or "")
        seen_date = _gdelt_date(raw_seen)
        if not seen_date or seen_date > as_of:
            continue
        title = _clean_text(str(article.get("title") or ""), max_chars=180)
        article_url = _clean_text(str(article.get("url") or ""), max_chars=500)
        domain = _clean_text(str(article.get("domain") or ""), max_chars=120)
        if not title or not article_url:
            continue
        topic = _topic_for_text(title)
        body = (
            f"Topic: {_topic_label(topic)}. Date: {seen_date}. Source: GDELT "
            f"ArticleList headline metadata from {domain or 'unknown domain'}. "
            f"Headline only, no article body included: {title}. "
            f"Source URL: {article_url}."
        )
        documents.append(
            SourceDocument(
                doc_id=_doc_id("gdelt", title, raw_seen or seen_date),
                title=title,
                body=body,
                created_time=_date_to_timestamp(seen_date),
                owner=f"{domain or 'gdelt'}@public-news.local",
                url=article_url,
                source_family="gdelt_headlines",
                topic=topic,
            )
        )
    return documents, SourceFetchRecord(
        source_family="gdelt_headlines",
        url=url,
        status="ok",
        record_count=len(documents),
        detail="headline metadata only",
    )


def _write_context_snapshot(
    *,
    documents: list[SourceDocument],
    fetch_records: list[SourceFetchRecord],
    as_of: str,
    start_date: str,
    windows: list[MacroWindow],
    output_path: Path,
) -> None:
    captured_at = _iso_now()
    source_families = {document.source_family for document in documents}
    mail_source_families = {"bea_current_releases", "federal_reserve_fomc"}
    docs_documents = [
        document
        for document in documents
        if document.source_family not in mail_source_families
    ]
    mail_documents = [
        document
        for document in documents
        if document.source_family in mail_source_families
    ]
    if documents and not mail_documents:
        mail_documents = documents[-min(len(documents), 8) :]
        docs_documents = [
            document for document in documents if document not in mail_documents
        ]
    notes = [
        "FRED, BLS, and Treasury rows are structured public observations.",
        "Federal Register rows add public regulatory and agency-action texture.",
        "BEA and Federal Reserve rows are official public release excerpts.",
        "The fixture mixes current macro records with similar and contrasting historical windows for better counterfactual variety.",
    ]
    if "gdelt_headlines" in source_families:
        notes.append(
            "GDELT rows use headline metadata only; no third-party article bodies are copied."
        )
    docs_payload = [
        {
            "doc_id": document.doc_id,
            "title": document.title,
            "body": document.body,
            "created_time": document.created_time,
            "modified_time": document.created_time,
            "owner": document.owner,
            "url": document.url,
            "metadata": {
                "source_family": document.source_family,
                "window_id": document.window_id,
                "window_label": document.window_label,
                "window_role": document.window_role,
            },
        }
        for document in docs_documents
    ]
    mail_payload = _mail_payload(mail_documents)
    snapshot = ContextSnapshot(
        organization_name="Current U.S. Macro Public History",
        organization_domain="current-macro.public",
        captured_at=captured_at,
        sources=[
            ContextSourceResult(
                provider="google",
                captured_at=captured_at,
                status="ok",
                record_counts={"documents": len(docs_payload)},
                data={
                    "documents": docs_payload,
                    "users": [],
                    "drive_shares": [],
                },
            ),
            ContextSourceResult(
                provider="mail_archive",
                captured_at=captured_at,
                status="ok",
                record_counts={
                    "threads": len(mail_payload["threads"]),
                    "messages": sum(
                        len(thread.get("messages") or [])
                        for thread in mail_payload["threads"]
                    ),
                },
                data=mail_payload,
            ),
        ],
        metadata={
            "snapshot_role": "public_history_demo",
            "dataset": "mixed_public_macro_sources",
            "source_kind": "mixed_official_macro_data_and_public_releases",
            "as_of_date": as_of,
            "start_date": start_date,
            "end_date": documents[-1].created_time[:10] if documents else as_of,
            "selected_event_count": len(documents),
            "selected_source_count": len({document.owner for document in documents}),
            "selected_topic_count": len({document.topic for document in documents}),
            "comparison_windows": [
                {
                    "window_id": window.window_id,
                    "label": window.label,
                    "start_date": window.start_date,
                    "end_date": window.end_date,
                    "role": window.role,
                }
                for window in windows
            ],
            "source_families": sorted(source_families),
            "notes": notes,
            "source_fetch_records": [
                {
                    "source_family": record.source_family,
                    "url": record.url,
                    "status": record.status,
                    "record_count": record.record_count,
                    "detail": record.detail,
                }
                for record in fetch_records
            ],
        },
    )
    output_path.write_text(snapshot.model_dump_json(indent=2), encoding="utf-8")
    write_canonical_history_sidecars(snapshot, output_path)


def _mail_payload(documents: list[SourceDocument]) -> dict[str, Any]:
    by_topic: dict[str, list[SourceDocument]] = {}
    for document in documents:
        by_topic.setdefault(document.topic, []).append(document)
    threads: list[dict[str, Any]] = []
    for topic, topic_documents in sorted(by_topic.items()):
        messages = []
        for document in sorted(
            topic_documents,
            key=lambda item: (item.created_time, item.doc_id),
        ):
            messages.append(
                {
                    "message_id": f"{document.doc_id}:mail",
                    "timestamp": document.created_time,
                    "from": document.owner,
                    "to": ["public-macro-operator@current-macro.public"],
                    "subject": document.title,
                    "body_text": document.body,
                    "thread_id": f"current_macro:{topic}",
                    "metadata": {
                        "source_family": document.source_family,
                        "url": document.url,
                        "window_id": document.window_id,
                        "window_label": document.window_label,
                        "window_role": document.window_role,
                    },
                }
            )
        if messages:
            threads.append(
                {
                    "thread_id": f"current_macro:{topic}",
                    "subject": _topic_label(topic),
                    "messages": messages,
                }
            )
    return {"threads": threads, "actors": [], "profile": {}}


def _build_public_manifest(
    *,
    documents: list[SourceDocument],
    as_of: str,
    start_date: str,
    windows: list[MacroWindow],
) -> dict[str, Any]:
    first_date = documents[0].created_time[:10] if documents else start_date
    last_date = documents[-1].created_time[:10] if documents else as_of
    record_count = len(documents)
    return {
        "source_id": SOURCE_ID,
        "title": "Public History: Current + Analog U.S. Macro World",
        "summary": (
            f"Choose a point from a mixed {record_count:,}-record U.S. macro "
            f"public record, {first_date} through {last_date}, spanning current, "
            "similar, and contrasting macro windows; then test a branch event or "
            "policy response from that evidence state."
        ),
        "record_count": record_count,
        "date_range": {"start": first_date, "end": last_date},
        "as_of_date": as_of,
        "comparison_windows": [
            {
                "window_id": window.window_id,
                "label": window.label,
                "start_date": window.start_date,
                "end_date": window.end_date,
                "role": window.role,
            }
            for window in windows
        ],
        "max_static_dates": 420,
        "launch_command": LAUNCH_COMMAND,
        "refresh_path": {
            "workspace_fixture": (
                "python scripts/build_current_macro_public_history_fixture.py "
                "--workspace docs/examples/current-public-history-macro/workspace"
            ),
            "state_point_run": (
                "vei whatif benchmark news-state-point "
                "--input current_macro=docs/examples/current-public-history-macro/workspace/context_snapshot.json "
                "--checkpoint docs/examples/news-public-history-demo/workspace/jepa_model.pt "
                "--topic all_public_record --as-of {as_of}"
            ),
            "static_assets": (
                "python scripts/export_public_history_static_assets.py "
                "--workspace docs/examples/current-public-history-macro/workspace "
                "--output /path/to/strangelab.ai/public/public-history-current"
            ),
        },
        "source_path": "context_snapshot.json",
        "source_manifest_path": "source_manifest.json",
        "saved_result_path": "current_macro_state_point_result.json",
        "jepa_checkpoint_path": JEPA_CHECKPOINT_PATH,
        "default_topic": "all_public_record",
        "default_as_of": as_of,
    }


def _build_project_manifest(public_manifest: dict[str, Any]) -> dict[str, Any]:
    record_count = int(public_manifest["record_count"])
    date_range = public_manifest["date_range"]
    return {
        "version": "1",
        "name": "current_public_history_macro",
        "title": "Current + Analog Macro Public History",
        "description": (
            f"Bounded {record_count:,}-record mixed U.S. macro public-history "
            f"workspace from {date_range['start']} through {date_range['end']}."
        ),
        "created_at": _iso_now(),
        "source_kind": "example",
        "source_ref": "current-public-history-macro",
        "source_registry_path": None,
        "source_sync_history_path": None,
        "active_scenario": "default",
        "scenarios": [],
        "metadata": {
            "ui_mode": "public_history",
            "public_demo": {
                key: public_manifest[key]
                for key in (
                    "source_id",
                    "record_count",
                    "date_range",
                    "as_of_date",
                    "launch_command",
                    "refresh_path",
                )
            },
        },
    }


def _dedupe_documents(documents: Iterable[SourceDocument]) -> list[SourceDocument]:
    seen: set[str] = set()
    deduped: list[SourceDocument] = []
    for document in documents:
        key = document.doc_id
        if key in seen:
            continue
        seen.add(key)
        deduped.append(document)
    return deduped


def _article_excerpt(html_text: str, *, title: str, max_paragraphs: int = 4) -> str:
    parser = _ParagraphParser()
    parser.feed(html_text)
    paragraphs = [
        paragraph
        for paragraph in parser.paragraphs
        if len(paragraph) >= 40
        and title.lower() not in paragraph.lower()
        and not _looks_like_page_chrome(paragraph)
    ]
    if not paragraphs:
        text = _clean_text(_strip_tags(html_text), max_chars=1600)
        return text
    return _clean_text(" ".join(paragraphs[:max_paragraphs]), max_chars=1600)


def _looks_like_page_chrome(value: str) -> bool:
    lowered = value.lower()
    return any(
        token in lowered
        for token in (
            "an official website",
            "official websites use .gov",
            "secure .gov websites use https",
            "skip to main content",
            "share sensitive information only on official",
            "accessibility statement",
            "privacy policy",
            "board of governors of the federal reserve system",
            "the federal reserve, the central bank of the united states",
            "review of monetary policy strategy",
            "mergers, acquisitions, and other applications",
            "financial stability coordination",
            "financial market utilities",
        )
    )


def _extract_page_date(html_text: str) -> str:
    time_match = re.search(r"<time[^>]+datetime=\"([^\"]+)\"", html_text)
    if time_match:
        return time_match.group(1)[:10]
    text = _strip_tags(html_text)
    month_match = re.search(
        r"\b(January|February|March|April|May|June|July|August|September|"
        r"October|November|December)\s+\d{1,2},\s+20\d{2}\b",
        text,
    )
    if not month_match:
        return ""
    parsed = datetime.strptime(month_match.group(0), "%B %d, %Y")
    return parsed.date().isoformat()


def _topic_for_text(text: str) -> str:
    lowered = text.lower()
    if any(
        token in lowered
        for token in (
            "job",
            "jobs",
            "employment",
            "unemployment",
            "payroll",
            "wage",
            "labor",
        )
    ):
        return "labor_work"
    if any(
        token in lowered
        for token in (
            "fed",
            "fomc",
            "federal reserve",
            "rate",
            "policy",
            "congress",
            "treasury",
            "gdp",
            "government",
        )
    ):
        return "government_policy"
    return "banking_markets"


def _topic_label(topic: str) -> str:
    return topic.replace("_", " ").title()


def _gdelt_date(value: str) -> str:
    match = re.match(r"(\d{4})(\d{2})(\d{2})T", value)
    if match:
        return f"{match.group(1)}-{match.group(2)}-{match.group(3)}"
    return ""


def _strip_tags(value: str) -> str:
    value = re.sub(r"(?is)<script.*?</script>", " ", value)
    value = re.sub(r"(?is)<style.*?</style>", " ", value)
    return re.sub(r"(?s)<[^>]+>", " ", html.unescape(value))


def _decode_response_text(response: requests.Response) -> str:
    return response.content.decode("utf-8-sig", errors="replace")


def _clean_text(value: str, *, max_chars: int) -> str:
    text = html.unescape(str(value or ""))
    text = unicodedata.normalize("NFKD", text).encode("ascii", "ignore").decode()
    text = re.sub(r"\s+", " ", text).strip()
    text = re.sub(r"[^A-Za-z0-9.,;:!?()'\"%/$&@#_+\\[\\] -]+", "", text)
    if len(text) <= max_chars:
        return text
    return text[: max_chars - 1].rsplit(" ", 1)[0].rstrip() + "."


def _doc_id(*parts: str) -> str:
    raw = "-".join(parts)
    slug = re.sub(r"[^a-z0-9]+", "-", raw.lower()).strip("-")
    return f"MACRO-{slug[:140]}"


def _date_to_timestamp(value: str) -> str:
    return f"{value[:10]}T00:00:00Z"


def _iso_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def _today_los_angeles() -> date:
    return datetime.now(ZoneInfo("America/Los_Angeles")).date()


def _is_float(value: str) -> bool:
    try:
        float(value)
    except ValueError:
        return False
    return True


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


if __name__ == "__main__":
    main()
