"""
Parse new format iShares N-CSR filing
"""
from bs4 import BeautifulSoup
from decimal import Decimal
from typing import Any, Dict, List
import bs4
import pandas as pd
import re


def scrub_text(val: str) -> str:
    val = re.sub(u"\xa0", " ", val)
    val = re.sub(r"\s+", " ", val)
    val = re.sub(r"\x92", "\'", val)
    return val


def scrub_tag(val: bs4.element.Tag) -> str:
    return scrub_text(val.text.strip()).strip()


def empty_tag(val: bs4.element.Tag) -> bool:
    return scrub_tag(val) == ""


def nonempty_td(val: bs4.element.Tag) -> List[bs4.element.Tag]:
    contents = []
    for td in val.find_all("td"):
        if not empty_tag(td):
            contents.append(td)
    return contents


def nonempty_p(val: bs4.element.Tag) -> List[bs4.element.Tag]:
    contents = []
    for p in val.find_all("p"):
        if not empty_tag(p):
            contents.append(p)
    return contents


def nonempty_td(val: bs4.element.Tag) -> List[bs4.element.Tag]:
    contents = []
    for p in val.find_all("td"):
        if not empty_tag(p):
            contents.append(p)
    return contents


def parse_row(row: bs4.element.Tag) -> List[bs4.element.Tag]:
    scrubbed = []
    for td in row:
        if scrub_tag(td) not in ["", "$"]:
            scrubbed.append(scrub_text(td.text))
    return scrubbed


def is_int(val: str) -> bool:
    scrubbed = scrub_text(val)
    scrubbed = scrub_text(val.replace(",", ""))
    return re.match("[0-9]+", scrubbed, re.DOTALL | re.IGNORECASE) is not None


def parse_int(val: str) -> int:
    val = val.strip().replace(",", "")
    val = re.sub("\s+", "", val)
    if val in ['\x96', '\x97', '(e)', '(f)']:
        return None
    return int(val)


def normalize_sector(sector: str) -> str:
    sector = re.sub("\(continued\)", "", sector).strip()
    if "---" in sector:
        sector = "---".join(sector.split("---")[:-1]).strip()
    return sector.strip()


def get_page_separators(buf: bytes) -> List[str]:
    """
    Enumerate the types of page separators that exist in the filing

    Parameters
    ----------
    buf: bytes
        The raw filing

    Returns
    -------
    List[str]
        Page separator strings
    """
    soup = BeautifulSoup(buf, "lxml")
    pb = re.compile("page\-break\-before\:always", re.IGNORECASE | re.DOTALL)
    page_separators = set()
    for p in soup.find_all("p"):
        c = str(p)
        if pb.search(c) is not None:
            page_separators = page_separators.union(set([c.replace("</p>", "").strip().encode()]))

    additional_page_separators = set()
    for sep in page_separators:
        new_sep1 = re.sub(b"\"", b"\'", sep)
        new_sep2  = re.sub(b"\'", b"\"", sep)
        additional_page_separators = additional_page_separators.union(set([new_sep1, new_sep2]))

    seps = page_separators.union(additional_page_separators)
    return list(seps)


def extract_pages(buf: bytes) -> list[BeautifulSoup]:
    """
    Get Russell 3000 Schedule of Investments from complete filing

    Parameters
    ----------
    buf: bytes
        The raw filing

    Returns
    -------
    List[BeautifulSoup]
        Individual pages of the Russell 3000 schedule of investments
    """
    # find page breaks
    matches = []
    matchers = [re.compile(sep, re.IGNORECASE) for sep in get_page_separators(buf)]
    for matcher in matchers:
        for match in matcher.finditer(buf):
            matches.append(match)
    matches = sorted(matches, key=lambda x: x.start())

    # segment pages
    pages = []
    for i in range(len(matches) - 1):
        start = matches[i].start()
        end = matches[i+1].start()
        pages.append(buf[start:end])

    # find non-summary schedules of investments for russell 3000 etf
    soi = re.compile(r".*schedule.*of.*investments.*russell.*3000", re.DOTALL|re.IGNORECASE)
    smr = re.compile(".*summary.*schedule.*of.*investments.*", re.DOTALL|re.IGNORECASE)
    nts = re.compile(".*notes\s+to\s+financial\s+statements.*", re.DOTALL|re.IGNORECASE)
    stn = re.compile(".*see.{0,7}notes\s+to\s+financial\s+statements.*", re.DOTALL|re.IGNORECASE)
    soups = []
    for p in pages:
        soup = BeautifulSoup(p, "lxml")
        text = soup.text

        # scedules only
        match = soi.match(text)
        if match is None:
            continue

        # no summary
        match = smr.match(text)
        if match is not None:
            continue

        # no notes
        match = nts.match(text)
        if match is not None and stn.match(text) is None:
            continue

        soups.append(soup)

    return soups


def get_subversion(page: bs4.element.Tag) -> int:
    """
    Determine whether it's the old or new version of the new filing format

    Parameters
    ----------
    page: bs4.element.Tag
        The first page in a sequence of holdings pages

    Returns
    -------
    int
        The sub-version of the new format filing
    """
    tables = page.find_all("table")
    soi = re.compile(r".*schedule.*of.*investments.*russell.*3000", re.DOTALL|re.IGNORECASE)
    if len(tables) == 3 and soi.search(tables[0].prettify().strip()) is None:
        return 1
    elif len(tables) == 3:
        return 2
    elif len(tables) == 4:  # new foramt head is table
        return 2
    raise ValueError(f"Unexpected number of tables in r3k holdings first page {len(tables)}")


def extract_holdings_columns(page: bs4.element.Tag, version: int) -> List[bs4.element.Tag]:
    """
    Extract the holdings columns from the raw page

    Parameters
    ----------
    page: bs4.element.Tag
        A page of the Russell 3000 holdings
    version: int
        The filing sub-version from get_subversion

    Returns
    -------
    List[bs4.element.Tag]
        The columns of the holdings table
    """
    tables = page.find_all("table")
    if version == 1:  # old format header is not table
        return tables[:2]
    elif version == 2:  # new format header is table
        return tables[1:3]
    raise ValueError(f"Unknown holdings version {version}")


def parse_header_info(page: bs4.element.Tag, version: int) -> Dict[str, Any]:
    """
    Structure the header of a holdings page in new format filings (both versions)

    Parameters
    ----------
    page: bs4.element.Tag
        A page of the Russell 3000 holdings
    version: int
        The filing sub-version from get_subversion

    Returns
    -------
    Dict[str, Any]
        The fund name and filing date
    """
    tables = page.find_all("table")

    if version == 1:
        tags = []
        for tag in page.find_all():
            if tag.name == "table":
                break
            elif tag.name == "p" and not empty_tag(tag):
                tags.append(tag)
        title = scrub_tag(tags[0])
        etf_name = scrub_tag(tags[1])
        report_date = scrub_tag(tags[2])
    elif version == 2:  # new format header is table
        header = tables[0]
        values = nonempty_p(header)
        if len(values) >= 3:  # through 2018-03-30
            title = scrub_tag(values[0])
            report_date = scrub_tag(values[1])
            etf_name = scrub_tag(values[2])
        else:  # after 2018-03-30
            values = nonempty_td(header)
            title = scrub_tag(values[0])
            etf_name = scrub_tag(values[1])
            report_date = scrub_tag(values[2])
    else:
        raise ValueError(f"Unknown holdings version {version}")

    assert re.match('.*schedule of investments.*', title, re.DOTALL | re.IGNORECASE) is not None
    assert etf_name.lower() == "ishares® russell 3000 etf" or etf_name.lower() == "ishares® russell 3000 index fund" or etf_name.lower() == "ishares russell 3000 index fund", etf_name

    return {
        "ETF_NAME": etf_name.lower(),
        "REPORT_DATE": pd.Timestamp(report_date)
    }


def parse_holdings_column(col: bs4.element.Tag, is_first_column: bool, current_sector: str) -> Dict[str, Any]:
    """
    Structure a column of the holdings in the new format filings

    Parameters
    ----------
    col: bs4.element.Tag
        A holdings column
    is_first_column: bool
        The is the first column of the first page in a filing
    current_sector: str
        The current sector

    Returns
    -------
    Dict[str, Any]
        The structured holdings
    """
    # parse individual rows of the table column
    rows = [[]]
    for row in col.find_all("tr"):
        if rows[-1]:
            rows.append([])
        for td in row.find_all("td"):
            if not empty_tag(td):
                rows[-1].append(td)

    # validate the table a little
    assert scrub_tag(rows[0][0]) == "Security"
    assert scrub_tag(rows[0][1]) == "Shares"
    assert scrub_tag(rows[0][2]) == "Value"

    if is_first_column:
        assert len(rows[1]) == 1
        assert re.match(".*common stocks.*", rows[1][0].text, re.IGNORECASE | re.DOTALL) is not None
        start = 2
    else:
        start = 1

    sector_totals = dict()
    records = []
    hit_total_common_stocks = False
    for i, row in enumerate(rows[start:]):
        parsed_row = parse_row(row)
        if not parsed_row:
            continue
        elif len(parsed_row) == 1:
            if is_int(parsed_row[0]):
                assert current_sector is not None
                assert current_sector not in sector_totals
                sector_totals[current_sector] = parse_int(parsed_row[0])
                if hit_total_common_stocks:
                    break
                else:
                    continue
            else:
                current_sector = parsed_row[0]
                current_sector = re.sub(u"\x97", "---", current_sector)
                if re.match(".*common.*stock", current_sector, re.DOTALL | re.IGNORECASE) is not None:
                    hit_total_common_stocks = True
                continue
        elif len(parsed_row) == 2:
            # they put it on separate lines starting in 2018-09-30
            # assert re.match('.*common.*stock.*', parsed_row[0], re.DOTALL | re.IGNORECASE) is not None
            sector_totals["Total Common Stocks"] = parse_int(parsed_row[1])
            hit_total_common_stocks = True
            break
        elif len(parsed_row) == 3:
            record = {
                "SECTOR": current_sector,
                "COMPANY_NAME": parsed_row[0].strip(),
                "SHARES": parse_int(parsed_row[1]),
                "VALUE": parse_int(parsed_row[2]),
            }
            records.append(record)
            continue
        elif len(parsed_row) == 4:
            m = re.compile('(\(a\)|\(b\)|\(c\)|\(d\)|\(e\)|\(f\))', re.IGNORECASE)
            assert m.search(parsed_row[3]) is not None
            record = {
                "SECTOR": current_sector,
                "COMPANY_NAME": parsed_row[0].strip(),
                "SHARES": parse_int(parsed_row[1]),
                "VALUE": parse_int(parsed_row[2]),
            }
            records.append(record)
            continue
        else:
            raise ValueError(f"Unexpcted row format {row}")

    return {
        "HOLDINGS": records,
        "SECTOR_TOTALS": sector_totals,
        "HIT_TOTAL_COMMON_STOCKS": hit_total_common_stocks,
        "CURRENT_SECTOR": current_sector
    }


def parse_holdings_page(page: bs4.element.Tag, is_first_page: bool, version: int, current_sector: str) -> Dict[str, Any]:
    """
    Extract data from a single holdings page

    Parameters
    ----------
    page: bs4.element.Tag
        A page of the Russell 3000 holdings
    is_first_page: bool
        Indicates whether this is the first page in a collection of pages
    version: int
        The filing sub-version from get_subversion
    current_sector: str
        The current sector

    Returns
    -------
    Dict[str, Any]
        The fund name, filing date, positions, and sector totals
    """
    header = parse_header_info(page, version)
    holdings = []
    sector_totals = dict()
    hit_total_common_stocks = False
    for i, col in enumerate(extract_holdings_columns(page, version)):
        h = parse_holdings_column(col, i==0 and is_first_page, current_sector)
        holdings.extend(h["HOLDINGS"])
        sector_totals = {**sector_totals, **h["SECTOR_TOTALS"]}
        current_sector = h["CURRENT_SECTOR"]
        if h["HIT_TOTAL_COMMON_STOCKS"]:
            hit_total_common_stocks = True
            break
    return {
        "ETF_NAME": header["ETF_NAME"],
        "REPORT_DATE": header["REPORT_DATE"],
        "HOLDINGS": holdings,
        "SECTOR_TOTALS": sector_totals,
        "HIT_TOTAL_COMMON_STOCKS": hit_total_common_stocks,
        "CURRENT_SECTOR": current_sector,
    }


def parse_filing(buf: bytes) -> Dict[str, Any]:
    """
    Extract Russel 3000 holdings from an iShares N-CSR filing

    Parameters
    ----------
    buf: bytes
        The raw filing bytes

    Returns
    -------
    Dict[str, Any]
        Parsed Russell 3000 holdings
    """
    pages = extract_pages(buf)
    version = get_subversion(pages[0])

    results = []
    current_sector = None
    for i, page in enumerate(pages):
        res = parse_holdings_page(page, i==0, version=version, current_sector=current_sector)
        current_sector = res["CURRENT_SECTOR"]
        results.append(res)
        if res["HIT_TOTAL_COMMON_STOCKS"]:
            break

    # aggregate holdings and derive sector holdings
    holdings = []
    sector_totals = dict()
    derived_sector_totals = dict()
    derived_common_totals = 0
    for i, r in enumerate(results):
        if i != 0:
            assert r["ETF_NAME"] == results[i-1]["ETF_NAME"]
            assert r["REPORT_DATE"] == results[i-1]["REPORT_DATE"]

        sector_totals = {**sector_totals, **r["SECTOR_TOTALS"]}
        
        for h in r["HOLDINGS"]:
            sector_name = h["SECTOR"]
            sector_name = normalize_sector(sector_name)
            h["SECTOR"] = sector_name

            holdings.append(h)

            val = 0 if h["VALUE"] is None else h["VALUE"]
            derived_sector_totals[sector_name] = derived_sector_totals.get(sector_name, 0) + val
            derived_common_totals += val

    # validate sector holdings
    assert sector_totals["Total Common Stocks"] == derived_common_totals
    sector_totals.pop("Total Common Stocks")

    for sector, total in sector_totals.items():
        sector = normalize_sector(sector)
        assert total == derived_sector_totals[sector]

    return {
        "ETF_NAME": results[0]["ETF_NAME"],
        "REPORT_DATE": results[0]["REPORT_DATE"],
        "HOLDINGS": holdings,
    }
