"""
Parse old format iShares N-CSR filing
"""
from bs4 import BeautifulSoup
from dataclasses import dataclass
from typing import Any, Dict, List, Union
import bs4
import pandas as pd
import re


from r3k.parse_new_ncsr import get_page_separators


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
    sector = re.sub("\x96", "---", sector)
    sector = re.sub("\x97", "---", sector)
    sector = re.sub("\&nbsp\;", " ", sector)
    sector = re.sub("—", "---", sector)
    if "---" in sector:
        sector = "---".join(sector.split("---")[:-1]).strip()
    elif " " in sector:
        sector = " ".join(sector.split(" ")[:-1]).strip()
    return sector.strip()


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
        Individual pages of the Russell 3000 Schedule of Investments
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

    # find schedule of investments for russell 3000
    soi = re.compile(r".*schedule\s+of\s+investments.*", re.DOTALL|re.IGNORECASE)
    smr = re.compile(".*summary.*schedule.*of.*investments.*", re.DOTALL|re.IGNORECASE)
    r3k = re.compile(r".*russell\s+3000\s+index\s+fund.*", re.DOTALL|re.IGNORECASE)
    cmn = re.compile(r".*total.*common.*stocks.*", re.DOTALL|re.IGNORECASE)
    soups = []
    store = False
    for p in pages:
        soup = BeautifulSoup(p, "lxml")
        text = soup.text

        is_soi = soi.match(text) is not None
        is_smr = smr.match(text) is not None
        is_r3k = r3k.match(text) is not None
        
        if is_soi and is_r3k and not is_smr:
            store = True

        if store:
            soups.append(soup)

        if store and cmn.match(text) is not None:
            break

    return soups


def parse_header_info(page: bs4.element.Tag) -> Dict[str, Any]:
    """
    Structure the header of the first holdings page in old filing format

    Parameters
    ----------
    page: bs4.element.Tag
        The first page of a Russell 3000 holdings section

    Returns
    -------
    Dict[str, Any]
        The fund name and filing date
    """
    tags = nonempty_p(page)
    soi = scrub_tag(tags[0])
    etf = scrub_tag(tags[1])
    dat = scrub_tag(tags[2])
    assert re.match("schedule\s+of\s+investments", soi, re.IGNORECASE) is not None
    assert re.match("iSHARES® RUSSELL 3000 INDEX FUND", etf, re.IGNORECASE) is not None
    return {
        "ETF_NAME": etf.lower(),
        "REPORT_DATE": pd.Timestamp(dat),
    }


def parse_holdings_column(col: bs4.element.Tag, is_first_column: bool, current_sector: str, check_header: bool = True) -> Dict[str, Any]:
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
    if check_header:
        assert scrub_tag(rows[0][0]) == "Security"
        assert scrub_tag(rows[0][1]) == "Shares"
        assert scrub_tag(rows[0][2]) == "Value"

    if is_first_column and check_header:
        assert len(rows[1]) == 1
        assert re.match(".*common stocks.*", rows[1][0].text, re.IGNORECASE | re.DOTALL) is not None
        start = 2
    elif check_header:
        start = 1
    else:
        start = 0

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
                current_sector = normalize_sector(current_sector)
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


def parse_filing(buf: bytes) -> Dict[str, Any]:
    """
    Extract Russell 3000 holdings from an iShares N-CSR filing

    Parameters
    ----------
    buf: bytes
        The raw filing bytes

    Returns
    -------
    Dict[str, Any]
        Parsed Russell 3000 holdings
    """
    # get all pages
    pages = extract_pages(buf)

    # header
    header = parse_header_info(pages[0])

    # extract holdings from each page
    results = []
    current_sector = None
    hit_common = False
    for i, page in enumerate(pages):
        tables = page.find_all("table")
        for table in tables:
            res = parse_holdings_column(table, i==0, current_sector, i==0)
            current_sector = res["CURRENT_SECTOR"]
            results.append(res)
            if res["HIT_TOTAL_COMMON_STOCKS"]:
                hit_common = True
                break
        if hit_common:
            break

    # sanity check results
    holdings = []
    sector_totals = dict()
    for res in results:
        holdings.extend(res["HOLDINGS"])
        sector_totals = {**sector_totals, **res["SECTOR_TOTALS"]}

    df = pd.DataFrame(holdings)
    assert df.VALUE.fillna(0).sum().item() == sector_totals["Total Common Stocks"]
    sector_totals.pop("Total Common Stocks")

    for sector, total in sector_totals.items():
        assert df.loc[df.SECTOR==sector, "VALUE"].fillna(0).sum().item() == total

    return {
        "ETF_NAME": header["ETF_NAME"],
        "REPORT_DATE": header["REPORT_DATE"],
        "HOLDINGS": holdings
    }

