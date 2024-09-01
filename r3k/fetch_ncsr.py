"""
Collect all the iShares form N-CSR and N-CSRS
"""
from bs4 import BeautifulSoup
from tqdm.auto import tqdm
from typing import Any, List, Dict
import bs4
import pandas as pd
import re
import requests
import time

import seclist.config as config


ISHARES_NCSR_IDX = "https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=S000004341&type=N-CSR&dateb=&count=100&scd=filings&search_text="


FIRST_NEW_DATE = pd.Timestamp("2010-09-30")


CUSTOM_OLD_DATES = [pd.Timestamp("2015-09-30")]


def _parse_sec_table(table: bs4.element.Tag) -> List[Dict[str, str]]:
    """
    Parse one of the EDGAR page tables

    Parameters
    ----------
    table: bs4.element.Tag
        One of the tables parsed out of an SEC page

    Returns
    -------
    List[Dict[str, Any]]
        Parsed table

    Examples
    --------
    [1] https://www.sec.gov/Archives/edgar/data/1100663/000119312518152961/0001193125-18-152961-index.htm
    [2] https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK=S000004341&type=N-CSR&dateb=&count=100&scd=filings&search_text=
    """
    is_first = True
    titles, records = [], []
    for row in table.find_all("tr"):
        # first row is header
        if is_first:
            for val in row.find_all("th"):
                titles.append(val.text.strip())
            is_first = False
            continue

        # subsequent rows are content
        vals = []
        for i, val in enumerate(row.find_all("td")):
            if val.find("a", href=True):
                vals.append(val.find_all("a", href=True)[0]["href"])
            else:
                vals.append(val.text.strip())
        assert len(vals) == len(titles)
        record = dict(zip(titles, vals))
        records.append(record)

    return records


def _get_ncsr_filing_index_index(user_agent: str) -> List[Dict[str, str]]:
    """
    Each filing has an index associated with it. Build an index of those indexes.

    Parameters
    ----------
    user_agent: str
        The user agent for the SEC query

    Returns
    -------
    List[Dict[str, str]]
        Each row has "Filings", "Format", "Description", "Filing Date", "File/Film Number"
    """
    headers = {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate"
    }
    resp = requests.get(ISHARES_NCSR_IDX, headers=headers)
    resp.raise_for_status()
    soup = BeautifulSoup(resp.content, "lxml")
    idx_table = soup.find_all("table")[-1]
    return _parse_sec_table(idx_table)


def _get_sec_filing_index(uri: str, user_agent: str) -> Dict[str, Any]:
    """
    All SEC filings have an associated index page that describes  when the
    document was filed and what items are associated with it. Parse that

    Parameters
    ----------
    uri: str
        A filing index uri
    user_agent: str
        The user agent for the SEC query

    Returns
    -------
    Dict[str, Any]
        Each row has "uri", "Filing Date", "Accepted", "Period of Report", "Effectiveness Date", "Num Documents", "Documents"
        "Documents" is a list of Dicts each of which has "Seq", "Description", "Document", "Type", "Size"
    """
    headers = {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate"
    }
    resp = requests.get(uri, headers=headers)
    resp.raise_for_status()

    soup = BeautifulSoup(resp.content, "lxml")

    # all fields except documents
    heads = soup.find_all("div", {"class", "infoHead"})
    infos = soup.find_all("div", {"class", "info"})
    record = {h.text.strip(): i.text.strip() for h, i in zip(heads, infos)}
    assert "Documents" in record
    record["Num Documents"] = record.pop("Documents")

    # get the documents
    table = soup.find_all("table")[0]
    record["Documents"] = sorted(_parse_sec_table(table), key=lambda r: r["Seq"])

    return record


def get_all_ncsr_uris(user_agent: str) -> List[Dict[str, str]]:
    """
    Get download links for all iShares NCSR and NCSRS filings

    Parameters
    ----------
    user_agent: str
        The user agent for the SEC query

    Returns
    ------
    List[Dict[str, str]]
        Each record has INDEX_DATE and URI

    Returns
    -------
    List[Dict[str, str]]
        Each row has "FILING_DATE", "PERIOD_OF_REPORT", "URI", "FORM_TYPE"
    """
    # index of indexes
    idx_of_idx = _get_ncsr_filing_index_index(user_agent=user_agent)

    # get indexes
    idxs = []
    for row in tqdm(idx_of_idx):
        uri = f"https://www.sec.gov{row['Format']}"
        idx = _get_sec_filing_index(uri, user_agent)
        idxs.append(idx)
        time.sleep(0.2)

    # extract main document from each index
    clean_idxs = []
    for idx in idxs:
        assert re.match(r".*N\-CSR.*", idx["Documents"][1]["Type"], re.DOTALL|re.IGNORECASE) is not None
        if pd.Timestamp(idx["Period of Report"]) < FIRST_NEW_DATE:
            version = 1
        elif pd.Timestamp(idx["Period of Report"]) in CUSTOM_OLD_DATES:
            version = 1
        else:
            version = 2

        row = {
            "FILING_DATE": idx["Filing Date"],
            "PERIOD_OF_REPORT": idx["Period of Report"],
            "FORM_TYPE": idx["Documents"][1]["Type"],
            "SIZE": int(idx["Documents"][1]["Size"]),
            "URI": idx["Documents"][1]["Document"],
            "VERSION": version,
        }
        clean_idxs.append(row)

    return clean_idxs
