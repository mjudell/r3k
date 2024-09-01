from tqdm.auto import tqdm
import argparse
import os
import pandas as pd
import requests
import shutil
import sys
import time

import r3k.fetch_ncsr
import r3k.parse_new_ncsr
import r3k.parse_old_ncsr


SKIP_FILES = [
    "2013-09-30_d609194dncsrs.htm",
    "2015-09-30_d93555dncsrs.htm",
    "2006-09-30_dncsrs.htm",
]


def main() -> int:
    parser = argparse.ArgumentParser(description="Collect Russell 3000 holdings from iShares N-CSR filings")
    parser.add_argument("task", help="[ pull | parse]", type=str)
    parser.add_argument("-i", "--input", help="Input directory", type=str)
    parser.add_argument("-o", "--output", help="Output directory", type=str)
    parser.add_argument("-r", "--replace-existing", help="Replace existing files", action="store_true")
    parser.add_argument("-a", "--user-agent", help="Name and email for SEC user agent", type=str)
    args = parser.parse_args()

    if args.task == "pull":
        return get_ncsr(args.output, args.user_agent, args.replace_existing)
    elif args.task == "parse":
        return parse_ncsr(args.input, args.output, args.replace_existing)

    parser.print_help()

    return 1


def get_ncsr(output_dir: str, user_agent: str, replace_existing: bool) -> None:
    """
    Collect all the NCSR data

    Parameters
    ----------
    output_dir: str
        Where to store the raw NCSR filings
    user_agent: str
        The user agent for the SEC query
    replace_existing: bool
        Delete existing data and replace entirely

    Parameters
    ----------
    replace_existing: bool
        Replace existing filings
    """
    idx = r3k.fetch_ncsr.get_all_ncsr_uris(user_agent=user_agent)
    idx = pd.DataFrame(idx, dtype=object)

    if os.path.exists(output_dir) and replace_existing:
        shutil.rmtree(output_dir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    idx.to_csv(os.path.join(output_dir, "filing-index.csv"), header=True, index=False)

    headers = {
        "User-Agent": user_agent,
        "Accept-Encoding": "gzip, deflate"
    }

    for _, row in tqdm(idx.iterrows(), total=idx.shape[0]):
        uri = f"https://www.sec.gov{row.URI}"
        fil = "_".join([row.PERIOD_OF_REPORT, row.URI.split("/")[-1]])
        pth = os.path.join(output_dir, fil)

        if not replace_existing and os.path.exists(pth):
            continue

        resp = requests.get(uri, headers=headers)
        resp.raise_for_status()

        with open(pth, "wb") as f:
            f.write(resp.content)

        time.sleep(0.2)

    return 0


def parse_ncsr(input_dir: str, output_dir: str, replace_existing: bool = False) -> None:
    """
    Parse all NCSR data

    Parameters
    ----------
    input_dir: str
        Directory containing the raw filings
    output_dir: str
        Directory containing the parsed filings
    replace_existing: bool
        Replace existing parses
    """
    if replace_existing and os.path.exists(output_dir):
        shutil.rmtree(output_dir)

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    idx = pd.read_csv(os.path.join(input_dir, "filing-index.csv"), header=0, dtype=object)

    parser_map = {
        "1": r3k.parse_old_ncsr.parse_filing,
        "2": r3k.parse_new_ncsr.parse_filing,
    }

    for _, row in tqdm(idx.iterrows(), total=idx.shape[0]):
        tgt = os.path.join(output_dir, row.PERIOD_OF_REPORT)
        if os.path.exists(tgt):
            continue

        fil = "_".join([row.PERIOD_OF_REPORT, row.URI.split("/")[-1]])
        src = os.path.join(input_dir, fil)

        if fil in SKIP_FILES:
            continue

        with open(src, "rb") as f:
            buf = f.read()

        hold = parser_map[row.VERSION](buf)

        hold_df = pd.DataFrame(hold["HOLDINGS"], dtype=object)
        hold_df["REPORT_DATE"] = hold["REPORT_DATE"]
        hold_df["ETF_NAME"] = hold["ETF_NAME"]
        hold_df.to_csv(tgt, header=True, index=False)

    return None


if __name__ == "__main__":
    sys.exit(main())
