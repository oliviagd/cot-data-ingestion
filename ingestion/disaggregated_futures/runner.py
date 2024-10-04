import argparse
from ingestion.disaggregated_futures.ingestor import COTIngestor


def run(start, end):

    for year in range(start, end + 1):

        print(f"Running ingestion for {year}...")
        ingestor = COTIngestor(year=year)
        ingestor.run()


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Ingestor for downloading data by year range."
    )

    parser.add_argument(
        "--start", type=int, help="The start year for downloading data", required=False
    )
    parser.add_argument(
        "--end", type=int, help="The end year for downloading data", required=False
    )
    parser.add_argument(
        "--year", type=int, help="Specific year to download", required=False
    )

    args = parser.parse_args()

    # param validation
    if args.year and (args.start or args.end):
        parser.error(
            "Cannot specify both --year and --start/--end options. Choose one."
        )

    if (args.start and not args.end) or (args.end and not args.start):
        parser.error("Both --start and --end must be specified together.")

    start = args.start or args.year
    end = args.end or args.year
    run(start, end)
