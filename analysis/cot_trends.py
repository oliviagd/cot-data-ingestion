import os
import re
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import glob


GROUPS = ["m_money", "prod_merc", "swap", "other_rept"]
METRICS = ["long", "short", "net"]
COLORS = ["blue", "red", "green"]


def extract_integer(s):
    """
    Helper to extract the number of contracts per bushel in the
    `contract_units` column

    Expects format "(CONTRACTS IN 1,000 BUSHELS)"
    """

    match = re.search(r"(\d{1,3}(?:,\d{3})*)", s)
    if match:
        return int(match.group(1).replace(",", ""))
    return None


def prepare_frame(dataframe):
    """
    Applies some cleaning and filtering to the dataframe
    """
    dataframe["as_of_date_in_form_yymmdd"] = pd.to_datetime(
        dataframe["as_of_date_in_form_yymmdd"], format="%y%m%d"
    )

    # filter for the last 5 years and wheat exchanges
    dataframe = dataframe[
        dataframe["as_of_date_in_form_yymmdd"]
        >= pd.to_datetime("today") - pd.DateOffset(years=5)
    ]
    dataframe = dataframe[
        dataframe["market_and_exchange_names"].str.lower().str.contains("wheat")
    ]

    # extract bushels per contract
    dataframe["bushels_per_contract"] = dataframe["contract_units"].apply(
        lambda x: extract_integer(x)
    )
    dataframe = dataframe[~dataframe.bushels_per_contract.isna()]

    columns = ["as_of_date_in_form_yymmdd"]

    # calculate positions by bushel
    for group in GROUPS:
        for column_type in METRICS:
            if column_type == "net":
                continue
            column_name = f"{group}_positions_{column_type}_all"
            dataframe[f"{column_name}_by_bushel"] = (
                dataframe[column_name] * dataframe["bushels_per_contract"]
            )
            columns.append(
                f"{column_name}_by_bushel"
            )  # collect columns for later aggregation

    return dataframe[columns]


def aggregate_frame(dataframe):
    dataframe = dataframe.groupby("as_of_date_in_form_yymmdd").sum().reset_index()

    # calculate net positions
    for group in GROUPS:
        dataframe[f"{group}_positions_net_all_by_bushel"] = (
            dataframe[f"{group}_positions_long_all_by_bushel"]
            - dataframe[f"{group}_positions_short_all_by_bushel"]
        )
    return dataframe


def plot(dataframe):
    fig = make_subplots(
        rows=2,
        cols=2,
        subplot_titles=(
            "Managed Money",
            "Producers",
            "Swap Dealers",
            "Other Reportable",
        ),
    )

    # add traces for each group
    add_traces(dataframe, fig, "m_money", 1, 1)
    add_traces(dataframe, fig, "prod_merc", 1, 2)
    add_traces(dataframe, fig, "swap", 2, 1)
    add_traces(dataframe, fig, "other_rept", 2, 2)

    fig.update_layout(
        title="Positions Trends Over the Past 5 Years", height=800, width=1000
    )

    fig.write_html("positions_trends_report.html")


def add_traces(dataframe, fig, group, row, col):
    """
    Add traces
    """
    for color, column_type in zip(COLORS, METRICS):
        fig.add_trace(
            go.Scatter(
                x=dataframe["as_of_date_in_form_yymmdd"],
                y=dataframe[f"{group}_positions_{column_type}_all_by_bushel"],
                mode="lines",
                name=column_type.capitalize(),
                line=dict(color=color),
                showlegend=True if row == 1 and col == 1 else False,
            ),
            row=row,
            col=col,
        )


if __name__ == "__main__":

    data_path = "./data/cot_historical_disaggregated/v1/"
    all_files = glob.glob(os.path.join(data_path, "**/*.parquet"), recursive=True)
    df_list = [pd.read_parquet(file) for file in all_files]
    df = pd.concat(df_list, ignore_index=True)

    df = prepare_frame(df)
    df = aggregate_frame(df)
    plot(df)
