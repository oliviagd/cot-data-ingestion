import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import glob
import os

# Read in dataframe
data_path = "./data/cot_historical_disaggregated/v1/"

all_files = glob.glob(os.path.join(data_path, "**/*.parquet"), recursive=True)
df_list = [pd.read_parquet(file) for file in all_files]
df = pd.concat(df_list, ignore_index=True)
print(df.shape)

df["as_of_date_in_form_yymmdd"] = pd.to_datetime(
    df["as_of_date_in_form_yymmdd"], format="%y%m%d"
)

df_filtered = df[
    df["as_of_date_in_form_yymmdd"] >= pd.to_datetime("today") - pd.DateOffset(years=5)
]

# Calculate net positions
df_filtered["m_money_positions_net_all"] = (
    df_filtered["m_money_positions_long_all"]
    - df_filtered["m_money_positions_short_all"]
)
df_filtered["prod_merc_positions_net_all"] = (
    df_filtered["prod_merc_positions_long_all"]
    - df_filtered["prod_merc_positions_short_all"]
)
df_filtered["swap_positions_net_all"] = (
    df_filtered["swap_positions_long_all"] - df_filtered["swap_positions_short_all"]
)
df_filtered["other_rept_positions_net_all"] = (
    df_filtered["other_rept_positions_long_all"]
    - df_filtered["other_rept_positions_short_all"]
)

cols = [
    "as_of_date_in_form_yymmdd",
    "m_money_positions_net_all",
    "m_money_positions_long_all",
    "m_money_positions_short_all",
    "prod_merc_positions_net_all",
    "prod_merc_positions_long_all",
    "prod_merc_positions_short_all",
    "other_rept_positions_net_all",
    "other_rept_positions_long_all",
    "other_rept_positions_short_all",
]
print(df_filtered.shape)
print(df_filtered[cols].head())
fig = make_subplots(
    rows=2,
    cols=2,
    subplot_titles=("Managed Money", "Producers", "Swap Dealers", "Other Reportable"),
)

# Managed Money
fig.add_trace(
    go.Scatter(
        x=df_filtered["as_of_date_in_form_yymmdd"],
        y=df_filtered["m_money_positions_long_all"],
        mode="lines",
        name="Long",
        line=dict(color="blue"),
    ),
    row=1,
    col=1,
)

fig.add_trace(
    go.Scatter(
        x=df_filtered["as_of_date_in_form_yymmdd"],
        y=df_filtered["m_money_positions_short_all"],
        mode="lines",
        name="Short",
        line=dict(color="red"),
    ),
    row=1,
    col=1,
)
fig.add_trace(
    go.Scatter(
        x=df_filtered["as_of_date_in_form_yymmdd"],
        y=df_filtered["m_money_positions_net_all"],
        mode="lines",
        name="Net",
        line=dict(color="green"),
    ),
    row=1,
    col=1,
)

# Producers
fig.add_trace(
    go.Scatter(
        x=df_filtered["as_of_date_in_form_yymmdd"],
        y=df_filtered["prod_merc_positions_long_all"],
        mode="lines",
        name="Long",
        line=dict(color="blue"),
    ),
    row=1,
    col=2,
)
fig.add_trace(
    go.Scatter(
        x=df_filtered["as_of_date_in_form_yymmdd"],
        y=df_filtered["prod_merc_positions_short_all"],
        mode="lines",
        name="Short",
        line=dict(color="red"),
    ),
    row=1,
    col=2,
)
fig.add_trace(
    go.Scatter(
        x=df_filtered["as_of_date_in_form_yymmdd"],
        y=df_filtered["prod_merc_positions_net_all"],
        mode="lines",
        name="Net",
        line=dict(color="green"),
    ),
    row=1,
    col=2,
)

# Swap Dealers
fig.add_trace(
    go.Scatter(
        x=df_filtered["as_of_date_in_form_yymmdd"],
        y=df_filtered["swap_positions_long_all"],
        mode="lines",
        name="Long",
        line=dict(color="blue"),
    ),
    row=2,
    col=1,
)
fig.add_trace(
    go.Scatter(
        x=df_filtered["as_of_date_in_form_yymmdd"],
        y=df_filtered["swap_positions_short_all"],
        mode="lines",
        name="Short",
        line=dict(color="red"),
    ),
    row=2,
    col=1,
)
fig.add_trace(
    go.Scatter(
        x=df_filtered["as_of_date_in_form_yymmdd"],
        y=df_filtered["swap_positions_net_all"],
        mode="lines",
        name="Net",
        line=dict(color="green"),
    ),
    row=2,
    col=1,
)

# Other Reportable
fig.add_trace(
    go.Scatter(
        x=df_filtered["as_of_date_in_form_yymmdd"],
        y=df_filtered["other_rept_positions_long_all"],
        mode="lines",
        name="Long",
        line=dict(color="blue"),
    ),
    row=2,
    col=2,
)
fig.add_trace(
    go.Scatter(
        x=df_filtered["as_of_date_in_form_yymmdd"],
        y=df_filtered["other_rept_positions_short_all"],
        mode="lines",
        name="Short",
        line=dict(color="red"),
    ),
    row=2,
    col=2,
)
fig.add_trace(
    go.Scatter(
        x=df_filtered["as_of_date_in_form_yymmdd"],
        y=df_filtered["other_rept_positions_net_all"],
        mode="lines",
        name="Net",
        line=dict(color="green"),
    ),
    row=2,
    col=2,
)

# Update layout
#fig.update_layout(
#    title="Positions Trends Over the Past 5 Years", height=800, width=1000
#)

fig.write_html("positions_trends_report.html")

for i, trace in enumerate(fig.data, 1):
    print(f"Trace {i}: {trace.name}")
