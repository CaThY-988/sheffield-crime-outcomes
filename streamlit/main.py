import os

import folium
import pandas as pd
import plotly.express as px
import streamlit as st
from databricks import sql
from dotenv import load_dotenv
from folium.plugins import MarkerCluster

load_dotenv()

st.set_page_config(
    page_title="Sheffield Crime Dashboard",
    layout="wide",
)

CHART_HEIGHT = 420
QUALITATIVE_PALETTE = px.colors.qualitative.Set2
SEQUENTIAL_PALETTE = px.colors.sequential.Blues


@st.cache_resource
def get_connection():
    return sql.connect(
        server_hostname=os.getenv("DATABRICKS_HOST"),
        http_path=os.getenv("DATABRICKS_HTTP_PATH"),
        access_token=os.getenv("DATABRICKS_TOKEN"),
    )


@st.cache_data(ttl=600)
def run_query(query: str) -> pd.DataFrame:
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [col[0] for col in cursor.description]
    return pd.DataFrame(rows, columns=columns)


def coerce_numeric(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    df = df.copy()
    for col in columns:
        df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def apply_standard_layout(fig, *, height=CHART_HEIGHT, xaxis_title="", yaxis_title=""):
    fig.update_layout(
        xaxis_title=xaxis_title,
        yaxis_title=yaxis_title,
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        height=height,
    )
    return fig


@st.cache_data(ttl=600)
def load_map_data() -> pd.DataFrame:
    query = """
    select
        crime_id,
        crime_month,
        crime_category,
        crime_latitude,
        crime_longitude,
        crime_street_name,
        latest_outcome_category
    from workspace.analytics_police.mart_crime_map
    where crime_latitude is not null
      and crime_longitude is not null
    """
    df = run_query(query)

    if df.empty:
        return df

    df = coerce_numeric(df, ["crime_latitude", "crime_longitude"])
    df = df.dropna(subset=["crime_latitude", "crime_longitude"])
    df["crime_month"] = df["crime_month"].astype(str)

    return df


@st.cache_data(ttl=600)
def load_timing_data() -> pd.DataFrame:
    query = """
    select
        crime_month,
        crime_category,
        crime_count,
        avg_months_to_outcome,
        median_months_to_outcome,
        min_months_to_outcome,
        max_months_to_outcome
    from workspace.analytics_police.mart_crime_outcome_timings
    """
    df = run_query(query)

    if df.empty:
        return df

    df = coerce_numeric(
        df,
        [
            "crime_count",
            "avg_months_to_outcome",
            "median_months_to_outcome",
            "min_months_to_outcome",
            "max_months_to_outcome",
        ],
    )
    df["crime_month"] = df["crime_month"].astype(str)

    return df


@st.cache_data(ttl=600)
def load_stop_search_reason_trends() -> pd.DataFrame:
    query = """
    select
        stop_search_month,
        object_of_search,
        stop_search_count,
        pct_of_month_total
    from workspace.analytics_police.mart_stop_search_reason_trends
    """
    df = run_query(query)

    if df.empty:
        return df

    df["stop_search_month"] = df["stop_search_month"].astype(str)
    df = coerce_numeric(df, ["stop_search_count", "pct_of_month_total"])

    return df


@st.cache_data(ttl=600)
def load_stop_search_outcome_mix() -> pd.DataFrame:
    query = """
    select
        object_of_search,
        outcome,
        stop_search_count,
        pct_within_object_of_search
    from workspace.analytics_police.mart_stop_search_outcome_mix
    """
    df = run_query(query)

    if df.empty:
        return df

    df = coerce_numeric(df, ["stop_search_count", "pct_within_object_of_search"])

    return df


def render_clustered_map(map_df: pd.DataFrame) -> None:
    st.markdown("**Clustered crime locations**")
    st.caption(
        "Each marker represents an individual crime. Nearby crimes are grouped "
        "into clusters to make dense areas easier to explore."
    )

    min_lat = map_df["lat"].min()
    max_lat = map_df["lat"].max()
    min_lon = map_df["lon"].min()
    max_lon = map_df["lon"].max()

    m = folium.Map(
        location=[map_df["lat"].mean(), map_df["lon"].mean()],
        tiles="CartoDB Positron",
        zoom_start=11,
    )

    marker_cluster = MarkerCluster().add_to(m)

    for _, row in map_df.iterrows():
        folium.Marker(
            [row["lat"], row["lon"]],
            tooltip=f"{row['crime_category']} - {row['crime_street_name']}",
        ).add_to(marker_cluster)

    if min_lat != max_lat and min_lon != max_lon:
        m.fit_bounds([[min_lat, min_lon], [max_lat, max_lon]])

    st.components.v1.html(m._repr_html_(), height=CHART_HEIGHT)


def render_crime_scatter_map(map_df: pd.DataFrame) -> None:
    st.markdown("**Crimes plotted by category**")
    st.caption(
        "Crimes are plotted individually and coloured by category, making it "
        "easier to compare how different crime types are distributed."
    )

    fig = px.scatter_map(
        map_df,
        lat="lat",
        lon="lon",
        color="crime_category",
        hover_name="crime_street_name",
        hover_data={
            "crime_month": True,
            "lat": False,
            "lon": False,
        },
        zoom=11,
        center={
            "lat": map_df["lat"].mean(),
            "lon": map_df["lon"].mean(),
        },
        map_style="carto-positron",
        opacity=0.7,
        color_discrete_sequence=QUALITATIVE_PALETTE,
    )

    fig.update_layout(
        margin={"r": 0, "t": 0, "l": 0, "b": 0},
        legend_title_text="Crime category",
        height=CHART_HEIGHT,
    )

    st.plotly_chart(fig, use_container_width=True)


def render_outcome_summary_chart(filtered_timing_df: pd.DataFrame) -> pd.DataFrame:
    st.markdown("**Average months to latest outcome by category**")
    st.caption(
        "Shows the average time from crime month to latest recorded outcome, "
        "measured in whole months."
    )

    category_summary_df = (
        filtered_timing_df.assign(
            weighted_months=lambda df: (
                df["avg_months_to_outcome"] * df["crime_count"]
            )
        )
        .groupby("crime_category", as_index=False)
        .agg(
            weighted_months=("weighted_months", "sum"),
            total_crimes=("crime_count", "sum"),
            median_months_to_outcome=("median_months_to_outcome", "mean"),
            min_months_to_outcome=("min_months_to_outcome", "min"),
            max_months_to_outcome=("max_months_to_outcome", "max"),
        )
    )

    category_summary_df["avg_months_to_outcome"] = (
        category_summary_df["weighted_months"]
        / category_summary_df["total_crimes"]
    )

    category_summary_df = category_summary_df.sort_values(
        "avg_months_to_outcome",
        ascending=False,
    )

    fig_bar = px.bar(
        category_summary_df,
        x="crime_category",
        y="avg_months_to_outcome",
        color="avg_months_to_outcome",
        color_continuous_scale=SEQUENTIAL_PALETTE,
        hover_data={
            "total_crimes": True,
            "median_months_to_outcome": ":.2f",
            "min_months_to_outcome": True,
            "max_months_to_outcome": True,
            "weighted_months": False,
        },
    )

    apply_standard_layout(
        fig_bar,
        xaxis_title="Crime category",
        yaxis_title="Average months to latest outcome",
    )
    fig_bar.update_layout(
        xaxis_tickangle=45,
        coloraxis_showscale=False,
    )

    st.plotly_chart(fig_bar, use_container_width=True)

    return category_summary_df


def render_outcome_trend_chart(filtered_timing_df: pd.DataFrame) -> pd.DataFrame:
    st.markdown("**Average outcome timing trends over time**")
    st.caption(
        "Tracks how average months to latest outcome change across crime months "
        "for each crime category."
    )

    trend_df = (
        filtered_timing_df.assign(
            weighted_months=lambda df: (
                df["avg_months_to_outcome"] * df["crime_count"]
            )
        )
        .groupby(
            ["crime_month", "crime_category"],
            as_index=False,
        )
        .agg(
            weighted_months=("weighted_months", "sum"),
            crime_count=("crime_count", "sum"),
        )
        .sort_values("crime_month")
    )

    trend_df["avg_months_to_outcome"] = (
        trend_df["weighted_months"] / trend_df["crime_count"]
    )

    fig_line = px.line(
        trend_df,
        x="crime_month",
        y="avg_months_to_outcome",
        color="crime_category",
        markers=True,
        color_discrete_sequence=QUALITATIVE_PALETTE,
        hover_data={
            "crime_count": True,
            "weighted_months": False,
        },
    )

    apply_standard_layout(
        fig_line,
        xaxis_title="Crime month",
        yaxis_title="Average months to latest outcome",
    )
    fig_line.update_layout(
        legend_title_text="Crime category",
    )

    st.plotly_chart(fig_line, use_container_width=True)

    return trend_df


def render_stop_search_trend_chart(
    stop_search_trends_df: pd.DataFrame,
    selected_month: str,
) -> None:
    if stop_search_trends_df.empty:
        st.info("No stop and search trend data available.")
        return

    st.markdown("**Stop and search reasons over time**")
    st.caption(
        "Shows how the volume of stop and search records changes by object of search "
        "across months."
    )

    trend_chart_df = stop_search_trends_df.copy()

    if selected_month != "All":
        trend_chart_df = trend_chart_df[
            trend_chart_df["stop_search_month"] == selected_month
        ]

    if trend_chart_df.empty:
        st.info("No stop and search trend data for the selected month.")
        return

    fig_stop_search_trend = px.bar(
        trend_chart_df.sort_values(["stop_search_month", "stop_search_count"]),
        x="stop_search_month",
        y="stop_search_count",
        color="object_of_search",
        color_discrete_sequence=QUALITATIVE_PALETTE,
        hover_data={
            "pct_of_month_total": ":.2%",
        },
        barmode="stack",
    )

    apply_standard_layout(
        fig_stop_search_trend,
        xaxis_title="Reporting month",
        yaxis_title="Stop and search count",
    )
    fig_stop_search_trend.update_layout(
        legend_title_text="Object of search",
    )

    st.plotly_chart(fig_stop_search_trend, use_container_width=True)

    with st.expander("Preview stop and search trend data"):
        st.dataframe(
            trend_chart_df.sort_values(
                ["stop_search_month", "stop_search_count"],
                ascending=[False, False],
            ),
            use_container_width=True,
            hide_index=True,
        )


def render_stop_search_heatmap(stop_search_outcome_df: pd.DataFrame) -> None:
    if stop_search_outcome_df.empty:
        st.info("No stop and search outcome mix data available.")
        return

    st.markdown("**Stop and search outcomes by object of search**")
    st.caption(
        "Shows how stop and search outcomes vary depending on the stated object "
        "of the search. Darker cells represent higher percentages."
    )

    outcome_chart_df = stop_search_outcome_df.copy()

    top_objects = (
        outcome_chart_df.groupby("object_of_search", as_index=False)
        .agg(total_searches=("stop_search_count", "sum"))
        .sort_values("total_searches", ascending=False)
        .head(10)["object_of_search"]
        .tolist()
    )

    outcome_chart_df = outcome_chart_df[
        outcome_chart_df["object_of_search"].isin(top_objects)
    ]

    if outcome_chart_df.empty:
        st.info("No stop and search outcome mix data to display.")
        return

    heatmap_df = outcome_chart_df.pivot(
        index="object_of_search",
        columns="outcome",
        values="pct_within_object_of_search",
    ).fillna(0)

    fig_outcome_heatmap = px.imshow(
        heatmap_df,
        aspect="auto",
        text_auto=".0%",
        color_continuous_scale=SEQUENTIAL_PALETTE,
    )

    apply_standard_layout(
        fig_outcome_heatmap,
        xaxis_title="Outcome",
        yaxis_title="Object of search",
    )
    fig_outcome_heatmap.update_layout(
        coloraxis_colorbar_title="Share",
    )

    st.plotly_chart(fig_outcome_heatmap, use_container_width=True)

    with st.expander("Preview stop and search outcome mix data"):
        st.dataframe(
            outcome_chart_df.sort_values(
                ["object_of_search", "stop_search_count"],
                ascending=[True, False],
            ),
            use_container_width=True,
            hide_index=True,
        )


def main():
    st.title("Sheffield Crime Dashboard")
    st.caption("Data Zoomcamp project 2026")

    map_data = load_map_data()
    timing_df = load_timing_data()
    stop_search_trends_df = load_stop_search_reason_trends()
    stop_search_outcome_df = load_stop_search_outcome_mix()

    if map_data.empty:
        st.error("No data returned from mart_crime_map.")
        return

    if timing_df.empty:
        st.warning("No data returned from mart_crime_outcome_timings.")

    st.sidebar.header("Filters")

    month_options = sorted(map_data["crime_month"].dropna().unique().tolist())
    selected_month = st.sidebar.selectbox(
        "Reporting month",
        options=["All"] + month_options,
        index=0,
    )

    filtered_map_df = map_data.copy()
    if selected_month != "All":
        filtered_map_df = filtered_map_df[
            filtered_map_df["crime_month"] == selected_month
        ]

    filtered_timing_df = timing_df.copy()
    if selected_month != "All":
        filtered_timing_df = filtered_timing_df[
            filtered_timing_df["crime_month"] == selected_month
        ]

    filtered_stop_search_trends_df = stop_search_trends_df.copy()
    if selected_month != "All":
        filtered_stop_search_trends_df = filtered_stop_search_trends_df[
            filtered_stop_search_trends_df["stop_search_month"] == selected_month
        ]

    total_crimes = len(filtered_map_df)
    total_stop_searches = (
        int(filtered_stop_search_trends_df["stop_search_count"].sum())
        if not filtered_stop_search_trends_df.empty
        else 0
    )

    kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
    kpi_col1.metric("Total crimes", f"{total_crimes:,}")
    kpi_col2.metric("Total stop and searches", f"{total_stop_searches:,}")
    kpi_col3.metric("Months available", f"{len(month_options):,}")

    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Crime map views")

        if filtered_map_df.empty:
            st.info("No map data for the selected reporting month.")
        else:
            map_df = filtered_map_df.rename(
                columns={
                    "crime_latitude": "lat",
                    "crime_longitude": "lon",
                }
            ).copy()

            render_clustered_map(map_df)
            render_crime_scatter_map(map_df)

            preview_df = (
                filtered_map_df.groupby(
                    ["crime_month", "crime_category"],
                    as_index=False,
                )
                .agg(sum_crimes=("crime_id", "count"))
                .sort_values(
                    ["crime_month", "sum_crimes"],
                    ascending=[False, False],
                )
            )

            with st.expander("Preview mapped records summary"):
                st.dataframe(
                    preview_df,
                    use_container_width=True,
                    hide_index=True,
                )

    with right_col:
        st.subheader("Crime outcome analysis")

        if timing_df.empty:
            st.info("No timing data available.")
        elif filtered_timing_df.empty:
            st.info("No outcome timing data for the selected reporting month.")
        else:
            category_summary_df = render_outcome_summary_chart(filtered_timing_df)
            trend_df = render_outcome_trend_chart(filtered_timing_df)

            preview_summary_df = category_summary_df[
                [
                    "crime_category",
                    "total_crimes",
                    "avg_months_to_outcome",
                    "median_months_to_outcome",
                    "min_months_to_outcome",
                    "max_months_to_outcome",
                ]
            ].copy()

            with st.expander("Preview outcome timing summary"):
                st.dataframe(
                    preview_summary_df,
                    use_container_width=True,
                    hide_index=True,
                )

            with st.expander("Preview outcome timing trend data"):
                st.dataframe(
                    trend_df[
                        [
                            "crime_month",
                            "crime_category",
                            "crime_count",
                            "avg_months_to_outcome",
                        ]
                    ],
                    use_container_width=True,
                    hide_index=True,
                )

    st.subheader("Stop and search analysis")

    stop_left_col, stop_right_col = st.columns(2)

    with stop_left_col:
        render_stop_search_trend_chart(stop_search_trends_df, selected_month)

    with stop_right_col:
        render_stop_search_heatmap(stop_search_outcome_df)

    st.subheader("About this dashboard")
    st.write(
        """
        This dashboard combines Sheffield police crime and stop-and-search data
        modelled in dbt and queried from Databricks.

        The crime views show mapped crime locations from `mart_crime_map` and
        outcome timing metrics from `mart_crime_outcome_timings`.

        The stop-and-search views show monthly trends in the stated object of
        search from `mart_stop_search_reason_trends` and the mix of recorded
        outcomes by object of search from `mart_stop_search_outcome_mix`.

        The reporting month filter applies across the crime views and the
        stop-and-search trend chart. The stop-and-search outcome heatmap shows
        the overall outcome mix and is not filtered by month.

        Outcome timing is measured in whole months because the public Police API
        provides crime dates at month level rather than exact offence dates.
        """
    )


if __name__ == "__main__":
    main()