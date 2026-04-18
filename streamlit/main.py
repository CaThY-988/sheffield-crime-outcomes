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

    df["crime_latitude"] = pd.to_numeric(df["crime_latitude"], errors="coerce")
    df["crime_longitude"] = pd.to_numeric(df["crime_longitude"], errors="coerce")
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

    numeric_cols = [
        "crime_count",
        "avg_months_to_outcome",
        "median_months_to_outcome",
        "min_months_to_outcome",
        "max_months_to_outcome",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["crime_month"] = df["crime_month"].astype(str)

    return df


def main():
    st.title("Sheffield Crime Dashboard")
    st.caption("Data Zoomcamp project 2026")

    map_data = load_map_data()
    timing_df = load_timing_data()

    if map_data.empty:
        st.error("No data returned from mart_crime_map.")
        return

    if timing_df.empty:
        st.warning("No data returned from mart_crime_outcome_timings.")

    st.sidebar.header("Filters")

    month_options = sorted(map_data["crime_month"].dropna().unique().tolist())
    selected_month = st.sidebar.selectbox(
        "Crime month",
        options=["All"] + month_options,
        index=0,
    )

    category_options = sorted(map_data["crime_category"].dropna().unique().tolist())
    selected_categories = st.sidebar.multiselect(
        "Crime category",
        options=category_options,
        default=category_options,
    )

    filtered_map_df = map_data.copy()

    if selected_month != "All":
        filtered_map_df = filtered_map_df[
            filtered_map_df["crime_month"] == selected_month
        ]

    if selected_categories:
        filtered_map_df = filtered_map_df[
            filtered_map_df["crime_category"].isin(selected_categories)
        ]
    else:
        filtered_map_df = filtered_map_df.iloc[0:0]

    kpi_col1, kpi_col2, kpi_col3 = st.columns(3)
    kpi_col1.metric("Mapped crimes", f"{len(filtered_map_df):,}")
    kpi_col2.metric("Crime categories selected", f"{len(selected_categories):,}")
    kpi_col3.metric("Months available", f"{len(month_options):,}")

    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Crime map views")

        if filtered_map_df.empty:
            st.info("No map data for the selected filters.")
        else:
            map_df = filtered_map_df.rename(
                columns={
                    "crime_latitude": "lat",
                    "crime_longitude": "lon",
                }
            ).copy()

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

            st.components.v1.html(m._repr_html_(), height=420)

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
            )

            fig.update_layout(
                margin={"r": 0, "t": 0, "l": 0, "b": 0},
                legend_title_text="Crime category",
                height=420,
            )

            st.plotly_chart(fig, use_container_width=True)

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
        else:
            filtered_timing_df = timing_df.copy()

            if selected_month != "All":
                filtered_timing_df = filtered_timing_df[
                    filtered_timing_df["crime_month"] == selected_month
                ]

            if selected_categories:
                filtered_timing_df = filtered_timing_df[
                    filtered_timing_df["crime_category"].isin(selected_categories)
                ]
            else:
                filtered_timing_df = filtered_timing_df.iloc[0:0]

            if filtered_timing_df.empty:
                st.info("No outcome timing data for the selected filters.")
            else:
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
                    hover_data={
                        "total_crimes": True,
                        "median_months_to_outcome": ":.2f",
                        "min_months_to_outcome": True,
                        "max_months_to_outcome": True,
                        "weighted_months": False,
                    },
                )

                fig_bar.update_layout(
                    xaxis_title="Crime category",
                    yaxis_title="Average months to latest outcome",
                    xaxis_tickangle=45,
                    margin={"r": 0, "t": 0, "l": 0, "b": 0},
                    height=420,
                    coloraxis_showscale=False,
                )

                st.plotly_chart(fig_bar, use_container_width=True)

                st.markdown("**Average outcome timing trends over time**")
                st.caption(
                    "Tracks how average months to latest outcome change across crime months "
                    "for each selected category."
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
                    hover_data={
                        "crime_count": True,
                        "weighted_months": False,
                    },
                )

                fig_line.update_layout(
                    xaxis_title="Crime month",
                    yaxis_title="Average months to latest outcome",
                    margin={"r": 0, "t": 0, "l": 0, "b": 0},
                    height=420,
                    legend_title_text="Crime category",
                )

                st.plotly_chart(fig_line, use_container_width=True)

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

    st.subheader("About this dashboard")
    st.write(
        """
        This dashboard shows crime locations from `mart_crime_map`
        and outcome timing metrics from `mart_crime_outcome_timings`.
        Outcome timing is measured in whole months, because the public
        Police data provides crime dates at month level rather than
        exact offence dates.
        Use the filters in the sidebar to focus on specific months
        and crime categories.
        """
    )


if __name__ == "__main__":
    main()