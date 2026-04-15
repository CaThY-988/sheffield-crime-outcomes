import os

import pandas as pd
import streamlit as st
from databricks import sql
from dotenv import load_dotenv
import matplotlib.pyplot as plt


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
        avg_days_to_outcome,
        median_days_to_outcome,
        min_days_to_outcome,
        max_days_to_outcome
    from workspace.analytics_police.mart_crime_outcome_timings
    """
    df = run_query(query)

    if df.empty:
        return df

    numeric_cols = [
        "crime_count",
        "avg_days_to_outcome",
        "median_days_to_outcome",
        "min_days_to_outcome",
        "max_days_to_outcome",
    ]

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce")

    df["crime_month"] = df["crime_month"].astype(str)

    return df


def main():
    st.title("Sheffield Crime Dashboard")
    st.caption("Data Zoomcamp project using dbt, Databricks, and Streamlit")

    map_df = load_map_data()
    timing_df = load_timing_data()

    if map_df.empty:
        st.error("No data returned from mart_crime_map.")
        return

    if timing_df.empty:
        st.warning("No data returned from mart_crime_outcome_timings.")

    st.sidebar.header("Filters")

    month_options = sorted(map_df["crime_month"].dropna().unique().tolist())
    selected_month = st.sidebar.selectbox(
        "Crime month",
        options=["All"] + month_options,
        index=0,
    )

    category_options = sorted(map_df["crime_category"].dropna().unique().tolist())
    selected_categories = st.sidebar.multiselect(
        "Crime category",
        options=category_options,
        default=category_options,
    )

    filtered_map_df = map_df.copy()

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
    kpi_col2.metric(
        "Crime categories selected",
        f"{len(selected_categories):,}",
    )
    kpi_col3.metric(
        "Months available",
        f"{len(month_options):,}",
    )

    left_col, right_col = st.columns(2)

    with left_col:
        st.subheader("Crime map")

        if filtered_map_df.empty:
            st.info("No map data for the selected filters.")
        else:
            map_plot_df = filtered_map_df.rename(
                columns={
                    "crime_latitude": "lat",
                    "crime_longitude": "lon",
                }
            )[["lat", "lon"]]

            st.map(map_plot_df, use_container_width=True)

            with st.expander("Preview mapped records"):
                st.dataframe(
                    filtered_map_df[
                        [
                            "crime_month",
                            "crime_category",
                            "crime_street_name",
                            "latest_outcome_category",
                        ]
                    ].head(20),
                    use_container_width=True,
                )

    with right_col:
        st.subheader("Median days to outcome by crime category")

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
                st.info("No timing data for the selected filters.")
            else:
                chart_df = (
                    filtered_timing_df.groupby("crime_category", as_index=False)
                    .agg(
                        median_days_to_outcome=("median_days_to_outcome", "mean"),
                        crime_count=("crime_count", "sum"),
                    )
                    .sort_values("median_days_to_outcome", ascending=False)
                )

                fig, ax = plt.subplots(figsize=(10, 6))
                ax.bar(
                    chart_df["crime_category"],
                    chart_df["median_days_to_outcome"],
                )
                ax.set_xlabel("Crime category")
                ax.set_ylabel("Median days to outcome")
                ax.set_title("Median days to outcome by crime category")
                plt.xticks(rotation=45, ha="right")
                plt.tight_layout()

                st.pyplot(fig, use_container_width=True)

                with st.expander("Preview timing summary"):
                    st.dataframe(chart_df, use_container_width=True)

    st.subheader("About this dashboard")
    st.write(
        """
        This dashboard shows crime locations from `mart_crime_map`
        and outcome timing metrics from `mart_crime_outcome_timings`.
        Use the filters in the sidebar to focus on specific months
        and crime categories.
        """
    )


if __name__ == "__main__":
    main()