"""Bike-share mobility dashboard (Streamlit).

Reads only the dbt/Bruin marts plus the live view, as the read-only
`dashboard_reader` role created by Terraform.
"""

from __future__ import annotations

import os
from datetime import date, timedelta

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Bike-share Mobility", page_icon="🚲", layout="wide")

# ------------------------------------------------------------------ design tokens
SURFACE = "#fcfcfb"
INK = "#0b0b0b"
INK_2 = "#52514e"
MUTED = "#898781"
GRID = "#e1e0d9"
AXIS = "#c3c2b7"
SERIES = {"member": "#2a78d6", "casual": "#eb6834"}          # categorical slots 1, 2 (validated)
DRY_WET = {"Dry": "#2a78d6", "Wet": "#eb6834"}
DIVERGING = {"neg": "#2a78d6", "mid": "#c3c2b7", "pos": "#e34948"}
SEQUENTIAL = ["#cde2fb", "#9ec5f4", "#6da7ec", "#3987e5", "#256abf", "#184f95", "#0d366b"]
STATUS = {                                                    # status palette, always with a label
    "OK": "#0ca30c",
    "Low bikes or docks": "#fab219",
    "Full": "#ec835a",
    "Empty": "#d03b3b",
    "Offline": MUTED,
}
FONT = 'system-ui, -apple-system, "Segoe UI", sans-serif'

st.markdown(
    f"""
    <style>
      [data-testid="stMetricValue"] {{ font-variant-numeric: normal; }}
      [data-testid="stMetric"] {{ background:{SURFACE}; border:1px solid rgba(11,11,11,.10);
                                  border-radius:8px; padding:12px 16px; }}
      .caption {{ color:{INK_2}; font-size:.9rem; margin-top:-.5rem; }}
    </style>
    """,
    unsafe_allow_html=True,
)


def style(fig: go.Figure, height: int = 360) -> go.Figure:
    fig.update_layout(
        height=height,
        margin=dict(l=8, r=8, t=16, b=8),
        paper_bgcolor=SURFACE,
        plot_bgcolor=SURFACE,
        font=dict(family=FONT, color=INK_2, size=13),
        legend=dict(orientation="h", yanchor="bottom", y=1.02, x=0, title_text="", font=dict(color=INK_2)),
        hoverlabel=dict(bgcolor="white", font=dict(family=FONT, color=INK)),
        barcornerradius=4,
        bargap=0.4,
        bargroupgap=0.08,
    )
    fig.update_xaxes(showgrid=False, linecolor=AXIS, tickfont=dict(color=MUTED), title_font=dict(color=INK_2))
    fig.update_yaxes(gridcolor=GRID, zerolinecolor=AXIS, linecolor=AXIS, tickfont=dict(color=MUTED),
                     title_font=dict(color=INK_2))
    return fig


# ------------------------------------------------------------------------ data
@st.cache_resource
def engine():
    user = os.getenv("PG_READER_USER", "dashboard_reader")
    password = os.getenv("PG_READER_PASSWORD", "")
    host = os.getenv("PG_HOST", "postgres")
    port = os.getenv("PG_PORT", "5432")
    db = os.getenv("PG_DATABASE", "bikeshare")
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}", pool_pre_ping=True)


def query(sql: str, **params) -> pd.DataFrame:
    try:
        with engine().connect() as conn:
            return pd.read_sql(text(sql), conn, params=params)
    except Exception as exc:  # table not built yet
        if "does not exist" in str(exc):
            return pd.DataFrame()
        raise


@st.cache_data(ttl=300)
def load_daily(city: str, start: date, end: date) -> pd.DataFrame:
    return query(
        """select * from marts.mart_daily_city_summary
           where city = :city and start_date between :start and :end order by start_date""",
        city=city, start=start, end=end,
    )


@st.cache_data(ttl=300)
def load_date_bounds(city: str) -> tuple[date, date] | None:
    df = query("select min(start_date) as lo, max(start_date) as hi from marts.mart_daily_city_summary where city = :c",
               c=city)
    if df.empty or pd.isna(df.loc[0, "lo"]):
        return None
    return df.loc[0, "lo"], df.loc[0, "hi"]


@st.cache_data(ttl=300)
def load_table(name: str, city: str) -> pd.DataFrame:
    return query(f"select * from marts.{name} where city = :city", city=city)


@st.cache_data(ttl=30)
def load_live(city: str) -> pd.DataFrame:
    return query("select * from marts.mart_live_station_status where city = :city", city=city)


@st.cache_data(ttl=300)
def load_revenue(city: str, start: date, end: date) -> pd.DataFrame:
    return query(
        """select * from bruin_mart.ebike_single_ride_revenue_daily
           where city = :city and start_date between :start and :end order by start_date""",
        city=city, start=start, end=end,
    )


def table_view(df: pd.DataFrame, label: str = "Show data") -> None:
    with st.expander(label):
        st.dataframe(df, width="stretch", hide_index=True)


def empty_state(what: str) -> None:
    st.info(f"No {what} yet. Run the `platform_backfill` flow in Kestra (http://localhost:8080), "
            "then refresh this page.")


# ---------------------------------------------------------------------- header
st.title("🚲 Bike-share Mobility")
st.markdown(
    '<p class="caption">Citi Bike trip history (Spark → dbt), hourly weather (dlt), and live station status '
    "(Kafka) - one local data platform.</p>",
    unsafe_allow_html=True,
)

# One filter row scoping every chart on the page
f1, f2, _ = st.columns([1, 2, 3])
city_label = f1.selectbox("City", ["Jersey City & Hoboken", "New York City"],
                          index=0 if os.getenv("DEFAULT_CITY", "JC") == "JC" else 1)
city = "JC" if city_label.startswith("Jersey") else "NYC"
bounds = load_date_bounds(city)
if bounds:
    lo, hi = bounds
    picked = f2.date_input("Trip dates", value=(lo, hi), min_value=lo, max_value=hi)
    start, end = (picked if isinstance(picked, tuple) and len(picked) == 2 else (lo, hi))
else:
    start, end = date.today() - timedelta(days=30), date.today()

tab_overview, tab_weather, tab_stations, tab_live, tab_quality = st.tabs(
    ["Demand", "Weather impact", "Stations & rebalancing", "Live network", "Data quality"]
)

# ------------------------------------------------------------------ demand tab
with tab_overview:
    daily = load_daily(city, start, end)
    if daily.empty:
        empty_state(f"trip data for {city_label}")
    else:
        total = int(daily["trips"].sum())
        members = int(daily["member_trips"].sum())
        ebikes = int(daily["ebike_trips"].sum())
        avg_dur = (daily["avg_duration_min"] * daily["trips"]).sum() / max(total, 1)
        busiest = daily.loc[daily["trips"].idxmax()]

        k1, k2, k3, k4 = st.columns(4)
        k1.metric("Trips", f"{total:,}")
        k2.metric("Avg trip duration", f"{avg_dur:.1f} min")
        k3.metric("Member share", f"{100 * members / max(total, 1):.0f}%")
        k4.metric("E-bike share", f"{100 * ebikes / max(total, 1):.0f}%")

        st.subheader("Daily trips by rider type")
        long = daily.melt(id_vars=["start_date"], value_vars=["member_trips", "casual_trips"],
                          var_name="rider", value_name="rider_trips")
        long["rider"] = long["rider"].str.replace("_trips", "")
        fig = px.line(long, x="start_date", y="rider_trips", color="rider", color_discrete_map=SERIES,
                      labels={"start_date": "", "rider_trips": "Trips per day", "rider": "Rider"})
        fig.update_traces(line=dict(width=2), hovertemplate="%{x|%a %d %b %Y}<br>%{y:,} trips<extra>%{fullData.name}</extra>")
        # direct labels at the line ends (2 series)
        last = long[long["start_date"] == long["start_date"].max()]
        for _, row in last.iterrows():
            fig.add_annotation(x=row["start_date"], y=row["rider_trips"], text=f" {row['rider']}", showarrow=False,
                               xanchor="left", font=dict(color=INK_2, size=12))
        fig.update_layout(hovermode="x unified")
        st.plotly_chart(style(fig), width="stretch")
        st.markdown(f'<p class="caption">Busiest day: <b>{busiest["start_date"]:%a %d %b %Y}</b> with '
                    f'{int(busiest["trips"]):,} trips ({busiest["day_weather_type"].lower()}, '
                    f'{busiest["avg_temperature_c"]}°C avg).</p>', unsafe_allow_html=True)
        table_view(daily)

        st.subheader("When do people ride?")
        profile = load_table("mart_hourly_demand_profile", city)
        if not profile.empty:
            rider = st.radio("Rider type", ["member", "casual"], horizontal=True, key="heatmap_rider")
            p = profile[profile["member_type"] == rider]
            grid = p.pivot_table(index="day_name", columns="hour_of_day", values="avg_trips", aggfunc="sum")
            grid = grid.reindex(["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"])
            heat = go.Figure(go.Heatmap(
                z=grid.values, x=[f"{h:02d}" for h in grid.columns], y=grid.index,
                colorscale=[[i / (len(SEQUENTIAL) - 1), c] for i, c in enumerate(SEQUENTIAL)],
                xgap=2, ygap=2, colorbar=dict(title=dict(text="Avg trips", font=dict(color=INK_2)), outlinewidth=0),
                hovertemplate="%{y} %{x}:00<br>%{z:.1f} trips on average<extra></extra>",
            ))
            heat.update_xaxes(title_text="Hour of day")
            st.plotly_chart(style(heat, 300), width="stretch")
            st.markdown('<p class="caption">Average trips starting in each hour. Members show commute peaks; '
                        "casual riders peak on weekend afternoons.</p>", unsafe_allow_html=True)
            table_view(p)

        revenue = load_revenue(city, start, end)
        if not revenue.empty:
            st.subheader("Estimated casual e-bike single-ride revenue")
            r1, r2 = st.columns([1, 3])
            r1.metric("Estimated revenue", f"${revenue['est_revenue_usd'].sum():,.0f}")
            r1.markdown('<p class="caption">Current GBFS single-ride e-bike price applied to casual e-bike trips '
                        "(Bruin asset).</p>", unsafe_allow_html=True)
            rev = px.bar(revenue, x="start_date", y="est_revenue_usd", labels={"start_date": "", "est_revenue_usd": "USD"})
            rev.update_traces(marker_color=SERIES["member"], hovertemplate="%{x|%d %b %Y}<br>$%{y:,.0f}<extra></extra>")
            r2.plotly_chart(style(rev, 260), width="stretch")

# ----------------------------------------------------------------- weather tab
with tab_weather:
    impact = load_table("mart_weather_impact", city)
    daily = load_daily(city, start, end)
    if impact.empty or daily.empty:
        empty_state("weather-joined trip data")
    else:
        st.subheader("Average daytime trips per hour, by temperature")
        day_kind = st.radio("Days", ["Weekdays", "Weekends"], horizontal=True)
        imp = impact[impact["is_weekend"] == (day_kind == "Weekends")].copy()
        imp["Conditions"] = imp["is_wet_hour"].map({True: "Wet", False: "Dry"})
        imp["Temperature"] = imp["temperature_band"].str.split(": ").str[1]
        imp = imp.sort_values("temperature_band")
        bars = px.bar(imp, x="Temperature", y="avg_daytime_trips_per_hour", color="Conditions", barmode="group",
                      color_discrete_map=DRY_WET, category_orders={"Conditions": ["Dry", "Wet"]},
                      labels={"avg_daytime_trips_per_hour": "Avg trips per hour (7:00-22:00)"},
                      custom_data=["observed_hours"])
        bars.update_traces(hovertemplate="%{x} · %{fullData.name}<br>%{y:.1f} trips/hour"
                                         "<br>based on %{customdata[0]} hours<extra></extra>")
        st.plotly_chart(style(bars), width="stretch")
        dry = imp.loc[~imp["is_wet_hour"], "avg_daytime_trips_per_hour"].mean()
        wet = imp.loc[imp["is_wet_hour"], "avg_daytime_trips_per_hour"].mean()
        if pd.notna(dry) and pd.notna(wet) and dry:
            st.markdown(f'<p class="caption">Across temperature bands, wet hours average '
                        f'<b>{100 * (wet - dry) / dry:+.0f}%</b> trips vs dry hours.</p>', unsafe_allow_html=True)
        table_view(imp[["Temperature", "Conditions", "observed_hours", "avg_trips_per_hour", "avg_daytime_trips_per_hour"]])

        st.subheader("Daily trips vs temperature")
        # three groups max: the first three categorical slots are validated for all-pairs (scatter) use
        days = daily.assign(day=daily["day_weather_type"].map(
            {"Dry day": "Dry", "Showers": "Wet", "Rainy day": "Wet", "Snow day": "Snow"})).dropna(subset=["day"])
        scatter = px.scatter(days, x="avg_temperature_c", y="trips", color="day",
                             color_discrete_map={"Dry": "#2a78d6", "Wet": "#eb6834", "Snow": "#1baf7a"},
                             category_orders={"day": ["Dry", "Wet", "Snow"]},
                             labels={"avg_temperature_c": "Average temperature (°C)", "trips": "Trips per day",
                                     "day": "Day"},
                             hover_data={"start_date": "|%a %d %b %Y", "precipitation_mm": True})
        scatter.update_traces(marker=dict(size=9, line=dict(width=2, color=SURFACE)))
        st.plotly_chart(style(scatter), width="stretch")

# ---------------------------------------------------------------- stations tab
with tab_stations:
    reb = load_table("mart_station_rebalancing", city)
    if reb.empty:
        empty_state("station flows")
    else:
        st.subheader("Stations with the largest daily imbalance")
        top = reb.nsmallest(15, "imbalance_rank").sort_values("avg_daily_net_flow")
        top["direction"] = top["avg_daily_net_flow"].apply(lambda v: "Loses bikes" if v < 0 else "Gains bikes")
        div = px.bar(top, x="avg_daily_net_flow", y="station_name", orientation="h", color="direction",
                     color_discrete_map={"Loses bikes": DIVERGING["neg"], "Gains bikes": DIVERGING["pos"]},
                     labels={"avg_daily_net_flow": "Average daily net flow (arrivals − departures)", "station_name": ""},
                     custom_data=["departures", "arrivals", "active_days"])
        div.update_traces(hovertemplate="<b>%{y}</b><br>net %{x:+.1f} bikes/day<br>%{customdata[0]:,} departures · "
                                        "%{customdata[1]:,} arrivals<br>%{customdata[2]} active days<extra></extra>")
        div.update_yaxes(gridcolor=SURFACE)
        st.plotly_chart(style(div, 480), width="stretch")
        st.markdown('<p class="caption">Blue stations drain (riders leave from them and end elsewhere) and need bikes '
                    "trucked in; red stations fill up and need bikes removed.</p>", unsafe_allow_html=True)

        mapped = reb.dropna(subset=["latitude", "longitude"])
        if not mapped.empty:
            st.subheader("Rebalancing profile map")
            fig = px.scatter_map(
                mapped, lat="latitude", lon="longitude", color="rebalancing_profile",
                color_discrete_map={"Drains (needs bikes)": DIVERGING["neg"], "Balanced": DIVERGING["mid"],
                                    "Fills up (needs docks)": DIVERGING["pos"]},
                size=mapped["avg_abs_daily_imbalance"].clip(lower=0.5), size_max=14, zoom=12 if city == "JC" else 11,
                hover_name="station_name",
                hover_data={"avg_daily_net_flow": ":+.1f", "departures": ":,", "latitude": False, "longitude": False},
                map_style="carto-positron", labels={"rebalancing_profile": "Profile"},
            )
            st.plotly_chart(style(fig, 520), width="stretch")
        table_view(reb.sort_values("imbalance_rank"))

# -------------------------------------------------------------------- live tab
with tab_live:

    @st.fragment(run_every=60)
    def live_panel() -> None:
        live = load_live(city)
        if live.empty:
            empty_state("live station data (is the producer running?)")
            return
        newest = live["minutes_since_report"].min()
        st.markdown(f'<p class="caption">Streaming from Kafka · auto-refreshes every 60 s · newest report '
                    f"{newest:.0f} min ago</p>", unsafe_allow_html=True)
        counts = live["availability_status"].value_counts()
        c1, c2, c3, c4, c5 = st.columns(5)
        c1.metric("Bikes available", f"{int(live['num_bikes_available'].sum()):,}")
        c2.metric("E-bikes available", f"{int(live['num_ebikes_available'].sum()):,}")
        c3.metric("🔴 Empty stations", int(counts.get("Empty", 0)))
        c4.metric("🟠 Full stations", int(counts.get("Full", 0)))
        c5.metric("⚪ Offline stations", int(counts.get("Offline", 0)))

        order = ["OK", "Low bikes or docks", "Full", "Empty", "Offline"]
        on_map = live.dropna(subset=["latitude", "longitude"]).assign(
            availability_status=lambda d: d["availability_status"].replace({"Low bikes": "Low bikes or docks",
                                                                             "Low docks": "Low bikes or docks"}))
        fig = px.scatter_map(
            on_map, lat="latitude", lon="longitude",
            color="availability_status", color_discrete_map=STATUS, category_orders={"availability_status": order},
            hover_name="station_name",
            hover_data={"num_bikes_available": True, "num_ebikes_available": True, "num_docks_available": True,
                        "minutes_since_report": True, "latitude": False, "longitude": False},
            zoom=12 if city == "JC" else 11, map_style="carto-positron",
            labels={"availability_status": "Status", "num_bikes_available": "Bikes", "num_ebikes_available": "E-bikes",
                    "num_docks_available": "Free docks", "minutes_since_report": "Minutes since report"},
        )
        fig.update_traces(marker=dict(size=9))
        st.plotly_chart(style(fig, 560), width="stretch")
        table_view(live.sort_values(["availability_status", "station_name"]), "Show all stations")

    live_panel()

# ----------------------------------------------------------------- quality tab
with tab_quality:
    st.subheader("Pipeline health")
    loads = load_table("mart_pipeline_load_audit", city)
    if not loads.empty:
        st.markdown("**Monthly trip loads** (Spark quality rules: valid timestamps, 1 min - 24 h, deduplicated)")
        st.dataframe(loads.sort_values("source_month"), width="stretch", hide_index=True)
    audit = query("select * from bruin_mart.network_health_snapshots where city = :c order by snapshot_at desc limit 48",
                  c=city)
    if not audit.empty:
        st.markdown("**Network health snapshots** (Bruin, appended on every run)")
        if audit["snapshot_at"].nunique() < 2:
            # a single point makes an empty-looking line chart; show the number instead
            latest = audit.iloc[0]
            m1, m2, m3 = st.columns(3)
            m1.metric("% of stations empty", f"{latest['pct_stations_empty']:.1f}%")
            m2.metric("Bikes available", f"{int(latest['bikes_available']):,}")
            m3.metric("Snapshot taken", f"{latest['snapshot_at']:%d %b %H:%M}")
            st.markdown('<p class="caption">The trend chart appears once the daily Bruin run has added a second '
                        "snapshot.</p>", unsafe_allow_html=True)
        else:
            hist = px.line(audit.sort_values("snapshot_at"), x="snapshot_at", y="pct_stations_empty", markers=True,
                           labels={"snapshot_at": "", "pct_stations_empty": "% of stations empty"})
            hist.update_traces(line=dict(color=SERIES["member"], width=2), marker=dict(size=8))
            st.plotly_chart(style(hist, 260), width="stretch")
        table_view(audit)
    else:
        st.info("No Bruin snapshots yet - run the `bruin_pipeline` flow.")
