import streamlit as st
import pyspark
import subprocess
import sys
import os
import pandas as pd
import json
import duckdb
import glob
from pymongo import MongoClient

import plotly.express as px
# Add the root directory to sys.path to allow imports
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
from utils.batch_ingestion.batch_ingestion import BatchIngestion
from utils.batch_ingestion.delta_manager import DeltaLakeManager

if 'delta_manager' not in st.session_state:
    st.session_state['delta_manager'] = DeltaLakeManager()

manager = st.session_state['delta_manager']


# Set up the page config for a good layout
st.set_page_config(page_title="Climate Change Impact Analysis", layout="wide", initial_sidebar_state="expanded")

# Set the theme to dark using Streamlit's theme options
st.markdown(
    """
    <style>
        body {
            background-color: #121212;
            color: white;
        }
        .sidebar .sidebar-content {
            background-color: #2C2C2C;
            color: white;
        }
        .stApp {
            background-color: #121212;
        }
        .stTextInput input, .stSelectbox select, .stButton button {
            background-color: #2C2C2C;
            color: white;
        }
        .stMarkdown {
            color: #A0A0A0;
        }
        .stTitle {
            color: white;
        }
        .stHeader {
            color: white;
        }
        .stSubheader {
            color: #A0A0A0;
        }
        .stFooter {
            color: #A0A0A0;
        }
    </style>
    """, unsafe_allow_html=True
)

# Title of the main page
st.title("Climate Change Impact Analysis: Big Data Management")


# Sidebar for navigation
st.sidebar.title("Navigation")
app_option = st.sidebar.selectbox(
    "Select an application:",
    ("Overview", "Landing Zone Inspection","Trusted Zone Inspection","Exploitation Zone Inspection","Streaming Data Pipeline", "Batch Data Consumption")
)

# Main page content based on the selected application
if app_option == "Overview":
    st.subheader("Welcome to the Climate Change Impact Analysis Project")
    st.markdown("""
    ## Project Overview
    Climate change represents one of the most significant challenges of our time, influencing various aspects of global society, economies, and the environment.
    This project seeks to integrate diverse data sources—structured, semi-structured, and unstructured—into a cohesive pipeline, offering valuable insights into the interconnectedness of climate data, socioeconomic factors, and public sentiment.
    
    By leveraging advanced analytical techniques and predictive models, the project aims to assess the far-reaching impacts of climate change, providing a data-driven foundation for informed decision-making and policy formulation.

    ## Data Pipeline Zones Overview

    - **Landing Zone:**  
      This is the initial ingestion layer where raw data lands in its original format. It includes diverse data types such as CSV files, JSON logs, or streaming data collected from various external sources. The data here is immutable and stored in a Delta Lake format, ensuring traceability and enabling reprocessing if needed.

    - **Trusted Zone:**  
      In this zone, data undergoes cleaning, validation, and standardization. The goal is to ensure high data quality by removing inconsistencies, handling missing values, and conforming to a unified schema. The trusted zone stores refined data in DuckDB databases, making it ready for analytical transformations.

    - **Exploitation Zone:**  
      This is the analytics-ready layer where data is aggregated, enriched, and modeled for reporting, visualization, and advanced analysis. Data is organized into star schemas or denormalized tables optimized for fast querying. It supports business intelligence dashboards, machine learning models, and domain-specific insights.

    - **Streaming Data Pipeline (Data Consumption):**  
      A specialized real-time ingestion and processing subsystem designed to capture live data streams (e.g., social media posts, sensor feeds). This pipeline leverages Kafka producers and consumers to deliver continuous data flow, integrating real-time analytics capabilities into the overall framework.

    - **Batch Data Consumption:**  
      Module including dashbords pulling data from the exploitation zone. 

    Together, these zones form a robust, scalable backbone that supports end-to-end big data management — from raw data ingestion to actionable insights.
    """)

    # Footer with credits
    st.markdown("""
        ---
        ### Project Credits:
        - **Anna Monsó**, **Eduardo Tejero**, **Joan Acero** 
    """)


###################################################
#               LANDING ZONE MANAGEMENT           #
###################################################

# Delta Lake Management Page
if app_option == "Landing Zone Inspection":
    st.subheader("🔧 Landing Zone Management Console")
    st.markdown("""
    Welcome to the **Data Lake Management** portal. Here you can maintain the integrity of your **Delta Lake** storage, monitor current datasets, and ensure smooth data ingestion into the pipeline.
    
    Use the tools below to **reset** the data lake when needed and to **inspect existing data** stored in the **landing zone**.
    """)

    # View Current Data Section
    st.markdown("### 📂 View Data Lake Contents")
    st.markdown("""
    Curious about what's currently in the **Delta Lake**? Click the button below to explore the **datasets** stored in the **landing zone**.  
    This helps ensure **data visibility** and supports auditing and verification processes across your pipeline.
    """)
    if st.button("See Existing Data in the Data Lake"):
        with st.spinner("Loading data..."):
            table_heads = manager.display_existing_data()
            if type(table_heads) != str:
                for table_name, head in table_heads.items():
                    st.subheader(f"📄 Table: `{table_name}`")
                    st.dataframe(head)
            else:
                st.warning("⚠️ No data found in the Data Lake. Try ingesting some data first!")

    st.markdown("---")

    # Reset Data Lake Section
    st.markdown("### ♻️ Reset Data Lake")
    st.markdown("Need a fresh start? This will remove all data from the Delta Lake and reset it to a clean state.")
    if st.button("Reset Data Lake"):
        with st.spinner("Resetting data lake..."):
            manager.reset_delta_lake()
            st.success("✅ Data Lake has been reset successfully!")


    st.markdown("---")


###################################################
#               TRUSTED ZONE MANAGEMENT           #
###################################################

if app_option == "Trusted Zone Inspection":

    st.subheader("🔒 Trusted Zone Management Console")
    st.markdown("""
    Manage and inspect the contents of the **Trusted Zone**. View the current data or clear all .duckdb files in the trusted directory.
    """)

    trusted_files = glob.glob("duckdb_data/trusted/*.duckdb")
    
    # View Trusted Zone Data
    st.markdown("### 📂 View Trusted Zone Contents")
    if st.button("See Existing Data in Trusted Zone"):
        print(trusted_files)
        if not trusted_files:
            st.warning("⚠️ No DuckDB files found in the Trusted Zone.")
        for db_file in trusted_files:
            st.markdown(f"#### 📄 File: `{os.path.basename(db_file)}`")
            try:
                con = duckdb.connect(db_file)
                tables = con.execute("SHOW TABLES").fetchall()
                if not tables:
                    st.info("ℹ️ No tables found in this database.")
                for table in tables:
                    df = con.execute(f"SELECT * FROM {table[0]}").df()
                    st.markdown(f"**Table: `{table[0]}`**")
                    st.write(f"Shape: {df.shape[0]} rows x {df.shape[1]} columns")
                    st.dataframe(df, use_container_width=True)
                con.close()
            except Exception as e:
                st.error(f"❌ Failed to read `{db_file}`: {e}")
    
    st.markdown("---")

    st.markdown("### ♻️ Reset Trusted Zone")
    st.markdown("Need a fresh start? This will remove all data from the Trusted Zone and reset it to a clean state.")
    if st.button("Reset Trusted Zone"):
        for db_file in glob.glob("duckdb_data/trusted/*.duckdb"):
            conn = duckdb.connect(db_file)
            tables = conn.execute("SHOW TABLES").fetchall()
            for (table_name,) in tables:
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.close()
        st.info("ℹ️ Trusted Zone cleaned.")

    st.markdown("---")

###################################################
#          EXPLOITATION ZONE MANAGEMENT           #
###################################################

if app_option == "Exploitation Zone Inspection":
    st.subheader("🔧 Exploitation Zone Management Console")
    st.markdown("""
    Manage and inspect the contents of the **Exploitation Zone**. View the current data or clear all .duckdb files in the trusted directory.
    """)

    trusted_files = glob.glob("duckdb_data/exploitation/*.duckdb")
    
    # View Trusted Zone Data
    st.markdown("### 📂 View Exploitation Zone Contents")
    if st.button("See Existing Data in Exploitation Zone"):
        print(trusted_files)
        if not trusted_files:
            st.warning("⚠️ No DuckDB files found in the Exploitation Zone.")
        for db_file in trusted_files:
            st.markdown(f"#### 📄 File: `{os.path.basename(db_file)}`")
            try:
                con = duckdb.connect(db_file)
                tables = con.execute("SHOW TABLES").fetchall()
                if not tables:
                    st.info("ℹ️ No tables found in this database.")
                for table in tables:
                    df = con.execute(f"SELECT * FROM {table[0]}").df()
                    st.markdown(f"**Table: `{table[0]}`**")
                    st.write(f"Shape: {df.shape[0]} rows x {df.shape[1]} columns")
                    st.dataframe(df, use_container_width=True)
                con.close()
            except Exception as e:
                st.error(f"❌ Failed to read `{db_file}`: {e}")

    st.markdown("---")

    st.markdown("### ♻️ Reset Trusted Zone")
    st.markdown("Need a fresh start? This will remove all data from the Exploitation Zone and reset it to a clean state.")
    if st.button("Reset Exploitation Zone"):
        for db_file in glob.glob("duckdb_data/exploitation/*.duckdb"):
            conn = duckdb.connect(db_file)
            tables = conn.execute("SHOW TABLES").fetchall()
            for (table_name,) in tables:
                conn.execute(f"DROP TABLE IF EXISTS {table_name}")
            conn.close()
        st.info("ℹ️ Exploitation Zone cleaned.")

    st.markdown("---")

###################################################
#             REAL TIME DATA PIPELINE             #
###################################################

if app_option == "Streaming Data Pipeline":

    # Initialize session state
    for key in ["bluesky_producer_process", "bluesky_streaming_process", "reddit_producer_process", "reddit_streaming_process"]:
        if key not in st.session_state:
            st.session_state[key] = None

    st.title(" Real-Time Data Pipeline")

    # Start Button
    if st.button("▶️ Start Ingesting Real-Time Data"):
        with st.spinner("Starting Kafka Producer and Streaming..."):
            try:
                
                # Bluesky producer
                bluesky_proc = subprocess.Popen(
                    ["python", "./utils/real_time_ingestion/kafka_producers/kafka_bluesky_producer.py"],
                    stdout=None,
                    stderr=None
                )
                st.session_state.bluesky_producer_process = bluesky_proc
                
                # Bluesky consumer
                bluesky_streaming_proc = subprocess.Popen(
                    ["python", "./utils/real_time_ingestion/kafka_consumers/kafka_bluesky_consumer.py"],
                    stdout=None,
                    stderr=None
                )
                st.session_state.bluesky_streaming_process = bluesky_streaming_proc

                # Reddit producer
                reddit_proc = subprocess.Popen(
                    ["python", "./utils/real_time_ingestion/kafka_producers/kafka_reddit_producer.py"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                st.session_state.reddit_producer_process = reddit_proc
                
                # Reddit consumer
                reddit_streaming_proc = subprocess.Popen(
                    ["python", "./utils/real_time_ingestion/kafka_consumers/kafka_reddit_consumer.py"],
                    stdout=subprocess.PIPE,
                    stderr=subprocess.PIPE
                )
                st.session_state.reddit_streaming_process = reddit_streaming_proc

                st.success("✅ Real-time ingestion started (Producer + Streaming).")
            except Exception as e:
                st.error(f"❌ Failed to start processes: {e}")

    # Stop Button
    if st.button("⛔ Stop Ingesting Real-Time Data"):
        with st.spinner("Stopping Kafka Producer and Streaming..."):
            try:
                # Stop bluesky producer and consumer
                proc = st.session_state.bluesky_producer_process
                if proc and proc.poll() is None:
                    proc.terminate()
                    proc.wait()
                    st.session_state.bluesky_producer_process = None

                proc = st.session_state.bluesky_streaming_process
                if proc and proc.poll() is None:
                    proc.terminate()
                    proc.wait()
                    st.session_state.bluesky_streaming_process = None

                # Stop reddit producer and consumer
                proc = st.session_state.reddit_producer_process
                if proc and proc.poll() is None:
                    proc.terminate()
                    proc.wait()
                    st.session_state.reddit_producer_process = None

                proc = st.session_state.reddit_streaming_process
                if proc and proc.poll() is None:
                    proc.terminate()
                    proc.wait()
                    st.session_state.reddit_streaming_process = None

                st.success("🛑 Real-time ingestion stopped (Producer + Streaming).")
            except Exception as e:
                st.error(f"❌ Failed to stop processes: {e}")

    # Preview Real-time Ingested Data
    streaming_path = "./landing_zone/streaming/"
    json_files = [f for f in os.listdir(streaming_path) if f.endswith(".json") or f.endswith(".jsonl")]
    if st.button("🔍 Inspect Real Time Ingested Data (stop the ingestion first)"):
        if json_files:
            for json_file in json_files:
                st.subheader(f"📄 File: `{json_file}`")

                try:
                    file_path = os.path.join(streaming_path, json_file)
                    with open(file_path, "r", encoding="utf-8") as f:
                        lines = f.readlines()
                        # Parse each line as JSON
                        data = [json.loads(line) for line in lines]

                    # Convert to DataFrame for display
                    df = pd.DataFrame(data)
                    if not df.empty:
                        st.dataframe(df.tail())  # Show only first few rows
                    else:
                        st.info("ℹ️ No data was gathered during this call.")
                except Exception as e:
                    st.error(f"❌ Error reading `{json_file}`: {e}")
        else:
            st.warning("⚠️ No JSON files found in `./landing_zone/streaming/`. Try ingesting data first!")
    
    st.divider()
    st.subheader("📊 City Sentiment Dashboard")

    if st.button("🔄 Refresh Data from Bluesky"):
        try:
            # Conexión a MongoDB
            client = MongoClient("mongodb://mongo:27017")
            db = client["bluesky_db"]
            collection = db["city_sentiment_stats"]

            # Obtener todos los datos
            data = list(collection.find({}))

            if data:
                df = pd.DataFrame(data)
                df = df.drop(columns=["_id", "last_updated"], errors="ignore")  # limpiar campos internos

                # Mostrar tabla
                st.markdown("### Sentiment Summary per City")
                st.dataframe(df)

                # Mostrar gráfico
                st.markdown("### 🏆 City Sentiment Ranking (positive - negative)")

                # Compute ranking score
                # Ensure no NaNs interfere
                df["positive"] = df["positive"].fillna(0)
                df["negative"] = df["negative"].fillna(0)

                # Now compute the score safely
                df["ranking_score"] = df["positive"] - df["negative"]


                # Sort by ranking_score descending
                df_sorted = df.sort_values(by="ranking_score", ascending=False).reset_index(drop=True)

                # Display as a plain ordered list
                
                medals = ["🥇", "🥈", "🥉"]

                for i, row in df_sorted.iterrows():
                    city = row["city"]
                    score = row["ranking_score"]
                    medal = medals[i] if i < 3 else ""
                    st.markdown(f"**{i+1}. {medal} {city}** — sentiment score: `{int(score)}`")


            else:
                st.info("No data found in MongoDB to display.")
        except Exception as e:
            st.error(f"❌ Failed to fetch data from MongoDB {e}")

    st.divider()
    st.subheader("🗒️ Recently Collected Posts")

    # Load the enriched post data
    try:
        exploitation_file = "./exploitation_zone/sentiment_analysis.json"
        if os.path.exists(exploitation_file):
            with open(exploitation_file, "r", encoding="utf-8") as f:
                lines = f.readlines()
                data = [json.loads(line) for line in lines if line.strip()]
            
            # Sort by timestamp (if needed)
            df_posts = pd.DataFrame(data)
            if "created_at" in df_posts.columns:
                df_posts["created_at"] = pd.to_datetime(df_posts["created_at"])
                df_posts = df_posts.sort_values("created_at", ascending=False)

            # Show latest 10
            st.dataframe(df_posts[["created_at", "city", "assessment", "score","text"]].head(10), use_container_width=True)
        else:
            st.info("No enriched posts found yet.")
    except Exception as e:
        st.error(f"Error loading posts: {e}")
    # Status Display
    st.divider()
    st.subheader("Current Process Status")
    prod_status = "🟢 Running" if (
        st.session_state.bluesky_producer_process and st.session_state.bluesky_producer_process.poll() is None
    ) else "🔴 Stopped"
    stream_status = "🟢 Running" if (
        st.session_state.bluesky_streaming_process and st.session_state.bluesky_streaming_process.poll() is None
    ) else "🔴 Stopped"

    st.write(f"**Kafka Producer:** {prod_status}")
    st.write(f"**Kafka Streaming:** {stream_status}")
    

###################################################
#                 DATA CONSUMPTION                #
###################################################

if app_option == "Batch Data Consumption":
    #todo
    st.title("Batch Data Consumption")
    # place here all dashboards
    exploitation_files = glob.glob("duckdb_data/exploitation/*.duckdb")
    
    # View Exploitation Zone Data
    if not exploitation_files:
        st.warning("⚠️ No DuckDB files found in the Exploitation Zone.")
    else:
        for db_file in exploitation_files:
            st.markdown(f"#### 📄 File: `{os.path.basename(db_file)}`")
            try:
                con = duckdb.connect(db_file)
                tables = con.execute("SHOW TABLES").fetchall()
                if not tables:
                    st.info("ℹ️ No tables found in this database.")
                for table in tables:
                    df = con.execute(f"SELECT * FROM {table[0]}").df()

                    # Visualization selector for numerical columns
                    numeric_cols = df.select_dtypes(include=['number']).columns.tolist()
                    if numeric_cols:
                        st.markdown("#### Data Visualization")

                        # Determine default visualization and default columns
                        if len(numeric_cols) >= 2:
                            default_viz = "Scatter Plot"
                            default_col_x = numeric_cols[0]
                            default_col_y = numeric_cols[1]
                        else:
                            default_viz = "Histogram"
                            default_col_x = numeric_cols[0]

                        viz_option = st.selectbox(
                            f"Select visualization type for `{table[0]}`",
                            ("None", "Histogram", "Scatter Plot"),
                            index=["None", "Histogram", "Scatter Plot"].index(default_viz),
                            key=f"viz_option_{table[0]}"
                        )

                        if viz_option == "Histogram":
                            col = st.selectbox(
                                f"Select numerical column for histogram in `{table[0]}`",
                                numeric_cols,
                                index=numeric_cols.index(default_col_x),
                                key=f"hist_col_{table[0]}"
                            )
                            fig = px.histogram(df, x=col, nbins=30, title=f"Histogram of {col} in {table[0]}")
                            st.plotly_chart(fig, use_container_width=True)

                        elif viz_option == "Scatter Plot":
                            if len(numeric_cols) >= 2:
                                col_x = st.selectbox(
                                    f"Select X-axis numerical column for scatter plot in `{table[0]}`",
                                    numeric_cols,
                                    index=numeric_cols.index(default_col_x),
                                    key=f"scatter_x_{table[0]}"
                                )
                                col_y = st.selectbox(
                                    f"Select Y-axis numerical column for scatter plot in `{table[0]}`",
                                    numeric_cols,
                                    index=numeric_cols.index(default_col_y),
                                    key=f"scatter_y_{table[0]}"
                                )
                                fig = px.scatter(df, x=col_x, y=col_y, title=f"Scatter Plot of {col_x} vs {col_y} in {table[0]}")
                                st.plotly_chart(fig, use_container_width=True)
                            else:
                                st.info("ℹ️ Not enough numerical columns for scatter plot.")
                    else:
                        st.info("ℹ️ No numerical columns available for visualization in this table.")

                con.close()
            except Exception as e:
                st.error(f"❌ Failed to read `{db_file}`: {e}")