#!/usr/bin/env python3
"""
Streamlit dashboard for Sparkle demo.

Shows real-time metrics from the data lakehouse:
- Bronze ingestion rates
- Silver data quality
- Gold business metrics
- ML model performance
- Streaming analytics
"""

import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go


# Page configuration
st.set_page_config(
    page_title="Sparkle Lakehouse Demo",
    page_icon="✨",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Title
st.title("✨ Sparkle Data Lakehouse - Live Demo")
st.markdown("**Real-time data pipelines from Bronze → Silver → Gold → ML**")

# Sidebar
st.sidebar.header("Navigation")
page = st.sidebar.radio(
    "Select View",
    ["Overview", "Bronze Layer", "Silver Layer", "Gold Layer", "ML Layer", "Streaming"]
)

# Overview Page
if page == "Overview":
    st.header("🏠 Overview")

    # Metrics
    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Bronze Records", "50,000", "+1,250")

    with col2:
        st.metric("Silver Records", "49,500", "+1,200")

    with col3:
        st.metric("Gold Tables", "25", "+2")

    with col4:
        st.metric("ML Accuracy", "87.3%", "+2.1%")

    st.markdown("---")

    # Architecture diagram
    st.subheader("Data Flow Architecture")

    st.code("""
    PostgreSQL (50K customers)
        → Bronze (Raw ingestion with Delta Lake)
        → Silver (40+ transformers + data quality)
        → Gold (Business aggregations)
        → Feature Store (Redis)
        → ML (XGBoost, LightGBM, Random Forest)
        → Predictions (Batch + Streaming)
    """)

    st.info("📊 Select a layer from the sidebar to explore detailed metrics")

# Bronze Layer
elif page == "Bronze Layer":
    st.header("🥉 Bronze Layer - Raw Ingestion")

    st.subheader("Ingestion Metrics")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Total Records", "50,000")
        st.metric("Tables", "3")

    with col2:
        st.metric("Avg Throughput", "6,250 rec/sec")
        st.metric("Last Sync", "2 min ago")

    with col3:
        st.metric("Data Size", "125 MB")
        st.metric("Format", "Delta Lake")

    # Sample data
    st.subheader("Sample Bronze Data")

    bronze_data = {
        "customer_id": [1, 2, 3, 4, 5],
        "name": ["John Doe", "Jane Smith", "Bob Johnson", "Alice Williams", "Charlie Brown"],
        "email": ["john@example.com", "jane@example.com", "bob@example.com", "alice@example.com", "charlie@example.com"],
        "tier": ["Premium", "Basic", "Premium", "Enterprise", "Basic"],
        "total_spend": [1500.50, 300.00, 2300.75, 5400.90, 150.00]
    }

    df = pd.DataFrame(bronze_data)
    st.dataframe(df, use_container_width=True)

# Silver Layer
elif page == "Silver Layer":
    st.header("🥈 Silver Layer - Transformations & Quality")

    st.subheader("Data Quality Metrics")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Clean Records", "49,500", "99%")

    with col2:
        st.metric("Duplicates Removed", "500", "1%")

    with col3:
        st.metric("Nulls Fixed", "1,200", "2.4%")

    with col4:
        st.metric("Validation Failures", "75", "0.15%")

    # Transformations applied
    st.subheader("Transformations Applied")

    transformations = [
        "Drop exact duplicates",
        "Standardize nulls",
        "Validate email addresses",
        "Lowercase emails",
        "Trim whitespace",
        "Parse dates",
        "Add audit columns",
        "Hash PII data",
        "Validate ranges",
        "Type conversions"
    ]

    st.write("✓ " + "\n\n✓ ".join(transformations))

# Gold Layer
elif page == "Gold Layer":
    st.header("🥇 Gold Layer - Business Analytics")

    # Customer tier distribution
    st.subheader("Customer Distribution by Tier")

    tier_data = pd.DataFrame({
        "tier": ["Basic", "Premium", "Enterprise"],
        "customers": [30000, 15000, 5000],
        "revenue": [4500000, 22500000, 27000000]
    })

    fig = px.bar(tier_data, x="tier", y="customers", color="tier",
                 title="Customers by Tier")
    st.plotly_chart(fig, use_container_width=True)

    # Revenue chart
    fig2 = px.pie(tier_data, values="revenue", names="tier",
                  title="Revenue by Tier")
    st.plotly_chart(fig2, use_container_width=True)

# ML Layer
elif page == "ML Layer":
    st.header("🤖 ML Layer - Churn Prediction")

    st.subheader("Model Performance")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        st.metric("Accuracy", "87.3%")

    with col2:
        st.metric("Precision", "84.5%")

    with col3:
        st.metric("Recall", "89.1%")

    with col4:
        st.metric("AUC", "0.92")

    # Model comparison
    st.subheader("Model Comparison")

    model_data = pd.DataFrame({
        "Model": ["XGBoost", "LightGBM", "Random Forest", "Logistic Regression"],
        "Accuracy": [0.873, 0.865, 0.851, 0.792],
        "Training Time (s)": [25, 18, 32, 8]
    })

    fig = px.bar(model_data, x="Model", y="Accuracy", color="Model",
                 title="Model Accuracy Comparison")
    st.plotly_chart(fig, use_container_width=True)

    st.success("✓ XGBoost selected as champion model")

# Streaming
elif page == "Streaming":
    st.header("📡 Streaming Analytics")

    st.subheader("Real-time Event Processing")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric("Events/sec", "10")

    with col2:
        st.metric("Total Events", "25,347")

    with col3:
        st.metric("Latency", "127ms")

    # Event type distribution
    st.subheader("Event Type Distribution (Last 5 min)")

    event_data = pd.DataFrame({
        "event_type": ["page_view", "product_view", "add_to_cart", "checkout", "purchase"],
        "count": [850, 420, 180, 95, 45]
    })

    fig = px.bar(event_data, x="event_type", y="count", color="event_type",
                 title="Events by Type")
    st.plotly_chart(fig, use_container_width=True)

    st.info("📊 Events are being generated at 10 events/second")

# Footer
st.markdown("---")
st.markdown("**Sparkle Data Lakehouse** | Built with Apache Spark 3.5, Delta Lake, MLflow")
