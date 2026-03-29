# PhonePe Pulse Transaction Insights (2018-2024) 💸

A comprehensive data engineering and analytics project to visualize and explore PhonePe's transaction, user, and insurance data across India.

## 🌟 Project Overview
This project performs an end-to-end analysis of the PhonePe Pulse dataset. It includes a robust ETL pipeline to extract data from multiple JSON structures, a structured SQLite database for high-performance querying, a master Exploratory Data Analysis (EDA) notebook, and an interactive Streamlit dashboard for real-time insights.

<img width="1856" height="927" alt="image" src="https://github.com/user-attachments/assets/3c5ae3d5-b9b5-4ef2-b5ac-bf180d9a0167" />

### 🍱 Main Features
- **ETL Pipeline**: Automated extraction and cleaning of 1.5 million+ records from the PhonePe Pulse repository.
- **Master EDA**: 20+ comprehensive visualizations addressing 5 key business cases including transaction dynamics, device dominance, and insurance penetration.
- **Interactive Dashboard**: A premium Streamlit UI with animated choropleth maps, growth charts, and performance leaderboards.
- **Multi-Level Insights**: Drill down from country-level trends to district-specific performance.

## 📁 Repository Structure

### 🏠 Project Root
- `requirements.txt`: Project dependencies for easy setup.
- `.gitignore`: Configured to exclude large data files and cache.

### 🚀 Core Application Files (`Core_Application_Files/`)
- `app.py`: The main Streamlit dashboard application.
- `db_utils.py`: Database utility layer for optimized SQL querying.
- `india_states.json`: GeoJSON data for rendering India's state-wise maps.

### 📊 Data Analysis & Extraction (`Data_Analysis_Files/`)
- `phonepe.db`: The SQLite database containing all processed records (~60MB).
- `PhonePe_Pulse_EDA_Final.ipynb`: Master Jupyter notebook with full EDA.
- `phonepe_etl.py`: The ETL script used to build the database from raw JSON files.

## 🛠️ Installation & Setup

### 1. Prerequisites
Ensure you have **Python 3.11+** installed on your system.

### 2. Setup Environment
```bash
git clone https://github.com/Sanskar567/PhonePe-Transaction-Insights
cd "PhonePe-Transaction-Insights"
pip install -r requirements.txt
```

### 3. (Optional) Run ETL to Refresh Data
If you have the raw PhonePe Pulse data in a `pulse/` folder in the root, you can rebuild the database:
```bash
python Data_Analysis_Files/phonepe_etl.py
```

## 🚀 Running the Dashboard
To launch the interactive dashboard, run the following command from the **project root**:
```bash
python -m streamlit run Core_Application_Files/app.py
```
The dashboard will open automatically in your browser at **`http://localhost:8501`**.

## 📊 Business Case Studies Answered
1. **Decoding Transaction Dynamics**: Volume vs. Value across regions over time.
2. **Device Dominance**: Identifying the hardware ecosystem of PhonePe's user base.
3. **Insurance Penetration**: Tracking growth in digital insurance policy purchases.
4. **Market Expansion**: Identifying top 10 districts for strategic activation.
5. **User Engagement**: Correlating registrations with app engagement (opens).

---
Built by **Sanskar** / [GitHub Profile](https://github.com/Sanskar567)
