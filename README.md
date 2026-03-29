# PhonePe Pulse Transaction Insights (2018-2024) 💸

A comprehensive data engineering and analytics project to visualize and explore PhonePe's transaction, user, and insurance data across India.

## 🌟 Project Overview
This project performs an end-to-end analysis of the PhonePe Pulse dataset. It includes a robust ETL pipeline to extract data from multiple JSON structures, a structured SQLite database for high-performance querying, a master Exploratory Data Analysis (EDA) notebook, and an interactive Streamlit dashboard for real-time insights.

### 🍱 Main Features
- **ETL Pipeline**: Automated extraction and cleaning of 1.5 million+ records from the PhonePe Pulse repository.
- **Master EDA**: 20+ comprehensive visualizations addressing 5 key business cases including transaction dynamics, device dominance, and insurance penetration.
- **Interactive Dashboard**: A premium Streamlit UI with animated choropleth maps, growth charts, and performance leaderboards.
- **Multi-Level Insights**: Drill down from country-level trends to district-specific performance.

## 📁 Repository Structure
- `app.py`: The main Streamlit dashboard application.
- `db_utils.py`: Database utility layer for optimized SQL querying.
- `phonepe_etl.py`: The ETL script used to build the database from raw JSON files.
- `phonepe.db`: The SQLite database containing all processed records (~60MB).
- `PhonePe_Pulse_EDA_Final.ipynb`: Master Jupyter notebook with full EDA.
- `india_states.json`: GeoJSON data for rendering India's state-wise maps.
- `requirements.txt`: Project dependencies for easy setup.

## 🛠️ Installation & Setup

### 1. Prerequisites
Ensure you have **Python 3.11+** installed on your system.

### 2. Clone and Setup Environment
```bash
git clone <your-repo-link>
cd "Phone Pe Project"
pip install -r requirements.txt
```

### 3. (Optional) Run ETL to Refresh Data
If you have the raw PhonePe Pulse data in a `pulse/` folder, you can rebuild the database:
```bash
python phonepe_etl.py
```

## 🚀 Running the Dashboard
To launch the interactive dashboard, run the following command:
```bash
python -m streamlit run app.py
```
The dashboard will open automatically in your browser at **`http://localhost:8501`**.

## 📊 Business Case Studies Answered
1. **Decoding Transaction Dynamics**: Volume vs. Value across regions.
2. **Device Dominance**: Analyzing the hardware ecosystem of PhonePe users.
3. **Insurance Penetration**: Identifying growth opportunities in the insurance sector.
4. **Market Expansion**: Pinpointing top districts for strategic activation.
5. **User Engagement**: Correlating registrations with application opens.

---
Built with ❤️ by your Name / Organization.
