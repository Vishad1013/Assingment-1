import pymysql
import requests
import pandas as pd
import streamlit as st
from streamlit_option_menu import option_menu

from config import DB_CONFIG
def get_connection():
    return pymysql.connect(**DB_CONFIG)

cursor = get_connection().cursor()

API_KEY = "d27e9385-c7db-4bfc-9818-57c8d4347c0e"

url = "https://api.harvardartmuseums.org/object"


from config import DB_CONFIG

def get_connection():
    return pymysql.connect(**DB_CONFIG)

cursor = get_connection().cursor()

DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "root",
    "database": "harvard_artifact"
}


def get_connection():
    """Establish and return a MySQL database connection."""
    return pymysql.connect(**DB_CONFIG)

# ==========================
# 🧱 TABLE CREATION
# ==========================
def create_tables():
    conn = get_connection()
    cursor = conn.cursor()

    # Drop existing tables
    cursor.execute("DROP TABLE IF EXISTS artifact_colors")
    cursor.execute("DROP TABLE IF EXISTS artifact_media")
    cursor.execute("DROP TABLE IF EXISTS artifact_metadata")

    # Create tables
    cursor.execute("""
    CREATE TABLE artifact_metadata (
        id INT PRIMARY KEY,
        title TEXT,
        culture TEXT,
        period TEXT,
        century TEXT,
        medium TEXT,
        dimensions TEXT,
        description TEXT,
        department TEXT,
        classification TEXT,
        accessionyear INT,
        accessionmethod TEXT
    )
    """)

    cursor.execute("""
    CREATE TABLE artifact_media (
        objectid INT,
        imagecount INT,
        mediacount INT,
        colorcount INT,
        `rank` INT,
        datebegin INT,
        dateend INT,
        FOREIGN KEY (objectid) REFERENCES artifact_metadata(id)
    )
    """)

    cursor.execute("""
    CREATE TABLE artifact_colors (
        objectid INT,
        color TEXT,
        spectrum TEXT,
        hue TEXT,
        percent REAL,
        css3 TEXT,
        FOREIGN KEY (objectid) REFERENCES artifact_metadata(id)
    )
    """)

    conn.commit()
    cursor.close()
    conn.close()
    st.success("✅ Tables created successfully!")

# ==========================
# 🔑 API CONFIGURATION
# ==========================
API_KEY = "d27e9385-c7db-4bfc-9818-57c8d4347c0e"
BASE_URL = "https://api.harvardartmuseums.org/object"

# ==========================
# 📥 FETCH DATA
# ==========================
def fetch_artifacts(classification, size=2500):
    params = {
        "apikey": API_KEY,
        "classification": classification,
        "size": 100,
        "page": 1
    }
    records = []
    while len(records) < size:
        res = requests.get(BASE_URL, params=params).json()
        records.extend(res.get("records", []))
        if not res.get("info", {}).get("next"):
            break
        params["page"] += 1
    return records[:size]

# ==========================
# 🔄 TRANSFORM DATA
# ==========================
def transform_data(records):
    metadata, media, colors = [], [], []
    for r in records:
        metadata.append({
            "id": r["id"],
            "title": r.get("title"),
            "culture": r.get("culture"),
            "period": r.get("period"),
            "century": r.get("century"),
            "medium": r.get("medium"),
            "dimensions": r.get("dimensions"),
            "description": r.get("description"),
            "department": r.get("department"),
            "classification": r.get("classification"),
            "accessionyear": r.get("accessionyear"),
            "accessionmethod": r.get("accessionmethod")
        })

        media.append({
            "objectid": r["id"],
            "imagecount": r.get("imagecount", 0),
            "mediacount": r.get("mediacount", 0),
            "colorcount": len(r.get("colors", [])),
            "rank": r.get("rank", 0),
            "datebegin": r.get("datebegin"),
            "dateend": r.get("dateend")
        })

        for c in r.get("colors", []):
            colors.append({
                "objectid": r["id"],
                "color": c.get("color"),
                "spectrum": c.get("spectrum"),
                "hue": c.get("hue"),
                "percent": c.get("percent"),
                "css3": c.get("css3")
            })

    return metadata, media, colors

# ==========================
# 🧮 PREDEFINED QUERIES
# ==========================
QUERIES = {
    "Artifacts from 11th century & Byzantine": 
        "SELECT * FROM artifact_metadata WHERE century='11th Century' AND culture='Byzantine'",
    "Unique cultures": 
        "SELECT DISTINCT culture FROM artifact_metadata",
    "Artifacts from Archaic Period": 
        "SELECT * FROM artifact_metadata WHERE period='Archaic Period'",
    "Titles by accession year": 
        "SELECT title FROM artifact_metadata ORDER BY accessionyear DESC",
    "Artifacts per department": 
        "SELECT department, COUNT(*) FROM artifact_metadata GROUP BY department",
    "Artifacts with >1 image": 
        "SELECT * FROM artifact_media WHERE imagecount > 1",
    "Average rank": 
        "SELECT AVG(`rank`) FROM artifact_media",
    "Colorcount > mediacount": 
        "SELECT * FROM artifact_media WHERE colorcount > mediacount",
    "Created between 1500–1600": 
        "SELECT * FROM artifact_media WHERE datebegin >= 1500 AND dateend <= 1600",
    "No media files": 
        "SELECT COUNT(*) FROM artifact_media WHERE mediacount = 0",
    "Distinct hues": 
        "SELECT DISTINCT hue FROM artifact_colors",
    "Top 5 colors": 
        "SELECT color, COUNT(*) AS freq FROM artifact_colors GROUP BY color ORDER BY freq DESC LIMIT 5",
    "Avg coverage per hue": 
        "SELECT hue, AVG(percent) FROM artifact_colors GROUP BY hue",
    "Total color entries": 
        "SELECT COUNT(*) FROM artifact_colors",
    "Titles with hues": 
        "SELECT m.title, c.hue FROM artifact_metadata m JOIN artifact_colors c ON m.id = c.objectid",
    "Titles & hues for Byzantine": 
        "SELECT m.title, c.hue FROM artifact_metadata m JOIN artifact_colors c ON m.id = c.objectid WHERE m.culture = 'Byzantine'",
    "Titles, cultures, ranks (period not null)": 
        "SELECT m.title, m.culture, md.rank FROM artifact_metadata m JOIN artifact_media md ON m.id = md.objectid WHERE m.period IS NOT NULL",
    "Top 10 titles with hue Grey": 
        "SELECT m.title FROM artifact_metadata m JOIN artifact_media md ON m.id = md.objectid JOIN artifact_colors c ON m.id = c.objectid WHERE c.hue = 'Grey' ORDER BY md.rank DESC LIMIT 10",
    "Artifacts per classification & avg media count": 
        "SELECT m.classification, COUNT(*), AVG(md.mediacount) FROM artifact_metadata m JOIN artifact_media md ON m.id = md.objectid GROUP BY m.classification"
}

# ==========================
# 🖥️ STREAMLIT UI
# ==========================
st.title("🏛️ Harvard Artifacts Explorer")

classification = st.selectbox(
    "Choose Classification",
    ["Coins", "Photographs", "Vessels", "Archival Material", "Fragments"]
)

# Create tables
if st.button("🗂️ Create Tables"):
    create_tables()

# Fetch Data
if st.button("📥 Collect Data"):
    raw_data = fetch_artifacts(classification)
    metadata, media, colors = transform_data(raw_data)
    st.session_state["metadata"] = metadata
    st.session_state["media"] = media
    st.session_state["colors"] = colors
    st.success(f"Fetched {len(metadata)} records for {classification}")

# Show data
if st.button("📊 Show Data"):
    if "metadata" in st.session_state:
        st.dataframe(pd.DataFrame(st.session_state["metadata"]))
    else:
        st.warning("No data collected yet. Click 'Collect Data' first.")

# Insert into SQL
if st.button("💾 Insert into SQL"):
    if "metadata" not in st.session_state:
        st.warning("Please collect data first.")
    else:
        conn = get_connection()
        cursor = conn.cursor()
        for record in st.session_state["metadata"]:
            cursor.execute("""
                INSERT INTO artifact_metadata (
                    id, title, culture, period, century, medium,
                    dimensions, description, department, classification,
                    accessionyear, accessionmethod
                ) VALUES (%(id)s, %(title)s, %(culture)s, %(period)s, %(century)s, %(medium)s,
                          %(dimensions)s, %(description)s, %(department)s, %(classification)s,
                          %(accessionyear)s, %(accessionmethod)s)
            """, record)
        conn.commit()
        cursor.close()
        conn.close()
        st.success("✅ Data inserted into SQL!")

# Run SQL Query
st.subheader("🔍 Run SQL Query")
query_name = st.selectbox("Choose a query", list(QUERIES.keys()))

if st.button("▶️ Run Query"):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute(QUERIES[query_name])
    rows = cursor.fetchall()
    columns = [desc[0] for desc in cursor.description]
    st.dataframe(pd.DataFrame(rows, columns=columns))
    cursor.close()
    conn.close()
