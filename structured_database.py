#!/usr/bin/env python
# coding: utf-8

# ## Fetching Data from USGS

# In[1]:


import requests
import pandas as pd

# Fetch earthquake data from USGS API
def fetch_usgs_data(start_time, end_time, min_magnitude):
    url = "https://earthquake.usgs.gov/fdsnws/event/1/query"
    params = {
        "format": "geojson",
        "starttime": start_time,
        "endtime": end_time,
        "minmagnitude": min_magnitude
    }
    response = requests.get(url, params=params)
    data = response.json()
    return pd.json_normalize(data['features'])

# Example usage
usgs_data = fetch_usgs_data("2023-01-01", "2023-12-31", 5.0)
print(usgs_data.head())


# ## Fetching Data from ISC

# In[2]:


def fetch_isc_data(start_time, end_time, min_magnitude):
    url = "http://isc-mirror.iris.washington.edu/fdsnws/event/1/query"
    params = {
        "format": "text",
        "starttime": start_time,
        "endtime": end_time,
        "minmagnitude": min_magnitude
    }
    response = requests.get(url, params=params)
    lines = response.text.splitlines()
    headers = lines[0].split('|')
    data = [line.split('|') for line in lines[1:]]
    return pd.DataFrame(data, columns=headers)

# Example usage
isc_data = fetch_isc_data("2023-01-01", "2023-12-31", 5.0)
print(isc_data.head())


# ## Process and Standardize Data

# In[3]:


def standardize_usgs_data(usgs_data):
    # Select relevant columns
    usgs_data['time'] = pd.to_datetime(usgs_data['properties.time'], unit='ms')
    usgs_data['magnitude'] = usgs_data['properties.mag']
    usgs_data['longitude'] = usgs_data['geometry.coordinates'].apply(lambda x: x[0])
    usgs_data['latitude'] = usgs_data['geometry.coordinates'].apply(lambda x: x[1])
    return usgs_data[['time', 'magnitude', 'longitude', 'latitude']]

def standardize_isc_data(isc_data):
    # Select relevant columns
    isc_data['time'] = pd.to_datetime(isc_data['Time'])
    isc_data['magnitude'] = isc_data['Magnitude'].astype(float)
    isc_data['longitude'] = isc_data['Longitude'].astype(float)
    isc_data['latitude'] = isc_data['Latitude'].astype(float)
    return isc_data[['time', 'magnitude', 'longitude', 'latitude']]

# Standardize data
usgs_data_clean = standardize_usgs_data(usgs_data)
isc_data_clean = standardize_isc_data(isc_data)

# Combine data from both sources
combined_data = pd.concat([usgs_data_clean, isc_data_clean], ignore_index=True)
print(combined_data.head())


# ## Store Data in a Structured Database

# ### a. Create a Database and Table

# In[4]:


import sqlite3

# Connect to SQLite database (or create it)
conn = sqlite3.connect('seismological_data.db')
cursor = conn.cursor()

# Create a table for earthquake data
cursor.execute('''
CREATE TABLE IF NOT EXISTS earthquakes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    time TIMESTAMP,
    magnitude FLOAT,
    longitude FLOAT,
    latitude FLOAT,
    source TEXT
)
''')


# ### b. Insert Data into the Table

# In[13]:


# Insert USGS data
for _, row in usgs_data_clean.iterrows():
    cursor.execute('''
    INSERT INTO earthquakes (time, magnitude, longitude, latitude, source)
    VALUES (?, ?, ?, ?, ?)
    ''', (row['time'].isoformat(), row['magnitude'], row['longitude'], row['latitude'], 'USGS'))



# Option 1: Remove rows with missing values
isc_data_clean = isc_data_clean.dropna(subset=['magnitude', 'longitude', 'latitude'])

# Option 2: Replace missing values with defaults
# isc_data_clean['magnitude'] = isc_data_clean['magnitude'].fillna(0)
# isc_data_clean['longitude'] = isc_data_clean['longitude'].fillna(0)
# isc_data_clean['latitude'] = isc_data_clean['latitude'].fillna(0)

# Verify that missing values are handled
print(isc_data_clean.isna().sum())  # Should print 0 for all columns

# Insert ISC data
for _, row in isc_data_clean.iterrows():
    try:
        cursor.execute('''
        INSERT INTO earthquakes (time, magnitude, longitude, latitude, source)
        VALUES (?, ?, ?, ?, ?)
        ''', (row['time'].isoformat(), row['magnitude'], row['longitude'], row['latitude'], 'ISC'))
    except Exception as e:
        print(f"Error inserting row: {row}")
        print(f"Error message: {e}")

# Commit and close
conn.commit()
conn.close()


# ## Query and Analyze Data

# In[14]:


# Query the database
conn = sqlite3.connect('seismological_data.db')
cursor = conn.cursor()

cursor.execute('''
SELECT * FROM earthquakes WHERE magnitude > 6.0
''')
results = cursor.fetchall()

# Convert to DataFrame
high_magnitude_quakes = pd.DataFrame(results, columns=['id', 'time', 'magnitude', 'longitude', 'latitude', 'source'])
print(high_magnitude_quakes)

conn.close()


# ## Visualize Data

# In[15]:


import matplotlib.pyplot as plt

# Plot earthquake locations
plt.scatter(combined_data['longitude'], combined_data['latitude'], c=combined_data['magnitude'], cmap='viridis')
plt.colorbar(label='Magnitude')
plt.title('Earthquake Locations (USGS + ISC)')
plt.xlabel('Longitude')
plt.ylabel('Latitude')
plt.show()


# ## Package the Tool

# In[16]:


import argparse

def main():
    parser = argparse.ArgumentParser(description="Seismological Data Integration Tool")
    parser.add_argument('--fetch', action='store_true', help="Fetch and process data")
    parser.add_argument('--query', type=str, help="Query the database (e.g., 'magnitude > 6.0')")
    args = parser.parse_args()

    if args.fetch:
        # Fetch and process data
        usgs_data = fetch_usgs_data("2023-01-01", "2023-12-31", 5.0)
        isc_data = fetch_isc_data("2023-01-01", "2023-12-31", 5.0)
        usgs_data_clean = standardize_usgs_data(usgs_data)
        isc_data_clean = standardize_isc_data(isc_data)
        combined_data = pd.concat([usgs_data_clean, isc_data_clean], ignore_index=True)

        # Store in database
        conn = sqlite3.connect('seismological_data.db')
        cursor = conn.cursor()
        for _, row in combined_data.iterrows():
            cursor.execute('''
            INSERT INTO earthquakes (time, magnitude, longitude, latitude, source)
            VALUES (?, ?, ?, ?, ?)
            ''', (row['time'], row['magnitude'], row['longitude'], row['latitude'], row.get('source', 'Unknown')))
        conn.commit()
        conn.close()
        print("Data fetched and stored successfully.")

    elif args.query:
        # Query the database
        conn = sqlite3.connect('seismological_data.db')
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM earthquakes WHERE {args.query}")
        results = cursor.fetchall()
        query_results = pd.DataFrame(results, columns=['id', 'time', 'magnitude', 'longitude', 'latitude', 'source'])
        print(query_results)
        conn.close()

if __name__ == "__main__":
    main()


# In[ ]:




