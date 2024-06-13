from flask import Flask, request, render_template_string, jsonify
import pyodbc
import pandas as pd

app = Flask(__name__)

# Database connection details
server = 'tcp:bontha3001.database.windows.net'
database = 'bontha3001'
username = 'bontha3001'
password = 'Arjunsuha1*'
driver = '{ODBC Driver 17 for SQL Server}'

# Correct the connection string
conn_str = (
    'DRIVER=' + driver + ';'
    'SERVER=' + server + ';'
    'PORT=1433;'
    'DATABASE=' + database + ';'
    'UID=' + username + ';'
    'PWD=' + password + ';'
    'Persist Security Info=False;'
    'MultipleActiveResultSets=False;'
    'Encrypt=True;'
    'TrustServerCertificate=False;'
    'Connection Timeout=30;'
)

# Connect to Azure SQL Database
conn = pyodbc.connect(conn_str)

# Load HTML templates
with open('index.html') as f:
    index_html = f.read()
with open('results.html') as f:
    results_html = f.read()

@app.route('/')
def index():
    return render_template_string(index_html)

@app.route('/earthquakes', methods=['POST'])
def get_earthquakes():
    L = float(request.form['latitude'])
    N = float(request.form['degrees'])
    cursor = conn.cursor()
    cursor.execute("""
        SELECT time, latitude, longitude, id FROM EarthquakeData
        WHERE latitude BETWEEN ? AND ?
    """, (L - N, L + N))
    quakes = cursor.fetchall()
    return render_template_string(results_html, quakes=quakes)

@app.route('/delete_net', methods=['POST'])
def delete_net():
    net_value = request.form['net']
    cursor = conn.cursor()
    cursor.execute("DELETE FROM EarthquakeData WHERE net = ?", (net_value,))
    conn.commit()
    cursor.execute("SELECT COUNT(*) FROM EarthquakeData")
    count = cursor.fetchone()[0]
    return f'{count} entries remain.'

@app.route('/add_earthquake', methods=['POST'])
def add_earthquake():
    data = request.form
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM EarthquakeData WHERE id = ?", (data['id'],))
    if cursor.fetchone()[0] > 0:
        return 'Error: ID already exists'
    cursor.execute("""
        INSERT INTO EarthquakeData (id, time, latitude, longitude, depth, magnitude, net, place)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (data['id'], data['time'], data['latitude'], data['longitude'], data['depth'], data['magnitude'], data['net'], data['place']))
    conn.commit()
    return 'Earthquake added successfully'

@app.route('/modify_earthquake', methods=['POST'])
def modify_earthquake():
    data = request.form
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM EarthquakeData WHERE id = ?", (data['id'],))
    if cursor.fetchone()[0] == 0:
        return 'Error: ID does not exist'
    cursor.execute("""
        UPDATE EarthquakeData
        SET time = ?, latitude = ?, longitude = ?, depth = ?, magnitude = ?, net = ?, place = ?
        WHERE id = ?
    """, (data['time'], data['latitude'], data['longitude'], data['depth'], data['magnitude'], data['net'], data['place'], data['id']))
    conn.commit()
    return 'Earthquake modified successfully'

if __name__ == '__main__':
    app.run(debug=True)
