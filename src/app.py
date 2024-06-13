import os
import pandas as pd
from flask import Flask, request, render_template, redirect, url_for
from sqlalchemy import create_engine, text

app = Flask(__name__)
app.secret_key = os.urandom(24)  # Generates a random secret key

connection_string = (
    "mssql+pyodbc://bontha3001:Arjunsuha1*@bontha3001.database.windows.net:1433/bontha3001"
    "?driver=ODBC+Driver+17+for+SQL+Server&Encrypt=yes&TrustServerCertificate=no&Connection Timeout=30"
)

# Create SQLAlchemy engine
engine = create_engine(connection_string)


def execute_query(query, params=None):
    with engine.connect() as connection:
        result = connection.execute(text(query), params)
        return result.fetchall()


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/upload', methods=['POST'])
def upload_file():
    file = request.files['file']
    if file:
        try:
            data = pd.read_csv(file)
            with engine.connect() as connection:
                for index, row in data.iterrows():
                    connection.execute(text('''
                        INSERT INTO earthquakes (
                            datetime, latitude, longitude, Magnitude, magType, nst, gap, dmin, rms, net, id_earthquake, updated, place, type, local_time
                        ) VALUES (:time, :latitude, :longitude, :Magnitude, :magType, :nst, :gap, :dmin, :rms, :net, :id_earthquake, :updated, :place, :type, :local_time)
                    '''), {
                        'time': row['time'],
                        'latitude': row['latitude'],
                        'longitude': row['longitude'],
                        'Magnitude': row['mag'],
                        'magType': row['magType'],
                        'nst': row['nst'],
                        'gap': row['gap'],
                        'dmin': row['dmin'],
                        'rms': row['rms'],
                        'net': row['net'],
                        'id_earthquake': row['id'],
                        'updated': row['updated'],
                        'place': row['place'],
                        'type': row['type'],
                        'local_time': row['local_time']
                    })
            return redirect(url_for('index'))
        except Exception as e:
            return str(e), 400
    return 'No file uploaded', 400


@app.route('/query', methods=['GET', 'POST'])
def query_data():
    if request.method == 'POST':
        try:
            min_mag = request.form.get('min_mag')
            max_mag = request.form.get('max_mag')
            start_date = request.form.get('start_date')
            end_date = request.form.get('end_date')
            lat = request.form.get('latitude')
            lon = request.form.get('longitude')
            place = request.form.get('place')
            distance = request.form.get('distance')
            night_time = request.form.get('night_time')

            query = '''
                SELECT datetime AS Datetime, latitude AS Latitude, longitude AS Longitude, Magnitude AS Magnitude, place AS Place
                FROM earthquakes
                WHERE 1=1
            '''
            params = {}

            if min_mag and max_mag:
                query += ' AND Magnitude BETWEEN :min_mag AND :max_mag'
                params['min_mag'] = min_mag
                params['max_mag'] = max_mag

            if start_date and end_date:
                if start_date <= end_date:
                    query += ' AND datetime BETWEEN :start_date AND :end_date'
                    params['start_date'] = start_date
                    params['end_date'] = end_date
                else:
                    return 'Error: Start date must be before end date.', 400

            if lat and lon:
                query += ' AND latitude = :latitude AND longitude = :longitude'
                params['latitude'] = lat
                params['longitude'] = lon

            if place:
                query += ' AND place LIKE :place'
                params['place'] = f'%{place}%'

            if distance:
                query += '''
                    AND TRY_CAST(LEFT(place, CHARINDEX(' km', place) - 1) AS INT) = :distance
                '''
                params['distance'] = distance

            if night_time:
                query += " AND Magnitude > 4.0 AND (DATEPART(HOUR, datetime) >= 18 OR DATEPART(HOUR, datetime) <= 6)"

            earthquakes = execute_query(query, params)
            return render_template('results.html', earthquakes=earthquakes)

        except Exception as e:
            return str(e), 400

    return render_template('query.html')


@app.route('/count', methods=['GET'])
def count_large_earthquakes():
    try:
        result = execute_query('SELECT COUNT(*) AS count FROM earthquakes WHERE Magnitude > 5.0')
        count = result[0][0]  # Accessing the first column directly
        return f'Total earthquakes with magnitude greater than 5.0: {count}'
    except Exception as e:
        return str(e), 400


@app.route('/night', methods=['GET'])
def large_earthquakes_night():
    try:
        result = execute_query('''
            SELECT COUNT(*) AS count 
            FROM earthquakes 
            WHERE Magnitude > 4.0 
            AND (DATEPART(HOUR, datetime) >= 18 OR DATEPART(HOUR, datetime) <= 6)
        ''')
        count = result[0][0]  # Accessing the first column directly
        return f'Total large earthquakes (>4.0 mag) at night: {count}'
    except Exception as e:
        return str(e), 400


# Part 10: Show earthquakes within latitude range
@app.route('/earthquakes_by_latitude', methods=['GET', 'POST'])
def earthquakes_by_latitude():
    if request.method == 'POST':
        L = float(request.form['latitude'])
        N = float(request.form['degrees'])
        query = '''
            SELECT datetime, latitude, longitude, id_earthquake
            FROM earthquakes
            WHERE latitude BETWEEN :lat_min AND :lat_max
        '''
        params = {'lat_min': L - N, 'lat_max': L + N}
        earthquakes = execute_query(query, params)
        return render_template('results.html', earthquakes=earthquakes)
    return render_template('latitude_query.html')


# Part 11: Count and delete entries by net value
@app.route('/delete_by_net', methods=['GET', 'POST'])
def delete_by_net():
    if request.method == 'POST':
        net_value = request.form['net']
        count_query = 'SELECT COUNT(*) AS count FROM earthquakes WHERE net = :net_value'
        delete_query = 'DELETE FROM earthquakes WHERE net = :net_value'

        # Count occurrences
        result = execute_query(count_query, {'net_value': net_value})
        count = result[0][0]  # Accessing the first column directly

        # Delete entries
        with engine.connect() as connection:
            connection.execute(text(delete_query), {'net_value': net_value})

        # Count remaining entries
        result = execute_query('SELECT COUNT(*) AS count FROM earthquakes')
        remaining_count = result[0][0]

        return f'{count} entries deleted. {remaining_count} entries remain.'
    return render_template('net_query.html')


# Part 12: Create a new earthquake entry
@app.route('/add_earthquake', methods=['GET', 'POST'])
def add_earthquake():
    if request.method == 'POST':
        data = request.form
        check_query = 'SELECT COUNT(*) AS count FROM earthquakes WHERE id_earthquake = :id_earthquake'
        insert_query = '''
            INSERT INTO earthquakes (
                datetime, latitude, longitude, Magnitude, magType, nst, gap, dmin, rms, net, id_earthquake, updated, place, type, local_time
            ) VALUES (:datetime, :latitude, :longitude, :Magnitude, :magType, :nst, :gap, :dmin, :rms, :net, :id_earthquake, :updated, :place, :type, :local_time)
        '''

        # Check if ID already exists
        result = execute_query(check_query, {'id_earthquake': data['id_earthquake']})
        if result[0][0] > 0:
            return 'Error: ID already exists'

        # Insert new entry
        with engine.connect() as connection:
            connection.execute(text(insert_query), {
                'datetime': data['datetime'],
                'latitude': data['latitude'],
                'longitude': data['longitude'],
                'Magnitude': data['Magnitude'],
                'magType': data['magType'],
                'nst': data['nst'],
                'gap': data['gap'],
                'dmin': data['dmin'],
                'rms': data['rms'],
                'net': data['net'],
                'id_earthquake': data['id_earthquake'],
                'updated': data['updated'],
                'place': data['place'],
                'type': data['type'],
                'local_time': data['local_time']
            })

        return redirect(url_for('index'))
    return render_template('add_earthquake.html')


# Part 13: Modify earthquake entry by ID
@app.route('/modify_earthquake', methods=['GET', 'POST'])
def modify_earthquake():
    if request.method == 'POST':
        data = request.form
        check_query = 'SELECT COUNT(*) AS count FROM earthquakes WHERE id_earthquake = :id_earthquake'
        update_query = '''
            UPDATE earthquakes
            SET datetime = :datetime, latitude = :latitude, longitude = :longitude, Magnitude = :Magnitude, magType = :magType,
                nst = :nst, gap = :gap, dmin = :dmin, rms = :rms, net = :net, updated = :updated, place = :place, type = :type, local_time = :local_time
            WHERE id_earthquake = :id_earthquake
        '''

        # Check if ID exists
        result = execute_query(check_query, {'id_earthquake': data['id_earthquake']})
        if result[0][0] == 0:
            return 'Error: ID does not exist'

        # Update entry
        with engine.connect() as connection:
            connection.execute(text(update_query), {
                'datetime': data['datetime'],
                'latitude': data['latitude'],
                'longitude': data['longitude'],
                'Magnitude': data['Magnitude'],
                'magType': data['magType'],
                'nst': data['nst'],
                'gap': data['gap'],
                'dmin': data['dmin'],
                'rms': data['rms'],
                'net': data['net'],
                'id_earthquake': data['id_earthquake'],
                'updated': data['updated'],
                'place': data['place'],
                'type': data['type'],
                'local_time': data['local_time']
            })

        return redirect(url_for('index'))
    return render_template('modify_earthquake.html')

if __name__ == '__main__':
    app.run(debug=True)
