# Import the dependencies.
from flask import Flask,jsonify
import sqlalchemy
from sqlalchemy.ext.automap import automap_base
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, func, desc
import datetime as dt

#################################################
# Database Setup
#################################################

engine = create_engine("sqlite:///Resources/hawaii.sqlite")

# reflect an existing database into a new model
Base=automap_base()

# reflect the tables
Base.prepare(autoload_with=engine,reflect=True)

# Save references to each table
Measurement=Base.classes.measurement
Station=Base.classes.station

# Create our session (link) from Python to the DB
session=Session(engine)

#################################################
# Flask Setup
#################################################

app = Flask(__name__)

# To Maintain order in jsonified responses
app.config['JSON_SORT_KEYS']=False

# Function to fetch date 12 months old from most recent date
def get_one_year_old_date():
    
    # Fetching most recent date
    recent_date=session.query(Measurement.date).order_by(Measurement.date.desc()).first()
    
    # Calculating last date from recent date 
    last_date = dt.datetime.strptime(recent_date[0], '%Y-%m-%d') - dt.timedelta(days=365)
  
    return last_date

# Function to start session
def start_session():
    session=Session(engine)

# Function to close session
def close_session():
    session.close()
#################################################
# Flask Routes
#################################################
@app.route("/")
def home():
    return (f"Hawaii weather analysis using SQLAlchemy and Flask!<br /><br />"
            f"Available routes: <br />"
            f"/api/v1.0/precipitation <br />"
            f"/api/v1.0/stations <br />"
            f"/api/v1.0/tobs <br />"
            f"/api/v1.0/&lt;start&gt; and /api/v1.0/&lt;start&gt;/&lt;end&gt; (Specify date(s) in following format: YYYY-MM-DD)<br />")

@app.route("/api/v1.0/precipitation")
def precipitation():
   
   # Get last date using our helper function
   last_date=get_one_year_old_date()

   # Query to retrieve the date and precipitation scores
   start_session()
   results=session.query(Measurement.date,Measurement.prcp).filter(Measurement.date>=last_date).all()
   close_session()
   
   # Creating dictionary from date and prcp values
   rows={result[0]:result[1] for result in results}
   
   # Returning jsonified results
   return jsonify(rows)

@app.route("/api/v1.0/stations")
def stations():

    # Query to fetch station data
    start_session()
    results = session.query(Station.station, Station.name, Station.latitude, Station.longitude, Station.elevation).all()
    close_session()

    # Creating all_stations list to return required data
    all_stations = []

    for result in results:
        station_result = {}
        station_result["Station"]=result[0]
        station_result["Name"]=result[1]
        station_result["Latitude"]=result[2]
        station_result["Longitude"]=result[3]
        station_result["Elevation"]=result[4]
        all_stations.append(station_result)
    
    # Returning jsonified results

    return jsonify(all_stations)

@app.route("/api/v1.0/tobs")
def tobs():

    # Fetching 12 month old date from recent date
    last_date=get_one_year_old_date()

    
    query_list=[Measurement.station,func.count(Measurement.station)]
    
    # Fetching active station id and linked data
    start_session()
    active_station=session.query(*query_list).group_by(Measurement.station).order_by(desc(func.count(Measurement.station))).first()
    active_station_data=session.query(Measurement.date,Measurement.tobs).filter(Measurement.date>=last_date).filter(Measurement.station==active_station[0]).all()
    close_session()

    rows=[{"date":result[0],"temperature":result[1]} for result in active_station_data]

    return jsonify(rows)

# Code to handle routes /api/v1.0/<start> and /api/v1.0/<start>/<end>
@app.route("/api/v1.0/<start>")
@app.route("/api/v1.0/<start>/<end>")
def start_end_temperature_data(start, end = None):

    # Calculating Min, Max and Avg temperature
    active_station_list=[func.min(Measurement.tobs),func.avg(Measurement.tobs),func.max(Measurement.tobs)]
    
    # Converting start date into datetime variable
    start_date = dt.datetime.strptime(start, '%Y-%m-%d').date()
    
    start_session()
    if not end:
        # Calculating the lowest, highest, and average temperature for given start time
        results = session.query(*active_station_list).filter(Measurement.date>=start_date).all()
    else:

        # Converting end date into datetime variable
        end_date = dt.datetime.strptime(end, '%Y-%m-%d').date()
        results = session.query(*active_station_list).filter(Measurement.date>=start_date).filter(Measurement.date<=end_date).all()
    
    close_session()

    rows= [{"TMIN":result[0],"TAVG":result[1],"TMAX":result[2]} for result in results]

    return jsonify(rows)

if __name__ == '__main__':
    app.run(debug=True)
