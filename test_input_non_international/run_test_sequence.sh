# This script was used to generate the model output for this test case.
# It assumes that your solution is in the directory ../solution

# Re-run the create_schema.txt script to clear and re-populate the database
# psql -h studdb1.csc.uvic.ca your_db_here your_name_here < create_schema.txt

# All of the data entry commands below should succeed and generate no output.
# The report programs should all succeed (and pipe their output to files)

python3 ../solution/add_airports.py  airports.txt
python3 ../solution/add_aircraft.py  aircraft.txt
python3 ../solution/manage_flights.py flights1.txt

python3 ../solution/report_all_flights.py > output_report_all_flights_round1.txt
python3 ../solution/report_aircraft.py > output_report_aircraft_round1.txt

# This command attempts to create a flight between two countries, but where one
# endpoint is not marked as an international airport (which should fail)
python3 ../solution/manage_flights.py flights2.txt

python3 ../solution/report_all_flights.py > output_report_all_flights_round2.txt
python3 ../solution/report_aircraft.py > output_report_aircraft_round2.txt

# This command attempts to modify an existing flight to change it to a 
# flight between two countries, even though one of the endpoints is not an 
# international airport. It should also fail.
python3 ../solution/manage_flights.py flights3.txt

python3 ../solution/report_all_flights.py > output_report_all_flights_round3.txt
python3 ../solution/report_aircraft.py > output_report_aircraft_round3.txt
