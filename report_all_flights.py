# report_all_flights.py
# CSC 370 - Summer 2020 - Starter code for Assignment 6
# Student Name: Treavor Gagne
# V-Number: V00890643
# B. Bird - 06/29/2020

import psycopg2, sys

# Open your DB connection here
psql_user = 'treavorgagne'
psql_db = 'treavorgagne'
psql_password = 'V00890643'
psql_server = 'studdb1.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db, user=psql_user, password=psql_password, host=psql_server, port=psql_port)
cursor = conn.cursor()

cursor.execute("""
    with all_flights as (
        select * from flights natural join aircrafts
    ),
    with_departure as (
        select flight_id, flight_airline, airports.airport_name as departure_name, 
            arrival_airport_code, departure_time, arrival_time, aircraft_id, aircraft_model, capacity
                from all_flights left outer join airports on all_flights.departure_airport_code = airports.airport_code
    ),
    with_both_and_time as (
    select flight_id, flight_airline, departure_name, airports.airport_name as arrival_name, 
        departure_time, arrival_time, extract(epoch from (arrival_time-departure_time)) / 60 as duration_min, aircraft_id, aircraft_model, capacity
            from with_departure left outer join airports on with_departure.arrival_airport_code = airports.airport_code
    ),
    reserved as (
    select flight_id, count(flight_id) as seats_full from reservations 
        group by flight_id
    union
    select distinct flight_id, 0 as seats_full from flights where flight_id not in (select flight_id from reservations)
    )
    select flight_id, flight_airline, departure_name, arrival_name, departure_time, arrival_time, duration_min::numeric::integer,
        aircraft_id, aircraft_model, capacity, seats_full
            from with_both_and_time natural join reserved
        order by departure_time;
""" )

def print_entry(flight_id, airline, source_airport_name, dest_airport_name, departure_time, arrival_time, duration_minutes, aircraft_id, aircraft_model, seating_capacity, seats_full):
    print("Flight %s (%s):"%(flight_id,airline))
    print("    [%s] - [%s] (%s minutes)"%(departure_time,arrival_time,duration_minutes))
    print("    %s -> %s"%(source_airport_name,dest_airport_name))
    print("    %s (%s): %s/%s seats booked"%(aircraft_id, aircraft_model,seats_full,seating_capacity))

while True:
	row = cursor.fetchone()
	if row is None:
		break
	print_entry(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10])
    
cursor.close()
conn.close()

