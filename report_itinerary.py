# report_itinerary.py
# CSC 370 - Summer 2020 - Starter code for Assignment 6
# Student Name: Treavor Gagne
# V-Number: V00890643
# B. Bird - 06/28/2020

import psycopg2, sys

# Open your DB connection here
psql_user = 'treavorgagne'
psql_db = 'treavorgagne'
psql_password = 'V00890643'
psql_server = 'studdb1.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db, user=psql_user, password=psql_password, host=psql_server, port=psql_port)
cursor = conn.cursor()


def print_header(passenger_id, passenger_name):
    print("Itinerary for %s (%s)"%(str(passenger_id), str(passenger_name)) )
    
def print_entry(flight_id, airline, source_airport_name, dest_airport_name, departure_time, arrival_time, duration_minutes, aircraft_id, aircraft_model):
    print("Flight %-4s (%s):"%(flight_id, airline))
    print("    [%s] - [%s] (%s minutes)"%(departure_time, arrival_time,duration_minutes))
    print("    %s -> %s (%s: %s)"%(source_airport_name, dest_airport_name, aircraft_id, aircraft_model))


if len(sys.argv) < 2:
    print('Usage: %s <passenger id>'%sys.argv[0], file=sys.stderr)
    sys.exit(1)
else:
    passenger_id = sys.argv[1]
    cursor.execute("""select passenger_id, passenger_name from passengers 
                        where passenger_id = %s""", (passenger_id,) )
    row = cursor.fetchone()
    if row is None:
        print('No passenger with the id %s'%sys.argv[1], file=sys.stderr)
        sys.exit(1)
    
    print_header(row[0], row[1])

    cursor.execute("""with pre_itinerary as (
                            select * from reservations natural join flights natural join aircrafts where passenger_id = %s
                        ),
                        with_departure as (
                            select flight_id, flight_airline, airports.airport_name as departure_name, 
                                arrival_airport_code, departure_time, arrival_time, aircraft_id, aircraft_model
                                    from pre_itinerary left outer join airports on pre_itinerary.departure_airport_code = airports.airport_code
                        ),
                        with_both_and_time as (
                        select flight_id, flight_airline, departure_name, airports.airport_name as arrival_name, 
                            departure_time, arrival_time, (extract(epoch from (arrival_time-departure_time)) / 60)::numeric::integer as duration_min, aircraft_id, aircraft_model
                                from with_departure left outer join airports on with_departure.arrival_airport_code = airports.airport_code
                        )
                        select * from with_both_and_time order by departure_time;""", (passenger_id,) )
    while True:
        row = cursor.fetchone()
        if row is None:
            break
        print_entry(row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8])
    
cursor.close()
conn.close()