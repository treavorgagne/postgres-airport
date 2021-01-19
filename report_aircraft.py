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

cursor.execute("""with all_aircrafts as (
	select * from aircrafts
),
grounded as (
	select distinct aircraft_id, 0 as num_flights, 0 as flight_hours, 0 as avg_seats_full 
		from aircrafts
			where aircraft_id not in (select aircraft_id from flights)
),
n_flights as (
	select aircraft_id, count(*) as num_flights 
		from aircrafts natural join flights
			group by aircraft_id
),
flight_time as (
	select aircraft_id, round(sum(extract(epoch from (arrival_time - departure_time)) / 60 / 60)) as flight_hours 
		from aircrafts natural join flights 
			group by aircraft_id
),
some_reserved as (
	select flight_id, aircraft_id, count(*) as reserved from flights natural join reservations
		group by flights.flight_id
),
total_reserved as (
	select aircraft_id, sum(reserved) as total_reservations from some_reserved
		group by aircraft_id
),
calculated_avg as (
	select aircraft_id, total_reservations/num_flights as avg_seats_full from n_flights natural join total_reserved
),
zero_reserved as (
	select aircraft_id, 0 as avg_seats_full from aircrafts 
		where aircraft_id not in (select aircraft_id from grounded union select aircraft_id from calculated_avg)
),
averages_combined as (
	select * from calculated_avg
	union
	select * from zero_reserved
),
together as (
	select * from averages_combined natural join flight_time natural join n_flights
	union 
	select * from grounded
)
select aircraft_id, airline_name, aircraft_model, num_flights, flight_hours, avg_seats_full, capacity from all_aircrafts natural join together 
	order by aircraft_id;
""")

def print_entry(aircraft_id, airline, model_name, num_flights, flight_hours, avg_seats_full, seating_capacity):
    print("%-5s (%s): %s"%(aircraft_id, model_name, airline))
    print("    Number of flights : %d"%num_flights)
    print("    Total flight hours: %d"%flight_hours)
    print("    Average passengers: (%.2f/%d)"%(avg_seats_full,seating_capacity))
    
while True:
	row = cursor.fetchone()
	if row is None:
		break
	print_entry(row[0], row[1], row[2], row[3], row[4], row[5], row[6])

cursor.close()
conn.close()