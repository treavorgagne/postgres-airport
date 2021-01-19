# manage_reservations.py
# CSC 370 - Summer 2020 - Starter code for Assignment 6
# Student Name: Treavor Gagne
# V-Number: V00890643
# B. Bird - 06/28/2020

import sys, csv, psycopg2

if len(sys.argv) < 2:
    print("Usage: %s <input file>"%sys.argv[0],file=sys.stderr)
    sys.exit(1)
    
input_filename = sys.argv[1]

# Open your DB connection here
psql_user = 'treavorgagne'
psql_db = 'treavorgagne'
psql_password = 'V00890643'
psql_server = 'studdb1.csc.uvic.ca'
psql_port = 5432

conn = psycopg2.connect(dbname=psql_db, user=psql_user, password=psql_password, host=psql_server, port=psql_port)
cursor = conn.cursor()
cursor.execute("start transaction; set constraints all deferred;")

with open(input_filename) as f:
    for row in csv.reader(f):
        if len(row) == 0:
            continue #Ignore blank rows
        if len(row) != 4:
            print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
            conn.rollback()
            cursor.close()
            conn.close()
            sys.exit(1)
        action,flight_id,passenger_id,passenger_name = row
        
        if action.upper() not in ('CREATE','DELETE'):
            print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
            conn.rollback()
            cursor.close()
            conn.close()
            sys.exit(1)
        elif action.upper() == 'DELETE':
            try:
                cursor.execute("delete from reservations where flight_id = %s and passenger_id = %s and passenger_name = %s;", (flight_id, passenger_id, passenger_name))
            except psycopg2.ProgrammingError as err:
                print("Caught a ProgrammingError:", file=sys.stderr)
                print(err, file=sys.stderr)
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)
            except psycopg2.IntegrityError as err:
                print("Caught a IntegrityError: ", file=sys.stderr)
                print(err, file=sys.stderr)
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)
            except psycopg2.InternalError as err:
                print("Caught a InternalError: ", file=sys.stderr)
                print(err, file=sys.stderr)
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)
            except psycopg2.DataError as err:
                print("Caught a DataError: ", file=sys.stderr)
                print(err, file=sys.stderr)
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)
        else:
            try:
                cursor.execute("insert into reservations values(%s, %s, %s);", (flight_id, passenger_id, passenger_name))
            except psycopg2.ProgrammingError as err:
                print("Caught a ProgrammingError:", file=sys.stderr)
                print(err, file=sys.stderr)
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)
            except psycopg2.IntegrityError as err:
                print("Caught a IntegrityError: ", file=sys.stderr)
                print(err, file=sys.stderr)
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)
            except psycopg2.InternalError as err:
                print("Caught a InternalError: ", file=sys.stderr)
                print(err, file=sys.stderr)
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)
            except psycopg2.DataError as err:
                print("Caught a DataError: ", file=sys.stderr)
                print(err, file=sys.stderr)
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)

try:
    conn.commit()
    cursor.close()
    conn.close()
except psycopg2.ProgrammingError as err:
    print("Caught a ProgrammingError:", file=sys.stderr)
    print(err, file=sys.stderr)
    conn.rollback()
    cursor.close()
    conn.close()
    sys.exit(1)
except psycopg2.IntegrityError as err:
    print("Caught a IntegrityError: ", file=sys.stderr)
    print(err, file=sys.stderr)
    conn.rollback()
    cursor.close()
    conn.close()
    sys.exit(1)
except psycopg2.InternalError as err:
    print("Caught a InternalError: ", file=sys.stderr)
    print(err, file=sys.stderr)
    conn.rollback()
    cursor.close()
    conn.close()
    sys.exit(1)
except psycopg2.DataError as err:
    print("Caught a DataError: ", file=sys.stderr)
    print(err, file=sys.stderr)
    conn.rollback()
    cursor.close()
    conn.close()
    sys.exit(1)
		