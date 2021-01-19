# manage_flights.py
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
        action = row[0]
        if action.upper() == 'DELETE':
            if len(row) != 2:
                print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)
            flight_id = row[1]
            try:
                cursor.execute("delete from flights where flight_id = %s", (flight_id,))
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
            
            if(cursor.rowcount == 0):
                print("Error: Cannot delete flight_id %s as it doesn't exists" %(flight_id))
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)

        elif action.upper() in ('CREATE','UPDATE'):
            if len(row) != 8:
                print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
                conn.rollback()
                cursor.close()
                conn.close()
                sys.exit(1)

            flight_id = row[1]
            airline = row[2]
            src,dest = row[3],row[4]
            departure, arrival = row[5],row[6]
            aircraft_id = row[7]
            if action.upper() == 'CREATE':
                try:
                    cursor.execute("insert into flights values(%s, %s, %s, %s, %s, %s, %s)", (flight_id, airline, src, dest, departure, arrival, aircraft_id))
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
                    cursor.execute("""update flights set flight_airline = %s, departure_airport_code = %s, arrival_airport_code = %s, 
                                            departure_time = %s, arrival_time = %s, aircraft_id = %s where flight_id = %s""", 
                                                (airline, src, dest, departure, arrival, aircraft_id, flight_id))
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
                
                if(cursor.rowcount == 0):
                    print("Error: No flight_id %s to update." %(flight_id))
                    conn.rollback()
                    cursor.close()
                    conn.close()
                    sys.exit(1)
        else:
            print("Error: Invalid input line \"%s\""%(','.join(row)), file=sys.stderr)
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
        