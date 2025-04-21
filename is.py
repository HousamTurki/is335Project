import datetime
import uuid
from flask import Flask, request, jsonify
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)

def get_db_connection():
    conn = psycopg2.connect(
        host="localhost",
        dbname="RideShare",
        user="postgres",
        password="Mmiteb1234",
        port="5432"
    )
    conn.autocommit = False
    return conn

class Ride:
    def __init__(self, rider_id, pickup_lat, pickup_lng, dropoff_lat, dropoff_lng):
        self.ride_id = str(uuid.uuid4())
        self.rider_id = rider_id
        self.pickup_lat = pickup_lat
        self.pickup_lng = pickup_lng
        self.dropoff_lat = dropoff_lat
        self.dropoff_lng = dropoff_lng
        self.status = "requested"
        self.request_time = datetime.datetime.now()

    def create_ride_request(self):
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute("BEGIN;")

            pickup_point = f"POINT({self.pickup_lng} {self.pickup_lat})"
            dropoff_point = f"POINT({self.dropoff_lng} {self.dropoff_lat})"

            cursor.execute("""
                SELECT ST_Distance(
                    ST_Transform(ST_SetSRID(ST_GeomFromText(%s), 4326), 3857),
                    ST_Transform(ST_SetSRID(ST_GeomFromText(%s), 4326), 3857)
                ) / 1000 AS distance_km
            """, (pickup_point, dropoff_point))
            distance = cursor.fetchone()['distance_km']
            duration = distance * 2
            price = distance * 2.0

            cursor.execute("""
                SELECT a."AreaID", a."SurgeMultiplier"
                FROM "SurgeArea" a
                WHERE ST_Contains(
                    a."Location",
                    ST_SetSRID(ST_GeomFromText(%s), 4326)
                )
            """, (pickup_point,))
            surge_area = cursor.fetchone()
            if surge_area:
                price *= float(surge_area['SurgeMultiplier'])

            cursor.execute("""
                INSERT INTO "Ride" (
                    "RideID",
                    "RiderID",
                    "Status",
                    "PickupLocation",
                    "DropOffLocation",
                    "StartTime",
                    "Route",
                    "Distance",
                    "Duration",
                    "Price"
                ) VALUES (
                    %s, %s, %s,
                    ST_SetSRID(ST_GeomFromText(%s), 4326),
                    ST_SetSRID(ST_GeomFromText(%s), 4326),
                    %s,
                    %s,
                    %s, %s, %s
                ) RETURNING "RideID";
            """, (
                self.ride_id,
                self.rider_id,
                self.status,
                pickup_point,
                dropoff_point,
                datetime.datetime.now(),
                0.0,
                distance,
                duration,
                price
            ))
            db_ride_id = cursor.fetchone()['RideID']

            cursor.execute("""
                SELECT d."DriverID",
                       ST_Distance(
                           ST_Transform(d."CurrentLocation", 3857),
                           ST_Transform(ST_SetSRID(ST_GeomFromText(%s), 4326), 3857)
                       ) / 1000 AS distance_km
                FROM "Driver" d
                WHERE d."Status" = 'available'
                AND ST_Distance(
                    ST_Transform(d."CurrentLocation", 3857),
                    ST_Transform(ST_SetSRID(ST_GeomFromText(%s), 4326), 3857)
                ) / 1000 < 5
                ORDER BY distance_km
                FOR UPDATE SKIP LOCKED
                LIMIT 5;
            """, (pickup_point, pickup_point))
            nearby_drivers = cursor.fetchall()

            for driver in nearby_drivers:
                cursor.execute("""
                    INSERT INTO "AvailableDrivers" (
                        "RideID", "DriverID", "Response"
                    ) VALUES (%s, %s, %s);
                """, (
                    self.ride_id,
                    driver['DriverID'],
                    'pending'
                ))

            conn.commit()
            return {
                "ride_id": db_ride_id,
                "ride_uuid": self.ride_id,
                "status": self.status,
                "estimated_price": price,
                "available_drivers": len(nearby_drivers)
            }
        except Exception as e:
            conn.rollback()
            print(f"Error in create_ride_request: {str(e)}")
            return {"error": str(e)}
        finally:
            cursor.close()
            conn.close()

class Driver:
    def __init__(self, driver_id):
        self.driver_id = driver_id

    def accept_ride(self, ride_id):
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute("BEGIN;")

            cursor.execute("""
                SELECT "RideID", "Status", "RiderID"
                FROM "Ride"
                WHERE "RideID" = %s AND "Status" = 'requested'
                FOR UPDATE NOWAIT;
            """, (ride_id,))
            ride = cursor.fetchone()
            if not ride:
                conn.rollback()
                return {"error": "Ride not found or already accepted"}

            cursor.execute("""
                SELECT "DriverID", "Status"
                FROM "Driver"
                WHERE "DriverID" = %s AND "Status" = 'available'
                FOR UPDATE NOWAIT;
            """, (self.driver_id,))
            driver = cursor.fetchone()
            if not driver:
                conn.rollback()
                return {"error": "Driver not found or not available"}

            cursor.execute("""
                UPDATE "Ride"
                SET "DriverID" = %s,
                    "Status" = 'accepted'
                WHERE "RideID" = %s;
            """, (self.driver_id, ride_id))

            cursor.execute("""
                UPDATE "Driver"
                SET "Status" = 'busy'
                WHERE "DriverID" = %s;
            """, (self.driver_id,))

            cursor.execute("""
                UPDATE "AvailableDrivers"
                SET "Response" = 'accepted'
                WHERE "DriverID" = %s AND "RideID" = %s;
            """, (self.driver_id, ride['RideID']))

            cursor.execute("""
                DELETE FROM "AvailableDrivers"
                WHERE "RideID" = %s AND "DriverID" != %s;
            """, (ride['RideID'], self.driver_id))

            conn.commit()
            return {
                "ride_id": ride_id,
                "ride_uuid": ride['RideID'],
                "status": "accepted",
                "driver_id": self.driver_id
            }
        except psycopg2.errors.LockNotAvailable:
            conn.rollback()
            return {"error": "Ride is currently being processed by another driver"}
        except Exception as e:
            conn.rollback()
            print(f"Error in accept_ride: {str(e)}")
            return {"error": str(e)}
        finally:
            cursor.close()
            conn.close()

    def check_ride_requests(self):
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        try:
            cursor.execute("""
                SELECT ad."RiderID", r."RideID", r."PickupLocation", r."DropOffLocation", r."Price"
                FROM "AvailableDrivers" ad
                JOIN "Ride" r ON ad."RiderID" = r."RiderID"
                WHERE ad."DriverID" = %s
                AND ad."Response" = 'pending'
                AND r."Status" = 'requested';
            """, (self.driver_id,))
            return cursor.fetchall()
        finally:
            cursor.close()
            conn.close()

@app.route('/api/rides/request', methods=['POST'])
def request_ride():
    try:
        data = request.get_json()
        required_fields = ['rider_id', 'pickup_lat', 'pickup_lng', 'dropoff_lat', 'dropoff_lng']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        ride = Ride(
            rider_id=data['rider_id'],
            pickup_lat=data['pickup_lat'],
            pickup_lng=data['pickup_lng'],
            dropoff_lat=data['dropoff_lat'],
            dropoff_lng=data['dropoff_lng']
        )
        result = ride.create_ride_request()
        if 'error' in result:
            return jsonify(result), 500

        return jsonify(result), 201
    except Exception as e:
        print(f"Error in request_ride: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/rides/accept', methods=['POST'])
def accept_ride():
    try:
        data = request.get_json()
        required_fields = ['ride_id', 'driver_id']
        for field in required_fields:
            if field not in data:
                return jsonify({"error": f"Missing required field: {field}"}), 400

        driver = Driver(driver_id=data['driver_id'])
        result = driver.accept_ride(ride_id=data['ride_id'])

        if 'error' in result:
            return jsonify(result), 400

        return jsonify(result), 200
    except Exception as e:
        print(f"Error in accept_ride: {str(e)}")
        return jsonify({"error": str(e)}), 500

@app.route('/api/drivers/<int:driver_id>/ride_requests', methods=['GET'])
def get_driver_ride_requests(driver_id):
    try:
        driver = Driver(driver_id=driver_id)
        ride_requests = driver.check_ride_requests()
        return jsonify({"ride_requests": ride_requests}), 200
    except Exception as e:
        print(f"Error in get_driver_ride_requests: {str(e)}")
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)