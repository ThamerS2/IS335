from flask import Flask, request, jsonify
import psycopg2
import random


app = Flask(__name__)


conn = psycopg2.connect(
    dbname="Ride",
    user="postgres",
    password="123",
    host="localhost",
    port="5432"
)

@app.route('/rides/request', methods=['POST'])
def request_ride():
    data = request.json
    pickup = data.get('pickup_location')
    destination = data.get('drop_off_location')
    vehicle_type = data.get('vehicle_type')


    if not pickup or not destination or not vehicle_type:
        return jsonify({"error": "Invalid input, missing pickup, destination, or vehicle type"}), 400

    try:
        with conn.cursor() as cursor:

            cursor.execute(
                """
                INSERT INTO ride (pickup_location, drop_off_location, vehicle_type, ride_status)
                VALUES (%s, %s, %s, 'Requested')
                RETURNING ride_id
                """,
                (pickup, destination, vehicle_type)
            )
            ride_id = cursor.fetchone()[0]
            conn.commit()
        return jsonify({'ride_id': ride_id, 'status': 'Requested'}), 201
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/rides/accept', methods=['POST'])
def accept_ride():
    data = request.json
    ride_id = data.get('ride_id')
    driver_id = data.get('driver_id')

    if not ride_id or not driver_id:
        return jsonify({'error': 'Invalid input, missing ride_id or driver_id'}), 400

    try:
        with conn.cursor() as cursor:
            cursor.execute("BEGIN;")
            cursor.execute("SELECT ride_id, ride_status FROM ride WHERE ride_id = %s FOR UPDATE;", (ride_id,))
            ride = cursor.fetchone()

            if not ride:
                return jsonify({'error': 'Ride not found'}), 404

            if ride[1] != 'Requested':

                return jsonify({'error': 'Ride cannot be accepted', 'current_status': ride[1]}), 400

            cursor.execute(
                "UPDATE ride SET ride_status = 'Accepted', driver_id = %s WHERE ride_id = %s;",
                (driver_id, ride_id)
            )
            conn.commit()

        return jsonify({'ride_id': ride_id, 'driver_id': driver_id, 'status': 'Accepted'}), 200

    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500

@app.route('/rides/<int:ride_id>', methods=['GET'])
def get_ride_details(ride_id):
    try:
        with conn.cursor() as cursor:
            cursor.execute("SELECT * FROM rides WHERE id = %s;", (ride_id,))
            ride = cursor.fetchone()
            if not ride:
                return jsonify({'error': 'Ride not found'}), 404
            ride_details = {
                'ride_id': ride[0],
                'pickup': ride[1],
                'destination': ride[2],
                'status': ride[3]
            }
        return jsonify(ride_details), 200
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/rides/complete', methods=['POST'])
def complete_ride():
    data = request.json
    ride_id = data['ride_id']
    try:
        with conn.cursor() as cursor:
            cursor.execute("BEGIN;")
            cursor.execute("SELECT * FROM rides WHERE id = %s FOR UPDATE;", (ride_id,))
            ride = cursor.fetchone()
            if not ride:
                return jsonify({'error': 'Ride not found'}), 404
            if ride[3] != 'Accepted':
                return jsonify({'error': 'Ride cannot be completed'}), 400


            cursor.execute("UPDATE rides SET status = 'Completed' WHERE id = %s;", (ride_id,))
            conn.commit()


            payment_status = random.choice(['success', 'failure'])
        return jsonify({'ride_id': ride_id, 'status': 'Completed', 'payment_status': payment_status}), 200
    except Exception as e:
        conn.rollback()
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True)
