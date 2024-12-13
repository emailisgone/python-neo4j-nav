from neo4j import GraphDatabase
import pytz
from flask import (Flask, request, jsonify, abort)
from datetime import datetime
import time
import math
import random

URI = "neo4j://localhost"
AUTH = ("neo4j", "adminadmin")

totalClientCount = 0

def haversine(lat1, lon1, lat2, lon2):
    dLat = (lat2-lat1)*math.pi/180.0
    dLon = (lon2-lon1)*math.pi/180.0
 
    lat1 = (lat1)*math.pi/180.0
    lat2 = (lat2)*math.pi/180.0
 
    a = (pow(math.sin(dLat/2), 2) + pow(math.sin(dLon / 2), 2) * math.cos(lat1) * math.cos(lat2))
    rad = 6371
    c = 2 * math.asin(math.sqrt(a))
    return rad * c

def create_app():
    global totalClientCount
    app = Flask(__name__)
    driver = GraphDatabase.driver(URI, auth=AUTH)
    driver.verify_connectivity()

    def runQuery(query, parameters=None):
        with driver.session() as session:
            return session.run(query, parameters=parameters).data()
        
    def initIndexes():
        with driver.session() as session:
            session.run("CREATE INDEX clientIdIndex IF NOT EXISTS FOR (c:Client) ON (c.clientId)")
            session.run("CREATE INDEX clientEmailIndex IF NOT EXISTS FOR (c:Client) ON (c.email)")
            session.run("CREATE INDEX licensePlateIndex IF NOT EXISTS FOR (v:Vehicle) ON (v.licensePlate)")
            session.run("CREATE INDEX tripIdIndex IF NOT EXISTS FOR (t:Trip) ON (t.tripId)")
            session.run("CREATE INDEX tripVehIdIndex IF NOT EXISTS FOR (t:Trip) ON (t.vehicleId)")
            session.run("CREATE INDEX posTripIdIndex IF NOT EXISTS FOR (p:Position) ON (p.tripId)")

    initIndexes()

    clientCount = runQuery(
        """
        MATCH (c:Client)
        RETURN COUNT(c) AS count
        """
    )
    totalClientCount = clientCount[0]['count']

    @app.route('/clients', methods=['POST'])
    def registerClient():
        global totalClientCount
        print(f"totalClientCount = {totalClientCount}")
        data = request.get_json()
        firstName, lastName, email, birthDate = data.get('firstName'), data.get('lastName'), data.get('email'), data.get('birthDate')

        if not firstName or not lastName or not email or not birthDate or firstName.strip()=="" or lastName.strip()=="" or email.strip()=="" or birthDate.strip()=="":
            return jsonify("Could not register the client: mandatory attributes are missing."), 400

        generatedId = (firstName[:3]+lastName[:3]).lower() + str(totalClientCount+1)

        result = runQuery(
            """
            MATCH (c:Client {email: $email})
            WITH COUNT(c) AS exists
            WHERE exists = 0
            MERGE (c:Client {email: $email, firstName: $firstName, lastName: $lastName, birthDate: date($birthDate), clientId: $clientId})
            RETURN exists
            """,
            {"firstName": firstName, "lastName": lastName, "email": email, "birthDate": birthDate, "clientId": generatedId}
        )

        if not result or result[0]['exists']>0:
            return jsonify("Client with this email already exists."), 400

        totalClientCount+=1

        return jsonify('Client succesfully registered.'), 200
        
    @app.route('/clients', methods=['GET'])
    def getClientInfo():
        clientId, email = request.args.get('clientId', default=None), request.args.get('email', default=None)

        if clientId:
            result = runQuery(
                """
                MATCH (c:Client {clientId: $clientId})
                RETURN c.firstName AS firstName, 
                    c.lastName AS lastName, 
                    c.email AS email, 
                    c.birthDate AS birthDate
                """,
                {"clientId": clientId}
            )
        elif email:
            result = runQuery(
                """
                MATCH (c:Client {email: $email})
                RETURN c.firstName AS firstName, 
                    c.lastName AS lastName, 
                    c.email AS email, 
                    c.birthDate AS birthDate
                """,
                {"email": email}
            )
        else:
            result = runQuery(
                """
                MATCH (c:Client)
                RETURN c.firstName AS firstName, 
                    c.lastName AS lastName, 
                    c.email AS email, 
                    c.birthDate AS birthDate
                """
            )

        formRes = []
        for client in result:
            formClient = {
                "firstName": client["firstName"],
                "lastName": client["lastName"],
                "email": client["email"],
                "birthDate": client["birthDate"].iso_format() if client["birthDate"] else None
            }
            formRes.append(formClient)

        return jsonify(formRes), 200

    @app.route('/<clientId>/vehicles', methods=['POST'])
    def registerVehicle(clientId):
        data = request.get_json()
        model, manufacturer, licensePlate, vin, manufactureYear, totalTripLength, totalTripDuration = data.get('model'), data.get('manufacturer'), data.get('licensePlate'), data.get('vin'), data.get('manufactureYear'), data.get('totalTripLength', 0), data.get('totalTripDuration', 0)


        if not model or not manufacturer or not licensePlate or not vin or not manufactureYear or str(model).strip() == "" or str(manufacturer).strip() == "" or str(licensePlate).strip() == "" or str(vin).strip() == "" or str(manufactureYear).strip() == "":
            return jsonify("Could not register the vehicle: mandatory attributes are missing."), 400

        vehicleExists = runQuery(
            """
            MATCH (v:Vehicle)
            WHERE v.licensePlate = $licensePlate OR v.vin = $vin
            RETURN COUNT(v) AS count
            """,
            {"licensePlate": licensePlate, "vin": vin}
        )

        if vehicleExists and vehicleExists[0]['count']>0:
            return jsonify("Vehicle with this license plate or VIN already exists."), 400
        
        result = runQuery(
            """
            MATCH (c:Client {clientId: $clientId})
            WITH c
            CREATE (v:Vehicle {
                model: $model,
                manufacturer: $manufacturer,
                licensePlate: $licensePlate,
                vin: $vin,
                manufactureYear: $manufactureYear,
                totalTripLength: $totalTripLength,
                totalTripDuration: $totalTripDuration
            })
            CREATE (c)-[:OWNS]->(v)
            RETURN COUNT(c) AS clientExists
            """,                # length input in km, duration - in hours
            {"clientId": clientId, "model": model, "manufacturer": manufacturer, "licensePlate": licensePlate, "vin": vin, "manufactureYear": manufactureYear, "totalTripLength": totalTripLength,"totalTripDuration": totalTripDuration}
        )

        if not result or result[0]["clientExists"] == 0:
            return jsonify("Client with provided ID does not exist."), 400

        return jsonify("Vehicle registered successfully."), 200

    @app.route('/<clientId>/vehicles', methods=['GET'])
    def getClientsVehicles(clientId):
        result = runQuery(
            """
            MATCH (c:Client {clientId: $clientId})-[:OWNS]->(v:Vehicle)
            RETURN v.model AS model,
                v.manufacturer AS manufacturer,
                v.licensePlate AS licensePlate,
                v.vin AS vin,
                v.manufactureYear AS manufactureYear,
                v.totalTripLength AS totalTripLength,  
                v.totalTripDuration AS totalTripDuration 
            """,
            {"clientId": clientId}
        )

        if not result: 
            return jsonify("No vehicles found for the given client ID or the ID doesn't exist."), 404

        return jsonify(result), 200

    @app.route('/<licensePlate>/startTrip', methods=['POST'])
    def startTrip(licensePlate):
        startTime = int(datetime.now().timestamp())

        result = runQuery(
            """
            MATCH (c:Client)-[:OWNS]->(v:Vehicle {licensePlate: $licensePlate})
            CREATE (t:Trip {
                vehicleId: $licensePlate,
                tripId: $tripId,
                startTime: datetime({epochSeconds: $startTime}),
                endTime: 0,
                length: 0,
                isCompleted: false
            })
            RETURN c.clientId AS clientId, t.tripId AS tripId, t.startTime AS startTime
            """,
            {"licensePlate": licensePlate, "tripId": f"{licensePlate}{startTime}{random.randint(1, 9999)}", "startTime": startTime}
        )

        if not result:
            return jsonify("Vehicle with provided license plate does not exist."), 404

        return jsonify({
            "clientId": result[0]["clientId"],
            "tripId": result[0]["tripId"],
            "startTime": result[0]["startTime"].iso_format()
        }), 200
    
    @app.route('/<tripId>/updatePosition', methods=['PUT'])
    def updatePosition(tripId):
        data = request.get_json()
        latitude = float(data["latitude"])
        longitude = float(data["longitude"])
        
        if latitude is None or longitude is None:
            return jsonify("Missing latitude or longitude."), 400

        currTime = datetime.now()

        result = runQuery(
            """
            MATCH (t:Trip {tripId: $tripId})
            OPTIONAL MATCH (prev:Position {tripId: $tripId})
            WHERE NOT (prev)-[:NEXT]->()
            CREATE (p:Position {
                tripId: $tripId,
                latitude: $latitude,
                longitude: $longitude,
                timestamp: datetime($timestamp)
            })
            WITH t, p, prev
            FOREACH (x IN CASE WHEN prev IS NULL THEN [1] ELSE [] END |
                CREATE (t)-[:STARTED_AT]->(p)
            )
            FOREACH (x IN CASE WHEN prev IS NOT NULL THEN [1] ELSE [] END |
                CREATE (prev)-[:NEXT]->(p)
            )
            RETURN p
            """,
            {"tripId": tripId, "latitude": latitude, "longitude": longitude, "timestamp": currTime.isoformat()}
        )

        if not result:
            return jsonify("Trip not found."), 404

        return jsonify("Position updated successfully."), 200

    @app.route('/<tripId>/stopTrip', methods=['POST'])
    def stopTrip(tripId):
        currTime = datetime.now()
        stopTimeEp = int(currTime.timestamp())

        positions = runQuery(
            """
            MATCH (t:Trip {tripId: $tripId})-[:STARTED_AT]->(start:Position)
            MATCH (lastPos:Position {tripId: $tripId})
            WHERE NOT (lastPos)-[:NEXT]->()
            WITH t, start, lastPos,
                datetime(t.startTime).epochSeconds AS startTimeEp
            RETURN t, start, lastPos, startTimeEp
            """,
            {"tripId": tripId}
        )

        if not positions:
            return jsonify("Trip or positions not found."), 404

        startPos, endPos, startTimeEp = positions[0]['start'], positions[0]['lastPos'], positions[0]['startTimeEp']

        tripLength = haversine(startPos['latitude'], startPos['longitude'], endPos['latitude'], endPos['longitude'])

        duration = (stopTimeEp-startTimeEp)/3600

        result = runQuery(
            """
            MATCH (t:Trip {tripId: $tripId})
            MATCH (lastPos:Position {tripId: $tripId})
            WHERE NOT (lastPos)-[:NEXT]->()
            MATCH (v:Vehicle {licensePlate: t.vehicleId})
            CREATE (lastPos)-[:ENDED_AT]->(t)
            SET t.endTime = datetime({epochSeconds: $stopTime}),
                t.isCompleted = true,
                t.length = $length
            SET v.totalTripLength = COALESCE(v.totalTripLength, 0) + $length,
                v.totalTripDuration = COALESCE(v.totalTripDuration, 0) + $duration
            RETURN t
            """,
            {"tripId": tripId, "stopTime": stopTimeEp, "length": tripLength, "duration": duration}
        )

        if not result:
            return jsonify("Failed to stop trip."), 500

        return jsonify("Trip stopped successfully."), 200
    
    @app.route('/<clientId>/trips', methods=['GET'])
    def getTrips(clientId):
        vehicleId = request.args.get('vehicleId')  

        if vehicleId:
            result = runQuery(
                """
                MATCH (c:Client {clientId: $clientId})-[:OWNS]->(v:Vehicle {licensePlate: $vehicleId})
                MATCH (t:Trip {vehicleId: v.licensePlate})
                OPTIONAL MATCH (t)-[:STARTED_AT]->(start:Position)
                OPTIONAL MATCH (end:Position)-[:ENDED_AT]->(t)
                RETURN t.tripId AS tripId,
                    t.startTime AS startTime,
                    t.endTime AS endTime,
                    t.isCompleted AS isCompleted,
                    t.length AS length,
                    v.licensePlate AS vehicleLicensePlate,
                    v.manufacturer AS vehicleManufacturer,
                    v.model AS vehicleModel,
                    start.latitude AS startLatitude,
                    start.longitude AS startLongitude,
                    end.latitude AS endLatitude,
                    end.longitude AS endLongitude
                ORDER BY t.startTime DESC
                """,
                {"clientId": clientId, "vehicleId": vehicleId}
            )
        else:
            result = runQuery(
                """
                MATCH (c:Client {clientId: $clientId})-[:OWNS]->(v:Vehicle)
                MATCH (t:Trip)
                WHERE t.vehicleId = v.licensePlate
                OPTIONAL MATCH (t)-[:STARTED_AT]->(start:Position)
                OPTIONAL MATCH (end:Position)-[:ENDED_AT]->(t)
                RETURN t.tripId AS tripId,
                    t.startTime AS startTime,
                    t.endTime AS endTime,
                    t.isCompleted AS isCompleted,
                    t.length AS length,
                    v.licensePlate AS vehicleLicensePlate,
                    v.manufacturer AS vehicleManufacturer,
                    v.model AS vehicleModel,
                    start.latitude AS startLatitude,
                    start.longitude AS startLongitude,
                    end.latitude AS endLatitude,
                    end.longitude AS endLongitude
                ORDER BY t.startTime DESC
                """,
                {"clientId": clientId}
            )

        if not result:
            return jsonify("No trips found for the given client ID."), 404

        formTrips = []
        for trip in result:
            formTrip = {
                "tripId": trip["tripId"],
                "startTime": trip["startTime"].iso_format() if trip["startTime"] else None,
                "endTime": trip["endTime"].iso_format() if trip["endTime"] else None,
                "isCompleted": trip["isCompleted"],
                "length": trip["length"],
                "vehicle": {
                    "licensePlate": trip["vehicleLicensePlate"],
                    "manufacturer": trip["vehicleManufacturer"],
                    "model": trip["vehicleModel"]
                },
                "locations": {
                    "start": {
                        "latitude": trip["startLatitude"],
                        "longitude": trip["startLongitude"]
                    } if trip["startLatitude"] and trip["startLongitude"] else None,
                    "end": {
                        "latitude": trip["endLatitude"],
                        "longitude": trip["endLongitude"]
                    } if trip["endLatitude"] and trip["endLongitude"] else None
                }
            }
            formTrips.append(formTrip)

        return jsonify(formTrips), 200
    
    @app.route('/<tripId>/info', methods=['GET'])
    def getTripInfo(tripId):
        result = runQuery(
            """
            MATCH (t:Trip {tripId: $tripId})
            WITH t,
                CASE 
                    WHEN t.startTime IS NOT NULL AND t.endTime IS NOT NULL
                    THEN duration.inSeconds(t.startTime, t.endTime).seconds/3600.0
                    ELSE null 
                END as tripDuration
            RETURN t.length AS tripLength,
                tripDuration
            """,
            {"tripId": tripId}
        )

        if not result:
            return jsonify("Trip not found."), 404

        trip = result[0]
        
        tripInfo = {
            "tripLength": trip["tripLength"],
            "tripDuration": trip["tripDuration"]
        }

        return jsonify(tripInfo), 200

    @app.route('/vehicles/<licensePlate>/info', methods=['GET'])
    def getCarInfo(licensePlate):
        result = runQuery(
            """
            MATCH (v:Vehicle {licensePlate: $licensePlate})
            RETURN v.totalTripLength AS totalLength,
                v.totalTripDuration AS totalDuration
            """,
            {"licensePlate": licensePlate}
        )

        if not result:
            return jsonify("Vehicle not found."), 404

        vehicle = result[0]
        
        vehicleInfo = {
            "totalTripLength": vehicle["totalLength"],
            "totalTripDuration": vehicle["totalDuration"]
        }

        return jsonify(vehicleInfo), 200

    @app.route('/cleanup', methods=['DELETE'])
    def cleanup():
        global totalClientCount
        runQuery(
            """
            MATCH (n) DETACH DELETE n;
            """
        )
        totalClientCount = 0

        return jsonify("Cleanup successful."), 200

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='127.0.0.1')