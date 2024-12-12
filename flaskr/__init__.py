from neo4j import GraphDatabase
import pytz
from flask import (Flask, request, jsonify, abort)

URI = "neo4j://localhost"
AUTH = ("neo4j", "adminadmin")

totalClientCount = 0
totalVehicleCount = 0

def create_app():
    global totalClientCount
    global totalVehicleCount
    app = Flask(__name__)
    driver = GraphDatabase.driver(URI, auth=AUTH)
    driver.verify_connectivity()

    def runQuery(query, parameters=None):
        with driver.session() as session:
            return session.run(query, parameters=parameters).data()

    clientCount = runQuery(
        """
        MATCH (c:Client)
        RETURN COUNT(c) AS count
        """
    )
    totalClientCount = clientCount[0]['count']

    vehicleCount = runQuery(
        """
        MATCH (v:Vehicle)
        RETURN COUNT(v) AS count
        """
    )
    totalVehicleCount = vehicleCount[0]['count']

    @app.route('/test', methods=['GET'])
    def test():
        firstName = "Nick"
        lastName = "Gain"
        generatedId = firstName[:3] + lastName[:3] + str(totalClientCount)
        print(generatedId)

        return jsonify(generatedId), 200

    @app.route('/clients', methods=['POST'])
    def registerClient():
        global totalClientCount
        print(f"totalClientCount = {totalClientCount}")
        data = request.get_json()
        firstName, lastName, email, birthDate = data.get('firstName'), data.get('lastName'), data.get('email'), data.get('birthDate')

        if not firstName or not lastName or not email or not birthDate or firstName.strip()=="" or lastName.strip()=="" or email.strip()=="" or birthDate.strip()=="":
            return jsonify("Could not register the client: mandatory attributes are missing."), 400

        generatedId = firstName[:3] + lastName[:3] + str(totalClientCount+1)

        result = runQuery(
            """
            MATCH (c:Client {email: $email})
            WITH COUNT(c) AS exists
            WHERE exists = 0
            MERGE (c:Client {email: $email, firstName: $firstName, lastName: $lastName, birthDate: $birthDate, clientId: $clientId})
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
                RETURN c.firstName AS firstName, c.lastName AS lastName, c.email AS email, c.birthDate AS birthDate
                """,
                {"clientId": clientId}
            )
        elif email:
            result = runQuery(
                """
                MATCH (c:Client {email: $email})
                RETURN c.firstName AS firstName, c.lastName AS lastName, c.email AS email, c.birthDate AS birthDate
                """,
                {"email": email}
            )
        else:
            result = runQuery(
                """
                MATCH (c:Client)
                RETURN c.firstName AS firstName, c.lastName AS lastName, c.email AS email, c.birthDate AS birthDate
                """
            )

        return jsonify(result), 200

    @app.route('/<clientId>/vehicles', methods=['POST'])
    def registerVehicle(clientId):
        data = request.get_json()
        model, manufacturer, licensePlate, vin, manufactureYear, totalTripLength, totalTripDuration = data.get('model'), data.get('manufacturer'), data.get('licensePlate'), data.get('vin'), data.get('manufactureYear'), data.get('totalTripLength', 0), data.get('totalTripDuration', 0)


        if not model or not manufacturer or not licensePlate or not vin or not manufactureYear or str(model).strip() == "" or str(manufacturer).strip() == "" or str(licensePlate).strip() == "" or str(vin).strip() == "" or str(manufactureYear).strip() == "":
            return jsonify("Could not register the vehicle: mandatory attributes are missing."), 400

        clientExists = runQuery(
            """
            MATCH (c:Client {clientId: $clientId})
            RETURN COUNT(c) AS count
            """,
            {"clientId": clientId}
        )

        if not clientExists or clientExists[0]['count']==0:
            return jsonify("Client with provided ID does not exist."), 400

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

        runQuery(
            """
            MATCH (c:Client {clientId: $clientId})
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
            """,
            {"clientId": clientId, "model": model, "manufacturer": manufacturer, "licensePlate": licensePlate, "vin": vin, "manufactureYear": manufactureYear, "totalTripLength": totalTripLength, "totalTripDuration": totalTripDuration,}
        )

        return jsonify("Vehicle registered successfully."), 200

    @app.route('/<clientId>/vehicles', methods=['GET'])
    def getClientsVehicles(clientId):
        
        return jsonify('test'), 200

    @app.route('/cleanup', methods=['DELETE'])
    def cleanup():
        global totalClientCount
        global totalVehicleCount
        runQuery(
            """
            MATCH (n) DETACH DELETE n;
            """
        )
        totalClientCount = 0
        totalVehicleCount = 0

        return jsonify("Cleanup successful."), 200

    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='127.0.0.1')
    