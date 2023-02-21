import random
import datetime
import time
import uuid
from kafka import KafkaProducer
import json

airplane_models = ['Boeing 747', 'Airbus A320', 'Boeing 787', 'Airbus A380']
airline_companies = {
    'Delta Air Lines': [
        {'departure_city': 'Atlanta', 'arrival_cities': ['Los Angeles', 'New York', 'Las Vegas', 'Miami']},
        {'departure_city': 'Los Angeles', 'arrival_cities': ['Atlanta', 'New York', 'San Francisco', 'Seattle']},
        {'departure_city': 'New York', 'arrival_cities': ['Atlanta', 'Los Angeles', 'Chicago', 'Las Vegas', 'Miami']},
        {'departure_city': 'Las Vegas', 'arrival_cities': ['Atlanta', 'Los Angeles', 'New York']},
        {'departure_city': 'Miami', 'arrival_cities': ['Atlanta', 'New York']}
    ],
    'United Airlines': [
        {'departure_city': 'Chicago', 'arrival_cities': ['Houston', 'San Francisco', 'Seattle', 'Phoenix']},
        {'departure_city': 'Houston', 'arrival_cities': ['Chicago', 'San Francisco', 'Seattle', 'Phoenix']},
        {'departure_city': 'San Francisco', 'arrival_cities': ['Chicago', 'Houston', 'Seattle', 'Phoenix']},
        {'departure_city': 'Seattle', 'arrival_cities': ['Chicago', 'Houston', 'San Francisco', 'Phoenix']},
        {'departure_city': 'Phoenix', 'arrival_cities': ['Chicago', 'Houston', 'San Francisco', 'Seattle']}
    ],
    'American Airlines': [
        {'departure_city': 'New York', 'arrival_cities': ['Los Angeles', 'Chicago', 'Las Vegas', 'Miami']},
        {'departure_city': 'Los Angeles', 'arrival_cities': ['New York', 'Chicago', 'Las Vegas']},
        {'departure_city': 'Chicago', 'arrival_cities': ['New York', 'Los Angeles', 'Las Vegas']},
        {'departure_city': 'Las Vegas', 'arrival_cities': ['New York', 'Los Angeles', 'Chicago']},
        {'departure_city': 'Miami', 'arrival_cities': ['New York', 'Chicago']}
    ],
    'Southwest Airlines': [
        {'departure_city': 'Houston', 'arrival_cities': ['Phoenix', 'Atlanta', 'Chicago', 'Seattle']},
        {'departure_city': 'Phoenix', 'arrival_cities': ['Houston', 'Atlanta', 'Chicago', 'Seattle']},
        {'departure_city': 'Atlanta', 'arrival_cities': ['Houston', 'Phoenix', 'Chicago', 'Seattle']},
        {'departure_city': 'Chicago', 'arrival_cities': ['Houston', 'Phoenix', 'Atlanta', 'Seattle']},
        {'departure_city': 'Seattle', 'arrival_cities': ['Houston', 'Phoenix', 'Atlanta', 'Chicago']}
    ]
}

flight_delay = 1 # delay in minutes

producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda m: json.dumps(m).encode('ascii'))

while True:
    for company, flights in airline_companies.items():
        for flight in flights:
            departure_city = flight['departure_city']
            for arrival_city in flight['arrival_cities']:
                # Generate random sensor data
                flight_speed = round(random.uniform(400, 500), 2)
                altitude = round(random.uniform(10000, 50000), 2)
                engine_performance            = round(random.uniform(0, 100), 2)
            temperature = round(random.uniform(0, 40), 2)
            humidity = round(random.uniform(20, 80), 2)
            pressure = round(random.uniform(900, 1100), 2)

            # Generate random flight data
            airplane_id = str(uuid.uuid4())
            flight_id = str(uuid.uuid4())
            airplane_model = random.choice(airplane_models)
            flight_number = random.randint(1000, 9999)
            departure_time = (datetime.datetime.now() + datetime.timedelta(hours=random.randint(1, 24))).strftime("%Y-%m-%d %H:%M:%S")
            arrival_time = (datetime.datetime.now() + datetime.timedelta(hours=random.randint(25, 48))).strftime("%Y-%m-%d %H:%M:%S")

            # Generate a random timestamp for the data
            current_time = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f")

            # Format the data as a JSON object
            data = {
                "timestamp": current_time,
                "airplane_id": airplane_id,
                "airplane_model": airplane_model,
                "airline_company": company,
                "departure_city": departure_city,
                "arrival_city": arrival_city,
                "flight_id": flight_id,
                "flight_number": flight_number,
                "departure_time": departure_time,
                "arrival_time": arrival_time,
                "flight_speed": flight_speed,
                "altitude": altitude,
                "engine_performance": engine_performance,
                "temperature": temperature,
                "humidity": humidity,
                "pressure": pressure
            }

            # Publish the data to the Kafka topic
            producer.send('airplane-data', value=data)

            # Wait for a delay before generating the next flight
            time.sleep(flight_delay * 60)

# Wait for 1 minute before generating the next data point
time.sleep(60)