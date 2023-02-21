from elasticsearch import Elasticsearch
from elasticsearch import Elasticsearch
from flask import Flask, jsonify, render_template

app = Flask(__name__, template_folder='path/scripts/template')

es = Elasticsearch()

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/average_flight_speed', methods=['GET'])
def get_average_flight_speed():
    # Query Elasticsearch for all enriched_data documents
    res = es.search(index='enriched_data', body={'query': {'match_all': {}}})
    
    # Calculate the average flight speed of all flights
    total_flight_speed = 0
    for hit in res['hits']['hits']:
        total_flight_speed += hit['_source']['FLIGHT_SPEED']
    average_flight_speed = total_flight_speed / res['hits']['total']['value']
    
    # Return the average flight speed as a JSON response
    return jsonify({'message': 'Average flight speed calculated successfully',
                    'average_flight_speed': average_flight_speed,
                    'unit': 'km/h'})



@app.route('/average_engine_performance', methods=['GET'])
def get_average_engine_performance():
    # Query Elasticsearch for all enriched_data documents
    res = es.search(index='enriched_data', body={'query': {'match_all': {}}})
    
    # Calculate the average engine performance of all flights
    total_engine_performance = 0
    for hit in res['hits']['hits']:
        total_engine_performance += hit['_source']['ENGINE_PERFORMANCE']
    average_engine_performance = total_engine_performance / res['hits']['total']['value']
    
    # Return the average engine performance as a JSON response
    return jsonify({'message': 'Average engine performance calculated successfully',
                    'average_engine_performance': average_engine_performance})

@app.route('/number_of_flights', methods=['GET'])
def get_number_of_flights():
    # Query Elasticsearch for all enriched_data documents
    res = es.search(index='enriched_data', body={'query': {'match_all': {}}})
    
    # Calculate the number of flights
    num_flights = res['hits']['total']['value']
    
    # Return the number of flights as a JSON response
    return jsonify({'message': 'Number of flights calculated successfully',
                    'number_of_flights': num_flights})


@app.route('/airline_with_most_flights', methods=['GET'])
def get_airline_with_most_flights():
    # Query Elasticsearch for all enriched_data documents
    res = es.search(index='enriched_data', body={'query': {'match_all': {}}})
    
    # Calculate the number of flights for each airline
    airlines = {}
    for hit in res['hits']['hits']:
        airline_company = hit['_source']['AIRLINE_COMPANY']
        if airline_company in airlines:
            airlines[airline_company] += 1
        else:
            airlines[airline_company] = 1
    
    # Find the airline with the most flights
    max_flights = 0
    max_airline = ''
    for airline, num_flights in airlines.items():
        if num_flights > max_flights:
            max_flights = num_flights
            max_airline = airline
    
    # Return the airline with the most flights as a JSON response
    return jsonify({'airline_with_most_flights': max_airline, 'number_of_flights': max_flights})

@app.route('/top_departure_cities', methods=['GET'])
def get_top_departure_cities():
    # Query Elasticsearch for all enriched_data documents
    res = es.search(index='enriched_data', body={'query': {'match_all': {}}})
    
    # Calculate the number of flights departing from each city
    departure_cities = {}
    for hit in res['hits']['hits']:
        departure_city = hit['_source']['DEPARTURE_CITY']
        if departure_city in departure_cities:
            departure_cities[departure_city] += 1
        else:
            departure_cities[departure_city] = 1
    
    # Sort the departure cities by the number of flights and get the top 5
    top_departure_cities = sorted(departure_cities.items(), key=lambda x: x[1], reverse=True)[:5]
    
    # Return the top departure cities as a JSON response
    return jsonify({'top_departure_cities': top_departure_cities})

@app.route('/top_arrival_cities', methods=['GET'])
def get_top_arrival_cities():
    # Query Elasticsearch for all enriched_data documents
    res = es.search(index='enriched_data', body={'query': {'match_all': {}}})

    # Calculate the number of flights arriving in each city
    arrival_city_counts = {}
    for hit in res['hits']['hits']:
        if 'ARRIVAL_CITY' in hit['_source']:
            arrival_city = hit['_source']['ARRIVAL_CITY']
            if arrival_city in arrival_city_counts:
                arrival_city_counts[arrival_city] += 1
            else:
                arrival_city_counts[arrival_city] = 1

    # Get the top arrival cities
    top_arrival_cities = dict(sorted(arrival_city_counts.items(), key=lambda item: item[1], reverse=True)[:5])

    # Return the top arrival cities as a JSON response
    return jsonify({'top_arrival_cities': top_arrival_cities})


@app.route('/average_temperature', methods=['GET'])
def get_average_temperature():
    # Query Elasticsearch for all enriched_data documents
    res = es.search(index='enriched_data', body={'query': {'match_all': {}}})

    # Calculate the average temperature of all flights
    total_temperature = 0
    for hit in res['hits']['hits']:
        total_temperature += hit['_source']['TEMPERATURE']
    average_temperature = total_temperature / res['hits']['total']['value']

    # Return the average temperature as a JSON response
    return jsonify({
        'message': 'Average temperature calculated successfully',
        'average_temperature': average_temperature,
        'unit': '°C'
    })
    
@app.route('/average_humidity', methods=['GET'])
def get_average_humidity():
    # Query Elasticsearch for all enriched_data documents
    res = es.search(index='enriched_data', body={'query': {'match_all': {}}})
    
    # Calculate the average humidity of all flights
    total_humidity = 0
    for hit in res['hits']['hits']:
        total_humidity += hit['_source']['HUMIDITY']
    average_humidity = total_humidity / res['hits']['total']['value']
    
    # Return the average humidity as a JSON response
    return jsonify({'average_humidity': average_humidity})

@app.route('/average_pressure', methods=['GET'])
def get_average_pressure():
    # Query Elasticsearch for all enriched_data documents
    res = es.search(index='enriched_data', body={'query': {'match_all': {}}})
    
    # Calculate the average pressure of all flights
    total_pressure = 0
    for hit in res['hits']['hits']:
        total_pressure += hit['_source']['PRESSURE']
    average_pressure = total_pressure / res['hits']['total']['value']
    
    # Return the average pressure as a JSON response
    return jsonify({
        'message': 'Average pressure calculated successfully',
        'average_pressure': average_pressure,
        'unit': 'hPa'
    })
    
@app.route('/total_distance_flown', methods=['GET'])
def get_total_distance_flown():
    # Query Elasticsearch for all enriched_data documents
    res = es.search(index='enriched_data', body={'query': {'match_all': {}}})
    
    # Calculate the total distance flown by all flights
    total_distance = 0
    for hit in res['hits']['hits']:
        altitude = hit['_source']['ALTITUDE']
        distance = altitude * 0.3048 / 1000
        total_distance += distance
    
    # Return the total distance flown as a JSON response
    return jsonify({
        'message': 'Total distance flown calculated successfully',
        'total_distance': total_distance,
        'unit': 'km'
    })
    

    


if __name__ == '__main__':
    app.run(debug=True)
