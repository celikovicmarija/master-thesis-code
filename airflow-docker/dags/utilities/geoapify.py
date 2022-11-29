import csv
from typing import List

import pandas as pd
import requests

from .location import Location
from .shared import prepare_headers


def read_api_keys():
    data = pd.read_csv('geoapify_keys.csv')
    return data['key'].tolist()


api_keys = read_api_keys()


# counters are my general idea for number of requests that have been sent by key

def send_and_receive_geocoding(df_for_geocoding):
    counter = 0

    locations = []
    valid_api_key = api_keys.pop()
    for index, row in df_for_geocoding.iterrows():
        try:
            counter += 1
            if counter == 2999:
                counter = 0
                valid_api_key = api_keys.pop()

            query = ''
            if row['street']:
                query += row['street'].replace(' ', '%20')
            elif row['micro_location']:
                query += ' ' + row['micro_location'].replace(' ', '%20')

            loc = send_request_to_find_geocoding_info(query, valid_api_key)
            if not loc or 'results' not in loc:
                continue

            location = extract_location_object(loc, row)
            locations.append(location)
        except Exception as e:
            print('Error: ' + str(e))
            continue
    try:
        with open('geocoded.csv', 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC, delimiter=',', lineterminator='\n')
            # cols=['street_oglasi','micro_location_oglasi','name','suburb','district','city','county','state','postcode','lon','lat','formatted','address_line1','address_line2','category',
            #         'result_type','match_type','confidence','popularity','place_id','bbox_lon1','bbox_lat1','bbox_lon2','bbox_lat2',]

            locations_to_write = []
            for location in locations:
                data = [location.street_oglasi, location.micro_location_oglasi, location.name, location.suburb,
                        location.district,
                        location.city, location.county, location.state, location.postcode, location.lon, location.lat,
                        location.formatted, location.address_line1, location.address_line2, location.category,
                        location.result_type, location.match_type, location.confidence,
                        location.popularity, location.place_id, location.bbox_lon1, location.bbox_lat1,
                        location.bbox_lon2, location.bbox_lat2]
                locations_to_write.append(data)

            writer.writerows(locations_to_write)
    except Exception as e:
        print(str(e))


def send_request_to_find_geocoding_info(query, valid_api_key):
    headers = prepare_headers()
    req_str = f'https://api.geoapify.com/v1/geocode/search?text={query}&filter=countrycode:rs&limit=1&format=json&apiKey={valid_api_key}'
    resp = requests.get(req_str, headers=headers)
    loc = resp.json()
    return loc


def extract_location_object(loc: dict, row: dict):
    location = Location()
    location.street_oglasi = row['street']
    location.micro_location_oglasi = row['micro_location']
    location.name = loc['results'][0]['name'] if 'name' in loc['results'][0] else ''
    location.suburb = loc['results'][0]['suburb'] if 'suburb' in loc['results'][0] else ''
    location.district = loc['results'][0]['district'] if 'district' in loc['results'][0] else ''
    location.city = loc['results'][0]['city'] if 'city' in loc['results'][0] else ''
    location.county = loc['results'][0]['county'] if 'county' in loc['results'][0] else ''
    location.state = loc['results'][0]['state'] if 'state' in loc['results'][0] else ''
    location.postcode = loc['results'][0]['postcode'] if 'postcode' in loc['results'][0] else ''
    location.lon = loc['results'][0]['lon'] if 'lon' in loc['results'][0] else ''
    location.lat = loc['results'][0]['lat'] if 'lat' in loc['results'][0] else ''
    location.formatted = loc['results'][0]['formatted'] if 'formatted' in loc['results'][0] else ''
    location.address_line1 = loc['results'][0]['address_line1'] if 'address_line1' in loc['results'][0] else ''
    location.address_line2 = loc['results'][0]['address_line2'] if 'address_line2' in loc['results'][0] else ''
    location.category = loc['results'][0]['category'] if 'category' in loc['results'][0] else ''
    location.result_type = loc['results'][0]['result_type'] if 'result_type' in loc['results'][0] else ''
    location.match_type = loc['results'][0]['rank']['match_type'] if 'match_type' in loc['results'][0][
        'rank'] else ''
    location.confidence = loc['results'][0]['rank']['confidence'] if 'confidence' in loc['results'][0][
        'rank'] else ''
    location.popularity = loc['results'][0]['rank']['popularity'] if 'popularity' in loc['results'][0][
        'rank'] else ''
    location.place_id = loc['results'][0]['place_id'] if 'place_id' in loc['results'][0] else ''
    location.bbox_lon1 = loc['results'][0]['bbox']['lon1'] if 'lon1' in loc['results'][0]['bbox'] else ''
    location.bbox_lat1 = loc['results'][0]['bbox']['lat1'] if 'lat1' in loc['results'][0]['bbox'] else ''
    location.bbox_lon2 = loc['results'][0]['bbox']['lon2'] if 'lon2' in loc['results'][0]['bbox'] else ''
    location.bbox_lat2 = loc['results'][0]['bbox']['lat2'] if 'lat2' in loc['results'][0]['bbox'] else ''
    return location


def send_and_receive_place_api():
    counter = 0
    places = []
    valid_api_key = api_keys.pop()

    with open('geocoded.csv', 'r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        p = 0

        for i, row in enumerate(reader):
            p += 1
            if p < 16185:
                continue
            try:
                counter += 1
                if counter == 2950:
                    counter = 0
                    valid_api_key = api_keys.pop()
                place_list = send_request_to_get_place(row, valid_api_key)
                for place_object in place_list:
                    place = extract_place_object(place_object, row)
                    places.append(place)
            except Exception as e:
                print('Error: ' + str(e))
                continue
        save_place_data_to_csv(places)


def save_place_data_to_csv(places: List[Location]) -> None:
    try:
        with open('places.csv', 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC, delimiter=',', lineterminator='\n')
            # cols = ['street_oglasi', 'micro_location_oglasi', 'name', 'city', 'county',
            #         'lon', 'lat', 'formatted', 'address_line1', 'address_line2',  'categories']
            for place_object in places:
                data = [place_object.street_oglasi, place_object.micro_location_oglasi, place_object.name,
                        place_object.city, place_object.county, place_object.lon, place_object.lat,
                        place_object.formatted, place_object.address_line1, place_object.address_line2,
                        place_object.categories]
                writer.writerow(data)
    except Exception as e:
        print(str(e))


def send_request_to_get_place(row: dict, valid_api_key: str) -> dict:
    headers = prepare_headers()
    req_str = f'https://api.geoapify.com/v2/places?categories=commercial,catering,education,healthcare,leisure,natural,childcare,office.security,office.educational_institution,activity,sport,public_transport,parking,pet,building&filter=circle:{row["lon"]},{row["lat"]},3000&bias=proximity:{row["lon"]},{row["lat"]}&lang=sr&limit=100&apiKey={valid_api_key}'
    resp = requests.get(req_str, headers=headers)
    place_list = resp.json()["features"]
    return place_list


def extract_place_object(place_object: dict, row: dict) -> Location:
    place = Location()
    place.street_oglasi = row['street_oglasi']
    place.micro_location_oglasi = row['micro_location_oglasi']
    place.name = place_object['properties']['name'] if 'name' in place_object['properties'] else ''
    place.city = place_object['properties']['city'] if 'city' in place_object['properties'] else ''
    place.county = place_object['properties']['county'] if 'county' in place_object[
        'properties'] else ''
    place.lat = place_object['properties']['lat'] if 'lat' in place_object['properties'] else ''
    place.lon = place_object['properties']['lon'] if 'lon' in place_object['properties'] else ''
    place.address_line1 = place_object['properties']['address_line1'] if 'address_line1' in \
                                                                         place_object[
                                                                             'properties'] else ''
    place.address_line2 = place_object['properties']['address_line2'] if 'address_line2' in \
                                                                         place_object[
                                                                             'properties'] else ''
    place.place_id = place_object['properties']['place_id'] if 'place_id' in place_object[
        'properties'] else ''
    place.distance = place_object['properties']['distance'] if 'distance' in place_object[
        'properties'] else ''
    place.formatted = place_object['properties']['formatted'] if 'formatted' in place_object[
        'properties'] else ''
    small_list = []
    for cat in place_object['properties']['categories']:
        small_list.append(cat)
    place.categories = small_list
    return place


def send_and_receive_place_details_api():
    counter = 0
    place_details = []
    valid_api_key = api_keys.pop()

    with open('geocoded.csv', 'r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            counter += 1
            if counter < 17998:
                continue
                # valid_api_key = api_keys.pop()
            try:
                place_details_list = send_request_to_find_place_details(row, valid_api_key)
                for place in place_details_list:
                    place_details_obj = fill_place_details_object(place, row)
                    small_list = []
                    if 'categories' in place['properties']:
                        for cat in place['properties']['categories']:
                            small_list.append(cat)
                    place_details_obj.categories = small_list
                    place_details.append(place_details_obj)
            except Exception as e:
                print('Error: ' + str(e))
                continue

        save_place_details_to_csv(place_details)


def save_place_details_to_csv(place_details: List[Location]) -> None:
    try:
        with open('place_details_more_details.csv', 'a', encoding='utf-8', newline='') as f:
            writer = csv.writer(f, quoting=csv.QUOTE_NONNUMERIC, delimiter=',', lineterminator='\n')
            # cols = ['street_oglasi', 'micro_location_oglasi', 'name', 'city', 'county',
            #         'lon', 'lat', 'formatted','feature_type', 'address_line1', 'address_line2', 'categories']
            for place_details_obj in place_details:
                data = [place_details_obj.street_oglasi, place_details_obj.micro_location_oglasi,
                        place_details_obj.name,
                        place_details_obj.city, place_details_obj.county, place_details_obj.lon,
                        place_details_obj.lat,
                        place_details_obj.formatted, place_details_obj.feature_type,
                        place_details_obj.address_line1,
                        place_details_obj.address_line2, place_details_obj.categories]
                writer.writerow(data)
    except Exception as e:
        print(str(e))


def send_request_to_find_place_details(row: dict, valid_api_key: str) -> dict:
    headers = prepare_headers()
    req_str = f"https://api.geoapify.com/v2/place-details?id={row['place_id']}7&features=details,building.places,radius_100.supermarket,radius_100.cafe,radius_500.supermarket,radius_500.restaurant,radius_500.tourism,radius_500.cafe,radius_500.school,radius_500.atm,radius_500.shopping_mall,radius_500.toilet,radius_1000.pharmacy,radius_1000.entertainment,radius_1000.cafe,radius_1000.shopping_mall,radius_1000.supermarket,radius_1000.tourism,radius_1000.park,radius_1000.atm,radius_1000.playground,radius_1000.school,walk_10.supermarket,walk_10.school,walk_10.pharmacy,walk_10.entertainment,walk_10.atm,walk_10.restaurant,walk_10.playground,walk_30.pharmacy,walk_30.school,walk_30.park,walk_30.playground,walk_30.supermarket,walk_30.shopping_mall,walk_30.cafe,walk_30.restaurant,walk_30.tourism,drive_5.supermarket,drive_10.supermarket,drive_15.supermarket,drive_5.shopping_mall,drive_10.shopping_mall,drive_15.shopping_mall,drive_5.school,drive_5.playground,drive_10.playground,drive_15.playground,drive_5.atm,drive_10.atm,drive_15.atm,drive_5.pharmacy,drive_10.pharmacy,drive_15.pharmacy,drive_5.hospital,drive_10.hospital,drive_15.hospital,drive_5.fuel,drive_10.fuel,drive_15.fuel,drive_5.parking,drive_10.parking,drive_15.parking&apiKey={valid_api_key}"
    resp = requests.get(req_str, headers=headers)
    place_details_list = resp.json()["features"]
    return place_details_list


def fill_place_details_object(place: dict, row: dict) -> Location:
    place_details_obj = Location()
    place_details_obj.street_oglasi = row['street_oglasi']
    place_details_obj.micro_location_oglasi = row['micro_location_oglasi']
    place_details_obj.name = place['properties']['name'] if 'name' in place['properties'] else ''
    place_details_obj.city = place['properties']['city'] if 'city' in place['properties'] else ''
    place_details_obj.city = place['properties']['street'] if 'street' in place['properties'] else ''
    place_details_obj.county = place['properties']['county'] if 'county' in place['properties'] else ''
    place_details_obj.lat = place['properties']['lat'] if 'lat' in place['properties'] else ''
    place_details_obj.lon = place['properties']['lon'] if 'lon' in place['properties'] else ''
    place_details_obj.feature_type = place['properties']['feature_type'] if 'feature_type' in place[
        'properties'] else ''
    place_details_obj.address_line1 = place['properties']['address_line1'] if 'address_line1' in place[
        'properties'] else ''
    place_details_obj.address_line2 = place['properties']['address_line2'] if 'address_line2' in place[
        'properties'] else ''
    place_details_obj.place_id = place['properties']['place_id'] if 'place_id' in place[
        'properties'] else ''
    place_details_obj.distance = place['properties']['distance'] if 'distance' in place[
        'properties'] else ''
    place_details_obj.formatted = place['properties']['formatted'] if 'formatted' in place[
        'properties'] else ''
    return place_details_obj
