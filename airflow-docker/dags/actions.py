import datetime
from typing import List

import requests
from sqlalchemy import select

from utilities.config import get_keys_and_constants
from utilities.db_connection import create_session
from utilities.models import MonitoringStation, AirQuality, ExchangeRate
from utilities.shared import prepare_headers

keys = get_keys_and_constants()
token_aqi = keys.token_aqi
token_exchange_rate = keys.token_exchange_rate


def find_eur_to_rsd_and_usd_exchange_rate_for_today():
    today = datetime.date.today()
    url = f"https://api.apilayer.com/exchangerates_data/{today}?symbols=USD%2CRSD&base=EUR"

    headers = {
        "apikey": token_exchange_rate
    }
    exchange_rate = None

    try:
        response = requests.get(url, headers=headers)
        result = response.json()
        exchange_rate = extract_daily_exchange_rate(result, today)
    except Exception as e:
        msg = f'Could not retrieve exchange rate for: {str(e)}'
        raise Exception(msg)

    save_exchange_rate_to_db(exchange_rate)


def save_exchange_rate_to_db(exchange_rate: ExchangeRate) -> None:
    try:
        if exchange_rate is not None:
            with create_session() as session:
                try:
                    session.add(exchange_rate)
                    session.commit()
                except Exception as e:
                    session.rollback()
                    msg = f'Could not save exchange rate to the db: {str(e)}'
                    raise Exception(msg)
    except Exception as e:
        msg = f'Could not connect to the database{str(e)}'
        raise Exception(msg)


def extract_daily_exchange_rate(result: dict, today: datetime.date) -> ExchangeRate:
    exchange_rate = ExchangeRate()
    exchange_rate.exchange_date = today
    exchange_rate.price_EUR_RSD = result['rates']['RSD']
    exchange_rate.price_EUR_USD = result['rates']['USD']
    return exchange_rate


def find_aqi_stations():
    stations = send_request_to_find_stations()
    for station_object in stations:
        station = extract_monitoring_station(station_object)
        save_monitoring_station_to_db(station)


def send_request_to_find_stations() -> dict:
    headers = prepare_headers()
    stations = None
    req_str = f'http://api.waqi.info/search/?keyword=Serbia&token={token_aqi}'
    try:
        response = requests.get(req_str, headers=headers)
        stations = response.json()['data']
    except Exception as e:
        msg = f'Could not retrieve air quality data: {str(e)}'
        raise Exception(msg)
    return stations


def save_monitoring_station_to_db(station: MonitoringStation) -> None:
    try:
        with create_session() as session:
            try:
                session.add(station)
                session.commit()
            except Exception as e:
                session.rollback()
                msg = f'Could not save station to the db: {str(e)}'
                raise Exception(msg)
    except Exception as e:
        msg = f'Could not connect to the database{str(e)}'
        raise Exception(msg)


def extract_monitoring_station(station_object: dict) -> MonitoringStation:
    station = MonitoringStation()
    station.monitoring_station_name = station_object['station']['name']
    station.lat = station_object['station']['geo'][0]
    station.lon = station_object['station']['geo'][1]
    return station


def fill_daily_stations_data():
    headers = prepare_headers()
    stations = find_monitoring_stations()
    for station_obj in stations:
        w_aqi = None
        try:
            w_aqi = fill_daily_air_quality_data_per_station(headers, station_obj[0])
        except Exception as e:
            msg = f'Could not find air quality data for the station. {str(e)}'
            raise Exception(msg)
        save_air_quality_to_db(w_aqi)


def save_air_quality_to_db(w_aqi: AirQuality) -> None:
    try:
        if w_aqi is not None:
            with create_session() as session:
                try:
                    session.add(w_aqi)
                    session.commit()
                except Exception as e:
                    session.rollback()
                    msg = f'Could not save measurement to the db: {str(e)}'
                    raise Exception(msg)
    except Exception as e:
        msg = f'Could not connect to the database{str(e)}'
        raise Exception(msg)


def find_monitoring_stations() -> List[MonitoringStation]:
    try:
        with create_session() as session:
            query = select(MonitoringStation)
            stations = session.execute(query).all()
    except Exception as e:
        msg = f'Could not read stations from db: {str(e)}'
        raise Exception(msg)
    return stations


def fill_daily_air_quality_data_per_station(headers: CaseInsensitiveDict, station_obj: MonitoringStation) -> AirQuality:
    req_str = f'http://api.waqi.info/feed/geo:{station_obj.lat};{station_obj.lon}/?token={token_aqi}'
    resp = requests.get(req_str, headers=headers)
    air_quality = resp.json()['data']
    return extract_air_quality_object(air_quality, station_obj)


def extract_air_quality_object(air_quality: dict, station_obj: MonitoringStation) -> AirQuality:
    w_aqi = AirQuality()
    w_aqi.date = datetime.datetime.today()
    w_aqi.monitoring_station_id = station_obj.monitoring_station_id
    w_aqi.co = air_quality['iaqi']['co']['v'] if 'co' in air_quality['iaqi'] else None
    w_aqi.o3 = air_quality['forecast']['daily']['o3'][0]['avg']
    w_aqi.pm25 = air_quality['iaqi']['pm25']['v'] if 'pm25' in air_quality['iaqi'] else None
    w_aqi.pm10 = air_quality['iaqi']['pm10']['v'] if 'pm10' in air_quality['iaqi'] else None
    w_aqi.no2 = air_quality['iaqi']['no2']['v'] if 'no2' in air_quality['iaqi'] else None
    w_aqi.so2 = air_quality['iaqi']['so2']['v'] if 'so2' in air_quality['iaqi'] else None
    return w_aqi


if __name__ == '__main__':
    fill_daily_stations_data()
    find_eur_to_rsd_and_usd_exchange_rate_for_today()
