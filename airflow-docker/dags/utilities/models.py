# coding: utf-8
from sqlalchemy import BigInteger, Column, DECIMAL, Date, ForeignKey, SmallInteger, String, text
from sqlalchemy.dialects.mysql import TEXT, TINYINT, VARCHAR
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()
metadata = Base.metadata


class AirQualityCdc(Base):
    __tablename__ = 'air_quality_cdc'

    id = Column(BigInteger, primary_key=True)
    date = Column(Date, nullable=False)
    pm25 = Column(SmallInteger)
    pm10 = Column(SmallInteger)
    o3 = Column(SmallInteger)
    no2 = Column(SmallInteger)
    so2 = Column(SmallInteger)
    co = Column(SmallInteger)
    monitoring_station_id = Column(TINYINT, nullable=False)
    action = Column(String(100, 'utf8mb4_general_ci'))


class Category(Base):
    __tablename__ = 'category'

    category_id = Column(BigInteger, primary_key=True)
    category_name = Column(VARCHAR(100))


class ExchangeRateCdc(Base):
    __tablename__ = 'exchange_rate_cdc'

    action = Column(String(100, 'utf8mb4_general_ci'))
    exchange_rate_id = Column(BigInteger)
    price_EUR_USD = Column(DECIMAL(20, 4))
    exchange_date = Column(Date, nullable=False)
    price_EUR_RSD = Column(DECIMAL(20, 4))
    cdc_id = Column(BigInteger, primary_key=True)


class ExchangeRate(Base):
    __tablename__ = 'exchange_rates'

    exchange_rate_id = Column(BigInteger, primary_key=True)
    price_EUR_USD = Column(DECIMAL(20, 4))
    exchange_date = Column(Date, nullable=False)
    price_EUR_RSD = Column(DECIMAL(20, 4))


class Geocode(Base):
    __tablename__ = 'geocode'

    geocode_id = Column(BigInteger, primary_key=True)
    bbox_lat1 = Column(DECIMAL(30, 26))
    bbox_lat2 = Column(DECIMAL(30, 26))
    bbox_lon1 = Column(DECIMAL(30, 26))
    bbox_lon2 = Column(DECIMAL(30, 26))
    place_id = Column(VARCHAR(500))
    result_type = Column(VARCHAR(100))
    category = Column(VARCHAR(200))
    formatted = Column(VARCHAR(200))
    address_line1 = Column(VARCHAR(200))
    address_line2 = Column(VARCHAR(200))
    feature_type = Column(VARCHAR(100))
    lat = Column(DECIMAL(30, 26))
    lon = Column(DECIMAL(30, 26))
    postcode = Column(String(100))
    state = Column(VARCHAR(200))
    county = Column(VARCHAR(200))
    city = Column(VARCHAR(200))
    district = Column(VARCHAR(200))
    suburb = Column(VARCHAR(200))
    name = Column(VARCHAR(200))
    micro_location_oglasi = Column(String(100))
    street_oglasi = Column(String(100))


class HeatingType(Base):
    __tablename__ = 'heating_type'

    heating_type_id = Column(TINYINT, primary_key=True)
    heating_type_name = Column(VARCHAR(200), nullable=False)


class InterestRateType(Base):
    __tablename__ = 'interest_rate_type'

    interest_rate_type_id = Column(TINYINT, primary_key=True)
    interest_rate_type_name = Column(VARCHAR(20), nullable=False)


class MonitoringStation(Base):
    __tablename__ = 'monitoring_station'

    monitoring_station_name = Column(VARCHAR(50))
    monitoring_station_id = Column(TINYINT, primary_key=True)
    lat = Column(DECIMAL(30, 26))
    lon = Column(DECIMAL(30, 26))


class RealEstatePostCdc(Base):
    __tablename__ = 'real_estate_post_cdc'

    cdc_id = Column(BigInteger, primary_key=True)
    additional = Column(TEXT)
    city = Column(VARCHAR(50))
    date = Column(Date)
    description = Column(TEXT)
    text = Column(VARCHAR(20))
    heating_type_id = Column(TINYINT)  # , server_default=text("'13'")
    link = Column(TEXT)
    location = Column(TEXT)
    micro_location = Column(VARCHAR(200))
    monthly_bills = Column(DECIMAL(18, 0))
    number_of_rooms = Column(TEXT)
    object_state = Column(VARCHAR(50))
    object_type = Column(VARCHAR(50))
    price = Column(DECIMAL(18, 0))
    price_per_unit = Column(DECIMAL(18, 0))
    real_estate_type_id = Column(TINYINT)
    size = Column(DECIMAL(18, 0))
    street = Column(VARCHAR(200))
    title = Column(TEXT)
    total_number_of_floors = Column(VARCHAR(20))
    transaction_type_id = Column(TINYINT)
    is_listed = Column(TINYINT(1))
    source_id = Column(TINYINT)
    measurement_id = Column(TINYINT)
    geocode_id = Column(BigInteger)
    action = Column(VARCHAR(100))
    floor_number = Column(TEXT)


class RealEstateType(Base):
    __tablename__ = 'real_estate_type'

    real_estate_type_id = Column(TINYINT, primary_key=True)
    real_estate_type_name = Column(VARCHAR(20), nullable=False)


class SizeMeasurement(Base):
    __tablename__ = 'size_measurement'

    measurement_id = Column(TINYINT, primary_key=True)
    measurement_name = Column(VARCHAR(10), nullable=False)


class Source(Base):
    __tablename__ = 'source'

    source_name = Column(VARCHAR(20), nullable=False)
    source_id = Column(TINYINT, primary_key=True)


class TransactionType(Base):
    __tablename__ = 'transaction_type'

    transaction_type_name = Column(VARCHAR(20))
    transaction_type_id = Column(TINYINT, primary_key=True)


class AirQuality(Base):
    __tablename__ = 'air_quality'

    measurement_id = Column(BigInteger, primary_key=True)
    date = Column(Date, nullable=False)
    pm25 = Column(SmallInteger)
    pm10 = Column(SmallInteger)
    o3 = Column(SmallInteger)
    no2 = Column(SmallInteger)
    so2 = Column(SmallInteger)
    co = Column(SmallInteger)
    monitoring_station_id = Column(
        ForeignKey('monitoring_station.monitoring_station_id', ondelete='CASCADE', onupdate='CASCADE'), nullable=False,
        index=True)

    monitoring_station = relationship('MonitoringStation')


class InterestRate(Base):
    __tablename__ = 'interest_rate'

    interest_rate_id = Column(BigInteger, primary_key=True)
    date = Column(Date)
    value = Column(DECIMAL(10, 3))
    period = Column(VARCHAR(10))
    interest_rate_type = Column(
        ForeignKey('interest_rate_type.interest_rate_type_id', ondelete='RESTRICT', onupdate='RESTRICT'), index=True)

    interest_rate_type1 = relationship('InterestRateType')


class Place(Base):
    __tablename__ = 'place'

    place_id = Column(BigInteger, primary_key=True)
    name = Column(VARCHAR(300))
    city = Column(VARCHAR(100))
    county = Column(VARCHAR(100))
    lat = Column(DECIMAL(30, 26))
    lon = Column(DECIMAL(30, 26))
    formatted = Column(VARCHAR(200))
    address_line1 = Column(VARCHAR(200))
    address_line2 = Column(VARCHAR(200))
    categories = Column(TEXT)
    geocode_id = Column(ForeignKey('geocode.geocode_id'), index=True)

    geocode = relationship('Geocode')


class PlaceDetail(Base):
    __tablename__ = 'place_details'

    place_details_id = Column(BigInteger, primary_key=True)
    name = Column(VARCHAR(100))
    city = Column(VARCHAR(100))
    county = Column(VARCHAR(100))
    feature_type = Column(VARCHAR(100))
    lat = Column(DECIMAL(30, 26))
    lon = Column(DECIMAL(30, 26))
    formatted = Column(VARCHAR(200))
    address_line1 = Column(VARCHAR(200))
    address_line2 = Column(VARCHAR(200))
    categories = Column(VARCHAR(500))
    geocode_id = Column(ForeignKey('geocode.geocode_id'), index=True)

    geocode = relationship('Geocode')


class RealEstatePost(Base):
    __tablename__ = 'real_estate_post'

    post_id = Column(BigInteger, primary_key=True)
    additional = Column(TEXT)
    city = Column(VARCHAR(50))
    date = Column(Date)
    description = Column(TEXT)
    floor_number = Column(TEXT)
    heating_type_id = Column(ForeignKey('heating_type.heating_type_id', ondelete='RESTRICT', onupdate='RESTRICT'),
                             index=True, server_default=text("'13'"))
    link = Column(TEXT)
    location = Column(TEXT)
    micro_location = Column(VARCHAR(200))
    monthly_bills = Column(DECIMAL(18, 0))
    number_of_rooms = Column(TEXT)
    object_state = Column(VARCHAR(50))
    object_type = Column(VARCHAR(50))
    price = Column(DECIMAL(18, 0))
    price_per_unit = Column(DECIMAL(18, 0))
    real_estate_type_id = Column(
        ForeignKey('real_estate_type.real_estate_type_id', ondelete='RESTRICT', onupdate='RESTRICT'), index=True)
    size = Column(DECIMAL(18, 0))
    street = Column(VARCHAR(200))
    title = Column(TEXT)
    total_number_of_floors = Column(VARCHAR(20))
    transaction_type_id = Column(
        ForeignKey('transaction_type.transaction_type_id', ondelete='RESTRICT', onupdate='RESTRICT'), index=True)
    is_listed = Column(TINYINT(1))
    source_id = Column(ForeignKey('source.source_id'), index=True)
    measurement_id = Column(ForeignKey('size_measurement.measurement_id', ondelete='RESTRICT', onupdate='RESTRICT'),
                            index=True)
    geocode_id = Column(ForeignKey('geocode.geocode_id'), index=True)

    geocode = relationship('Geocode')
    heating_type = relationship('HeatingType')
    measurement = relationship('SizeMeasurement')
    real_estate_type = relationship('RealEstateType')
    source = relationship('Source')
    transaction_type = relationship('TransactionType')


class Subcategory(Base):
    __tablename__ = 'subcategory'

    subcategory_id = Column(BigInteger, primary_key=True)
    subcategory_name = Column(String(100, 'utf8mb4_general_ci'))
    category_id = Column(ForeignKey('category.category_id'), index=True)

    category = relationship('Category')


class PlaceDetailsSubcategory(Base):
    __tablename__ = 'place_details_subcategory'

    place_details_id = Column(ForeignKey('place_details.place_details_id'), nullable=False, index=True)
    subcategory_id = Column(ForeignKey('subcategory.subcategory_id', ondelete='RESTRICT', onupdate='RESTRICT'),
                            nullable=False, index=True)
    id = Column(BigInteger, primary_key=True)

    place_details = relationship('PlaceDetail')
    subcategory = relationship('Subcategory')


class PlaceSubcategory(Base):
    __tablename__ = 'place_subcategory'

    place_id = Column(ForeignKey('place.place_id'), nullable=False, index=True)
    subcategory_id = Column(ForeignKey('subcategory.subcategory_id'), nullable=False, index=True)
    id = Column(BigInteger, primary_key=True)

    place = relationship('Place')
    subcategory = relationship('Subcategory')
