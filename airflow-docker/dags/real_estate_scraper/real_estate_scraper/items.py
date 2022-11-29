# Define here the models for your scraped items
#
# See documentation in:
# https://docs.scrapy.org/en/latest/topics/items.html

import re
from urllib.parse import urljoin

import scrapy
from itemloaders.processors import TakeFirst, MapCompose, Join
from scrapy import Field
from w3lib.html import remove_tags


def remove_currency(value):
    return value.replace('\xa0€', '').strip()


def remove_new_lines(value):
    return re.sub("(\s)|(,)", " ", value).strip()


def create_full_url(value):
    return urljoin('https://www.halooglasi.com', value)


class RealEstateScraperItem(scrapy.Item):
    # Primary fields
    title = Field(input_processor=MapCompose(remove_tags, remove_new_lines), output_processor=TakeFirst())
    city = Field(input_processor=MapCompose(remove_tags, remove_new_lines), output_processor=TakeFirst())
    link = Field(input_processor=MapCompose(remove_tags),
                 output_processor=TakeFirst())
    location = Field(input_processor=MapCompose(remove_tags, remove_new_lines), output_processor=TakeFirst())  # opstina
    micro_location = Field(input_processor=MapCompose(remove_tags, remove_new_lines), output_processor=TakeFirst())
    street = Field(input_processor=MapCompose(remove_tags, remove_new_lines), output_processor=TakeFirst())
    price = Field(input_processor=MapCompose(remove_tags, remove_currency, remove_new_lines),
                  output_processor=TakeFirst())
    price_per_unit = Field(input_processor=MapCompose(remove_tags, remove_currency, remove_new_lines),
                           output_processor=TakeFirst())
    real_estate_type = Field(input_processor=MapCompose(remove_tags, remove_new_lines), output_processor=TakeFirst())
    size_in_squared_meters = Field(input_processor=MapCompose(remove_tags, remove_new_lines),
                                   output_processor=TakeFirst())

    number_of_rooms = Field(input_processor=MapCompose(remove_tags, remove_new_lines), output_processor=TakeFirst())
    monthly_bills = Field(input_processor=MapCompose(remove_tags, remove_currency, remove_new_lines),
                          output_processor=TakeFirst())
    object_type = Field(input_processor=MapCompose(remove_tags, remove_new_lines), output_processor=TakeFirst())
    object_state = Field(input_processor=MapCompose(remove_tags, remove_new_lines), output_processor=TakeFirst())
    heating_type = Field(input_processor=MapCompose(remove_tags, remove_new_lines), output_processor=TakeFirst())
    floor_number = Field(input_processor=MapCompose(remove_tags, remove_new_lines), output_processor=TakeFirst())
    total_number_of_floors = Field(input_processor=MapCompose(remove_tags, remove_new_lines),
                                   output_processor=TakeFirst())
    additional = Field(input_processor=MapCompose(remove_tags, remove_new_lines), output_processor=Join())
    description = Field(input_processor=MapCompose(remove_tags, remove_new_lines), output_processor=TakeFirst())
    w_type = Field(input_processor=MapCompose(remove_tags, remove_new_lines), output_processor=Join())

    # Housekeeping fields
    date = Field(output_processor=TakeFirst())
