import itertools
import logging
import re as regex
from datetime import datetime

import html_to_json
import scrapy
from scrapy.loader import ItemLoader
from scrapy.utils.log import configure_logging

from ..items import RealEstateScraperItem


def find_list_to_scrape():
    activities = ['prodaja', 'izdavanje']
    types = ['stanova', 'vikendica', 'kuca', 'placeva', 'poslovnog-prostora', 'garaza', 'salasa']
    urls_test = ['https://www.oglasi.rs/nekretnine/' + '-'.join(r) for r in itertools.product(activities, types)]
    urls_test.remove('https://www.oglasi.rs/nekretnine/izdavanje-poslovnog-prostora')
    urls_test.remove('https://www.oglasi.rs/nekretnine/izdavanje-salasa')
    urls_test.append('https://www.oglasi.rs/nekretnine/izdavanje-poslovni-prostor-lokal-magacin')
    urls_test.append('https://www.oglasi.rs/nekretnine/izdavanje-soba')
    return urls_test


class OglasiSpider(scrapy.Spider):
    name = 'oglasi'
    allowed_domains = ['www.oglasi.rs']
    configure_logging(install_root_handler=False)
    logging.basicConfig(
        handlers=[logging.FileHandler(
            fr'real_estate_scraper\\logs\\log_{name}_{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).replace(" ", "_").replace(":", "-")}.txt',
            'w')], #, 'cp65001'
        format='%(levelname)s: %(asctime)s  %(message)s:',
        level=logging.DEBUG
    )
    start_urls = find_list_to_scrape()

    def parse(self, response, **kwargs):
        for re in response.css('div.fpogl-holder.advert_list_item_top_oglas'):
            item = ItemLoader(item=RealEstateScraperItem(), selector=re)
            link_ext = re.css('a.fpogl-list-title::attr(href)').get()
            link = 'https://www.oglasi.rs' + str(link_ext)
            item.add_value('link', link)
            yield scrapy.Request(link, callback=self.parse_individual_real_estate, meta={'item': item})

        for re in response.css('div.fpogl-holder.advert_list_item_istaknut'):
            item = ItemLoader(item=RealEstateScraperItem(), selector=re)
            link_ext = re.css('a.fpogl-list-title::attr(href)').get()
            link = 'https://www.oglasi.rs' + str(link_ext)
            item.add_value('link', link)
            yield scrapy.Request(link, callback=self.parse_individual_real_estate, meta={'item': item})

        for re in response.css('div.fpogl-holder.advert_list_item_normalan'):
            item = ItemLoader(item=RealEstateScraperItem(), selector=re)
            link_ext = response.css('a.fpogl-list-title::attr(href)').get()
            link = 'https://www.oglasi.rs' + str(link_ext)
            item.add_value('link', link)
            yield scrapy.Request(link, callback=self.parse_individual_real_estate, meta={'item': item})

        next_pages = response.css('nav ul li a::attr(href)').getall()

        for next_page in next_pages:
            if 'nekretnine' in next_page and '?p=' in next_page:
                next_url = 'https://www.oglasi.rs' + next_page
                logging.info(f'Found the next page: {next_url}')
                yield scrapy.Request(next_url, callback=self.parse)

    def parse_individual_real_estate(self, response):
        loader = response.meta['item']
        logging.info('I am in the subpage')
        title = response.css('h1.fpogl-title.text-primary::text').get()
        loader.add_value('title', title)
        table = response.xpath('//table').get()
        tables = html_to_json.convert_tables(table)  # list of lists
        tables = list(itertools.chain(*tables))
        tablesDict = {item[0]: item[1] for item in tables}
        location = tablesDict['Lokacija:'] if 'Lokacija:' in tablesDict else ''
        loader.add_value('location', location)
        street = tablesDict['Ulica i broj:'] if 'Ulica i broj:' in tablesDict else ''
        loader.add_value('street', street)
        size_in_squared_meters = tablesDict['Kvadratura:'] if 'Kvadratura:' in tablesDict else ''
        povrsina_zemljista = tablesDict['Površina Zemljišta:'] if 'Površina Zemljišta::' in tablesDict else ''
        loader.add_value('size_in_squared_meters', size_in_squared_meters)

        if size_in_squared_meters == '':
            loader.add_value('size_in_squared_meters', povrsina_zemljista)

        number_of_rooms = tablesDict['Sobnost:'] if 'Sobnost:' in tablesDict else ''
        loader.add_value('number_of_rooms', number_of_rooms)
        object_state = tablesDict['Stanje objekta:'] if 'Stanje objekta:' in tablesDict else ''
        loader.add_value('object_state', object_state)
        heating_type = tablesDict['Grejanje:'] if 'Grejanje:' in tablesDict else ''
        loader.add_value('heating_type', heating_type)
        floor_number = tablesDict['Nivo u zgradi:'] if 'Nivo u zgradi:' in tablesDict else ''
        loader.add_value('floor_number', floor_number)
        price = response.css('span[itemprop="price"]::text').get()
        loader.add_value('price', price)

        additional = response.css('p::text').get()
        loader.add_value('additional', additional)
        description = []
        for item in response.css('div[itemprop="description"] p::text').getall():
            description.append(item)

        if len(description) == 0:
            for item in response.css('div[itemprop="description"]::text').getall():
                description.append(item)
        desc = ' '.join([el for el in description])
        desc = regex.sub("(\s) | (,)", "", desc).strip()

        loader.add_value('description', desc)
        loader.add_value('date', datetime.today().strftime('%d/%m/%Y'))

        return loader.load_item()
