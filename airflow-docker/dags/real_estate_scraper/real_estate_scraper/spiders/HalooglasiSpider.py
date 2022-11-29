"""
#### Had to comment the whole file because of docker and python packages
#### version incompatibilities. This script is not used anymore anyway.
import itertools
import logging
import re as regex
from datetime import datetime

import scrapy
from scrapy.loader import ItemLoader
from scrapy.utils.log import configure_logging
from scrapy_splash import SplashRequest

from ..items import RealEstateScraperItem


def find_list_to_scrape(urls_test):
    sublist_sell_apt = [urls_test[0] + '?page=' + str(i) for i in range(1, 1100)]
    sublist_rent_apt = [urls_test[5] + '?page=' + str(i) for i in range(1, 300)]
    sublist_rent_room = [urls_test[-5] + '?page=' + str(i) for i in range(1, 10)]
    sublist_rent_garage = [urls_test[2] + '?page=' + str(i) for i in range(1, 15)]
    sublist_sell_garage = [urls_test[-3] + '?page=' + str(i) for i in range(1, 10)]
    sublist_rent_house = [urls_test[-4] + '?page=' + str(i) for i in range(1, 30)]
    sublist_sell_house = [urls_test[1] + '?page=' + str(i) for i in range(1, 500)]
    sublist_sell_land = [urls_test[3] + '?page=' + str(i) for i in range(1, 250)]
    sublist_rent_land = [urls_test[-2] + '?page=' + str(i) for i in range(1, 10)]
    sublist_rent_other = [urls_test[-1] + '?page=' + str(i) for i in range(1, 10)]
    sublist_sell_other = [urls_test[4] + '?page=' + str(i) for i in range(1, 10)]
    return list(itertools.chain(sublist_rent_apt, sublist_sell_apt, sublist_sell_land,
                                sublist_sell_house, sublist_sell_garage,
                                sublist_sell_other, sublist_rent_garage, sublist_rent_land,
                                sublist_rent_other, sublist_rent_room, sublist_rent_house))


class HalooglasiSpider(scrapy.Spider):
    name = 'halooglasi'
    allowed_domains = ['www.halooglasi.com']

    configure_logging(install_root_handler=False)
    logging.basicConfig(
        handlers=[logging.FileHandler(
            fr'real_estate_scraper\\logs\\log_{name}_{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).replace(" ", "_").replace(":", "-")}.txt',
            'w')],
        format='%(levelname)s: %(asctime)s  %(message)s:',
        level=logging.DEBUG
    )

    activities = ['prodaja', 'izdavanje']
    types = ['stanova', 'soba', 'kuca', 'garaza', 'zemljista', 'ostalog-stambenog-prostora']
    urls_test = ['https://www.halooglasi.com/nekretnine/' + '-'.join(r) for r in itertools.product(activities, types)]
    urls_test.remove('https://www.halooglasi.com/nekretnine/prodaja-soba')
    urls = find_list_to_scrape(urls_test)

    def start_requests(self):
        for url in self.urls:
            yield SplashRequest(url, callback=self.parse, args={'wait': 0.5, 'timeout': 3000})

    def parse(self, response):
        for re in response.css('div.product-item.product-list-item.Top.real-estates.my-product-placeholder'):
            link = 'https://www.halooglasi.com' + re.css('h3.product-title a::attr(href)').get()
            yield SplashRequest(link, endpoint='render.html', callback=self.parse_individual_real_estate,
                                args={'wait': 0.5, 'timeout': 3000}, meta={'original_url': link})

        for re in response.css(
                'div.product-item.product-list-item.Premium.real-estates.my-product-placeholder'):
            link = 'https://www.halooglasi.com' + re.css('h3.product-title a::attr(href)').get()
            yield SplashRequest(link, endpoint='render.html', callback=self.parse_individual_real_estate,
                                args={'wait': 0.5, 'timeout': 3000}, meta={'original_url': link})

        for st_re in response.css(
                'div.product-item.product-list-item.Standard.real-estates.my-product-placeholder'):
            link = 'https://www.halooglasi.com' + st_re.css('h3.product-title a::attr(href)').get()
            yield SplashRequest(link, callback=self.parse_individual_real_estate, endpoint='render.html',
                                args={'wait': 0.5, 'timeout': 3000}, meta={'original_url': link})

    def parse_individual_real_estate(self, response):
        url = response.meta['original_url']
        loader = ItemLoader(item=RealEstateScraperItem(), selector=response.css('div#wrapper'))
        price = response.css('span.offer-price-value::text').get()
        loader.add_value('price', price)
        price_per_unit = response.css('div.price-by-surface span::text').get()
        loader.add_value('price_per_unit', price_per_unit)
        loader.add_value('link', url)

        title = response.css('span#plh1::text').get()
        loader.add_value('title', title)
        city = response.css('span#plh2::text').get()
        loader.add_value('city', city)
        location = response.css('span#plh3::text').get()
        loader.add_value('location', location)
        micro_location = response.css('span#plh4::text').get()
        loader.add_value('micro_location', micro_location)
        street = response.css('span#plh5::text').get()
        loader.add_value('street', street)

        info = {}

        for re in response.css('div.prominent ul'):
            list_to_compare = [el for el in re.css('span.field-value ::text').getall() if
                               regex.sub("(\s) | (,)", "", el).strip() != '']
            for k, v in zip(re.css('span.field-name ::text').getall(), list_to_compare):
                info[k.replace('\\n', '').strip()] = v.replace('\\n', '').strip()

        real_estate_type = info['Tip nekretnine'] if 'Tip nekretnine' in info else ''
        loader.add_value('real_estate_type', real_estate_type)

        size_in_squared_meters = info['Kvadratura'] if 'Kvadratura' in info else ''
        loader.add_value('size_in_squared_meters', size_in_squared_meters)

        number_of_rooms = info['Broj soba'] if 'Broj soba' in info else ''
        loader.add_value('number_of_rooms', number_of_rooms)

        for re in response.css('div.product-basic-details'):
            list_to_compare = [el for el in re.css('div.col-lg-7.col-md-7.datasheet-features-type span::text').getall()
                               if
                               regex.sub("(\s) | (,)", "", el).strip() != '']
            for k, v in zip(re.css('div.col-lg-5.col-md-5::text').getall(), list_to_compare):
                info[k.strip().replace('\\n', '').strip()] = v.strip().replace('\\n', '').strip()

        floor_number = info['Sprat'] if 'Sprat' in info else ''
        loader.add_value('floor_number', floor_number)

        total_number_of_floors = info['Ukupna Spratnost'] if 'Ukupna Spratnost' in info else ''
        loader.add_value('total_number_of_floors', total_number_of_floors)

        heating_type = info['Grejanje'] if 'Grejanje' in info else ''
        loader.add_value('heating_type', heating_type)

        object_state = info['Stanje objekta'] if 'Stanje objekta' in info else ''
        loader.add_value('object_state', object_state)

        object_type = info['Tip objekta'] if 'Tip objekta' in info else ''
        loader.add_value('object_type', object_type)

        monthly_bills = info['Mesečne režije'] if 'Mesečne režije' in info else ''
        loader.add_value('monthly_bills', monthly_bills)

        additional = []
        spratnost = info['Spratnost'] if 'Spratnost' in info else ''
        vrsta_zemljista = info['Vrsta zemljišta'] if 'Vrsta zemljišta' in info else ''
        povrsina_placa = info['Površina placa'] if 'Površina placa' in info else ''

        a = 'Spratnost:' + spratnost if spratnost else ''
        if additional != '':
            additional.append(a)

        a = 'Vrsta zemljišta:' + vrsta_zemljista if vrsta_zemljista else ''
        if additional != '':
            additional.append(a)

        a = 'Površina placa:' + povrsina_placa if povrsina_placa else ''
        if additional != '':
            additional.append(a)

        for el in response.css('div.flags-container span label::text').getall():  # or span#plh21
            additional.append(el)

        additional = ' '.join([e for e in additional]).strip()

        loader.add_value('additional', additional)

        description = ''

        for el in response.css('div#tabTopHeader3 span ::text').getall():
            el = regex.sub("(\s) | (,)", " ", el).strip()
            description += el
        loader.add_value('description', description)

        loader.add_value('date', datetime.today().strftime('%d/%m/%Y'))

        yield loader.load_item()
"""