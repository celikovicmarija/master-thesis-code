import itertools
import logging
from datetime import datetime

import scrapy
from inline_requests import inline_requests
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
    return list(itertools.chain(sublist_rent_apt, sublist_rent_house, sublist_rent_garage, sublist_sell_land,
                                sublist_sell_house, sublist_sell_garage,
                                sublist_sell_other, sublist_sell_apt,
                                sublist_rent_land,
                                sublist_rent_other, sublist_rent_room))


class HalooglasiSpider(scrapy.Spider):
    name = 'halooglasi'
    # allowed_domains = ['halooglasi.com']

    configure_logging(install_root_handler=False)
    logging.basicConfig(
        handlers=[logging.FileHandler(
            fr'real_estate_scraper\\logs\\log_{name}_{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).replace(" ", "_").replace(":", "-")}.txt',
            'w', 'cp65001')],
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
            logging.log(logging.INFO, f'I CAME TO THE URL {url}')
            yield SplashRequest(url=url, callback=self.parse, args={"timeout": 3600})

    @inline_requests
    def parse(self, response):
        # next_page = response.css('a.page-link.next::attr(href)').get()
        # # response.css('a.page-link.next::attr(href)').get()
        # if next_page is not None:
        #     print('FOUND THE NEXT PAGE')
        #     next_url = 'https://www.halooglasi.com' + next_page
        #     self.urls.append(next_url)
        for re in response.css('div.product-item.product-list-item.Top.real-estates.my-product-placeholder'):
            l = ItemLoader(item=RealEstateScraperItem(), selector=re)
            l.add_css('link', 'h3.product-title a::attr(href)')
            logging.info(f'I found this page: {l.get_output_value("link")}')
            res = yield SplashRequest(l.get_output_value('link'), endpoint='render.html',
                                      args={'wait': 0.5, 'timeout': 3000})

            yield self.parse_individual_real_estate(res, l, l.get_output_value('link'))

        for re in response.css(
                'div.product-item.product-list-item.Premium.real-estates.my-product-placeholder'):
            l = ItemLoader(item=RealEstateScraperItem(), selector=re)
            l.add_css('link', 'h3.product-title a::attr(href)')
            logging.info(f'I found this page: {l.get_output_value("link")}')
            res = yield SplashRequest(l.get_output_value('link'), endpoint='render.html',
                                      args={'wait': 0.5, 'timeout': 3000})

            yield self.parse_individual_real_estate(res, l, l.get_output_value('link'))

        for st_re in response.css(
                'div.product-item.product-list-item.Standard.real-estates.my-product-placeholder'):
            l = ItemLoader(item=RealEstateScraperItem(), selector=st_re)
            l.add_css('link', 'h3.product-title a::attr(href)')
            logging.info(f'I found this page: {l.get_output_value("link")}')
            res = yield SplashRequest(l.get_output_value('link'), endpoint='render.html',
                                      args={'wait': 0.5, 'timeout': 3000})

            yield self.parse_individual_real_estate(res, l, l.get_output_value('link'))

    def parse_individual_real_estate(self, response, loader, link):
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
        real_estate_type = response.css('span#plh11::text').get()
        loader.add_value('real_estate_type', real_estate_type)
        size_in_squared_meters = response.css('span#plh12::text').get()
        loader.add_value('size_in_squared_meters', size_in_squared_meters)
        number_of_rooms = response.css('span#plh13::text').get()
        loader.add_value('number_of_rooms', number_of_rooms)

        response.css('div.datasheet.product-basic-details div.basic-view').getall()

        advertiser = response.css('span#plh14::text').get()
        loader.add_value('advertiser', advertiser)
        object_type = response.css('span#plh15::text').get()
        loader.add_value('object_type', object_type)
        object_state = response.css('span#plh16::text').get()
        loader.add_value('object_state', object_state)
        heating_type = response.css('span#plh17::text').get()
        loader.add_value('heating_type', heating_type)
        floor_number = response.css('span#plh18::text').get()
        loader.add_value('floor_number', floor_number)
        total_number_of_floors = response.css('span#plh19::text').get()
        loader.add_value('total_number_of_floors', total_number_of_floors)
        monthly_bills = response.css('span#plh20::text').get()
        loader.add_value('monthly_bills', monthly_bills)
        price = response.css('span#plh6 span.offer-price-value::text').get()
        loader.add_value('price', price)
        price_per_unit = response.css('span#plh7inner::text').get()
        loader.add_value('price_per_unit', price_per_unit)
        loader.add_value('link', link)
        additional = []
        for item in response.css('div.flags-container span label::text').getall():  # or span#plh21
            additional.append(item)

        for item in response.css('div.flags-container span#plh22 label::text').getall():
            additional.append(item)
        description = ''
        loader.add_value('additional', additional)
        for item in response.xpath("descendant-or-self::span[@id = 'plh50']/descendant-or-self::*/p/text()").extract():
            description += str(item)
        loader.add_value('description', description)

        city_buses = []
        for item in response.css('div.city-lines ul li::text').getall():
            city_buses.append(item)
        loader.add_value('city_lines', city_buses)

        # image_urls = []
        # for url in response.css('div.fotorama__nav__shaft img::attr(src)').getall():
        #     if '/Content/Quiddita/Widgets/Product/Stylesheets/img/' in url:
        #         continue
        #     image_urls.append(url)
        #
        # image_names = []
        # for image_url in image_urls:
        #     image_names.append('halooglasi' + '_'.join(image_url.split('/')[-2:]))
        # loader.add_value('image_urls', image_urls)
        # loader.add_value('image_name', image_names)

        loader.add_value('date', datetime.today().strftime('%d-%m-%Y'))

        return loader.load_item()
