import itertools
import logging
from datetime import datetime
import html_to_json
import scrapy
from inline_requests import inline_requests
from scrapy.loader import ItemLoader
from scrapy_splash import SplashRequest

from ..items import RealEstateScraperItem


def find_list_to_scrape(urls_test):
    sublist_sell_apt = [urls_test[0] + '?p=' + str(i) for i in range(1, 2400)]
    sublist_sell_vikendice = [urls_test[1] + '?p=' + str(i) for i in range(1, 20)]
    sublist_sell_house = [urls_test[2] + '?p=' + str(i) for i in range(1, 500)]
    sublist_sell_plac = [urls_test[3] + '?p=' + str(i) for i in range(1, 200)]
    sublist_sell_posl_prostor = [urls_test[4] + '?p=' + str(i) for i in range(1, 120)]
    sublist_sell_salas = [urls_test[6] + '?page=' + str(i) for i in range(1, 3)]
    sublist_sell_garage = [urls_test[5] + '?page=' + str(i) for i in range(1, 15)]

    sublist_rent_apt = [urls_test[7] + '?p=' + str(i) for i in range(1, 200)]
    sublist_rent_vikendice = [urls_test[8] + '?p=' + str(i) for i in range(1, 10)]
    sublist_rent_house = [urls_test[9] + '?p=' + str(i) for i in range(1, 18)]
    sublist_rent_plac = [urls_test[10] + '?p=' + str(i) for i in range(1, 4)]
    sublist_rent_garage = [urls_test[11] + '?page=' + str(i) for i in range(1, 10)]
    sublist_rent_posl_prostor = [urls_test[12] + '?p=' + str(i) for i in range(1, 90)]
    sublist_rent_room = [urls_test[13] + '?p=' + str(i) for i in range(1, 10)]

    return list(itertools.chain(sublist_sell_apt, sublist_sell_salas, sublist_sell_plac, sublist_sell_vikendice,
                                sublist_sell_house, sublist_sell_garage, sublist_sell_posl_prostor,
                                sublist_rent_apt, sublist_rent_plac, sublist_rent_vikendice, sublist_rent_house,
                                sublist_rent_garage, sublist_rent_posl_prostor, sublist_rent_room))


class OglasiSpider(scrapy.Spider):
    name = 'oglasi'

    activities = ['prodaja', 'izdavanje']
    types = ['stanova', 'vikendica', 'kuca', 'placeva', 'poslovnog-prostora', 'garaza', 'salasa']
    urls_test = ['https://www.oglasi.rs/nekretnine/' + '-'.join(r) for r in itertools.product(activities, types)]
    urls_test.remove('https://www.oglasi.rs/nekretnine/izdavanje-poslovnog-prostora')
    urls_test.remove('https://www.oglasi.rs/nekretnine/izdavanje-salasa')
    urls_test.append('https://www.oglasi.rs/nekretnine/izdavanje-poslovni-prostor-lokal-magacin')
    urls_test.append('https://www.oglasi.rs/nekretnine/izdavanje-soba')
    urls = find_list_to_scrape(urls_test)

    def start_requests(self):
        for url in self.urls:
            yield SplashRequest(url=url, callback=self.parse)

    @inline_requests
    def parse(self, response):
        for re in response.css('div.fpogl-holder.advert_list_item_top_oglas'):
            l = ItemLoader(item=RealEstateScraperItem(), selector=re)
            link_ext = re.css('a.fpogl-list-title::attr(href)').get()
            link = 'https://www.oglasi.rs' + str(link_ext)
            logging.info( f'I found this page: {link}')
            res = yield SplashRequest(link, endpoint='render.html',
                                      args={'wait': 0.5, 'timeout': 3000})
            yield self.parse_individual_real_estate(res, l, link)

        for re in response.css('div.fpogl-holder.advert_list_item_istaknut'):
            l = ItemLoader(item=RealEstateScraperItem(), selector=re)
            link_ext = re.css('a.fpogl-list-title::attr(href)').get()
            link = 'https://www.oglasi.rs' + str(link_ext)
            logging.info( f'I found this page: {link}')
            res = yield SplashRequest(link, endpoint='render.html',
                                      args={'wait': 0.5, 'timeout': 3000})

            yield self.parse_individual_real_estate(res, l, link)
        for re in response.css('div.fpogl-holder.advert_list_item_normalan'):
            l = ItemLoader(item=RealEstateScraperItem(), selector=re)
            link_ext = response.css('a.fpogl-list-title::attr(href)').get()
            link = 'https://www.oglasi.rs' + str(link_ext)
            logging.info(f'I found this page: {link}')
            res = yield SplashRequest(link, endpoint='render.html',
                                      args={'wait': 0.5, 'timeout': 3000})

            yield self.parse_individual_real_estate(res, l, link)

        # next_page = response.css('ul li a:nth-last-child(1)::attr(href)').get()
        # print(f'THIS IS THE NEXT PAGE: {next_page}')
        # if next_page is not None:
        #     print('FOUND THE NEXT PAGE')
        #     next_url = 'https://www.oglasi.rs' + next_page
        #     yield SplashRequest(next_url, callback=self.parse)

    def parse_individual_real_estate(self, response, loader, link):
        logging.info('I am in the subpage')
        title = response.css('h1.fpogl-title.text-primary::text').get()
        loader.add_value('title', title)
        table = response.xpath('//table').get()
        tables = html_to_json.convert_tables(table) #list of lists
        tables = list(itertools.chain(*tables))
        tablesDict = {item[0]: item[1] for item in tables}
        location = tablesDict['Lokacija:'] if 'Lokacija:' in tablesDict else ''
        loader.add_value('location', location)
        street = tablesDict['Ulica i broj:'] if 'Ulica i broj:' in tablesDict else ''
        loader.add_value('street', street)
        size_in_squared_meters = tablesDict['Kvadratura:'] if 'Kvadratura:' in tablesDict else ''
        loader.add_value('size_in_squared_meters', size_in_squared_meters)
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
        loader.add_value('link', link)

        additional = response.css('p::text').get()
        loader.add_value('additional', additional)
        description = []
        for item in response.css('div[itemprop="description"] p::text').getall():
            description.append(item)
        loader.add_value('description', description)

        loader.add_value('date', datetime.today().strftime('%d-%m-%Y'))


        return loader.load_item()