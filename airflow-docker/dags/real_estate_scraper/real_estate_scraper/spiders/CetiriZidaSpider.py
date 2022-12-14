import itertools
import logging
from datetime import datetime

from itemloaders import ItemLoader
from scrapy import Spider, Request
from scrapy.utils.log import configure_logging
from scrapy_splash import SplashRequest

from ..items import RealEstateScraperItem


def find_list_to_scrape():
    activities = ['prodaja', 'izdavanje']
    types = ['stanova', 'poslovnih-prostora', 'kuca', 'garaza-i-parkinga', 'placeva']
    urls_test = ['https://www.4zida.rs/' + '-'.join(r) for r in itertools.product(activities, types)]

    strukturastan = ['garsonjera', 'jednosoban', 'jednoiposoban', 'dvosoban', 'dvoiposoban', 'trosoban', 'troiposoban',
                     'cetvorosoban', 'cetvoroiposoban-i-vise']
    strukturakuce = ['jednoetazna', 'dvoetazna', 'troetazna', 'cetvoroetazna', 'petoetazna', 'sestoetazna']
    struktura_placeva = ['gradjevinsko-zemljiste', 'poljoprivredno-zemljiste', 'industrijsko-zemljiste']
    struktura_garaze = ['garaza', 'parking']
    struktura_poslovni_prostor = ['lokal', 'kancelarija', 'ordinacija', 'atelje', 'magacin', 'hala',
                                  'ugostiteljski-objekat', 'kiosk', 'proizvodni-pogon',
                                  'sportski_objekat', 'stovariste', 'turisticki-objekat', 'poslovna-zgrada']

    apts = ['?struktura='.join(r) for r in itertools.product([urls_test[0], urls_test[5]], strukturastan)]
    houses = ['?struktura='.join(r) for r in itertools.product([urls_test[2], urls_test[7]], strukturakuce)]
    placevi = ['?struktura='.join(r) for r in itertools.product([urls_test[4], urls_test[9]], struktura_placeva)]
    garaze = ['?struktura='.join(r) for r in itertools.product([urls_test[3], urls_test[8]], struktura_garaze)]
    poslovni_prostor = ['?struktura='.join(r) for r in
                        itertools.product([urls_test[1], urls_test[6]], struktura_poslovni_prostor)]
    sublist_apts = ['&strana='.join(r) for r in itertools.product(apts, [str(i) for i in range(1, 100)])]
    sublist_houses = ['&strana='.join(r) for r in itertools.product(houses, [str(i) for i in range(1, 100)])]
    sublist_posl_prostor = ['&strana='.join(r) for r in
                            itertools.product(poslovni_prostor, [str(i) for i in range(1, 100)])]
    sublist_garaze = ['&strana='.join(r) for r in itertools.product(garaze, [str(i) for i in range(1, 100)])]
    sublist_placevi = ['&strana='.join(r) for r in itertools.product(placevi, [str(i) for i in range(1, 100)])]

    return list(itertools.chain(sublist_apts,
                                sublist_houses,
                                sublist_posl_prostor, sublist_garaze, sublist_placevi))


class CetiriZidaSpider(Spider):
    name = 'cetirizida'

    configure_logging(install_root_handler=False)
    logging.basicConfig(
        handlers=[logging.FileHandler(
            fr'real_estate_scraper\\logs\\log_{name}_{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).replace(" ", "_").replace(":", "-")}.txt',
            'w')],
        format='%(levelname)s: %(asctime)s  %(message)s:',
        level=logging.DEBUG
    )

    urls = find_list_to_scrape()

    def start_requests(self):
        for url in self.urls:
            yield Request(url=url, callback=self.parse)

    def parse(self, response, **kwargs):
        for re in response.css('div.ed-card-details'):
            link_ext = re.css('a::attr(href)').get()
            link = 'https://www.4zida.rs' + str(link_ext)
            yield SplashRequest(link, endpoint='render.html', callback=self.parse_individual_real_estate,
                                args={'wait': 0.5, 'timeout': 3000}, meta={'original_url': link})
        for re in response.css('app-premium-ads a.relative.premium-slide-wrapper.ng-star-inserted::attr(href)'):
            link_ext = re.css('a::attr(href)').get()
            link = 'https://www.4zida.rs' + str(link_ext)
            yield SplashRequest(link, endpoint='render.html', callback=self.parse_individual_real_estate,
                                args={'wait': 0.5, 'timeout': 3000}, meta={'original_url': link})

    def parse_individual_real_estate(self, response):
        url = response.meta['original_url']
        loader = ItemLoader(item=RealEstateScraperItem(),
                            selector=response.css('body.mat-typography.mat-app-background'))
        title = response.css('h1::text').get()
        loader.add_value('title', title)
        city = response.css('app-place-info strong::text').get()
        loader.add_value('city', city)
        location = response.css('app-place-info span::text').get()
        loader.add_value('location', location)

        price = response.css('span strong.font-extrabold::text').get()
        loader.add_value('price', price)

        loader.add_value('link', url)
        info = {}
        for re in response.css('app-info-item.ng-star-inserted'):

            for k, v in zip(re.css('div.label::text').getall(),
                            re.css('strong.value::text').getall()):
                info[k] = v

        for re in response.css('div.info-item.ng-star-inserted'):

            for k, v in zip(re.css('div.label::text').getall(),
                            re.css('strong.value::text').getall()):
                info[k] = v

        price_per_unit = info['Cena po m'] if 'Cena po m' in info else ''
        loader.add_value('price_per_unit', price_per_unit)

        size_in_squared_meters = info['Povr??ina:'] if 'Povr??ina:' in info else ''
        loader.add_value('size_in_squared_meters', size_in_squared_meters)

        object_type = info['Stanje:'] if 'Stanje:' in info else ''
        loader.add_value('object_type', object_type)

        number_of_rooms = info['Broj soba:'] if 'Broj soba:' in info else ''
        loader.add_value('number_of_rooms', number_of_rooms)

        real_estate_type = info['Tip:'] if 'Tip:' in info else ''
        loader.add_value('real_estate_type', real_estate_type)

        heating_type = info['Grejanje:'] if 'Grejanje:' in info else ''
        loader.add_value('heating_type', heating_type)

        object_type = info['Stanje:'] if 'Stanje:' in info else ''
        loader.add_value('object_type', object_type)

        total_number_of_floors = info['Spratnost:'] if 'Spratnost:' in info else ''
        loader.add_value('total_number_of_floors', total_number_of_floors)

        floor_number = info['Spratnost:'] if 'Spratnost:' in info else ''
        loader.add_value('floor_number', floor_number)

        additional = ''
        uknjizenost = info['Uknji??enost:'] if 'Uknji??enost:' in info else ''
        additional += str(uknjizenost) + ' '

        useljivo = info['Useljivo:'] if 'Useljivo:' in info else ''
        additional += str(useljivo) + ' '

        infra = info['Infrastruktura:'] if 'Infrastruktura:' in info else ''
        additional += str(infra) + ' '

        intra = info['Unutra??nje prostorije:'] if 'Unutra??nje prostorije:' in info else ''
        additional += str(intra) + ' '

        god_izgdradnje = info['Godina izgradnje:'] if 'Godina izgradnje:' in info else ''
        additional += 'Godina izgradnje: ' + str(god_izgdradnje) + ' '

        pogled = info['Pogled na:'] if 'Pogled na:' in info else ''
        additional += 'Pogled na: ' + str(pogled) + ' '

        orijentacija = info['Orijentacija nekretnine:'] if 'Orijentacija nekretnine:' in info else ''
        additional += 'Orijentacija nekretnine: ' + str(orijentacija) + ' '

        internet = info['Internet:'] if 'Internet:' in info else ''
        additional += 'Internet: ' + str(internet) + ' '

        parking = info['Parking:'] if 'Parking:' in info else ''
        additional += 'Parking: ' + str(parking) + ' '

        loader.add_value('additional', floor_number)
        item = ''

        for el in response.css('h2.mat-headline::text').getall():
            if 'Opis oglasa' not in el:
                continue
            else:
                item = response.css("pre.ed-description.collapsed-description.ng-star-inserted::text").get()
                break
        loader.add_value('description', str(item))

        loader.add_value('date', datetime.today().strftime('%d/%m/%Y'))

        yield loader.load_item()
