import itertools
import logging
from datetime import datetime

from inline_requests import inline_requests
from itemloaders import ItemLoader
from scrapy import Spider, Request
from scrapy.utils.log import configure_logging

from ..items import RealEstateScraperItem


def find_list_to_scrape(urls_test):
    sublist_stambeni_objekti = [urls_test[0] + 'strana/' + str(i) for i in range(1, 1700)]
    sublist_stambeni_zemlja = [urls_test[0] + 'strana/' + str(i) for i in range(1, 180)]
    sublist_stambeni_apt = [urls_test[0] + 'strana/' + str(i) for i in range(1, 15)]
    sublist_posl = [urls_test[0] + 'strana/' + str(i) for i in range(1, 310)]
    return list(itertools.chain(sublist_posl, sublist_stambeni_apt, sublist_stambeni_objekti, sublist_stambeni_zemlja))


class NekrentineSpider(Spider):
    name = 'nekretnine'
    configure_logging(install_root_handler=False)
    logging.basicConfig(
        handlers=[logging.FileHandler(
            fr'real_estate_scraper\\logs\\log_{name}_{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).replace(" ", "_").replace(":", "-")}.txt',
            'w', 'cp65001')],
        format='%(levelname)s: %(asctime)s  %(message)s:',
        level=logging.DEBUG
    )

    urls_test = ['https://www.nekretnine.rs/stambeni-objekti/lista/po-stranici/20/',
                 'https://www.nekretnine.rs/zemljista/lista/po-stranici/20/',
                 'https://www.nekretnine.rs/apartmani/lista/po-stranici/20/',
                 'https://www.nekretnine.rs/poslovni-objekti/lista/po-stranici/20/']
    urls = find_list_to_scrape(urls_test)

    def start_requests(self):
        for url in self.urls:
            logging.log(logging.INFO, f'I CAME TO THE URL {url}')
            yield Request(url=url, callback=self.parse)

    @inline_requests
    def parse(self, response):

        for re in response.css('div.row.offer'):
            l = ItemLoader(item=RealEstateScraperItem(), selector=re)
            link_ext = re.css('a::attr(href)').get()
            link = 'https://www.nekretnine.rs' + str(link_ext)
            logging.info(f'I found this page: {link}')
            l.add_value('link', link)
            yield Request(link, meta={'item': l}, callback=self.parse_individual_real_estate)

        # for r in response.css('div.row a::attr(href)'):
        #     print(r)
        #     if r.css('span::text').get() == 'Sledeća strana':
        #         next_page = r.css('a::attr(href)').get()
        #         next_link = 'https://www.nekretnine.rs' + next_page
        #         if next_link:
        #             yield response.follow(next_page, callback=self.parse)

    def parse_individual_real_estate(self, response):
        loader = response.meta['item']
        logging.info('I am in the subpage')
        title = response.css('h3.detail-title.pt-3.pb-2::text').get()
        loader.add_value('title', title)

        location = response.css('div.property__location ul li::text').getall()
        loader.add_value('location', location)
        price = response.css('h4.stickyBox__price::text').get()
        loader.add_value('price', price)
        price_per_unit = response.css('h4.stickyBox__price span::text').get()
        loader.add_value('price_per_unit', price_per_unit)
        size_in_squared_meters = response.css('h4.stickyBox__size::text').get()
        loader.add_value('size_in_squared_meters', size_in_squared_meters)

        description = ''.join([x.strip().strip('\\n') for x in response.css('div.property__description ::text').getall()])
        loader.add_value('description', description)
        info = {}
        additional = ''
        for subdivision in response.css('divproperty__amenities h3'):
            if subdivision.css('h3:"text') == 'Podaci o nekretnini':
                for k, v in zip(subdivision.css('ul li::text').getall(),
                                subdivision.css('strong::text').getall()):
                    info[k] = v
            elif subdivision.css('h3:"text') == 'Ostalo':
                for k, v in zip(subdivision.css('ul li::text').getall(),
                                subdivision.css('strong::text').getall()):
                    info[k] = v

            else:
                additional += str(response.css('ul li::text')).get()

        total_number_of_floors = info['Ukupan broj spratova:'] if 'Ukupan broj spratova:' in info else ''
        loader.add_value('total_number_of_floors', total_number_of_floors)

        total_number_of_rooms = info['Ukupan broj soba:'] if 'Ukupan broj soba:' in info else ''
        number_of_bathrooms = info['Broj kupatila:'] if 'Broj kupatila:' in info else ''
        stanje_nekretnine = info['Stanje nekretnine:'] if 'Stanje nekretnine:' in info else ''

        teren = info['Teren:'] if 'Teren:' in info else ''

        transaction = info['Transakcija:'] if 'Transakcija:' in info else ''
        # loader.add_value('transaction', transaction) #izdavanje, prodaja

        kategorija = info['Kategorija:'] if 'Kategorija:' in info else ''  # garsonjera, Porodična kuća

        vrsta_kuce = info['Vrsta kuće:'] if 'Vrsta kuće:' in info else ''  # garsonjera, Porodična kuća

        povrsina_zemljista = info[
            'Površina zemljišta:'] if 'Površina zemljišta:' in info else ''  # garsonjera, Porodična kuća

        uknjizeno = info['Uknjiženo:'] if 'Uknjiženo:' in info else ''
        pozicija = info['Pozicija:'] if 'Pozicija:' in info else ''  # mirna lokacija

        heating_type = info['Grejanje:'] if 'Grejanje:' in info else ''
        loader.add_value('heating_type', heating_type)

        # key value that needs cleaning
        k = response.css('div.property__amenities strong ::text').getall()
        # street = response.css('span#plh5::text').get()
        # loader.add_value('street', street) city might be a street

        # floor_number = response.css('div.basic-info span:nth-child(3)::text').get()
        # loader.add_value('floor_number', floor_number)
        # total_number_of_floors = response.css('div.basic-info span:nth-child(3)::text').get()
        # loader.add_value('total_number_of_floors', total_number_of_floors)

        info = {}
        for re in response.css('app-info-item.ng-star-inserted'):

            for k, v in zip(re.css('div.label::text').getall(),
                            re.css('strong.value::text').getall()):
                info[k] = v

        for re in response.css('div.info-item.ng-star-inserted'):

            for k, v in zip(re.css('div.label::text').getall(),
                            re.css('strong.value::text').getall()):
                info[k] = v
                floor_number = info['Spratnost:'] if 'Spratnost:' in info else ''
                loader.add_value('floor_number', floor_number)

        object_type = info['Stanje:'] if 'Stanje:' in info else ''
        loader.add_value('object_type', object_type)

        number_of_rooms = info['Broj soba:'] if 'Broj soba:' in info else ''
        loader.add_value('number_of_rooms', number_of_rooms)

        real_estate_type = info['Tip:'] if 'Tip:' in info else ''
        loader.add_value('real_estate_type', real_estate_type)

        object_type = info['Stanje:'] if 'Stanje:' in info else ''
        loader.add_value('object_type', object_type)

        additional = ''

        useljivo = info['Useljivo:'] if 'Useljivo:' in info else ''
        additional += str(useljivo) + ' '

        infra = info['Infrastruktura:'] if 'Infrastruktura:' in info else ''
        additional += str(infra) + ' '

        intra = info['Unutrašnje prostorije:'] if 'Unutrašnje prostorije:' in info else ''
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

        # if 'Opis oglasa' in response.css('h2.mat-headline::text').get():
        #     item = response.css("pre.ed-description.collapsed-description.ng-star-inserted::text").get()
        #     loader.add_value('description', str(item))

        loader.add_value('date', datetime.today().strftime('%d-%m-%Y'))

        return loader.load_item()
