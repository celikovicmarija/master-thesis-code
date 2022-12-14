import logging
from datetime import datetime

from itemloaders import ItemLoader
from scrapy import Spider, Request
from scrapy.utils.log import configure_logging

from ..items import RealEstateScraperItem


def find_list_to_scrape():
    urls_test = ['https://www.nekretnine.rs/stambeni-objekti/lista/po-stranici/20/',
                 'https://www.nekretnine.rs/zemljista/lista/po-stranici/20/',
                 'https://www.nekretnine.rs/apartmani/lista/po-stranici/20/',
                 'https://www.nekretnine.rs/poslovni-objekti/lista/po-stranici/20/']
    return urls_test


class NekrentineSpider(Spider):
    name = 'nekretnine'
    configure_logging(install_root_handler=False)
    logging.basicConfig(
        handlers=[logging.FileHandler(
            fr'real_estate_scraper\\logs\\log_{name}_{str(datetime.now().strftime("%Y-%m-%d %H:%M:%S")).replace(" ", "_").replace(":", "-")}.txt',
            'w')],
        format='%(levelname)s: %(asctime)s  %(message)s:',
        level=logging.DEBUG
    )

    start_urls = find_list_to_scrape()

    def parse(self, response, **kwargs):
        for re in response.css('div.row.offer'):
            item = ItemLoader(item=RealEstateScraperItem(), selector=re)
            link_ext = re.css('a::attr(href)').get()
            link = 'https://www.nekretnine.rs' + str(link_ext)
            item.add_value('link', link)
            yield Request(link, meta={'item': item}, callback=self.parse_individual_real_estate)

        next_page = response.css('a.d-block.next-article-button.m-auto::attr(href)').get()
        if next_page is not None:
            next_url = 'https://www.nekretnine.rs' + next_page
            logging.info(f'Found the next page: {next_url}')
            yield Request(next_url, callback=self.parse)

    def parse_individual_real_estate(self, response):
        loader = response.meta['item']
        logging.info('I am in the subpage')
        title = response.css('h1.detail-title.pt-3.pb-2::text').get()
        loader.add_value('title', title)

        location = response.css('div.property__location ul li::text').getall()
        loader.add_value('location', ' '.join(location))
        price = response.css('h4.stickyBox__price::text').get()
        loader.add_value('price', price)
        price_per_unit = response.css('h4.stickyBox__price span::text').get()
        loader.add_value('price_per_unit', price_per_unit)
        size_in_squared_meters = response.css('h4.stickyBox__size::text').get()
        loader.add_value('size_in_squared_meters', size_in_squared_meters)

        description = ''.join(
            [x.replace('\\n', '').strip() for x in response.css('div.property__description ::text').getall()])
        loader.add_value('description', description)
        info = {}
        additional = ''
        kv_pairs = []
        for subdivision in response.css('div.property__amenities'):
            if subdivision.css('h3::text').get().strip().replace('\\n', '') == 'Podaci o nekretnini':
                amenities_list = subdivision.css('ul')
                new_list = [e for e in amenities_list.css('li::text').getall() if e.strip().replace('\\n', '') != '']
                for k, v in zip(new_list, amenities_list.css('li strong::text').getall()):
                    if k.strip().replace('\\n', '') and v.strip().replace('\\n', ''):
                        info[k.strip().replace('\\n', '')] = v.strip().replace('\\n', '')
            else:
                for el in subdivision.css('ul li::text').getall():
                    kv_pairs.append(el.strip().replace('\\n', ''))

        for kv in kv_pairs:
            if ':' in kv:
                info[kv.split(':')[0].strip().replace('\\n', '') + ':'] = kv.split(':')[1].strip().replace('\\n', '')

        total_number_of_floors = info['Ukupan broj spratova:'] if 'Ukupan broj spratova:' in info else ''
        loader.add_value('total_number_of_floors', total_number_of_floors)

        floor_number = info['Spratnost:'] if 'Spratnost:' in info else ''
        loader.add_value('floor_number', floor_number)

        heating_type = info['Grejanje:'] if 'Grejanje:' in info else ''
        loader.add_value('heating_type', heating_type)

        number_of_rooms = info['Ukupan broj soba:'] if 'Ukupan broj soba:' in info else ''
        loader.add_value('number_of_rooms', number_of_rooms)

        za_studenta = (
                'Za studenta:' + info['Za studenta:'] + ',') if 'Za studenta:' in info else ''
        additional += za_studenta

        komunalne_usluge = (
                'Komunalne usluge:' + info['Komunalne usluge:'] + ',') if 'Komunalne usluge:' in info else ''
        additional += komunalne_usluge

        topla_voda = (
                'Topla voda:' + info['Topla voda:'] + ',') if 'Topla voda:' in info else ''
        additional += topla_voda

        uknjizeno = (
                'Uknji??eno:' + info['Uknji??eno:'] + ',') if 'Uknji??eno:' in info else ''
        additional += uknjizeno

        broj_kupatila = (
                'Broj kupatila:' + info['Broj kupatila:'] + ',') if 'Broj kupatila:' in info else ''

        additional += broj_kupatila

        teren = (
                'Teren:' + info['Teren:'] + ',') if 'Teren:' in info else ''
        additional += teren

        if size_in_squared_meters:
            povrsina_zemljista = (
                    'Povr??ina zemlji??ta:' + info['Povr??ina zemlji??ta:'] + ',') if 'Povr??ina zemlji??ta:' in info else ''
            additional += povrsina_zemljista
        else:
            povrsina_zemljista = info['Povr??ina zemlji??ta:'] if 'Povr??ina zemlji??ta:' in info else ''
            loader.add_value('size_in_squared_meters', povrsina_zemljista)

        pozicija = (
                'Pozicija:' + info['Pozicija:'] + ',') if 'Pozicija:' in info else ''  # mirna lokacija
        additional += pozicija

        transaction = info['Transakcija:'] if 'Transakcija:' in info else ''
        loader.add_value('w_type', transaction)

        has_furniture = info['Opremljenost nekretnine:'] if 'Opremljenost nekretnine:' in info else ''
        loader.add_value('object_state', has_furniture)

        vrsta_kuce = (
                'Vrsta ku??e:' + info['Vrsta ku??e:'] + ',') if 'Vrsta ku??e:' in info else ''
        additional += vrsta_kuce

        loader.add_value('additional', additional)

        object_type = info['Stanje nekretnine:'] if 'Stanje nekretnine:' in info else ''
        built_year = info['Godina izgradnje:'] if 'Godina izgradnje:' in info else ''
        loader.add_value('object_type', object_type + ' ' + built_year)

        real_estate_type = info['Kategorija:'] if 'Kategorija:' in info else ''  # needs cleaning
        loader.add_value('real_estate_type', real_estate_type)

        loader.add_value('date', datetime.today().strftime('%d/%m/%Y'))

        return loader.load_item()
