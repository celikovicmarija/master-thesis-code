import itertools
import logging
from datetime import datetime

import scrapy
from inline_requests import inline_requests
from itemloaders import ItemLoader
from scrapy_splash import SplashRequest

from ..items import RealEstateScraperItem


def find_list_to_scrape(urls_test):
    sublist_sell_bg = [urls_test[0] + '?currentPage=' + str(i) for i in range(1, 23)]
    sublist_rent_bg = [urls_test[1] + '?currentPage=' + str(i) for i in range(1, 25)]
    sublist_sell_ns = [urls_test[2] + '?currentPage=' + str(i) for i in range(1, 15)]
    sublist_rent_ns = [urls_test[3] + '?currentPage=' + str(i) for i in range(1, 4)]
    sublist_sell_nis = [urls_test[4] + '?currentPage=' + str(i) for i in range(1, 3)]
    sublist_rent_nis = [urls_test[5] + '?currentPage=' + str(i) for i in range(1, 3)]

    return list(itertools.chain(sublist_rent_ns, sublist_rent_nis, sublist_rent_bg, sublist_sell_nis, sublist_sell_ns,
                                sublist_sell_bg))


class CityExpertSpider(scrapy.Spider):
    name = 'cityexpert'

    activities = ['prodaja-nekretnina', 'izdavanje-nekretnina']
    places = ['beograd', 'novi-sad', 'nis']
    urls_test = ['https://cityexpert.rs/' + '/'.join(r) for r in itertools.product(activities, places)]
    urls = find_list_to_scrape(urls_test)

    def start_requests(self):
        for url in self.urls:
            yield SplashRequest(url=url, endpoint='render.html', args={'wait':1,
                                          'timeout': 900
                                      }, callback=self.parse)

    @inline_requests
    def parse(self, response):
        for re in response.css('div.property-card.property-card--serp.ng-star-inserted'):
            l = ItemLoader(item=RealEstateScraperItem(), selector=re)
            link_ext = re.css('a::attr(href)').get()
            link = str(link_ext)
            logging.info(f'I found this page: {link}')
            res = yield SplashRequest(link, endpoint='render.html',
                                      args={'wait':1,
                                          'timeout': 900
                                      })
            yield self.parse_individual_real_estate(res, l, link)

    def parse_individual_real_estate(self, response, loader, link):
        info = {}
        rs = response.data['childFrames'][0]['html']

        title = response.css('span.propIdNumber::text').get()
        loader.add_value('title', title)

        street = response.css('span.addressStreet::text').get()
        loader.add_value('street', street)

        location = response.css('span.addressMncp a::text').get()
        loader.add_value('location', location)

        city = response.css('span.propId span:n-th-child(2)::text').get()
        loader.add_value('city', city)

        micro_location = response.css('span.addressNbh.ng-star-inserted a::text').get()
        loader.add_value('micro_location', micro_location)

        loader.add_value('link', link)

        for re in response.css('div.propertyViewSection.basicInfoWrap'):
            for k, v in zip(re.css('h6::text').getall(),
                            re.css('h5::text').getall()):
                info[k] = v

        additional = ''  # opremljenost objekta
        # PRODAJA NEKRETNINA
        price_per_squared_meter = info['Cena po m²'] if 'Cena po m²' in info else ''
        loader.add_value('price_per_squared_meter', price_per_squared_meter)
        uknjizena_povrsina = info['Uknjižena površina'] if 'Uknjižena površina' in info else ''
        uknjizenost = info['Uknjiženost'] if 'Uknjiženost' in info else ''
        loader.add_value('is_listed', uknjizenost + uknjizena_povrsina)

        number_of_rooms = info['Struktura'] if 'Struktura' in info else ''
        loader.add_value('number_of_rooms', number_of_rooms)
        floor_number = info['Sprat'] if 'Sprat' in info else ''
        loader.add_value('floor_number', floor_number)

        # TAKE CARE OF THIS
        built_year = info['Godina gradnje'] if 'Godina gradnje' in info else ''
        renoviranost = info['Renoviranost'] if 'Renoviranost' in info else ''
        loader.add_value('object_type', built_year + renoviranost)  # pretvori u neki if else

        size_in_squared_meters = info['Površina'] if 'Površina' in info else ''
        loader.add_value('size_in_squared_meters', size_in_squared_meters)
        total_number_of_floors = info['Ukupno spratova'] if 'Ukupno spratova' in info else ''
        loader.add_value('total_number_of_floors', total_number_of_floors)
        has_furniture = info['Nameštenost'] if 'Nameštenost' in info else ''
        loader.add_value('object_state',
                         has_furniture)  # namesteno je object state, renovirano i godina izgradnje su object type
        number_of_sleeping_rooms = (
                'Broj spavaćih soba:' + info['Broj spavaćih soba'] + ',') if 'Broj spavaćih soba' in info else ''

        additional += number_of_sleeping_rooms
        broj_kupatila = (
                'Broj kupatila:' + info['Broj kupatila'] + ',') if 'Broj kupatila' in info else ''

        additional += broj_kupatila
        stolarija = (
                'Stolarija:' + info['Stolarija'] + ',') if 'Stolarija' in info else ''
        additional += stolarija
        ceiling_height = (
                'Visina plafona:' + info['Visina plafona'] + ',') if 'Visina plafona' in info else ''
        additional += ceiling_height
        number_of_bathrooms = (
                'Broj kupatila:' + info['Broj kupatila'] + ',') if 'Broj kupatila' in info else ''
        additional += number_of_bathrooms
        last_time_renovated = (
                'Poslednji put renoviran:' + info[
            'Poslednji put renoviran'] + ',') if 'Poslednji put renoviran' in info else ''
        additional += last_time_renovated

        for re in response.css('div.extra-room-tags'):
            for k in re.css('h6::text').getall():
                v = ''
                for helper in re.css('div.tagElement ng-star-inserted::text').getall():
                    v += helper
            info[k] = v

        heating_type = info['Grejanje'] if 'Grejanje' in info else ''
        loader.add_value('heating_type', heating_type)
        what_was_renovated = (
                'Stvari koje su renoviran:' + info[
            'Stvari koje su renoviran'] + ',') if 'Stvari koje su renovirane' in info else ''
        additional += what_was_renovated

        parking = (
                'Parking:' + info['Parking'] + ',') if 'Parking' in info else ''
        additional += parking
        additional_space_available = (
                'Dodatni prostor na raspolaganju:' + info[
            'Dodatni prostor na raspolaganju'] + ',') if 'Dodatni prostor na raspolaganju' in info else ''
        additional += additional_space_available

        for re in response.css('div.rent-termsoflease.ng-star-inserted'):
            for k in re.css('h6::text').getall():
                v = ''
                for helper in re.css('h5::text').getall():
                    v += helper
            info[k] = v

        description = ''
        price = info['Depozit'] if 'Depozit' in info else ''
        description += price
        loader.add_value('price', price)
        is_rentable = info['Useljivo'] if 'Useljivo' in info else ''
        is_rentable = ('Useljivo: ' + is_rentable + ',') if is_rentable else ''
        description += is_rentable
        pets_allowed = info['Dozvoljeni ljubimci'] if 'Dozvoljeni ljubimci' in info else ''
        pets_allowed = ('Dozvoljeni ljubimci: ' + pets_allowed + ',') if pets_allowed else ''
        description += pets_allowed
        max_people = info['Maksimalan broj stanara'] if 'Maksimalan broj stanara' in info else ''
        max_people = ('Maksimalan broj stanara: ' + max_people + ',') if max_people else ''
        description += max_people
        payment_period = info['Period plaćanja'] if 'Period plaćanja' in info else ''
        payment_period = ('Period plaćanja: ' + payment_period + ',') if payment_period else ''
        description += payment_period
        min_period = info['Minimalni zakup'] if 'Minimalni zakup' in info else ''
        min_period = ('Minimalni zakup: ' + min_period + ',') if min_period else ''
        description += min_period

        desc = response.css('div.property-description p::text').get()
        if description == '':
            loader.add_value('description', desc)
        else:
            loader.add_value('description', description)

        for re in response.css('div.expenses.ng-star-inserted'):
            for k, v in zip(re.css('h6::text').getall(),
                            re.css('45::text').getall()):
                info[k] = v

        monthly_bills = ''
        infostan = info['Infostan'] if 'Infostan' in info else ''
        infostan = ('Infostan: ' + infostan + ', ') if infostan else ''
        monthly_bills += infostan
        infostan_grejanje = info['Infostan, Grejanje'] if 'Infostan, Grejanje' in info else ''
        infostan_grejanje = ('Infostan, Grejanje: ' + infostan_grejanje + ', ') if infostan_grejanje else ''
        monthly_bills += infostan_grejanje
        kablovska = info['Kablovska'] if 'Kablovska' in info else ''
        kablovska = ('Kablovska: ' + kablovska + ', ') if kablovska else ''
        monthly_bills += kablovska

        heating_electricity = info['Grejanje, Struja'] if 'Grejanje, Struja' in info else ''
        heating_electricity = ('Grejanje, Struja: ' + heating_electricity + ', ') if heating_electricity else ''
        monthly_bills += heating_electricity
        upravnik = info['Upravnik zgrade'] if 'Upravnik zgrade' in info else ''
        upravnik = ('Upravnik zgrade: ' + ', ') if upravnik else ''
        monthly_bills += upravnik

        vojno_odrzavanje = info['Vojno održavanje zgrade'] if 'Vojno održavanje zgrade' in info else ''
        vojno_odrzavanje = ('Vojno održavanje zgrade: ' + ', ') if vojno_odrzavanje else ''
        monthly_bills += vojno_odrzavanje
        porez = info['Porez na imovinu (godišnje)'] if 'Porez na imovinu (godišnje)' in info else ''
        porez = ('Porez na imovinu (godišnje): ' + ', ') if porez else ''
        monthly_bills += porez

        loader.add_value('monthly_bills', monthly_bills)

        equipment = []
        for re in response.css('div.propertyViewSection.equipmentWrap'):
            equipment.append(str(re.css('h5::text')))

        for re in response.css('div.propertyViewElement.ng-star-inserted'):
            for el in re.css('div.info-box-content p::text').getall():
                additional += el

        object_equipment = ''
        for re in response.css('div.propertyViewElement'):
            object_equipment += re.css('h5::text').get()

        additional += object_equipment

        loader.add_value('additional', additional)

        image_urls = []
        for url in response.css('div.photo-gallery-thumbnails.ng-star-inserted img::attr(src)').getall():
            image_urls.append(url)

        image_names = []
        for image_url in image_urls:
            image_names.append('cityexpert' + '_'.join(image_url.split('/')[-2:]))
        loader.add_value('image_urls', image_urls)
        loader.add_value('image_name', image_names)

        loader.add_value('date', datetime.today().strftime('%d-%m-%Y'))

        return loader.load_item()
