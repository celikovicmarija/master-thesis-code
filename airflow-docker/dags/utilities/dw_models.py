# coding: utf-8
from sqlalchemy import BigInteger, Column, DECIMAL, Date, Float, ForeignKey, Index, Integer, String, Table, Text
from sqlalchemy.dialects.mysql import TEXT, TINYINT, VARCHAR
from sqlalchemy.orm import relationship
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()
metadata = Base.metadata


class DimCategory(Base):
    __tablename__ = 'dim_category'

    category_id = Column(BigInteger, primary_key=True)
    category_name = Column(VARCHAR(100))


class DimDate(Base):
    __tablename__ = 'dim_date'

    date_id = Column(BigInteger, primary_key=True)
    fulldate = Column(Date)
    dayofmonth = Column(Integer)
    dayofyear = Column(Integer)
    dayofweek = Column(Integer)
    dayname = Column(String(10))
    monthnumber = Column(Integer)
    monthname = Column(String(10))
    year = Column(Integer)
    quarter = Column(TINYINT)


class DimFunctionalUnit(Base):
    __tablename__ = 'dim_functional_units'

    unit_id = Column(TINYINT, primary_key=True)
    unit_name = Column(String(50))


class DimGeocode(Base):
    __tablename__ = 'dim_geocode'

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
    postcode = Column(VARCHAR(100))
    state = Column(VARCHAR(200))
    county = Column(VARCHAR(200))
    city = Column(VARCHAR(200))
    district = Column(VARCHAR(200))
    suburb = Column(VARCHAR(200))
    name = Column(VARCHAR(200))
    micro_location_oglasi = Column(VARCHAR(100))
    street_oglasi = Column(VARCHAR(100))


class DimHeatingType(Base):
    __tablename__ = 'dim_heating_type'

    heating_type_id = Column(TINYINT, primary_key=True)
    heating_type_name = Column(VARCHAR(200), nullable=False)


class DimMonitoringStation(Base):
    __tablename__ = 'dim_monitoring_station'

    monitoring_station_name = Column(VARCHAR(50))
    monitoring_station_id = Column(TINYINT, primary_key=True)
    lat = Column(DECIMAL(30, 26))
    lon = Column(DECIMAL(30, 26))


class DimPlace(Base):
    __tablename__ = 'dim_place'

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
    geocode_id = Column(BigInteger, index=True)


class DimPlaceDetail(Base):
    __tablename__ = 'dim_place_details'

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
    geocode_id = Column(BigInteger, index=True)


class DimPlaceDetailsSubcategory(Base):
    __tablename__ = 'dim_place_details_subcategory'

    place_details_id = Column(BigInteger, nullable=False, index=True)
    subcategory_id = Column(BigInteger, nullable=False, index=True)
    id = Column(BigInteger, primary_key=True)


class DimPlaceSubcategory(Base):
    __tablename__ = 'dim_place_subcategory'

    place_id = Column(BigInteger, nullable=False, index=True)
    subcategory_id = Column(BigInteger, nullable=False, index=True)
    id = Column(BigInteger, primary_key=True)


class DimProperty(Base):
    __tablename__ = 'dim_property'

    property_id = Column(BigInteger, primary_key=True)
    property_description = Column(Text)
    link = Column(Text)
    title = Column(Text)
    is_listed = Column(TINYINT(1))
    number_of_rooms = Column(TEXT)
    floor_number = Column(TEXT)
    monthly_bills = Column(DECIMAL(18, 0))
    measurement_name = Column(VARCHAR(10), nullable=False)
    location = Column(TEXT)
    micro_location = Column(VARCHAR(200))
    total_number_of_floors = Column(VARCHAR(20))
    city = Column(VARCHAR(50))
    object_state = Column(VARCHAR(50))


class DimPropertyType(Base):
    __tablename__ = 'dim_property_type'

    property_type_id = Column(TINYINT, primary_key=True)
    property_type_name = Column(VARCHAR(20), nullable=False)


class DimRegion(Base):
    __tablename__ = 'dim_region'

    region_id = Column(TINYINT, primary_key=True)
    region_name = Column(String(50))
    unit_id = Column(TINYINT)


class DimSource(Base):
    __tablename__ = 'dim_source'

    source_id = Column(TINYINT, primary_key=True, unique=True)
    source_name = Column(String(100))


class DimTransactionType(Base):
    __tablename__ = 'dim_transaction_type'

    transaction_type_name = Column(VARCHAR(20))
    transaction_type_id = Column(TINYINT, primary_key=True)


class FactCountryParameter(Base):
    __tablename__ = 'fact_country_parameters'
    __table_args__ = (
        Index('fact_country_parameters_FK', 'municipality_name', 'municipality_name_cirilic', 'municipality_name_srlat'),
    )

    id = Column(BigInteger, nullable=False, unique=True)
    time_id = Column(BigInteger)
    location_id = Column(BigInteger)
    broj_registrovanih_nepokretnosti = Column(Integer)
    broj_nepokretnosti_vlasnici_fizicka_lica = Column(Integer)
    iskljucivo_vlasnistvo_zena = Column(Integer)
    iskljucivo_vlasnistvo_muskaraca = Column(Integer)
    zajednicko_suvlasnicko_vlasnistvo = Column(String(100))
    monitoring_station_id = Column(BigInteger)
    month = Column(String(50), primary_key=True, nullable=False)
    year = Column(Integer, primary_key=True, nullable=False)
    municipality_name = Column(VARCHAR(100))
    municipality_name_cirilic = Column(VARCHAR(100), primary_key=True, nullable=False)
    municipality_name_srlat = Column(VARCHAR(100))
    broj_nepokretnosti_vlasnici_fizicka_lica_sa_jmbg = Column(Float(asdecimal=True))
    broj_zavrsenih_stanova = Column(Float(asdecimal=True))
    prosecna_povrsina_prodatih_stanova_novogradnje_m2 = Column(Float(asdecimal=True))
    prosecna_cena_prodatih_stanova_novogradnje_rsd = Column(Float(asdecimal=True))
    povrsina_stanovi_novogradnja_m2 = Column(Integer)
    cena_gradj_zemljista_po_m2_rsd = Column(Integer)
    cena_gradjenja_po_m2_rsd = Column(Integer)
    novogradnja_ostali_troskovi_gradjenja_po_m2_rsd = Column(Integer)
    location = Column(String(50))
    prosecne_neto_zarade = Column(Integer)
    prosecne_bruto_zarade = Column(Integer)
    nVrTacka = Column(Integer)
    procena_stanovnistva_musko_starost_0 = Column(Integer)
    procena_stanovnistva_zensko_starost_0 = Column(Integer)
    procena_stanovnistva_musko_starost_15_19 = Column('procena_stanovnistva_musko_starost_15-19', Integer)
    procena_stanovnistva_zensko_starost_15_19 = Column('procena_stanovnistva_zensko_starost_15-19', Integer)
    procena_stanovnistva_musko_starost_20_24 = Column('procena_stanovnistva_musko_starost_20-24', Integer)
    procena_stanovnistva_zensko_starost_20_24 = Column('procena_stanovnistva_zensko_starost_20-24', Integer)
    procena_stanovnistva_musko_starost_25_29 = Column('procena_stanovnistva_musko_starost_25-29', Integer)
    procena_stanovnistva_zensko_starost_25_29 = Column('procena_stanovnistva_zensko_starost_25-29', Integer)
    procena_stanovnistva_musko_starost_30_34 = Column('procena_stanovnistva_musko_starost_30-34', Integer)
    procena_stanovnistva_zensko_starost_30_34 = Column('procena_stanovnistva_zensko_starost_30-34', Integer)
    procena_stanovnistva_musko_starost_35_39 = Column('procena_stanovnistva_musko_starost_35-39', Integer)
    procena_stanovnistva_zensko_starost_35_39 = Column('procena_stanovnistva_zensko_starost_35-39', Integer)
    procena_stanovnistva_musko_starost_40_44 = Column('procena_stanovnistva_musko_starost_40-44', Integer)
    procena_stanovnistva_zensko_starost_40_44 = Column('procena_stanovnistva_zensko_starost_40-44', Integer)
    procena_stanovnistva_musko_starost_45_49 = Column('procena_stanovnistva_musko_starost_45-49', Integer)
    procena_stanovnistva_zensko_starost_45_49 = Column('procena_stanovnistva_zensko_starost_45-49', Integer)
    procena_stanovnistva_musko_starost_50_54 = Column('procena_stanovnistva_musko_starost_50-54', Integer)
    procena_stanovnistva_zensko_starost_50_54 = Column('procena_stanovnistva_zensko_starost_50-54', Integer)
    procena_stanovnistva_musko_starost_55_59 = Column('procena_stanovnistva_musko_starost_55-59', Integer)
    procena_stanovnistva_zensko_starost_55_59 = Column('procena_stanovnistva_zensko_starost_55-59', Integer)
    procena_stanovnistva_musko_starost_60_64 = Column('procena_stanovnistva_musko_starost_60-64', Integer)
    procena_stanovnistva_zensko_starost_60_64 = Column('procena_stanovnistva_zensko_starost_60-64', Integer)
    procena_stanovnistva_musko_starost_65_69 = Column('procena_stanovnistva_musko_starost_65-69', Integer)
    procena_stanovnistva_zensko_starost_65_69 = Column('procena_stanovnistva_zensko_starost_65-69', Integer)
    procena_stanovnistva_musko_starost_70_74 = Column('procena_stanovnistva_musko_starost_70-74', Integer)
    procena_stanovnistva_zensko_starost_70_74 = Column('procena_stanovnistva_zensko_starost_70-74', Integer)
    procena_stanovnistva_musko_starost_75_79 = Column('procena_stanovnistva_musko_starost_75-79', Integer)
    procena_stanovnistva_zensko_starost_75_79 = Column('procena_stanovnistva_zensko_starost_75-79', Integer)
    procena_stanovnistva_musko_starost_80_84 = Column('procena_stanovnistva_musko_starost_80-84', Integer)
    procena_stanovnistva_zensko_starost_80_84 = Column('procena_stanovnistva_zensko_starost_80-84', Integer)
    procena_stanovnistva_musko_starost_85_i_više = Column('procena_stanovnistva_musko_starost_85 i više', Integer)
    procena_stanovnistva_zensko_starost_85_i_više = Column('procena_stanovnistva_zensko_starost_85 i više', Integer)
    stopa_zivorodjenih_na_1000_stanovnika = Column(Float(asdecimal=True))
    stopa_umrlih_na_1000_stanovnika = Column(Float(asdecimal=True))
    stopa_prirodnog_prirastaja_na_1000_stanovnika = Column(Integer)
    domacinstva_gde_troskovi_stanovanja_znacajno_opterecuju_budzet = Column(Integer)
    domacinstva_gde_troskovi_stanovanja_izvesno_opterecuju_budzet = Column(Float(asdecimal=True))
    domacinstva_gde_troskovi_stanovanja_ne_opterecuju_budzet = Column(Float(asdecimal=True))
    domacinstva_koja_ne_mogu_da_priuste_adekvatno_zagrevanje_proc = Column(Float(asdecimal=True))
    stopa_rizika_od_siromastva_bar_tri_odrasle_osobe = Column(Float(asdecimal=True))
    stopa_riz_siromastvo_bar_3_odrasle_osobe = Column(Float(asdecimal=True))
    stopa_riz_siromastvo_bar_3_odrasle_osobe_sa_decom = Column(Float(asdecimal=True))
    stopa_riz_siromastvo_jednocl_dmcnstv_lice_bar_65_god = Column(Float(asdecimal=True))
    stopa_riz_siromastvo_jednocl_dmcnstv_lice_mladje_od_65_god = Column(Float(asdecimal=True))
    stopa_riz_siromastvo_jednocl_dmcnstv_zensko = Column(Float(asdecimal=True))
    stopa_riz_siromastvo_jednocl_dmcnstv_musko = Column(Float(asdecimal=True))
    stopa_riz_siromastvo_2_odrasle_osobe = Column(Float(asdecimal=True))
    stopa_riz_siromastvo_2_odrasle_osobe_1_dete = Column(Float(asdecimal=True))
    stopa_riz_siromastvo_2_odrasle_osobe_2_dece = Column(Float(asdecimal=True))
    stopa_riz_siromastvo_2_odrasle_osobe_obe_mladje_od_65_god = Column(Float(asdecimal=True))
    stopa_riz_siromastvo_2_odrasle_osobe_bar_jedna_preko_65_god = Column(Float(asdecimal=True))
    stopa_riz_siromastvo_2_odrasle_osobe_bar_3_dece = Column(Float(asdecimal=True))
    stopa_riz_siromastvo_sva_domacinstva_bez_dece = Column(Float(asdecimal=True))
    domacinstvo_veoma_tesko_sastavlja_kraj_s_krajem = Column(Float(asdecimal=True))
    domacinstvo_tesko_sastavlja_kraj_s_krajem = Column(Float(asdecimal=True))
    domacinstvo_sa_teskocama_sastavlja_kraj_s_krajem = Column(Float(asdecimal=True))
    domacinstvo_prilicno_lako_sastavlja_kraj_s_krajem = Column(Float(asdecimal=True))
    domacinstvo_lako_sastavlja_kraj_s_krajem = Column(Float(asdecimal=True))
    domacinstvo_veoma_lako_sastavlja_kraj_s_krajem = Column(Float(asdecimal=True))
    gradovi_strukt_potrosnje__Hrana_bezalk_pica = Column('gradovi_strukt_potrosnje%_Hrana_bezalk_pica', Float(asdecimal=True))
    gradovi_strukt_potrosnje__Stanovanje_voda_el_eja_gas_goriva = Column('gradovi_strukt_potrosnje%_Stanovanje_voda_el.eja_gas_goriva', String(50))
    gradovi_strukt_potrosnje__Oprema_za_stan_odrzavanje = Column('gradovi_strukt_potrosnje%_Oprema_za_stan_odrzavanje', Float(asdecimal=True))
    gradovi_strukt_potrosnje__Zdravlje = Column('gradovi_strukt_potrosnje%_Zdravlje', Float(asdecimal=True))
    gradovi_strukt_potrosnje__Transport = Column('gradovi_strukt_potrosnje%_Transport', Float(asdecimal=True))
    gradovi_strukt_potrosnje__Rekreacija_kultura = Column('gradovi_strukt_potrosnje%_Rekreacija_kultura', Float(asdecimal=True))
    gradovi_strukt_potrosnje__Obrazovanje = Column('gradovi_strukt_potrosnje%_Obrazovanje', Float(asdecimal=True))
    gradovi_strukt_potrosnje__Ostali_licni_predmeti_usluge = Column('gradovi_strukt_potrosnje%_Ostali_licni_predmeti_usluge', Float(asdecimal=True))
    vangradska_naselja_strukt_potrosnje__Hrana_bezalk_pica = Column('vangradska_naselja_strukt_potrosnje%_Hrana_bezalk_pica', Integer)
    vangradska_sred_strukt_potrosnje__Hrana_bezalk_pica = Column('vangradska_sred_strukt_potrosnje%_Hrana_bezalk_pica', Integer)
    vangrad_sred_strukt_potrosnje_Stanovanje_voda_el_eja_gas_goriva = Column('vangrad_sred_strukt_potrosnje%Stanovanje_voda_el.eja_gas_goriva', Float(asdecimal=True))
    vangradska_sred_strukt_potrosnje__Oprema_za_stan_odrzavanje = Column('vangradska_sred_strukt_potrosnje%_Oprema_za_stan_odrzavanje', Float(asdecimal=True))
    vangradska_sred_strukt_potrosnje__Zdravlje = Column('vangradska_sred_strukt_potrosnje%_Zdravlje', Integer)
    vangradska_sred_strukt_potrosnje__Transport = Column('vangradska_sred_strukt_potrosnje%_Transport', Float(asdecimal=True))
    vangradska_sred_strukt_potrosnje__Rekreacija_kultura = Column('vangradska_sred_strukt_potrosnje%_Rekreacija_kultura', Float(asdecimal=True))
    vangradska_sred_strukt_potrosnje__Obrazovanje = Column('vangradska_sred_strukt_potrosnje%_Obrazovanje', Float(asdecimal=True))
    br_gradj_dozvola_Zgrade_Novogradnja = Column(Integer)
    br_gradj_dozvola_Zgrade_Dogradnja = Column(Integer)
    br_gradj_dozvola_Zgrade_uvodjenje_instalacija = Column(Integer)
    br_gradj_dozvola_Poslovne_zgrade_Novogradnja = Column(Integer)
    br_gradj_dozvola_Garaže_Novogradnja = Column(Integer)
    broj_gradj_dozvola_novogradnja_vrtici = Column(Integer)
    br_gradj_dozvola_Zgrade_OS_Novogradnja = Column(Integer)
    broj_gradj_dozvola_novogradnja_bolnice = Column(Integer)
    broj_gradj_dozvola_novogradnja_putevi = Column(Integer)
    broj_gradj_dozvola_dogradnja_putevi = Column(Integer)
    broj_gradj_dozvola_novogradnja_cevovodi_vodovodi = Column(Integer)
    broj_gradj_dozvola_dogradnja_cevovodi_vodovodi = Column(Integer)
    br_gradj_dozvola_Parovodi_toplovodi_Novogradnja = Column(Integer)
    br_gradj_dozvola_Parovodi_toplovodi_Dogradnja = Column(Integer)
    br_gradj_dozvola_električni_vodovi_Novogradnja = Column(Integer)
    br_gradj_dozvola_električni_vodovi_Dogradnja = Column(Integer)
    br_gradj_dozvola_Zgrade_srednjih_škola_Novogradnja = Column(Integer)
    br_gradj_dozvola_Zgrade_fakulteta_Novogradnja = Column(Integer)
    br_gradj_dozvola_Autoputevi_Novogradnja = Column(Integer)
    br_gradj_dozvola_Autoputevi_Dogradnja = Column(Integer)
    br_gradj_dozvola_građevine_za_sport_rekreaciju_Novogradnja = Column(Integer)
    gradj_dozvole_Novogradnja_stamb_zgrade_broj = Column(Integer)
    gradj_dozvole_Novogradnja_stamb_zgrade_površina_m2 = Column(Integer)
    gradj_dozvole_Dogradnja_stamb_zgrade_broj = Column(Integer)
    gradj_dozvole_Dogradnja_stamb_zgrade_površina_m2 = Column(Integer)
    gradj_dozvole_Adaptacija_u_stamb_zgradu_broj = Column(Integer)
    gradj_dozvole_zgrade_Adaptacija_u_stambenu_površina_m2 = Column(Integer)


class DimCounty(Base):
    __tablename__ = 'dim_county'

    county_id = Column(Integer, primary_key=True)
    county_name = Column(String(100))
    region_id = Column(ForeignKey('dim_region.region_id'), index=True)

    region = relationship('DimRegion')


class DimSubcategory(Base):
    __tablename__ = 'dim_subcategory'

    subcategory_id = Column(BigInteger, primary_key=True)
    subcategory_name = Column(VARCHAR(100))
    category_id = Column(ForeignKey('dim_category.category_id'), index=True)

    category = relationship('DimCategory')


class FactAirQuality(Base):
    __tablename__ = 'fact_air_quality'

    pm25 = Column(Integer)
    pm10 = Column(Integer)
    o3 = Column(Integer)
    no2 = Column(Integer)
    so2 = Column(Integer)
    monitoring_station_id = Column(ForeignKey('dim_monitoring_station.monitoring_station_id'), index=True)
    id = Column(BigInteger, primary_key=True)
    date = Column(Date)
    date_id = Column(ForeignKey('dim_date.date_id'), index=True)
    co = Column(Integer)

    date1 = relationship('DimDate')
    monitoring_station = relationship('DimMonitoringStation')


class FactFinancialIndicator(Base):
    __tablename__ = 'fact_financial_indicators'

    id = Column(BigInteger, primary_key=True)
    euribor_1m = Column(DECIMAL(10, 3))
    euribor_6m = Column(DECIMAL(10, 3))
    euribor_3m = Column(DECIMAL(10, 3))
    belibor_1m = Column(DECIMAL(10, 3))
    belibor_6m = Column(DECIMAL(10, 3))
    belibor_3m = Column(DECIMAL(10, 3))
    date_id = Column(ForeignKey('dim_date.date_id'), index=True)
    gotov_novac_u_opticaju = Column(Integer)
    bankarske_dinarske_rezerve_kod_nbs = Column(Integer)
    dinarski_primarni_novac = Column(Integer)
    ukupni_primarni_novac = Column(Float(asdecimal=True))
    M1 = Column(Float(asdecimal=True))
    M2 = Column(Integer)
    M3 = Column(Float(asdecimal=True))
    devizne_rezerve_nbs_u_milionima_evra = Column(Integer)
    devizne_rezerve_banaka_u_milionima_evra = Column(Integer)
    dinarska_devizno_indeksirana_stednja_do_pet_godina = Column(Integer)
    dinarska_devizno_indeksirana_stednja_preko_pet_godina = Column(Integer)
    devizna_stednja_do_pet_godina = Column(Integer)
    devizna_stednja_preko_pet_godina = Column(Integer)
    price_EUR_RSD = Column(Float(asdecimal=True))
    price_EUR_USD = Column(Float(asdecimal=True))
    indeksi_cena_na_malo_roba_rs = Column(Float(asdecimal=True))
    indeksi_cena_na_malo_industrijski_prehramb_proizvodi_rs = Column(Float(asdecimal=True))
    indeksi_cena_na_malo_industrijski_neprehramb_proizvodi_rs = Column(Float(asdecimal=True))
    indeksi_cena_na_malo_usluge_rs = Column(Float(asdecimal=True))
    indeksi_troskova_zivota_bazni_hrana = Column(Float(asdecimal=True))
    indeksi_troskova_zivota_bazni_stanovanje = Column(Float(asdecimal=True))
    indeksi_troskova_zivota_bazni_obrazovanje_kultura_razonoda = Column(Float(asdecimal=True))
    indeksi_troskova_zivota_bazni_saobracajna_sredstva_usluge = Column(Float(asdecimal=True))
    bdp_u_tekucim_cenama_mil_RSD = Column(Float(asdecimal=True))
    bdp_u_tekucim_cenama_po_stanovniku_RSD = Column(Float(asdecimal=True))
    BDP_ucesce__BDV = Column('BDP_ucesce%_BDV', Float(asdecimal=True))
    BDP_cene_prehodne_god_porezi_na_proizvode = Column(Float(asdecimal=True))
    BDP_cene_prehodne_god_subvencije_na_proizvode = Column(Float(asdecimal=True))
    BDP_cene_prehodne_god_BDP = Column(Float(asdecimal=True))
    BDP_ucesce__porezi_na_proizvode = Column('BDP_ucesce%_porezi_na_proizvode', Float(asdecimal=True))
    BDP_stope_realnog_rasta_BDP = Column(Float(asdecimal=True))
    BDP_stope_realnog_rasta_porezi_na_proizvode = Column(Float(asdecimal=True))
    BDP_stope_realnog_rasta_subvencije_na_proizvode = Column(Float(asdecimal=True))
    BDP_stope_realnog_rasta_BDV = Column(Float(asdecimal=True))
    harmonized_index_consumer_prices_EU = Column(Float(asdecimal=True))
    eu_gdp = Column(BigInteger)
    Godina = Column(Integer)
    Mesec = Column(String(50))

    date = relationship('DimDate')


class DimMunicipality(Base):
    __tablename__ = 'dim_municipality'

    municipality_id = Column(BigInteger, primary_key=True)
    municipality_name = Column(VARCHAR(100), nullable=False)
    county_id = Column(ForeignKey('dim_county.county_id'), index=True)
    size_in_squared_km = Column(Integer)
    municipality_name_cirilic = Column(String(100), nullable=False)
    municipality_name_srlat = Column(String(100), nullable=False)

    county = relationship('DimCounty')


t_fact_census = Table(
    'fact_census', metadata,
    Column('id', BigInteger, nullable=False, unique=True),
    Column('broj_nastanjenih_stanova', Integer),
    Column('broj_privremeno_nastanjenih_stanova', Integer),
    Column('broj_nastanjenih_poslovnih_prostora', Integer),
    Column('povrsina_nastanjenih_stanova_m2', Integer),
    Column('povrsina_privremeno_nastanjenih_stanova_m2', Integer),
    Column('povrsina_nastanjenih_poslovnih_prostora_m2', Integer),
    Column('broj_stanova_vangradske_sredine_do 30m2', Integer),
    Column('broj_stanova_vangradske_sredine_31m2_do_ 40m2', Integer),
    Column('broj_stanova_vangradske_sredine_41m2_do_ 50m2', Integer),
    Column('broj_stanova_vangradske_sredine_51m2_do_ 60m2', Integer),
    Column('broj_stanova_vangradske_sredine_61m2_do_ 80m2', Integer),
    Column('broj_stanova_vangradske_sredine_81m2_do_ 100m2', Integer),
    Column('broj_stanova_vangradske_sredine_101m2_do_ 120m2', Integer),
    Column('broj_stanova_vangradske_sredine_121m2_do_ 150m2', Integer),
    Column('broj_stanova_vangradske_sredine_preko_151m2', Integer),
    Column('povrsina_stanova_ m2_vangradske_sredine_do 30m2', Integer),
    Column('povrsina_stanova_ m2_vangradske_sredine_31m2_do_ 40m2', Integer),
    Column('povrsina_stanova_ m2_vangradske_sredine_41m2_do_ 50m2', Integer),
    Column('povrsina_stanova_ m2_vangradske_sredine_51m2_do_ 60m2', Integer),
    Column('povrsina_stanova_ m2_vangradske_sredine_61m2_do_ 80m2', Integer),
    Column('povrsina_stanova_ m2_vangradske_sredine_81m2_do_ 100m2', Integer),
    Column('povrsina_stanova_ m2_vangradske_sredine_101m2_do_ 120m2', Integer),
    Column('povrsina_stanova_ m2_vangradske_sredine_121m2_do_ 150m2', Integer),
    Column('povrsina_stanova_ m2_vangradske_sredine_preko_151m2', Integer),
    Column('broj_stanova_gradska_naselja_do 30m2', Integer),
    Column('broj_stanova_gradska_naselja_31m2_do_ 40m2', Integer),
    Column('broj_stanova_gradska_naselja_41m2_do_ 50m2', Integer),
    Column('broj_stanova_gradska_naselja_51m2_do_ 60m2', Integer),
    Column('broj_stanova_gradska_naselja_61m2_do_ 80m2', Integer),
    Column('broj_stanova_gradska_naselja_81m2_do_ 100m2', Integer),
    Column('broj_stanova_gradska_naselja_101m2_do_ 120m2', Integer),
    Column('broj_stanova_gradska_naselja_121m2_do_ 150m2', Integer),
    Column('broj_stanova_gradska_naselja_preko_151m2', Integer),
    Column('povrsina_stanova_ m2_gradska_naselja_do 30m2', Integer),
    Column('povrsina_stanova_ m2_gradska_naselja_31m2_do_ 40m2', Integer),
    Column('povrsina_stanova_ m2_gradska_naselja_41m2_do_ 50m2', Integer),
    Column('povrsina_stanova_ m2_gradska_naselja_51m2_do_ 60m2', Integer),
    Column('povrsina_stanova_ m2_gradska_naselja_61m2_do_ 80m2', Integer),
    Column('povrsina_stanova_ m2_gradska_naselja_81m2_do_ 100m2', Integer),
    Column('povrsina_stanova_ m2_gradska_naselja_101m2_do_ 120m2', Integer),
    Column('povrsina_stanova_ m2_gradska_naselja_121m2_do_ 150m2', Integer),
    Column('povrsina_stanova_ m2_gradska_naselja_preko_151m2', Integer),
    Column('broj_stanova_vangradska_sredina_Podrum', Integer),
    Column('broj_stanova_vangradska_sredina_Suteren', Integer),
    Column('broj_stanova_vangradska_sredina_Prizemlje', Integer),
    Column('broj_stanova_vangradska_sredina_1sprat', Integer),
    Column('broj_stanova_vangradska_sredina_2sprat', Integer),
    Column('broj_stanova_vangradska_sredina_3sprat', Integer),
    Column('broj_stanova_vangradska_sredina_4sprat', Integer),
    Column('broj_stanova_vangradska_sredina_5-9sprat', Integer),
    Column('broj_stanova_vangradska_sredina_Potkrovlje', Integer),
    Column('broj_stanova_gradska_naselja_Podrum', Integer),
    Column('broj_stanova_gradska_naselja_Suteren', Integer),
    Column('broj_stanova_gradska_naselja_Prizemlje', Integer),
    Column('broj_stanova_gradska_naselja_1sprat', Integer),
    Column('broj_stanova_gradska_naselja_2sprat', Integer),
    Column('broj_stanova_gradska_naselja_3sprat', Integer),
    Column('broj_stanova_gradska_naselja_4sprat', Integer),
    Column('broj_stanova_gradska_naselja_5-9sprat', Integer),
    Column('broj_stanova_gradska_naselja_10-14sprat', Integer),
    Column('broj_stanova_gradska_naselja_preko_15sprat', Integer),
    Column('broj_stanova_gradska_naselja_Potkrovlje', Integer),
    Column('broj_stanova_vangradska_sredina_do 10m2_po_osobi', Integer),
    Column('broj_stanova_vangradska_sredina_10_do_14.9m2_po_osobi', Integer),
    Column('broj_stanova_vangradska_sredina_15_do_19.9m2_po_osobi', Integer),
    Column('broj_stanova_vangradska_sredina_20_do_29.9_po_osobi', Integer),
    Column('broj_stanova_vangradska_sredina_30_do_39.9_m2_po_osobi', Integer),
    Column('broj_stanova_vangradska_sredina_40_do_59.9m2_po_osobi', Integer),
    Column('broj_stanova_vangradska_sredina_preko_60m2_po_osobi', Integer),
    Column('broj_stanova_gradska_naselja_do 10m2_po_osobi', Integer),
    Column('broj_stanova_gradska_naselja_10_do_14.9_po_osobi', Integer),
    Column('broj_stanova_gradska_naselja_15_do_19.9_po_osobi', Integer),
    Column('broj_stanova_gradska_naselja_20_do_29.9m2_po_osobi', Integer),
    Column('broj_stanova_gradska_naselja30_do_39.9', Integer),
    Column('broj_stanova_gradska_naselja_40_do_59.9m2_po_osobi', Integer),
    Column('broj_stanova_gradska_naselja_preko_60m2_po_osobi', Integer),
    Column('broj_stanova_CG_Ugalj', Integer),
    Column('broj_stanova_CG_Drvo_i_sl', Integer),
    Column('broj_stanova_CG_Mazut_i_ulje_za_loženje', Integer),
    Column('broj_stanova_CG_Plinsko/gasno_gorivo', Integer),
    Column('broj_stanova_CG_Električna_energija', Integer),
    Column('broj_stanova_CG_Druga_vrsta_energije', Integer),
    Column('broj_stanova_EG_Ugalj', Integer),
    Column('broj_stanova_EG_Drvo_i_sl', Integer),
    Column('broj_stanova_EG_Mazut_i_ulje_za_loženje', Integer),
    Column('broj_stanova_EG_Plinsko/gasno_gorivo', Integer),
    Column('broj_stanova_EG_Električna_energija', Integer),
    Column('broj_stanova_EG_Druga_vrsta_energije', Integer),
    Column('broj_stanova_bez_CG/EG_Ugalj', Integer),
    Column('broj_stanova_bez_CG/EG_Drvo_i_sl', Integer),
    Column('broj_stanova_bez_CG/EG_Mazut_i_ulje_za_loženje', Integer),
    Column('broj_stanova_bez_CG/EG_Plinsko/gasno_gorivo', Integer),
    Column('broj_stanova_bez_CG/EG_Električna_energija', Integer),
    Column('broj_stanova_bez_CG/EG_Druga_vrsta_energije', Integer),
    Column('broj_stanova_negradska_sredina_broj_stanova_sa_1_licem', Integer),
    Column('broj_stanova_negradska_sredina_broj_stanova_sa_2_lica', Integer),
    Column('broj_stanova_negradska_sredina_broj_stanova_sa_3_lica', Integer),
    Column('broj_stanova_negradska_sredina_broj_stanova_sa_4_lica', Integer),
    Column('broj_stanova_negradska_sredina_broj_stanova_sa_5_lica', Integer),
    Column('broj_stanova_negradska_sredina_broj_stanova_sa_6_lica', Integer),
    Column('broj_stanova_negradska_sredina_broj_stanova_sa_bar_7_lica', Integer),
    Column('broj_stanova_Gradska_naselja_broj_stanova_sa_1_licem', Integer),
    Column('broj_stanova_Gradska_naselja_broj_stanova_sa_2_lica', Integer),
    Column('broj_stanova_Gradska_naselja_broj_stanova_sa_3_lica', Integer),
    Column('broj_stanova_Gradska_naselja_broj_stanova_sa_4_lica', Integer),
    Column('broj_stanova_Gradska_naselja_broj_stanova_sa_5_lica', Integer),
    Column('broj_stanova_Gradska_naselja_broj_stanova_sa_6_lica', Integer),
    Column('broj_stanova_Gradska_naselja_broj_stanova_sa_bar_7_lica', Integer),
    Column('broj_stanova_Jednosobni', Integer),
    Column('broj_stanova_Dvosobni', Integer),
    Column('broj_stanova_Trosobni', Integer),
    Column('broj_stanova_Cetvorosobni', Integer),
    Column('broj_stanova_Petosobni', Integer),
    Column('broj_stanova_Sestosobni', Integer),
    Column('broj_stanova_Sedmosobni', Integer),
    Column('broj_stanova_Osmosobni', Integer),
    Column('broj_stanova_bar_Devetosobni', Integer),
    Column('stanovi_povrsina_m2_Jednosobni', Integer),
    Column('stanovi_povrsina_m2_Dvosobni', Integer),
    Column('stanovi_povrsina_m2_Trosobni', Integer),
    Column('stanovi_povrsina_m2_Cetvorosobni', Integer),
    Column('stanovi_povrsina_m2_Petosobni', Integer),
    Column('stanovi_povrsina_m2_Sestosobni', Integer),
    Column('stanovi_povrsina_m2_Sedmosobni', Integer),
    Column('stanovi_povrsina_m2_Osmosobni', Integer),
    Column('stanovi_povrsina_m2_bar_Devetosobni', Integer),
    Column('broj_stanova_vangradska_sredina_godina_izgradnje_1919_do_1945', Integer),
    Column('broj_stanova_vangradska_sredina_godina_izgradnje_1919', Integer),
    Column('broj_stanova_vangradska_sredina_godina_izgradnje_1946_do_1960', Integer),
    Column('broj_stanova_vangradska_sredina_godina_izgradnje_1961_do_1970', Integer),
    Column('broj_stanova_vangradska_sredina_godina_izgradnje_1971_do_1980', Integer),
    Column('broj_stanova_vangradska_sredina_godina_izgradnje_1981_do_1990', Integer),
    Column('broj_stanova_vangradska_sredina_godina_izgradnje_1991_do_2000', Integer),
    Column('broj_stanova_vangradska_sredina_godina_izgradnje_2001_do_2005', Integer),
    Column('broj_stanova_vangradska_sredina_godina_izgradnje_od_2006', Integer),
    Column('broj_stanova_do_Gradska_naselja_godina_izgradnje_1919_do_1945', Integer),
    Column('broj_stanova_do_Gradska_naselja_godina_izgradnje_pre_1919', Integer),
    Column('broj_stanova_do_Gradska_naselja_godina_izgradnje_1946_do_1960', Integer),
    Column('broj_stanova_do_Gradska_naselja_godina_izgradnje_1961_do_1970', Integer),
    Column('broj_stanova_do_Gradska_naselja_godina_izgradnje_1971_do_1980', Integer),
    Column('broj_stanova_do_Gradska_naselja_godina_izgradnje_1981_do_1990', Integer),
    Column('broj_stanova_do_Gradska_naselja_godina_izgradnje_1991_do_2000', Integer),
    Column('broj_stanova_do_Gradska_naselja_godina_izgradnje_2001_do_2005', Integer),
    Column('broj_stanova_do_Gradska_naselja_godina_izgradnje_od_2006', Integer),
    Column('broj_stanova_Privatna_svojina_1_lica_Vlasnistvo', Integer),
    Column('broj_stanova_Privatna_svojina_1_lica_Zakup', Integer),
    Column('broj_stanova_Privatna_svojina_1_lica_Podstanarstvo', Integer),
    Column('broj_stanova_Privatna_svojina_1_lica_Srodstvo', Integer),
    Column('broj_stanova_Privatna_svojina_bar_2_lica_Vlasnistvo', Integer),
    Column('broj_stanova_Privatna_svojina_bar_2_lica_Zakup', Integer),
    Column('broj_stanova_Privatna_svojina_bar_2_lica_Podstanarstvo', Integer),
    Column('broj_stanova_Privatna_svojina_bar_2_lica_Srodstvo', Integer),
    Column('broj_stanova_Javna_(drzavna)_svojina_Zakup', Integer),
    Column('broj_stanova_Javna_(drzavna)_svojina_Podstanarstvo', Integer),
    Column('broj_stanova_Javna_(drzavna)_svojina_Srodstvo', Integer),
    Column('broj_stanova_Ostali_oblici_vlasnistva_Zakup', Integer),
    Column('broj_stanova_Ostali_oblici_vlasnistva_Podstanarstvo', Integer),
    Column('broj_stanova_Ostali_oblici_vlasnistva_Srodstvo', Integer),
    Column('n_domacinstva_vangradska_sred_prihodi_poljoprivr_1_clan', Integer),
    Column('n_domacinstva_vangradska_sred_prihodi_poljoprivr_2_clana', Integer),
    Column('n_domacinstva_vangradska_sred_prihodi_poljoprivr_3_clana', Integer),
    Column('n_domacinstva_vangradska_sred_prihodi_poljoprivr_4_clana', Integer),
    Column('n_domacinstva_vangradska_sred_prihodi_poljoprivr_5_clanova', Integer),
    Column('n_domacinstva_vangradska_sred_prihodi_poljoprivr_bar_6_clanova', Integer),
    Column('n_domacinstva_vangradska_sred_prihodi_nepoljoprivr_1_clan', Integer),
    Column('n_domacinstva_vangradska_sred_prihodi_nepoljoprivr_2_clana', Integer),
    Column('n_domacinstva_vangradska_sred_prihodi_nepoljoprivr_3_clana', Integer),
    Column('n_domacinstva_vangradska_sred_prihodi_nepoljoprivr_4_clana', Integer),
    Column('n_domacinstva_vangradska_sred_prihodi_nepoljoprivr_5_clanova', Integer),
    Column('n_domacinstva_vangradska_sred_prihodi_nepoljoprivr_bar_6_clanova', Integer),
    Column('n_domacinstva_vangradska_sred_Penzija_1_clan', Integer),
    Column('n_domacinstva_vangradska_sred_Penzija_2_clana', Integer),
    Column('n_domacinstva_vangradska_sred_Penzija_3_clana', Integer),
    Column('n_domacinstva_vangradska_sred_Penzija_4_clana', Integer),
    Column('n_domacinstva_vangradska_sred_Penzija_5_clanova', Integer),
    Column('n_domacinstva_vangradska_sred_Penzija_bar_6_clanova', Integer),
    Column('n_domacinstva_vangradska_sred_soc_prihodi_1_clan', Integer),
    Column('n_domacinstva_vangradska_sred_soc_prihodi_2_clana', Integer),
    Column('n_domacinstva_vangradska_sred_soc_prihodi_3_clana', Integer),
    Column('n_domacinstva_vangradska_sred_soc_prihodi_4_clana', Integer),
    Column('n_domacinstva_vangradska_sred_soc_prihodi_5_clanova', Integer),
    Column('n_domacinstva_vangradska_sred_soc_prihodi_bar_6_clanova', Integer),
    Column('n_domacinstva_vangradska_sred_Ostali_prihodi_1_clan', Integer),
    Column('n_domacinstva_vangradska_sred_Ostali_prihodi_2_clana', Integer),
    Column('n_domacinstva_vangradska_sred_Ostali_prihodi_3_clana', Integer),
    Column('n_domacinstva_vangradska_sred_Ostali_prihodi_4_clana', Integer),
    Column('n_domacinstva_vangradska_sred_Ostali_prihodi_5_clanova', Integer),
    Column('n_domacinstva_vangradska_sred_Ostali_prihodi_bar_6_clanova', Integer),
    Column('n_domacinstva_vangradska_sred_Mesoviti_prihodi_1_clan', Integer),
    Column('n_domacinstva_vangradska_sred_Mesoviti_prihodi_2_clana', Integer),
    Column('n_domacinstva_vangradska_sred_Mesoviti_prihodi_3_clana', Integer),
    Column('n_domacinstva_vangradska_sred_Mesoviti_prihodi_4_clana', Integer),
    Column('n_domacinstva_vangradska_sred_Mesoviti_prihodi_5_clanova', Integer),
    Column('n_domacinstva_vangradska_sred_Mesoviti_prihodi_bar_6_clanova', Integer),
    Column('n_domacinstva_vangradska_sred_Bez_prihoda_1_clan', Integer),
    Column('n_domacinstva_vangradska_sred_Bez_prihoda_2_clana', Integer),
    Column('n_domacinstva_vangradska_sred_Bez_prihoda_3_clana', Integer),
    Column('n_domacinstva_vangradska_sred_Bez_prihoda_4_clana', Integer),
    Column('n_domacinstva_vangradska_sred_Bez_prihoda_5_clanova', Integer),
    Column('n_domacinstva_vangradska_sred_Bez_prihoda_bar_6_clanova', Integer),
    Column('n_domacinstva_Gradska_sred_prihodi_poljoprivr_1_clan', Integer),
    Column('n_domacinstva_Gradska_sred_prihodi_poljoprivr_2_clana', Integer),
    Column('n_domacinstva_Gradska_sred_prihodi_poljoprivr_3_clana', Integer),
    Column('n_domacinstva_Gradska_sred_prihodi_poljoprivr_4_clana', Integer),
    Column('n_domacinstva_Gradska_sred_prihodi_poljoprivr_5_clanova', Integer),
    Column('n_domacinstva_Gradska_sred_prihodi_poljoprivr_bar_6_clanova', Integer),
    Column('n_domacinstva_Gradska_sred_prihodi_nepoljoprivr_1_clan', Integer),
    Column('n_domacinstva_Gradska_sred_prihodi_nepoljoprivr_2_clana', Integer),
    Column('n_domacinstva_Gradska_sred_prihodi_nepoljoprivr_3_clana', Integer),
    Column('n_domacinstva_Gradska_sred_prihodi_nepoljoprivr_4_clana', Integer),
    Column('n_domacinstva_Gradska_sred_prihodi_nepoljoprivr_5_clanova', Integer),
    Column('n_domacinstva_Gradska_sred_prihodi_nepoljoprivr_bar_6_clanova', Integer),
    Column('n_domacinstva_Gradska_sred_Penzija_1_clan', Integer),
    Column('n_domacinstva_Gradska_sred_Penzija_2_clana', Integer),
    Column('n_domacinstva_Gradska_sred_Penzija_3_clana', Integer),
    Column('n_domacinstva_Gradska_sred_Penzija_4_clana', Integer),
    Column('n_domacinstva_Gradska_sred_Penzija_5_clanova', Integer),
    Column('n_domacinstva_Gradska_sred_Penzija_bar_6_clanova', Integer),
    Column('n_domacinstva_Gradska_sred_soc_prihodi_1_clan', Integer),
    Column('n_domacinstva_Gradska_sred_soc_prihodi_2_clana', Integer),
    Column('n_domacinstva_Gradska_sred_soc_prihodi_3_clana', Integer),
    Column('n_domacinstva_Gradska_sred_soc_prihodi_4_clana', Integer),
    Column('n_domacinstva_Gradska_sred_soc_prihodi_5_clanova', Integer),
    Column('n_domacinstva_Gradska_sred_soc_prihodi_bar_6_clanova', Integer),
    Column('n_domacinstva_Gradska_sred_Ostali_prihodi_1_clan', Integer),
    Column('n_domacinstva_Gradska_sred_Ostali_prihodi_2_clana', Integer),
    Column('n_domacinstva_Gradska_sred_Ostali_prihodi_3_clana', Integer),
    Column('n_domacinstva_Gradska_sred_Ostali_prihodi_4_clana', Integer),
    Column('n_domacinstva_Gradska_sred_Ostali_prihodi_5_clanova', Integer),
    Column('n_domacinstva_Gradska_sred_Ostali_prihodi_bar_6_clanova', Integer),
    Column('n_domacinstva_Gradska_sred_Mesoviti_prihodi_1_clan', Integer),
    Column('n_domacinstva_Gradska_sred_Mesoviti_prihodi_2_clana', Integer),
    Column('n_domacinstva_Gradska_sred_Mesoviti_prihodi_3_clana', Integer),
    Column('n_domacinstva_Gradska_sred_Mesoviti_prihodi_4_clana', Integer),
    Column('n_domacinstva_Gradska_sred_Mesoviti_prihodi_5_clanova', Integer),
    Column('n_domacinstva_Gradska_sred_Mesoviti_prihodi_bar_6_clanova', Integer),
    Column('n_domacinstva_Gradska_sred_Bez_prihoda_1_clan', Integer),
    Column('n_domacinstva_Gradska_sred_Bez_prihoda_2_clana', Integer),
    Column('n_domacinstva_Gradska_sred_Bez_prihoda_3_clana', Integer),
    Column('n_domacinstva_Gradska_sred_Bez_prihoda_4_clana', Integer),
    Column('n_domacinstva_Gradska_sred_Bez_prihoda_5_clanova', Integer),
    Column('n_domacinstva_Gradska_sred_Bez_prihoda_bar_6_clanova', Integer),
    Column('15-24_male_Strucnjaci_i_umetnici', Integer),
    Column('15-24_male_Administrativni_sluzbenici', Integer),
    Column('15-24_male_Usluzna_i_trgovacka_zanimanja', Integer),
    Column('15-24_male_Zanatlije_idr', Integer),
    Column('15-24_male_Jednostavna_zanimanja', Integer),
    Column('15-24_male_Vojna_zanimanja', Integer),
    Column('15-24_female_Strucnjaci_i_umetnici', Integer),
    Column('15-24_female_Administrativni_sluzbenici', Integer),
    Column('15-24_female_Usluzna_i_trgovacka_zanimanja', Integer),
    Column('15-24_female_Zanatlije_idr', Integer),
    Column('15-24_female_Jednostavna_zanimanja', Integer),
    Column('15-24_female_Vojna_zanimanja', Integer),
    Column('25-34_male_Strucnjaci_i_umetnici', Integer),
    Column('25-34_male_Administrativni_sluzbenici', Integer),
    Column('25-34_male_Usluzna_i_trgovacka_zanimanja', Integer),
    Column('25-34_male_Zanatlije_idr', Integer),
    Column('25-34_male_Jednostavna_zanimanja', Integer),
    Column('25-34_male_Vojna_zanimanja', Integer),
    Column('25-34_female_Strucnjaci_i_umetnici', Integer),
    Column('25-34_female_Administrativni_sluzbenici', Integer),
    Column('25-34_female_Usluzna_i_trgovacka_zanimanja', Integer),
    Column('25-34_female_Zanatlije_idr', Integer),
    Column('25-34_female_Jednostavna_zanimanja', Integer),
    Column('25-34_female_Vojna_zanimanja', Integer),
    Column('35-44_male_Strucnjaci_i_umetnici', Integer),
    Column('35-44_male_Administrativni_sluzbenici', Integer),
    Column('35-44_male_Usluzna_i_trgovacka_zanimanja', Integer),
    Column('35-44_male_Zanatlije_idr', Integer),
    Column('35-44_male_Jednostavna_zanimanja', Integer),
    Column('35-44_male_Vojna_zanimanja', Integer),
    Column('35-44_female_Strucnjaci_i_umetnici', Integer),
    Column('35-44_female_Administrativni_sluzbenici', Integer),
    Column('35-44_female_Usluzna_i_trgovacka_zanimanja', Integer),
    Column('35-44_female_Zanatlije_idr', Integer),
    Column('35-44_female_Jednostavna_zanimanja', Integer),
    Column('35-44_female_Vojna_zanimanja', Integer),
    Column('45-54_male_Strucnjaci_i_umetnici', Integer),
    Column('45-54_male_Administrativni_sluzbenici', Integer),
    Column('45-54_male_Usluzna_i_trgovacka_zanimanja', Integer),
    Column('45-54_male_Zanatlije_idr', Integer),
    Column('45-54_male_Jednostavna_zanimanja', Integer),
    Column('45-54_male_Vojna_zanimanja', Integer),
    Column('45-54_female_Strucnjaci_i_umetnici', Integer),
    Column('45-54_female_Administrativni_sluzbenici', Integer),
    Column('45-54_female_Usluzna_i_trgovacka_zanimanja', Integer),
    Column('45-54_female_Zanatlije_idr', Integer),
    Column('45-54_female_Jednostavna_zanimanja', Integer),
    Column('45-54_female_Vojna_zanimanja', Integer),
    Column('55-64_male_Strucnjaci_i_umetnici', Integer),
    Column('55-64_male_Administrativni_sluzbenici', Integer),
    Column('55-64_male_Usluzna_i_trgovacka_zanimanja', Integer),
    Column('55-64_male_Zanatlije_idr', Integer),
    Column('55-64_male_Jednostavna_zanimanja', Integer),
    Column('55-64_male_Vojna_zanimanja', Integer),
    Column('55-64_female_Strucnjaci_i_umetnici', Integer),
    Column('55-64_female_Administrativni_sluzbenici', Integer),
    Column('55-64_female_Usluzna_i_trgovacka_zanimanja', Integer),
    Column('55-64_female_Zanatlije_idr', Integer),
    Column('55-64_female_Jednostavna_zanimanja', Integer),
    Column('55-64_female_Vojna_zanimanja', Integer),
    Column('65_i_vise_godina_male_Strucnjaci_i_umetnici', Integer),
    Column('65_i_vise_godina_male_Administrativni_sluzbenici', Integer),
    Column('65_i_vise_godina_male_Usluzna_i_trgovacka_zanimanja', Integer),
    Column('65_i_vise_godina_male_Zanatlije_idr', Integer),
    Column('65_i_vise_godina_male_Jednostavna_zanimanja', Integer),
    Column('65_i_vise_godina_male_Vojna_zanimanja', Integer),
    Column('65_i_vise_godina_female_Strucnjaci_i_umetnici', Integer),
    Column('65_i_vise_godina_female_Administrativni_sluzbenici', Integer),
    Column('65_i_vise_godina_female_Usluzna_i_trgovacka_zanimanja', Integer),
    Column('65_i_vise_godina_female_Zanatlije_idr', Integer),
    Column('65_i_vise_godina_female_Jednostavna_zanimanja', Integer),
    Column('date_id', ForeignKey('dim_date.date_id'), index=True),
    Column('municipality_id', ForeignKey('dim_municipality.municipality_id'), index=True)
)


class FactRealEstate(Base):
    __tablename__ = 'fact_real_estate'

    id = Column(BigInteger, primary_key=True)
    date_id = Column(ForeignKey('dim_date.date_id'), index=True)
    property_id = Column(ForeignKey('dim_property.property_id'), index=True)
    price = Column(DECIMAL(18, 0))
    price_per_unit = Column(DECIMAL(18, 0))
    municipality_id = Column(ForeignKey('dim_municipality.municipality_id'), index=True)
    source_id = Column(ForeignKey('dim_source.source_id'), index=True)
    heating_type_id = Column(ForeignKey('dim_heating_type.heating_type_id'), index=True)
    property_type_id = Column(ForeignKey('dim_property_type.property_type_id'), index=True)
    transaction_type_id = Column(ForeignKey('dim_transaction_type.transaction_type_id'), index=True)
    geocode_id = Column(ForeignKey('dim_geocode.geocode_id'), index=True)

    date = relationship('DimDate')
    geocode = relationship('DimGeocode')
    heating_type = relationship('DimHeatingType')
    municipality = relationship('DimMunicipality')
    property = relationship('DimProperty')
    property_type = relationship('DimPropertyType')
    source = relationship('DimSource')
    transaction_type = relationship('DimTransactionType')
