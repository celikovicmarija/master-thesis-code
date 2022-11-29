from typing import Any


class Location:
    name: str
    suburb: str
    district: str
    city: str
    county: str
    state: str
    postcode: str
    lon: float
    lat: float
    formatted: str
    address_line1: str
    address_line2: str
    category: str
    result_type: str
    match_type: str
    confidence: str
    popularity: str
    place_id: str
    bbox_lon1: str
    bbox_lat1: str
    bbox_lon2: str
    bbox_lat2: str
    categories: list
    feature_type: str
    street_oglasi: str
    micro_location_oglasi: str

    def __int__(self, name, suburb, district,
                city, county, state, postcode, lon, lat,
                formatted, address_line1, address_line2, category,
                result_type, match_type, confidence,
                popularity, place_id, bbox_lon1, bbox_lat1,
                bbox_lon2, bbox_lat2, categories, feature_type, street_oglasi, micro_location_oglasi):
        self.name = name
        self.suburb = suburb
        self.district = district
        self.city = city
        self.county = county
        self.state = state
        self.postcode = postcode
        self.lon = lon
        self.lat = lat
        self.formatted = formatted
        self.address_line1 = address_line1
        self.address_line2 = address_line2
        self.category = category
        self.result_type = result_type
        self.match_type = match_type
        self.confidence = confidence
        self.popularity = popularity
        self.place_id = place_id
        self.bbox_lon1 = bbox_lon1
        self.bbox_lat1 = bbox_lat1
        self.bbox_lon2 = bbox_lon2
        self.bbox_lat2 = bbox_lat2
        self.categories = categories
        self.feature_type = feature_type
        self.street_oglasi = street_oglasi
        self.micro_location_oglasi = micro_location_oglasi

    def __setattr__(self, name: str, value: Any) -> None:
        super().__setattr__(name, value)

    def __getattribute__(self, name: str) -> Any:
        return super().__getattribute__(name)
