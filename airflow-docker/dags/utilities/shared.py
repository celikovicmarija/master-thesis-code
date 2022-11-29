from requests.structures import CaseInsensitiveDict


def prepare_headers():
    headers = CaseInsensitiveDict()
    headers["Accept"] = "application/json"
    return headers
