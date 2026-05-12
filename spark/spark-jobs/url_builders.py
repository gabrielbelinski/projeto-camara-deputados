BASE_URL = "https://dadosabertos.camara.leg.br/api/v2"

def build_deputies_url():
    return f"{BASE_URL}/deputados?"

def build_expenses_url(dep_id):
    return f"{BASE_URL}/deputados/{str(dep_id)}/despesas?"

def build_legislatures_url():
    return f"{BASE_URL}/legislaturas?"

def build_parties_url():
    return f"{BASE_URL}/partidos?"