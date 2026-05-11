def append_query_param(base_url : str, params_dict: dict) -> str:
    filtered_params = {k: v for k, v  in params_dict.items() if v}

    if not filtered_params:
        return base_url
    
    separator = "&" if "?" in base_url else "?"
    query_string = "&".join(f"{k}={v}" for k, v in filtered_params.items())

    return f"{base_url}{separator}{query_string}"

def build_url_pattern_1(base_url: str, id_value, endpoint: str, id_param_name: str, params_dict: dict) -> str:
    id_param_dict = {id_param_name : id_value}
    if not base_url.endswith('/'):
        base_url += '/'
    return append_query_param(base_url + endpoint, {**id_param_dict, **params_dict})   

def build_url_pattern_2(base_url: str, id_value, endpoint: str, params_dict : dict) -> str:
    if not base_url.endswith('/'):
        base_url += '/'
    return append_query_param(base_url + str(id_value) + "/" + endpoint, params_dict)

def detect_url_pattern(**kwargs) -> int:
    if 'endpoint' in kwargs.keys():
        if 'id_param_name' in kwargs.keys():
            return 1
        else:
            return 2
    return None