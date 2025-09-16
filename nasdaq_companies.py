# nasdaq_companies.py
# Lista de Empresas NASDAQ-100 con información para consultas en Google Play Store

NASDAQ_100_COMPANIES = {
    # MAMAA + Tesla (Los 7 Magníficos)
    "AAPL": ["Apple Inc.", "Apple", ["Apple Inc."]],
    "MSFT": ["Microsoft Corporation", "Microsoft Corporation", ["Microsoft", "LinkedIn Corporation", "Skype Communications", "Mojang Studios", "Activision Publishing"]],
    "AMZN": ["Amazon.com Inc.", "Amazon Mobile LLC", ["Amazon", "Amazon.com Services LLC", "Twitch Interactive", "Ring", "Whole Foods Market", "IMDb", "Audible"]],
    "GOOGL": ["Alphabet Inc.", "Google LLC", ["Google", "YouTube", "Waze Mobile", "Nest Labs", "DeepMind"]],
    "GOOG": ["Alphabet Inc.", "Google LLC", ["Google", "YouTube", "Waze Mobile", "Nest Labs", "DeepMind"]],
    "META": ["Meta Platforms Inc.", "Meta Platforms Inc.", ["Facebook", "Instagram", "WhatsApp", "Oculus VR"]],
    "TSLA": ["Tesla Inc.", "Tesla Inc.", ["Tesla"]],
    
    # Tecnología - Semiconductores
    "NVDA": ["NVIDIA Corporation", "NVIDIA Corporation", ["NVIDIA"]],
    "AVGO": ["Broadcom Inc.", "Broadcom Corporation", ["Broadcom"]],
    "AMD": ["Advanced Micro Devices Inc.", "AMD", ["Advanced Micro Devices"]],
    "QCOM": ["QUALCOMM Incorporated", "QUALCOMM Incorporated", ["Qualcomm"]],
    "TXN": ["Texas Instruments Incorporated", "Texas Instruments", ["Texas Instruments"]],
    "INTC": ["Intel Corporation", "Intel Corporation", ["Intel", "Mobileye"]],
    
    # Tecnología - Software
    "ADBE": ["Adobe Inc.", "Adobe", ["Adobe Systems"]],
    "CRM": ["Salesforce Inc.", "salesforce.com inc.", ["Salesforce", "Slack Technologies", "Tableau Software", "MuleSoft"]],
    "INTU": ["Intuit Inc.", "Intuit Inc.", ["Intuit", "Credit Karma", "Mailchimp"]],
    "WDAY": ["Workday Inc.", "Workday Inc.", ["Workday"]],
    "TEAM": ["Atlassian Corporation", "Atlassian", ["Atlassian"]],
    "ADSK": ["Autodesk Inc.", "Autodesk Inc.", ["Autodesk"]],
    "CRWD": ["CrowdStrike Holdings Inc.", "CrowdStrike Inc.", ["CrowdStrike"]],
    "PANW": ["Palo Alto Networks Inc.", "Palo Alto Networks", ["Palo Alto Networks"]],
    "ZS": ["Zscaler Inc.", "Zscaler Inc.", ["Zscaler"]],
    
    # Tecnología - Hardware y Sistemas
    "CSCO": ["Cisco Systems Inc.", "Cisco Systems Inc.", ["Cisco", "Webex"]],
    "ORCL": ["Oracle Corporation", "Oracle Corporation", ["Oracle"]],
    
    # Streaming y Entretenimiento
    "NFLX": ["Netflix Inc.", "Netflix Inc.", ["Netflix"]],
    
    # E-commerce y Retail
    "SHOP": ["Shopify Inc.", "Shopify Inc.", ["Shopify"]],
    "COST": ["Costco Wholesale Corporation", "Costco Wholesale", ["Costco"]],
    "BKNG": ["Booking Holdings Inc.", "Booking.com", ["Booking.com", "Priceline", "Kayak", "OpenTable"]],
    
    # Servicios de Transporte y Delivery
    "UBER": ["Uber Technologies Inc.", "Uber Technologies Inc.", ["Uber"]],
    "DASH": ["DoorDash Inc.", "DoorDash", ["DoorDash"]],
    "ABNB": ["Airbnb Inc.", "Airbnb Inc.", ["Airbnb"]],
    
    # Fintech y Pagos
    "PYPL": ["PayPal Holdings Inc.", "PayPal Inc.", ["PayPal", "Venmo", "Braintree", "Xoom"]],
    "SQ": ["Block Inc.", "Square Inc.", ["Square", "Cash App", "Afterpay"]],
    
    # Comunicaciones y Social Media  
    "ZM": ["Zoom Video Communications Inc.", "Zoom", ["Zoom"]],
    "SNAP": ["Snap Inc.", "Snap Inc.", ["Snapchat"]],
    "PINS": ["Pinterest Inc.", "Pinterest", ["Pinterest"]],
    "MTCH": ["Match Group Inc.", "Match Group", ["Tinder", "Hinge", "OkCupid", "Match.com", "PlentyOfFish"]],
    
    # Gaming
    "EA": ["Electronic Arts Inc.", "ELECTRONIC ARTS", ["EA", "Origin", "Respawn Entertainment", "DICE", "BioWare"]],
    "TTWO": ["Take-Two Interactive Software Inc.", "Rockstar Games", ["Take-Two Interactive", "Rockstar Games", "2K Games"]],
    "RBLX": ["Roblox Corporation", "Roblox Corporation", ["Roblox"]],
    
    # Telecomunicaciones
    "TMUS": ["T-Mobile US Inc.", "T-Mobile USA", ["T-Mobile", "Sprint"]],
    "CHTR": ["Charter Communications Inc.", "Charter Communications", ["Spectrum"]],
    "CMCSA": ["Comcast Corporation", "Comcast Corporation", ["Comcast", "NBCUniversal", "Xfinity", "Peacock TV"]],
    
    # Servicios Empresariales
    "ADP": ["Automatic Data Processing Inc.", "ADP Inc.", ["ADP"]],
    "PAYX": ["Paychex Inc.", "Paychex Inc.", ["Paychex"]],
    
    # Alimentación y Bebidas
    "PEP": ["PepsiCo Inc.", "PepsiCo Inc.", ["Pepsi", "Frito-Lay", "Tropicana", "Quaker"]],
    "SBUX": ["Starbucks Corporation", "Starbucks Coffee Company", ["Starbucks"]],
    
    # Retail Físico
    "LULU": ["lululemon athletica inc.", "lululemon athletica", ["Lululemon"]],
    "MAR": ["Marriott International Inc.", "Marriott International", ["Marriott", "Ritz-Carlton", "Sheraton", "W Hotels"]],
    
    # Química y Materiales
    "HON": ["Honeywell International Inc.", "Honeywell International Inc.", ["Honeywell"]],
    
    # Otros servicios importantes (muestra reducida)
    "MELI": ["MercadoLibre Inc.", "MercadoLibre", ["MercadoLibre"]],
    "PDD": ["PDD Holdings Inc.", "PDD Holdings", ["Temu", "Pinduoduo"]],
    "PLTR": ["Palantir Technologies Inc.", "Palantir Technologies", ["Palantir"]],
}

def get_company_info(symbol):
    """Obtiene información detallada de una empresa"""
    symbol = symbol.upper()
    if symbol in NASDAQ_100_COMPANIES:
        info = NASDAQ_100_COMPANIES[symbol]
        return {
            "symbol": symbol,
            "company_name": info[0],
            "play_store_developer": info[1],
            "subsidiaries": info[2],
            "search_terms": [info[1]] + info[2]
        }
    return None