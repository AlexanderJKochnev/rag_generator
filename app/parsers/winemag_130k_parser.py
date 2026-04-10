import pandas as pd
from typing import Dict, Any


def parse_winemag_130k(row: pd.Series, filename: str) -> Dict[str, Any]:
    title = row.get('title', '')
    description = row.get('description', '')
    if pd.isna(description):
        description = ''
    
    attributes = {}
    if pd.notna(row.get('designation')):
        attributes['designation'] = str(row['designation'])
    if pd.notna(row.get('variety')):
        attributes['variety'] = str(row['variety'])
    if pd.notna(row.get('province')):
        attributes['province'] = str(row['province'])
    if pd.notna(row.get('region_1')):
        attributes['region_1'] = str(row['region_1'])
    if pd.notna(row.get('region_2')):
        attributes['region_2'] = str(row['region_2'])
    if pd.notna(row.get('taster_name')):
        attributes['taster_name'] = str(row['taster_name'])
    
    price = None
    if pd.notna(row.get('price')):
        try:
            price = float(row['price'])
        except:
            pass
    
    country = row.get('country')
    country = country if pd.notna(country) else None
    rating = row.get('points')
    rating = float(rating) if pd.notna(rating) else None
    brand = row.get('winery')
    brand = brand if pd.notna(brand) else None
    
    return {'name': str(title).strip(), 'description': str(description)[:5000], 'category': 'wine', 'country': country,
            'brand': brand, 'abv': None, 'price': price, 'rating': rating, 'rate_count': None, 'attributes': attributes,
            'source_file': filename}