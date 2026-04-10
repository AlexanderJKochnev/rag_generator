import pandas as pd
from typing import Dict, Any


def parse_winemag_first150k(row: pd.Series, filename: str) -> Dict[str, Any]:
    winery = row.get('winery', '')
    designation = row.get('designation', '')
    name_parts = []
    if pd.notna(winery) and winery:
        name_parts.append(str(winery))
    if pd.notna(designation) and designation:
        name_parts.append(str(designation))
    name = ' '.join(name_parts).strip()
    if not name:
        name = designation if pd.notna(designation) else str(winery)
    
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
    brand = winery if pd.notna(winery) else None
    
    return {'name': str(name)[:500], 'description': str(description)[:5000], 'category': 'wine', 'country': country,
            'brand': brand, 'abv': None, 'price': price, 'rating': rating, 'rate_count': None, 'attributes': attributes,
            'source_file': filename}