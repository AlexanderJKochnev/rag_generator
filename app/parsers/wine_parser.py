import pandas as pd
from typing import Dict, Any


def parse_wine(row: pd.Series, filename: str) -> Dict[str, Any]:
    winery = row.get('winery', '')
    designation = row.get('designation', '')
    varietal = row.get('varietal', '')
    name_parts = [str(winery)]
    if pd.notna(designation) and designation:
        name_parts.append(str(designation))
    if pd.notna(varietal) and varietal:
        name_parts.append(str(varietal))
    name = ' '.join(name_parts).strip()
    if not name:
        name = 'Unknown wine'
    
    description = row.get('review', '')
    if pd.isna(description):
        description = ''
    
    attributes = {}
    if pd.notna(row.get('appellation')):
        attributes['appellation'] = str(row['appellation'])
    if pd.notna(row.get('varietal')):
        attributes['varietal'] = str(row['varietal'])
    if pd.notna(row.get('reviewer')):
        attributes['reviewer'] = str(row['reviewer'])
    
    abv = None
    if pd.notna(row.get('alcohol')):
        try:
            abv = float(str(row['alcohol']).replace('%', ''))
        except:
            pass
    
    price = None
    if pd.notna(row.get('price')):
        try:
            price = float(row['price'])
        except:
            pass
    
    country = row.get('country')
    country = country if pd.notna(country) else None
    rating = row.get('rating')
    rating = float(rating) if pd.notna(rating) else None
    
    return {'name': str(name)[:500], 'description': str(description)[:5000], 'category': 'wine', 'country': country,
            'brand': str(winery) if pd.notna(winery) else None, 'abv': abv, 'price': price, 'rating': rating,
            'rate_count': None, 'attributes': attributes, 'source_file': filename}