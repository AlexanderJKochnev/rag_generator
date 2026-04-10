import pandas as pd
from typing import Dict, Any


def parse_scotch(row: pd.Series, filename: str) -> Dict[str, Any]:
    name = row.get('name', '')
    description = row.get('description', '')
    if pd.isna(description):
        description = ''
    
    attributes = {}
    if pd.notna(row.get('category')):
        attributes['whisky_category'] = str(row['category'])
    
    price = None
    if pd.notna(row.get('price')):
        try:
            price_str = str(row['price']).replace('$', '').replace('£', '').strip()
            price = float(price_str)
        except:
            pass
    
    rating = row.get('review.point')
    rating = float(rating) if pd.notna(rating) else None
    
    return {'name': str(name).strip(), 'description': str(description)[:5000], 'category': 'whisky',
            'country': 'Scotland', 'brand': name.split()[0] if name else None, 'abv': None, 'price': price,
            'rating': rating, 'rate_count': None, 'attributes': attributes, 'source_file': filename}