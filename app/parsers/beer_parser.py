import pandas as pd
from typing import Dict, Any


def parse_beer(row: pd.Series, filename: str) -> Dict[str, Any]:
    name = row.get('Name', '')
    description = row.get('Description', '')
    if pd.isna(description) or description == '':
        tasting = row.get('Tasting Notes', '')
        categories = row.get('Categories', '')
        description = f"{categories}. {tasting}".strip()
    
    attributes = {}
    if pd.notna(row.get('Tasting Notes')):
        attributes['tasting_notes'] = str(row['Tasting Notes'])
    if pd.notna(row.get('Categories')):
        attributes['categories'] = str(row['Categories'])
    if pd.notna(row.get('IBU')):
        attributes['ibu'] = int(row['IBU'])
    if pd.notna(row.get('Food Pairing')):
        attributes['food_pairing'] = str(row['Food Pairing'])
    if pd.notna(row.get('Suggested Serving Temperature')):
        attributes['serving_temp'] = str(row['Suggested Serving Temperature'])
    
    abv = None
    if pd.notna(row.get('ABV')):
        try:
            abv = float(str(row['ABV']).replace('%', ''))
        except:
            pass
    
    price = None
    if pd.notna(row.get('Price')):
        try:
            price = float(str(row['Price']).replace('$', ''))
        except:
            pass
    
    # Обработка NaN
    country = row.get('Country')
    country = country if pd.notna(country) else None
    brand = row.get('Brand')
    brand = brand if pd.notna(brand) else None
    rating = row.get('Rating')
    rating = float(rating) if pd.notna(rating) else None
    rate_count = row.get('Rate Count')
    rate_count = int(rate_count) if pd.notna(rate_count) else None
    
    return {'name': str(name).strip(), 'description': str(description)[:5000], 'category': 'beer', 'country': country,
            'brand': brand, 'abv': abv, 'price': price, 'rating': rating, 'rate_count': rate_count,
            'attributes': attributes, 'source_file': filename}