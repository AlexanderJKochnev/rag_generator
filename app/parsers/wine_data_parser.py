import pandas as pd
from typing import Dict, Any


def parse_wine_data(row: pd.Series, filename: str) -> Dict[str, Any]:
    name = row.get('Name', '')
    description = row.get('Description', '')
    if pd.isna(description):
        description = ''
    
    attributes = {}
    if pd.notna(row.get('Categories')):
        attributes['categories'] = str(row['Categories'])
    if pd.notna(row.get('Tasting Notes')):
        attributes['tasting_notes'] = str(row['Tasting Notes'])
    if pd.notna(row.get('Food Pairing')):
        attributes['food_pairing'] = str(row['Food Pairing'])
    if pd.notna(row.get('Suggested Glassware')):
        attributes['glassware'] = str(row['Suggested Glassware'])
    if pd.notna(row.get('Suggested Serving Temperature')):
        attributes['serving_temp'] = str(row['Suggested Serving Temperature'])
    if pd.notna(row.get('Sweet-Dry Scale')):
        attributes['sweet_dry_scale'] = str(row['Sweet-Dry Scale'])
    if pd.notna(row.get('Body')):
        attributes['body'] = str(row['Body'])
    
    price = None
    if pd.notna(row.get('Price')):
        try:
            price = float(str(row['Price']).replace('$', ''))
        except:
            pass
    
    country = row.get('Country')
    country = country if pd.notna(country) else None
    brand = row.get('Brand')
    brand = brand if pd.notna(brand) else None
    rating = row.get('Rating')
    rating = float(rating) if pd.notna(rating) else None
    rate_count = row.get('Rate Count')
    rate_count = int(rate_count) if pd.notna(rate_count) else None
    
    return {'name': str(name).strip(), 'description': str(description)[:5000], 'category': 'wine', 'country': country,
            'brand': brand, 'abv': None, 'price': price, 'rating': rating, 'rate_count': rate_count,
            'attributes': attributes, 'source_file': filename}