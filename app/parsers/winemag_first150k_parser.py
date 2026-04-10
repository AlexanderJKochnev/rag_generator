import pandas as pd
from typing import Dict, Any


def parse_winemag_first150k(row: pd.Series, filename: str) -> Dict[str, Any]:
    """Парсинг winemag-data_first150k.csv"""

    description = row.get('description', '')

    # Формируем имя из winery + designation
    winery = row.get('winery', '')
    designation = row.get('designation', '')
    name = f"{winery} {designation}".strip()

    attributes = {
        'designation': designation,
        'variety': row.get('variety', ''),
        'province': row.get('province', ''),
        'region_1': row.get('region_1', ''),
        'region_2': row.get('region_2', '')
    }
    country = row.get('country')
    country = country if pd.notna(country) else None
    brand = row.get('winery')
    brand = brand if pd.notna(brand) else None
    return {
        'name': str(name),
        'description': str(description),
        'category': 'wine',
        'country': country,
        'brand': winery,
        'abv': None,
        'price': float(row['price']) if pd.notna(row.get('price')) else None,
        'rating': float(row['points']) if pd.notna(row.get('points')) else None,
        'rate_count': None,
        'attributes': attributes,
        'source_file': filename
    }
