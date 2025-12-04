#!/usr/bin/env python3

import pandas as pd
import logging
import os
import re

class DataCleaner:
    def __init__(self, raw_filename='krisha_apartments.csv', raw_dir='src/data', cleaned_dir='src/data'):
        self.raw_path = os.path.join(raw_dir, raw_filename)
        self.cleaned_path = os.path.join(cleaned_dir, 'cleaned_apartments.csv')
        os.makedirs(cleaned_dir, exist_ok=True)
        self.setup_logging()
    
    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def load_raw_data(self):
        """Загружаем CSV с сырыми данными"""
        try:
            df = pd.read_csv(self.raw_path, encoding='utf-8')
            logging.info(f"Loaded raw data: {len(df)} records")
            return df
        except Exception as e:
            logging.error(f"Error loading data: {e}")
            return pd.DataFrame()

    @staticmethod
    def parse_title(title):
        """Парсим заголовок для извлечения rooms, area, floor и max_floor"""
        rooms = area = floor = max_floor = None
        if isinstance(title, str):
            room_match = re.search(r'(\d+)\s*комн', title, re.IGNORECASE)
            area_match = re.search(r'(\d+\.?\d*)\s*(м²|m²)', title, re.IGNORECASE)
            floor_match = re.search(r'(\d+)\s*/\s*(\d+)\s*этаж', title, re.IGNORECASE)
            if room_match:
                rooms = int(room_match.group(1))
            if area_match:
                area = float(area_match.group(1))
            if floor_match:
                floor = int(floor_match.group(1))
                max_floor = int(floor_match.group(2))
        return pd.Series([rooms, area, floor, max_floor])
    def clean_data(self, df):
        if df.empty:
            logging.error("No raw data found")
            return df

        logging.info("Starting data cleaning...")

        df_clean = df.copy()


        numeric_cols = ['price','rooms','area','floor','max_floor']
        for col in numeric_cols:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.replace(r'[^\d.]','', regex=True)
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')


        text_cols = ['title','location','district','address','currency','city']
        for col in text_cols:
            if col not in df_clean.columns:
                df_clean[col] = 'unknown'
            else:
                df_clean[col] = df_clean[col].fillna('unknown').astype(str).str.strip()


        df_clean['currency'] = 'KZT'  # всегда KZT
        df_clean['city'] = 'Алматы'   # всегда Алматы


        def split_location(loc, district, address):
            if (district.lower() in ['unknown','nan','none'] or not district.strip()) or \
            (address.lower() in ['unknown','nan','none'] or not address.strip()):
                parts = loc.split(',', 1)
                new_district = parts[0].strip() if parts else 'unknown'
                new_address = parts[1].strip() if len(parts) > 1 else 'unknown'
                return pd.Series([new_district, new_address])
            return pd.Series([district, address])

        df_clean[['district','address']] = df_clean.apply(
            lambda row: split_location(row['location'], row['district'], row['address']), axis=1
        )

        parsed = df_clean['title'].apply(self.parse_title)
        parsed.columns = ['rooms_parsed','area_parsed','floor_parsed','max_floor_parsed']

        col_map = {'rooms':'rooms_parsed','area':'area_parsed','floor':'floor_parsed','max_floor':'max_floor_parsed'}
        for col, parsed_col in col_map.items():
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].combine_first(parsed[parsed_col])
            else:
                df_clean[col] = parsed[parsed_col]


        if 'url' in df_clean.columns:
            df_clean = df_clean.drop_duplicates(subset=['url'])

        logging.info(f"Cleaned data contains {len(df_clean)} records")
        return df_clean


    def save_cleaned_data(self, df):
        try:
            df.to_csv(self.cleaned_path, index=False, encoding='utf-8')
            logging.info(f"Cleaned data saved to {self.cleaned_path}")
        except Exception as e:
            logging.error(f"Error saving cleaned data: {e}")


def main():
    cleaner = DataCleaner()
    df_raw = cleaner.load_raw_data()
    if df_raw.empty:
        return
    df_clean = cleaner.clean_data(df_raw)
    cleaner.save_cleaned_data(df_clean)


if __name__ == "__main__":
    main()
