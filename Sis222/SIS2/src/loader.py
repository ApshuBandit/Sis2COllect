import sqlite3
import pandas as pd
import logging
import os

class DataLoader:
    def __init__(self, db_path='data/krisha_apartments.db'):
        self.db_path = db_path
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s'
        )

    def create_connection(self):
        try:
            os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
            conn = sqlite3.connect(self.db_path)
            return conn
        except Exception as e:
            logging.error(f"Error connecting to DB: {e}")
            return None

    def create_table(self):
        conn = self.create_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()
            create_table_query = '''
            CREATE TABLE IF NOT EXISTS apartments (
                listing_id INTEGER PRIMARY KEY AUTOINCREMENT,
                url TEXT UNIQUE,
                title TEXT,
                price REAL,
                rooms INTEGER,
                area REAL,
                floor INTEGER,
                max_floor INTEGER,
                location TEXT,
                district TEXT,
                address TEXT,
                currency TEXT DEFAULT 'KZT',
                city TEXT DEFAULT 'Алматы',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            '''
            cursor.execute(create_table_query)
            conn.commit()
            logging.info("Table apartments created or verified successfully")
            return True
        except Exception as e:
            logging.error(f"Error creating table: {e}")
            return False
        finally:
            conn.close()

    def load_cleaned_data(self, filepath):
        try:
            df = pd.read_csv(filepath, encoding='utf-8')
            logging.info(f"Loaded cleaned data: {len(df)} records")
            return df
        except Exception as e:
            logging.error(f"Error loading cleaned data: {e}")
            return pd.DataFrame()

    def insert_data(self, df):
        if df.empty:
            logging.warning("No data to insert")
            return False

        if not self.create_table():
            return False

        conn = self.create_connection()
        if not conn:
            return False

        try:
            cursor = conn.cursor()


            records = []
            for _, row in df.iterrows():
                def get_val(key):
                    val = row.get(key, None)
                    if pd.isna(val):
                        return None
                    return val
                record = (
                    get_val('url'),
                    get_val('title'),
                    get_val('price'),
                    get_val('rooms'),
                    get_val('area'),
                    get_val('floor'),
                    get_val('max_floor'),
                    get_val('location'),
                    get_val('district'),
                    get_val('address'),
                    get_val('currency') or 'KZT',
                    get_val('city') or 'Алматы'
                )
                records.append(record)

            insert_query = '''
            INSERT INTO apartments (
                url, title, price, rooms, area, floor, max_floor,
                location, district, address, currency, city
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(url) DO UPDATE SET
                title=excluded.title,
                price=excluded.price,
                rooms=excluded.rooms,
                area=excluded.area,
                floor=excluded.floor,
                max_floor=excluded.max_floor,
                location=excluded.location,
                district=excluded.district,
                address=excluded.address,
                currency=excluded.currency,
                city=excluded.city,
                updated_at=CURRENT_TIMESTAMP
            '''
            cursor.executemany(insert_query, records)
            conn.commit()
            logging.info(f"Inserted/updated {len(records)} records successfully")
            return True
        except Exception as e:
            logging.error(f"Error inserting data: {e}")
            conn.rollback()
            return False
        finally:
            conn.close()

    def verify_data(self):
        conn = self.create_connection()
        if not conn:
            return False
        try:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM apartments")
            total = cursor.fetchone()[0]
            logging.info(f"Total records in DB: {total}")
            cursor.execute("SELECT * FROM apartments LIMIT 5")
            sample = cursor.fetchall()
            logging.info("Sample records:")
            for rec in sample:
                logging.info(rec)
            return True
        except Exception as e:
            logging.error(f"Error verifying DB: {e}")
            return False
        finally:
            conn.close()


def main():
    loader = DataLoader()
    df = loader.load_cleaned_data('src/data/cleaned_apartments.csv')
    if df.empty:
        return
    loader.insert_data(df)
    loader.verify_data()


if __name__ == "__main__":
    main()
