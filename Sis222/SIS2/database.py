import sqlite3
import logging

def create_database_schema():
    conn = sqlite3.connect('data/krisha_apartments.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS apartments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            listing_id TEXT UNIQUE,
            title TEXT,
            price INTEGER,
            currency TEXT,
            city TEXT,
            district TEXT,
            address TEXT,
            rooms INTEGER,
            area REAL,
            floor INTEGER,
            max_floor INTEGER,
            building_type TEXT,
            bathroom TEXT,
            ceiling_height REAL,
            renovation TEXT,
            furniture TEXT,
            description TEXT,
            seller_type TEXT,
            posted_date TEXT,
            url TEXT,
            scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        )
    ''')
    
    conn.commit()
    conn.close()
    logging.info("Database created")

if __name__ == "__main__":
    create_database_schema()