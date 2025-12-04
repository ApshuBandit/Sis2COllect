#!/usr/bin/env python3
import os
import time
import logging
import pandas as pd

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

class KrishaScraper:
    def __init__(self, city="almaty", max_pages=5, headless=False):
        self.city = city
        self.max_pages = max_pages
        self.data = []

        options = Options()
        if headless:
            options.add_argument("--headless")
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")
        options.add_argument("--disable-blink-features=AutomationControlled")
        options.add_experimental_option("excludeSwitches", ["enable-automation"])
        options.add_experimental_option('useAutomationExtension', False)

        self.driver = webdriver.Chrome(options=options)

    def scrape_page(self, page_num):
        url = f"https://krisha.kz/arenda/kvartiry/{self.city}/?page={page_num}"
        logging.info("Загружаю страницу %s", url)
        self.driver.get(url)


        self.driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(2)

        wait = WebDriverWait(self.driver, 10)
        try:

            wait.until(
                EC.presence_of_element_located(
                    (By.CSS_SELECTOR, "article.a-card, div.a-card__inc, div[data-id]")
                )
            )
        except Exception as e:
            logging.warning("Не найдены объявления на странице %s — %s", page_num, e)
            return


        cards = self.driver.find_elements(By.CSS_SELECTOR, "article.a-card")
        if not cards:
            cards = self.driver.find_elements(By.CSS_SELECTOR, "div.a-card__inc")
        if not cards:
            cards = self.driver.find_elements(By.CSS_SELECTOR, "div[data-id]")

        logging.info("Найдено карточек: %d", len(cards))

        for card in cards:
            try:

                html = card.get_attribute("outerHTML")
                try:
                    link = card.find_element(By.CSS_SELECTOR, "a").get_attribute("href")
                except:
                    link = None


                try:
                    price = card.find_element(By.CSS_SELECTOR, ".a-card__price").text.strip()
                except:
                    price = None


                try:
                    title = card.find_element(By.CSS_SELECTOR, ".a-card__title").text.strip()
                except:
                    title = None


                try:
                    location = card.find_element(By.CSS_SELECTOR, ".a-card__subtitle").text.strip()
                except:
                    location = None


                rooms = area = floor = None
                try:
                    details = card.find_elements(By.CSS_SELECTOR, ".a-card__info-text")
                    for d in details:
                        txt = d.text.lower()
                        if "комн" in txt or "комн." in txt:
                            rooms = txt
                        elif "м²" in txt or "м2" in txt:
                            area = txt
                        elif "этаж" in txt:
                            floor = txt
                except:
                    pass

                self.data.append({
                    "url": link,
                    "title": title,
                    "price": price,
                    "location": location,
                    "rooms": rooms,
                    "area": area,
                    "floor": floor
                })

            except Exception as e:
                logging.warning("Ошибка при парсинге карточки: %s", e)

    def scrape(self):
        for page in range(1, self.max_pages + 1):
            self.scrape_page(page)

        self.driver.quit()
        return self.data


if __name__ == "__main__":
    scraper = KrishaScraper(city="almaty", max_pages=5, headless=False)
    data = scraper.scrape()

    if not data:
        logging.error("Не удалось собрать ни одного объявления")
    else:
        logging.info("Собрано %d объявлений", len(data))
        df = pd.DataFrame(data)

        BASE_DIR = os.path.dirname(os.path.abspath(__file__))
        DATA_DIR = os.path.join(BASE_DIR, "data")
        os.makedirs(DATA_DIR, exist_ok=True)
        csv_path = os.path.join(DATA_DIR, "krisha_apartments.csv")
        df.to_csv(csv_path, index=False, encoding="utf-8-sig")
        logging.info("Сохранено в %s", csv_path)
