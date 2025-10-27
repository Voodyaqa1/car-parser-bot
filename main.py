import os
import requests
from bs4 import BeautifulSoup
import time
import json
import logging
from telegram import Bot
from telegram.error import TelegramError
import schedule
import threading
from datetime import datetime
import re

# Настройка логирования для Render
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

class CarParserBot:
    def __init__(self):
        # Получаем переменные окружения
        self.telegram_token = os.environ.get('TELEGRAM_BOT_TOKEN')  # Исправлено: используйте имя переменной окружения
        self.chat_id = os.environ.get('TELEGRAM_CHAT_ID')  # Исправлено: используйте имя переменной окружения

        if not self.telegram_token or not self.chat_id:
            raise ValueError("TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID must be set")

        self.bot = Bot(token=self.telegram_token)
        self.seen_ads = set()
        self.load_seen_ads()

        # Настройки фильтров
        self.MIN_PRICE = 300000
        self.MAX_PRICE = 500000
        self.MAX_OWNERS = 2

        # Заголовки для имитации браузера
        self.headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }

    def load_seen_ads(self):
        """Загрузка уже просмотренных объявлений"""
        try:
            with open('/app/seen_ads.json', 'r') as f:
                self.seen_ads = set(json.load(f))
        except (FileNotFoundError, json.JSONDecodeError):
            self.seen_ads = set()

    def save_seen_ads(self):
        """Сохранение просмотренных объявлений"""
        try:
            with open('/app/seen_ads.json', 'w') as f:
                json.dump(list(self.seen_ads), f)
        except Exception as e:
            logger.error(f"Ошибка сохранения seen_ads: {e}")

    def extract_price(self, price_text):
        """Извлечение числового значения цены из текста"""
        try:
            # Удаляем все символы кроме цифр
            price_clean = re.sub(r'[^\d]', '', price_text)
            return int(price_clean) if price_clean else 0
        except:
            return 0

    def extract_owners(self, text):
        """Извлечение количества владельцев из текста"""
        try:
            text_lower = text.lower()
            patterns = [
                r'(\d+)\s*владел',
                r'(\d+)\s*хозя',
                r'(\d+)\s*собствен',
                r'владел[а-я]*\s*:\s*(\d+)'
            ]

            for pattern in patterns:
                match = re.search(pattern, text_lower)
                if match:
                    return int(match.group(1))

            if 'один владелец' in text_lower or '1 владелец' in text_lower:
                return 1
            elif 'два владельца' in text_lower or '2 владельца' in text_lower:
                return 2
            elif 'три владельца' in text_lower or '3 владельца' in text_lower:
                return 3

            return None
        except:
            return None

    def meets_criteria(self, ad):
        """Проверка объявления на соответствие критериям"""
        price = self.extract_price(ad['price'])
        if price == 0:
            return False, "Цена не определена"

        if price < self.MIN_PRICE or price > self.MAX_PRICE:
            return False, f"Цена {price} вне диапазона"

        owners = self.extract_owners(ad.get('full_info', '') + ' ' + ad.get('info', ''))
        if owners is not None and owners > self.MAX_OWNERS:
            return False, f"Слишком много владельцев: {owners}"

        return True, "OK"

    def parse_drom_general(self):
        """Парсинг Drom.ru - все объявления"""
        ads = []
        try:
            urls = [
                "https://www.drom.ru/auto/all/",
                "https://www.drom.ru/auto/all/page2/"
            ]

            for url in urls:
                try:
                    response = requests.get(url, headers=self.headers, timeout=15)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')  # Изменено: используем html.parser

                    listings = soup.find_all('a', {'data-ftid': 'bulls-list_bull'})

                    for listing in listings:
                        try:
                            ad_id = listing.get('href', '').split('/')[-1].split('?')[0]
                            if not ad_id or ad_id in self.seen_ads:
                                continue

                            title_elem = listing.find('span', {'data-ftid': 'bull_title'})
                            price_elem = listing.find('span', {'data-ftid': 'bull_price'})
                            info_elem = listing.find('div', {'data-ftid': 'bull_description'})

                            if title_elem and price_elem:
                                title = title_elem.text.strip()
                                price = price_elem.text.strip()
                                info = info_elem.text.strip() if info_elem else ""
                                url_full = "https://www.drom.ru" + listing['href'] if listing['href'].startswith('/') else listing['href']

                                full_info = self.get_drom_details(url_full)

                                ad_data = {
                                    'id': ad_id,
                                    'title': title,
                                    'price': price,
                                    'info': info,
                                    'full_info': full_info,
                                    'url': url_full,
                                    'source': 'Drom.ru'
                                }

                                meets_criteria, reason = self.meets_criteria(ad_data)
                                if meets_criteria:
                                    ads.append(ad_data)

                        except Exception as e:
                            logger.error(f"Ошибка объявления Drom: {e}")
                            continue

                    time.sleep(3)

                except Exception as e:
                    logger.error(f"Ошибка страницы Drom {url}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Ошибка парсинга Drom: {e}")

        return ads

    def get_drom_details(self, url):
        """Получение детальной информации с Drom"""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')  # Изменено: используем html.parser
            info_sections = soup.find_all('div', class_=re.compile(r'info|description|params'))
            return ' '.join([section.get_text() for section in info_sections])
        except:
            return ""

    def parse_auto_ru_general(self):
        """Парсинг Auto.ru - все объявления"""
        ads = []
        try:
            urls = [
                "https://auto.ru/moskva/cars/used/",
                "https://auto.ru/moskva/cars/used/?page=2"
            ]

            for url in urls:
                try:
                    response = requests.get(url, headers=self.headers, timeout=15)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')  # Изменено: используем html.parser

                    listings = soup.find_all('a', href=re.compile(r'\/auto\.ru\/cars\/used\/sale\/'))

                    for listing in listings:
                        try:
                            href = listing.get('href', '')
                            ad_id = href.split('/')[-2] if '/' in href else href

                            if not ad_id or ad_id in self.seen_ads:
                                continue

                            title_elem = listing.find('span', class_=re.compile(r'Link.*OfferTitle'))
                            price_elem = listing.find('span', class_=re.compile(r'Price'))

                            if title_elem and price_elem:
                                title = title_elem.text.strip()
                                price = price_elem.text.strip()
                                url_full = "https://auto.ru" + href if href.startswith('/') else href

                                info_elems = listing.find_all('span', class_=re.compile(r'ListItem'))
                                info = ' | '.join([elem.text.strip() for elem in info_elems[:3]])

                                full_info = self.get_auto_ru_details(url_full)

                                ad_data = {
                                    'id': ad_id,
                                    'title': title,
                                    'price': price,
                                    'info': info,
                                    'full_info': full_info,
                                    'url': url_full,
                                    'source': 'Auto.ru'
                                }

                                meets_criteria, reason = self.meets_criteria(ad_data)
                                if meets_criteria:
                                    ads.append(ad_data)

                        except Exception as e:
                            logger.error(f"Ошибка объявления Auto.ru: {e}")
                            continue

                    time.sleep(3)

                except Exception as e:
                    logger.error(f"Ошибка страницы Auto.ru {url}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Ошибка парсинга Auto.ru: {e}")

        return ads

    def get_auto_ru_details(self, url):
        """Получение детальной информации с Auto.ru"""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')  # Изменено: используем html.parser
            description_elem = soup.find('div', class_=re.compile(r'Description'))
            if description_elem:
                return description_elem.get_text()
            return ""
        except:
            return ""

    def parse_avito_general(self):
        """Парсинг Avito.ru - все объявления"""
        ads = []
        try:
            urls = [
                "https://www.avito.ru/moskva/avtomobili",
                "https://www.avito.ru/moskva/avtomobili?p=2"
            ]

            for url in urls:
                try:
                    response = requests.get(url, headers=self.headers, timeout=15)
                    response.raise_for_status()
                    soup = BeautifulSoup(response.content, 'html.parser')  # Изменено: используем html.parser

                    listings = soup.find_all('div', {'data-marker': 'item'})

                    for listing in listings:
                        try:
                            link_elem = listing.find('a', {'data-marker': 'item-title'})
                            if not link_elem:
                                continue

                            href = link_elem.get('href', '')
                            ad_id = href.split('_')[-1] if '_' in href else href.split('/')[-1]

                            if not ad_id or ad_id in self.seen_ads:
                                continue

                            title = link_elem.text.strip()
                            price_elem = listing.find('span', {'data-marker': 'item-price'})
                            price = price_elem.text.strip() if price_elem else "Цена не указана"

                            info_elem = listing.find('div', {'class': re.compile(r'.*description.*')})
                            info = info_elem.text.strip() if info_elem else ""

                            url_full = "https://www.avito.ru" + href if href.startswith('/') else href

                            full_info = self.get_avito_details(url_full)

                            ad_data = {
                                'id': ad_id,
                                'title': title,
                                'price': price,
                                'info': info,
                                'full_info': full_info,
                                'url': url_full,
                                'source': 'Avito.ru'
                            }

                            meets_criteria, reason = self.meets_criteria(ad_data)
                            if meets_criteria:
                                ads.append(ad_data)

                        except Exception as e:
                            logger.error(f"Ошибка объявления Avito: {e}")
                            continue

                    time.sleep(3)

                except Exception as e:
                    logger.error(f"Ошибка страницы Avito {url}: {e}")
                    continue

        except Exception as e:
            logger.error(f"Ошибка парсинга Avito: {e}")

        return ads

    def get_avito_details(self, url):
        """Получение детальной информации с Avito"""
        try:
            response = requests.get(url, headers=self.headers, timeout=10)
            soup = BeautifulSoup(response.content, 'html.parser')  # Изменено: используем html.parser
            desc_elem = soup.find('div', {'data-marker': 'item-view/item-description'})
            if desc_elem:
                return desc_elem.get_text()
            return ""
        except:
            return ""

    def format_message(self, ad):
        """Форматирование сообщения для Telegram"""
        price_num = self.extract_price(ad['price'])
        owners = self.extract_owners(ad.get('full_info', '') + ' ' + ad.get('info', ''))

        message = f"🚗 *{ad['source']}*\n"
        message += f"📌 *{ad['title']}*\n"
        message += f"💰 *{ad['price']}* ({price_num:,} руб.)\n"

        if owners:
            message += f"👥 *Владельцев: {owners}*\n"
        else:
            message += f"👥 *Информация о владельцах: не указана*\n"

        if ad['info']:
            message += f"📝 {ad['info'][:100]}...\n" if len(ad['info']) > 100 else f"📝 {ad['info']}\n"

        message += f"🔗 [Ссылка на объявление]({ad['url']})"

        return message

    def send_telegram_message(self, message):
        """Отправка сообщения в Telegram"""
        try:
            self.bot.send_message(
                chat_id=self.chat_id,
                text=message,
                parse_mode='Markdown',
                disable_web_page_preview=False
            )
            return True
        except TelegramError as e:
            logger.error(f"Ошибка отправки в Telegram: {e}")
            return False

    def check_new_ads(self):
        """Проверка новых объявлений"""
        logger.info("🔍 Начинаем проверку объявлений...")

        all_ads = []

        try:
            all_ads.extend(self.parse_drom_general())
            all_ads.extend(self.parse_auto_ru_general())
            all_ads.extend(self.parse_avito_general())

            new_ads_count = 0

            for ad in all_ads:
                if ad['id'] not in self.seen_ads:
                    message = self.format_message(ad)
                    if self.send_telegram_message(message):
                        self.seen_ads.add(ad['id'])
                        new_ads_count += 1
                        time.sleep(1)

            if new_ads_count > 0:
                logger.info(f"✅ Отправлено {new_ads_count} новых объявлений")
                summary = f"✅ Найдено {new_ads_count} подходящих объявлений!\nФильтры: цена {self.MIN_PRICE:,}-{self.MAX_PRICE:,} руб., до {self.MAX_OWNERS} владельцев"
                self.send_telegram_message(summary)
            else:
                logger.info("❌ Новых объявлений не найдено")

            self.save_seen_ads()

        except Exception as e:
            logger.error(f"❌ Ошибка при проверке объявлений: {e}")
            self.send_telegram_message(f"❌ Ошибка при проверке объявлений: {e}")

    def run(self):
        """Основной цикл бота"""
        logger.info("🤖 Бот запущен на Render")
        self.send_telegram_message("🤖 Бот запущен! Начинаю поиск автомобилей...")

        # Проверяем сразу при запуске
        self.check_new_ads()

        # Запускаем планировщик
        schedule.every(20).minutes.do(self.check_new_ads)

        logger.info("⏰ Планировщик запущен (каждые 20 минут)")

        while True:
            try:
                schedule.run_pending()
                time.sleep(60)  # Проверяем каждую минуту
            except Exception as e:
                logger.error(f"Ошибка в основном цикле: {e}")
                time.sleep(60)

def main():
    try:
        bot = CarParserBot()
        bot.run()
    except Exception as e:
        logger.error(f"Не удалось запустить бота: {e}")
        time.sleep(60)
        main()  # Перезапуск при ошибке

if __name__ == "__main__":
    # Для Render нужно слушать порт
    port = int(os.environ.get("PORT", 5000))

    # Запускаем бота в отдельном потоке
    bot_thread = threading.Thread(target=main)
    bot_thread.daemon = True
    bot_thread.start()

    # Простой HTTP сервер для Render
    from flask import Flask

    app = Flask(__name__)

    @app.route('/')
    def home():
        return "Car Parser Bot is running!"

    @app.route('/health')
    def health():
        return "OK"

    app.run(host='0.0.0.0', port=port)