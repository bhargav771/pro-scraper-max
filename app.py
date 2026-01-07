import io
import logging
import random
import re
import threading
import time
import urllib.parse
import os
import json
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from functools import wraps
import pandas as pd
import requests
import usaddress
from flask import Flask, jsonify, render_template, request, send_file
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException, TimeoutException, WebDriverException
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager
from webdriver_manager.core.os_manager import ChromeType
from restaurant_intelligence import RestaurantIntelligence
from nearby_competitor_scraper import NearbyCompetitorAnalyzer
from social_media_analyzer import SocialMediaAnalyzer
from hunter_io import find_owner_email_hunter

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
app = Flask(__name__)

# Initialize components
rest_intel = RestaurantIntelligence()
nearby_analyzer = NearbyCompetitorAnalyzer()
social_analyzer = SocialMediaAnalyzer()

SESSIONS = {}
SESSIONS_LOCK = threading.Lock()
MAX_CONCURRENT_SESSIONS = 20

# Hunter.io API Key
HUNTER_API_KEY = '01f4350e419bf9a8ff77daa3a25bc3f0c295685a'

def get_session(session_id):
    with SESSIONS_LOCK:
        if session_id not in SESSIONS:
            SESSIONS[session_id] = {
                "scraping_active": False, "stop_scraping_flag": False, "status_message": "Ready!",
                "link_collection_progress": 0.0, "detail_scraping_progress": 0.0,
                "link_count": 0, "scraped_count": 0, "total_to_scrape": 0,
                "results_df": pd.DataFrame(), "collected_links": [],
                "lock": threading.Lock()
            }
        return SESSIONS[session_id]
EMAIL_REGEX = re.compile(r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}")
PHONE_REGEX = re.compile(r'(\+?\d[\d\s\-\(\)]{8,})')
USER_AGENTS = ["Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36"]
DATA_DIR = "scraper_data"
if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

def update_status(session_id, message, link_progress=None, detail_progress=None, link_count=None, scraped_count=None, total_to_scrape=None):
    session = get_session(session_id)
    with session["lock"]:
        session["status_message"] = message
        if link_progress is not None: session["link_collection_progress"] = link_progress
        if detail_progress is not None: session["detail_scraping_progress"] = detail_progress
        if link_count is not None: session["link_count"] = link_count
        if scraped_count is not None: session["scraped_count"] = scraped_count
        if total_to_scrape is not None: session["total_to_scrape"] = total_to_scrape

def build_chrome(headless_mode=False, proxy=None):
    opts = Options()
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--disable-software-rasterizer")
    opts.add_argument("--disable-images")
    opts.add_argument("--disable-plugins")
    opts.add_argument("--disable-extensions")
    opts.add_argument("--page-load-strategy=none")
    opts.add_argument("--aggressive-cache-discard")
    opts.add_argument("--blink-settings=imagesEnabled=false")
    opts.add_argument("--disable-blink-features=AutomationControlled")
    opts.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
    if headless_mode: opts.add_argument("--headless=new")
    if proxy: opts.add_argument(f"--proxy-server={proxy}")
    
    chrome_paths = [
        "/usr/bin/google-chrome-stable",
        "/usr/bin/google-chrome",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser"
    ]
    
    for path in chrome_paths:
        if os.path.exists(path):
            opts.binary_location = path
            break
    
    try:
        driver_path = ChromeDriverManager().install()
    except:
        driver_path = "/usr/bin/chromedriver"
    
    return webdriver.Chrome(service=Service(driver_path), options=opts)

def find_emails(html):
    if not html: return []
    original = html
    html = html.lower()
    html = html.replace('[at]','@').replace('(at)','@').replace('[dot]','.').replace('(dot)','.')
    html = html.replace(' at ','@').replace(' dot ','.').replace('_at_','@').replace('_dot_','.')
    html = html.replace('&#64;','@').replace('&#46;','.').replace('&commat;','@')
    html = html.replace('%40','@').replace('%2e','.').replace('\\u0040','@')
    html = html.replace(' @ ','@').replace(' . ','.').replace('[a]','@').replace('[d]','.')
    html = html.replace('&lt;','<').replace('&gt;','>').replace('&quot;','"')
    
    emails = EMAIL_REGEX.findall(html)
    
    invalid_extensions = ['.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.css', '.js', '.ico']
    invalid_domains = ['sentry.io', 'example.com', 'test.com', 'localhost', 'w3.org', 'schema.org', 'ingest.sentry.io', 'ingest.us.sentry.io']
    
    valid_emails = []
    for e in emails:
        e = e.strip().lower()
        
        # CRITICAL: Skip URLs completely
        if 'http' in e or 'https' in e or 'www.' in e:
            continue
        if '.com/' in e or '/maps' in e or '/place' in e:
            continue
        if 'maps.google' in e or 'goo.gl' in e or 'g.page' in e or 'google.com' in e:
            continue
        # Skip sentry and tracking domains
        if 'sentry' in e or 'ingest' in e or 'o244637' in e:
            continue
            
        if e.count('@') != 1: continue
        if any(ext in e for ext in invalid_extensions): continue
        
        try:
            local, domain = e.split('@')
            if len(local) < 2 or len(domain) < 4: continue
            if any(d in domain for d in invalid_domains): continue
            if '.' not in domain: continue
            
            # Domain must not have slashes or special chars
            if '/' in domain or '?' in domain or '&' in domain or '=' in domain:
                continue
            
            # Local part should not have weird patterns
            if len(local) > 50 or any(c in local for c in ['/', '?', '&', '=']):
                continue
                
            valid_emails.append(e)
        except:
            continue
    
    return sorted(set(valid_emails))
def find_phone_numbers(html): return list(set([m.strip() for m in PHONE_REGEX.findall(re.sub(r'<[^>]+>', ' ', html)) if len(re.sub(r'[^\d]','',m))>=10]))

def collect_gmaps_links(session_id, config):
    session = get_session(session_id)
    driver = build_chrome(config.get("headless_mode", True), config.get("proxy"))
    queries = [f"{config.get('general_search_term','')} {cat} {zipc}".strip() for cat in config.get('categories',[]) for zipc in config.get('zipcodes',[])]
    for i, query in enumerate(queries):
        if session["stop_scraping_flag"]: break
        driver.get(f"https://www.google.com/maps/search/{urllib.parse.quote(query)}")
        try:
            feed = WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.XPATH, '//div[@role="feed"]')))
            for _ in range(config.get("max_scrolls", 10)):
                driver.execute_script("arguments[0].scrollBy(0, 3000);", feed)
                time.sleep(0.3)
                cards = driver.find_elements(By.CSS_SELECTOR, 'a.hfpxzc')
                for c in cards[-30:]:
                    href = c.get_attribute("href")
                    if href and "/maps/place/" in href:
                        with session["lock"]:
                            if (href, query, "") not in session["collected_links"]:
                                session["collected_links"].append((href, query, ""))
                with session["lock"]: link_count = len(session["collected_links"])
                update_status(session_id, f"Query {i+1}/{len(queries)}: Found {link_count} links", link_count=link_count, link_progress=(i+1)/len(queries))
        except: pass
    driver.quit()

def extract_social_links(html):
    socials = {"Facebook": "", "Instagram": "", "Twitter": "", "LinkedIn": ""}
    patterns = {"Facebook": r'https?://(?:www\.)?facebook\.com/[^\s"\'\'<>]+', "Instagram": r'https?://(?:www\.)?instagram\.com/[^\s"\'\'<>]+', "Twitter": r'https?://(?:www\.)?(?:twitter|x)\.com/[^\s"\'\'<>]+', "LinkedIn": r'https?://(?:[a-z]{2,3}\.)?linkedin\.com/[^\s"\'\'<>]+'}
    for k, p in patterns.items():
        m = re.findall(p, html, re.IGNORECASE)
        if m: socials[k] = m[0].split('?')[0].rstrip('/')
    return socials

def scrape_website_selenium(url, headless_mode, proxy=None):
    driver = None
    try:
        driver = build_chrome(headless_mode, proxy)
        driver.set_page_load_timeout(8)
        driver.get(url)
        time.sleep(1)
        
        emails = set()
        
        # Deep scroll
        for _ in range(4):
            driver.execute_script("window.scrollBy(0, 1200);")
            time.sleep(0.15)
        
        # Extract from page source
        emails.update(find_emails(driver.page_source))
        
        # Extract mailto links
        try:
            mailto_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'mailto')]")
            for link in mailto_links[:20]:
                href = link.get_attribute('href') or ''
                emails.update(find_emails(href))
        except: pass
        
        # Extract from elements with @
        try:
            email_elements = driver.find_elements(By.XPATH, "//*[contains(text(), '@')]")
            for elem in email_elements[:25]:
                emails.update(find_emails(elem.text))
        except: pass
        
        socials = extract_social_links(driver.page_source)
        
        # Check 3 contact pages
        try:
            all_links = driver.find_elements(By.TAG_NAME, 'a')
            visited = 0
            for link in all_links[:25]:
                if visited >= 3: break
                try:
                    href = link.get_attribute('href')
                    text = link.text.lower()
                    
                    if href and url in href and ('contact' in href.lower() or 'contact' in text or 'about' in href.lower() or 'team' in href.lower()):
                        driver.get(href)
                        time.sleep(0.6)
                        visited += 1
                        
                        for _ in range(3):
                            driver.execute_script("window.scrollBy(0, 1200);")
                            time.sleep(0.15)
                        
                        emails.update(find_emails(driver.page_source))
                        
                        if len(emails) >= 5:
                            break
                        
                        driver.back()
                        time.sleep(0.4)
                except:
                    continue
        except: pass
        
        return list(emails), socials
    except Exception as e:
        logging.error(f"Selenium error for {url}: {e}")
        return [], {"Facebook": "", "Instagram": "", "Twitter": "", "LinkedIn": ""}
    finally:
        if driver: driver.quit()

def scrape_website_data(url, headless_mode, proxy=None):
    all_emails = set()
    socials = {"Facebook": "", "Instagram": "", "Twitter": "", "LinkedIn": ""}
    
    # Use requests first
    try:
        session = requests.Session()
        r = session.get(url, headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=5, allow_redirects=True)
        if r.status_code == 200:
            all_emails.update(find_emails(r.text))
            socials = extract_social_links(r.text)
    except:
        pass
    
    # Use Selenium for deep scraping
    try:
        selenium_emails, selenium_socials = scrape_website_selenium(url, headless_mode, proxy)
        all_emails.update(selenium_emails)
        for k, v in selenium_socials.items():
            if v and not socials.get(k): socials[k] = v
    except:
        pass
    
    return list(all_emails), socials

def extract_facebook_phone(driver):
    phones = set()
    selectors = [
        "//a[contains(@href, 'tel:')]",
        "//div[contains(text(), 'Phone')]/following-sibling::*",
        "//span[contains(text(), 'Phone')]/following-sibling::*",
        "//div[@role='button'][contains(., '+')]",
        "//span[contains(., '+1')]",
        "//span[contains(., '(')][contains(., ')')]",
    ]
    
    for sel in selectors:
        try:
            elems = driver.find_elements(By.XPATH, sel)
            for elem in elems:
                text = elem.text or elem.get_attribute('href') or ''
                if 'tel:' in text: text = text.replace('tel:', '')
                found = PHONE_REGEX.findall(text)
                phones.update(found)
        except: pass
    
    return list(phones)

def scrape_instagram_page(insta_url, headless_mode, proxy=None):
    if not insta_url: return []
    driver = None
    try:
        driver = build_chrome(headless_mode, proxy)
        driver.set_page_load_timeout(8)
        driver.get(insta_url)
        time.sleep(1.5)
        
        emails = set()
        for _ in range(2):
            driver.execute_script("window.scrollBy(0, 800);")
            time.sleep(0.3)
        
        emails.update(find_emails(driver.page_source))
        return list(emails)
    except:
        return []
    finally:
        if driver: driver.quit()

def scrape_linkedin_page(li_url, headless_mode, proxy=None):
    if not li_url: return []
    driver = None
    try:
        driver = build_chrome(headless_mode, proxy)
        driver.set_page_load_timeout(8)
        driver.get(li_url)
        time.sleep(1.5)
        
        emails = set()
        for _ in range(2):
            driver.execute_script("window.scrollBy(0, 800);")
            time.sleep(0.3)
        
        emails.update(find_emails(driver.page_source))
        return list(emails)
    except:
        return []
    finally:
        if driver: driver.quit()

def scrape_facebook_page(fb_url, headless_mode, proxy=None):
    if not fb_url: return [], []
    driver = None
    try:
        opts = Options()
        opts.add_argument("--no-sandbox")
        opts.add_argument("--disable-dev-shm-usage")
        opts.add_argument("--disable-gpu")
        opts.add_argument(f"--user-agent={random.choice(USER_AGENTS)}")
        if headless_mode: opts.add_argument("--headless=new")
        
        chrome_paths = ["/usr/bin/google-chrome-stable", "/usr/bin/google-chrome", "/usr/bin/chromium", "/usr/bin/chromium-browser"]
        for path in chrome_paths:
            if os.path.exists(path):
                opts.binary_location = path
                break
        
        try:
            driver_path = ChromeDriverManager().install()
        except:
            driver_path = "/usr/bin/chromedriver"
        driver = webdriver.Chrome(service=Service(driver_path), options=opts)
        driver.set_page_load_timeout(12)
        
        all_emails = set()
        all_phones = set()
        
        pages = [
            fb_url.rstrip('/') + '/about',
            fb_url.rstrip('/') + '/about_contact_and_basic_info',
            fb_url.rstrip('/'),
        ]
        
        for page_url in pages:
            try:
                driver.get(page_url)
                time.sleep(2)
                
                # Scroll
                for i in range(4):
                    driver.execute_script("window.scrollBy(0, 1000);")
                    time.sleep(0.3)
                
                html = driver.page_source
                all_emails.update(find_emails(html))
                all_phones.update(extract_facebook_phone(driver))
                all_phones.update(find_phone_numbers(html))
                
                # Click expandable elements
                try:
                    clickable = driver.find_elements(By.XPATH, "//div[@role='button'] | //span[contains(text(), 'See')] | //span[contains(text(), 'More')]")
                    for elem in clickable[:15]:
                        try:
                            driver.execute_script("arguments[0].click();", elem)
                            time.sleep(0.4)
                        except: pass
                    
                    for i in range(3):
                        driver.execute_script("window.scrollBy(0, 1200);")
                        time.sleep(0.25)
                    
                    all_emails.update(find_emails(driver.page_source))
                    all_phones.update(extract_facebook_phone(driver))
                except: pass
                
                # Extract from elements with @
                try:
                    fb_elements = driver.find_elements(By.XPATH, "//*[contains(text(), '@')]")
                    for elem in fb_elements[:25]:
                        all_emails.update(find_emails(elem.text))
                except: pass
                
                # Final extraction
                html = driver.page_source
                all_emails.update(find_emails(html))
                all_phones.update(extract_facebook_phone(driver))
                all_phones.update(find_phone_numbers(html))
                
            except: continue
        
        return list(all_emails), list(all_phones)
    except:
        return [], []
    finally:
        if driver: driver.quit()

def get_best_email(emails):
    if not emails: return ""
    priority_domains = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com']
    business_emails = [e for e in emails if not any(d in e for d in priority_domains)]
    return business_emails[0] if business_emails else emails[0]

def get_domain_matched_email(emails, website_url):
    if not emails or not website_url: return ""
    try:
        domain = urllib.parse.urlparse(website_url).netloc.replace('www.', '').split(':')[0]
        for email in emails:
            if domain in email:
                return email
    except: pass
    return get_best_email(emails)

def extract_owner_from_website(website_html):
    """Extract owner/founder name and email from website HTML - ENHANCED"""
    owner_name = ""
    owner_email = ""
    if not website_html: return owner_name, owner_email
    try:
        # Enhanced patterns for owner name extraction
        owner_patterns = [
            r'(?:founded|owned|started|created|established)\s+by[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})',
            r'(?:owner|founder|co-founder|ceo|president)[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})',
            r'(?:meet|about)\s+(?:the\s+)?(?:owner|founder)[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})',
            r'<h[1-6][^>]*>([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})</h[1-6]>\s*(?:<[^>]+>)*\s*(?:owner|founder|ceo)',
            r'(?:my name is|i am|i\'m)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s*(?:,|\.|and)\s*(?:i|the)\s*(?:own|founded|started)',
            r'([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,2})\s*[-–—]\s*(?:owner|founder|ceo|president)',
            r'(?:proprietor|restaurateur)[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){1,3})',
        ]
        for pattern in owner_patterns:
            matches = re.findall(pattern, website_html, re.IGNORECASE)
            if matches:
                name = matches[0].strip() if isinstance(matches[0], str) else matches[0]
                # Validate name (not common words)
                if name and len(name) > 3 and name.lower() not in ['about us', 'contact us', 'our team', 'the team']:
                    owner_name = name
                    break
        
        # Enhanced email patterns
        html_lower = website_html.lower()
        owner_email_patterns = [
            r'(owner@[a-z0-9.-]+\.[a-z]{2,})',
            r'(founder@[a-z0-9.-]+\.[a-z]{2,})',
            r'(ceo@[a-z0-9.-]+\.[a-z]{2,})',
            r'(president@[a-z0-9.-]+\.[a-z]{2,})',
            r'(info@[a-z0-9.-]+\.[a-z]{2,})',
            r'(contact@[a-z0-9.-]+\.[a-z]{2,})',
        ]
        for pattern in owner_email_patterns:
            matches = re.findall(pattern, html_lower)
            if matches:
                owner_email = matches[0]
                break
    except: pass
    return owner_name, owner_email

def scrape_business_entry(gmaps_url, search_query_used, zipcode, timeout, headless_mode, proxy=None):
    driver = build_chrome(headless_mode, proxy)
    
    # Initialize defaults
    contact_quality = 0
    business_size = "Unknown"
    rating_val = 0
    reviews_val = 0
    
    try:
        driver.get(gmaps_url)
        WebDriverWait(driver, timeout).until(EC.presence_of_element_located((By.CSS_SELECTOR, 'h1.DUwDvf, h1.lfPIob')))
        time.sleep(1)
        
        html = driver.page_source
        closure_status = "Open"
        if re.search(r'\bPermanently closed\b', html, re.IGNORECASE):
            closure_status = "Permanently Closed"
        elif re.search(r'\bTemporar(?:il)?y closed\b', html, re.IGNORECASE):
            closure_status = "Temporarily Closed"
        
        place_id = re.search(r'(ChIJ[a-zA-Z0-9_-]+)', gmaps_url).group(0) if re.search(r'(ChIJ[a-zA-Z0-9_-]+)', gmaps_url) else ""
        name = driver.find_element(By.CSS_SELECTOR, 'h1.DUwDvf, h1.lfPIob').text.strip()
        
        # 📍 ADDRESS - MAXIMUM ACCURACY EXTRACTION
        address = ""
        address_selectors = [
            'button[data-item-id="address"]',
            'button[data-tooltip="Copy address"]',
            '[data-item-id="address"]',
            'div[aria-label*="Address"]',
            'button.CsEnBe[aria-label*="Address"]',
            'button[aria-label*="address"]'
        ]
        
        for sel in address_selectors:
            try:
                elems = driver.find_elements(By.CSS_SELECTOR, sel)
                if elems:
                    address = elems[0].text.strip() or elems[0].get_attribute('aria-label') or ''
                    if address and len(address) > 10:  # Valid address should be longer
                        break
            except: pass
        
        # Try XPath if still no address
        if not address:
            try:
                addr_xpath = driver.find_elements(By.XPATH, 
                    "//button[contains(@aria-label, 'Address')] | "
                    "//button[contains(@data-item-id, 'address')] | "
                    "//div[contains(@aria-label, 'Address')]"
                )
                if addr_xpath:
                    address = addr_xpath[0].text.strip() or addr_xpath[0].get_attribute('aria-label') or ''
            except: pass
        
        # Extract from structured data
        if not address:
            try:
                addr_match = re.search(r'"address"\s*:\s*"([^"]+)"', html)
                if addr_match:
                    address = addr_match.group(1).strip()
            except: pass
        
        # 📞 PHONE NUMBER - ULTRA DEEP EXTRACTION
        phone = ""
        phone_selectors = [
            'button[data-item-id^="phone"]',
            'button[data-tooltip="Copy phone number"]',
            'button[aria-label*="Phone"]',
            'button[aria-label*="phone"]',
            'a[href^="tel:"]',
            '[data-item-id="phone:tel"]',
            'div[aria-label*="Phone"]'
        ]
        
        for sel in phone_selectors:
            try:
                elems = driver.find_elements(By.CSS_SELECTOR, sel)
                if elems:
                    phone = elems[0].text.strip() or elems[0].get_attribute('aria-label') or ''
                    if not phone:
                        href = elems[0].get_attribute('href')
                        if href and 'tel:' in href:
                            phone = href.replace('tel:', '').strip()
                    if phone:
                        break
            except: pass
        
        # Try XPath if still no phone
        if not phone:
            try:
                phone_xpath = driver.find_elements(By.XPATH, 
                    "//button[contains(@aria-label, 'Phone')] | "
                    "//button[contains(@data-item-id, 'phone')] | "
                    "//a[contains(@href, 'tel:')]"
                )
                if phone_xpath:
                    phone = phone_xpath[0].text.strip() or phone_xpath[0].get_attribute('aria-label') or ''
                    if not phone:
                        href = phone_xpath[0].get_attribute('href')
                        if href and 'tel:' in href:
                            phone = href.replace('tel:', '').strip()
            except: pass
        
        # Extract from page source as last resort
        if not phone:
            try:
                phone_matches = re.findall(r'\+?1?[\s.-]?\(?\d{3}\)?[\s.-]?\d{3}[\s.-]?\d{4}', html)
                if phone_matches:
                    phone = phone_matches[0].strip()
            except: pass
        # 🌐 WEBSITE - DEEP EXTRACTION
        website = ""
        website_selectors = [
            'a[data-item-id="authority"]',
            'a[aria-label*="Website"]',
            'a[aria-label*="website"]',
            'a[data-tooltip*="website"]',
            'a.CsEnBe[aria-label*="Website"]'
        ]
        
        for sel in website_selectors:
            try:
                elems = driver.find_elements(By.CSS_SELECTOR, sel)
                if elems:
                    website = elems[0].get_attribute("href") or ''
                    if website and 'http' in website:
                        break
            except: pass
        
        # Try XPath
        if not website:
            try:
                web_xpath = driver.find_elements(By.XPATH, 
                    "//a[contains(@aria-label, 'Website')] | "
                    "//a[contains(@data-item-id, 'authority')]"
                )
                if web_xpath:
                    website = web_xpath[0].get_attribute("href") or ''
            except: pass
        # 🏪 CATEGORY - ACCURATE EXTRACTION
        category = ""
        category_selectors = [
            'button[jsaction*="category"]',
            'button[aria-label*="Categories"]',
            'button.DkEaL',
            'span.YhemCb'
        ]
        
        for sel in category_selectors:
            try:
                elems = driver.find_elements(By.CSS_SELECTOR, sel)
                if elems:
                    category = elems[0].text.strip()
                    if category:
                        break
            except: pass
        
        # Try XPath
        if not category:
            try:
                cat_xpath = driver.find_elements(By.XPATH, 
                    "//button[contains(@jsaction, 'category')] | "
                    "//button[contains(@aria-label, 'Categories')]"
                )
                if cat_xpath:
                    category = cat_xpath[0].text.strip()
            except: pass
        # 💰 PRICE RANGE - ACCURATE EXTRACTION
        price = ""
        try:
            # Method 1: From aria-label
            price_elem = driver.find_elements(By.CSS_SELECTOR, '[aria-label^="Price:"]')
            if price_elem:
                price = price_elem[0].get_attribute('aria-label').replace('Price:', '').strip()
        except: pass
        
        # Method 2: From price symbols ($ $$ $$$ $$$$)
        if not price:
            try:
                price_symbols = driver.find_elements(By.XPATH, "//span[contains(text(), '$')]")
                for elem in price_symbols:
                    txt = elem.text.strip()
                    if re.match(r'^\$+$', txt):  # Only $ symbols
                        price = txt
                        break
            except: pass
        
        # Method 3: From page source
        if not price:
            try:
                price_match = re.search(r'"priceRange"\s*:\s*"([\$]+)"', html)
                if price_match:
                    price = price_match.group(1)
            except: pass
        
        # ⭐ RATING & REVIEWS - ULTRA ACCURATE EXTRACTION (SPAN ARIA-LABEL)
        rating, reviews = "", ""
        
        # Wait for page to fully load
        time.sleep(3)
        
        # Scroll to top to ensure rating/review section is visible
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(0.5)
        
        # Method 1: Direct span with aria-label (MOST ACCURATE)
        try:
            # Target span elements with aria-label containing rating/reviews
            span_elements = driver.find_elements(By.XPATH, 
                "//span[@aria-label] | "
                "//span[@role='img'] | "
                "//div[@role='img'][@aria-label]"
            )
            for span in span_elements:
                aria = span.get_attribute('aria-label') or ''
                if not aria:
                    continue
                    
                # Pattern: "4.5 stars" or "1,234 reviews"
                if not rating:
                    if m := re.search(r'(\d+\.\d+)\s*(?:stars?|out of)', aria, re.IGNORECASE):
                        rating = m.group(1)
                    elif m := re.search(r'(\d+)\s*(?:stars?|out of)', aria, re.IGNORECASE):
                        rating = m.group(1) + ".0"
                
                if not reviews:
                    if m := re.search(r'([\d,]+)\s*reviews?', aria, re.IGNORECASE):
                        reviews = m.group(1).replace(',', '')
        except: pass
        
        # Method 2: Extract reviews from button text (CRITICAL FOR REVIEWS)
        if not reviews:
            try:
                # Find buttons/elements with review count in text
                review_elements = driver.find_elements(By.XPATH, 
                    "//button[contains(text(), 'review')] | "
                    "//button[contains(text(), 'Review')] | "
                    "//span[contains(text(), 'review')] | "
                    "//div[contains(text(), 'review')]"
                )
                for elem in review_elements:
                    text = elem.text.strip()
                    # Pattern: "1,234 reviews" or "(1,234)"
                    if m := re.search(r'([\d,]+)\s*reviews?', text, re.IGNORECASE):
                        reviews = m.group(1).replace(',', '')
                        break
                    elif m := re.search(r'\(([\d,]+)\)', text):
                        reviews = m.group(1).replace(',', '')
                        break
            except: pass
        
        # Method 3: From button aria-label
        if not rating or not reviews:
            try:
                review_buttons = driver.find_elements(By.XPATH, 
                    "//button[@aria-label] | "
                    "//a[@aria-label]"
                )
                for btn in review_buttons:
                    aria = btn.get_attribute('aria-label') or ''
                    if not rating:
                        if m := re.search(r'(\d+\.\d+)\s*(?:stars?|out)', aria, re.IGNORECASE):
                            rating = m.group(1)
                    if not reviews:
                        if m := re.search(r'([\d,]+)\s*reviews?', aria, re.IGNORECASE):
                            reviews = m.group(1).replace(',', '')
            except: pass
        
        # Method 4: Page source regex (JSON data)
        if not rating or not reviews:
            try:
                if not rating:
                    if m := re.search(r'"ratingValue"\s*:\s*"?(\d+\.\d+)"?', html):
                        rating = m.group(1)
                
                if not reviews:
                    if m := re.search(r'"reviewCount"\s*:\s*"?(\d+)"?', html):
                        reviews = m.group(1)
                    elif m := re.search(r'"userRatingCount"\s*:\s*"?(\d+)"?', html):
                        reviews = m.group(1)
            except: pass
        
        # Convert to numeric values
        rating_val = float(rating.replace(',', '.')) if rating else 0
        reviews_val = int(reviews) if reviews else 0
        
        # 📅 NEWEST REVIEW DATE - EXTRACT FROM REVIEWS SECTION
        newest_review_date = "Unknown"
        try:
            # Scroll to reviews area
            for _ in range(3):
                driver.execute_script("window.scrollBy(0, 500);")
                time.sleep(0.05)
            
            # Try to find review dates with "ago" pattern
            date_elements = driver.find_elements(By.XPATH, 
                "//span[contains(text(), 'ago')] | "
                "//span[contains(text(), 'day')] | "
                "//span[contains(text(), 'week')] | "
                "//span[contains(text(), 'month')]"
            )
            
            if date_elements:
                # Get first date (usually newest)
                first_date = date_elements[0].text.strip()
                if first_date and any(x in first_date.lower() for x in ['ago', 'day', 'week', 'month', 'year']):
                    newest_review_date = first_date
            
            # Fallback: Check page source for "ago" pattern
            if newest_review_date == "Unknown":
                ago_match = re.search(r'(\d+\s+(?:day|week|month|year)s?\s+ago|a\s+(?:day|week|month|year)\s+ago)', html, re.IGNORECASE)
                if ago_match:
                    newest_review_date = ago_match.group(1)
        except:
            pass
        
        city, state = "", ""
        if address:
            try:
                tagged, _ = usaddress.tag(address)
                city = tagged.get('PlaceName', '')
                state = tagged.get('StateName', '')
            except:
                parts = address.split(', ')
                if len(parts) >= 3: city = parts[-3]; state = parts[-2].split(' ')[0] if len(parts[-2].split(' ')) > 1 else ''
        
        # 🔥 GOOGLE MAPS EMAIL EXTRACTION - MAXIMUM DEPTH
        maps_emails = set()
        
        # STEP 1: Deep scroll (20 times)
        for _ in range(20):
            driver.execute_script("window.scrollBy(0, 800);")
            time.sleep(0.08)
        
        # STEP 2: Extract from page source
        html = driver.page_source
        maps_emails.update(find_emails(html))
        
        # STEP 3: Click ALL expandable sections
        try:
            expandable = driver.find_elements(By.XPATH, 
                "//button[contains(@aria-label, 'More')] | "
                "//button[contains(., 'More')] | "
                "//div[@role='button'][contains(., 'See')] | "
                "//button[contains(@aria-label, 'Show')] | "
                "//button[contains(@jsaction, 'expand')]"
            )
            for btn in expandable[:25]:
                try:
                    driver.execute_script("arguments[0].scrollIntoView(true);", btn)
                    time.sleep(0.05)
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(0.15)
                except: pass
            maps_emails.update(find_emails(driver.page_source))
        except: pass
        
        # STEP 4: Check ALL tabs (About, Reviews, etc)
        try:
            tabs = driver.find_elements(By.XPATH, "//button[@role='tab'] | //div[@role='tab']")
            for tab in tabs[:5]:
                try:
                    driver.execute_script("arguments[0].click();", tab)
                    time.sleep(0.3)
                    for _ in range(5):
                        driver.execute_script("window.scrollBy(0, 600);")
                        time.sleep(0.05)
                    maps_emails.update(find_emails(driver.page_source))
                except: pass
        except: pass
        
        # STEP 5: Extract mailto links
        try:
            mailto_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'mailto')]")
            for link in mailto_links:
                href = link.get_attribute('href') or ''
                maps_emails.update(find_emails(href.replace('mailto:', '')))
        except: pass
        
        # STEP 6: Extract from ALL text elements with @
        try:
            email_elements = driver.find_elements(By.XPATH, "//*[contains(text(), '@')]")
            for elem in email_elements[:50]:
                try:
                    maps_emails.update(find_emails(elem.text))
                    maps_emails.update(find_emails(elem.get_attribute('innerHTML') or ''))
                except: pass
        except: pass
        
        # STEP 7: Check data attributes
        try:
            all_elements = driver.find_elements(By.XPATH, "//*[@data-email or @data-contact or contains(@data-tooltip, '@')]")
            for elem in all_elements[:30]:
                try:
                    for attr in ['data-email', 'data-contact', 'data-tooltip', 'title', 'aria-label']:
                        val = elem.get_attribute(attr)
                        if val:
                            maps_emails.update(find_emails(val))
                except: pass
        except: pass
        
        # STEP 8: Final deep scroll and extract
        for _ in range(10):
            driver.execute_script("window.scrollBy(0, 1000);")
            time.sleep(0.05)
        maps_emails.update(find_emails(driver.page_source))
        
        maps_email = get_best_email(maps_emails)
        
        website_emails, socials = [], {"Facebook": "", "Instagram": "", "Twitter": "", "LinkedIn": ""}
        if website:
            try:
                website_emails, socials = scrape_website_data(website, headless_mode, proxy)
            except:
                website_emails, socials = [], {"Facebook": "", "Instagram": "", "Twitter": "", "LinkedIn": ""}
        
        all_website_emails = ", ".join(website_emails)
        website_email = get_domain_matched_email(website_emails, website)
        
        fb_emails, fb_phones = [], []
        fb_social_data = {'has_page': False, 'followers': 0, 'posting_frequency': 'Unknown', 
                         'last_post_days_ago': 999, 'posts_last_7_days': 0, 'posts_last_30_days': 0}
        if socials["Facebook"]:
            try:
                fb_emails, fb_phones = scrape_facebook_page(socials["Facebook"], headless_mode, proxy)
            except:
                pass
            # Analyze Facebook presence with NEW driver
            try:
                fb_driver = build_chrome(headless_mode, proxy)
                fb_social_data = social_analyzer.analyze_facebook_presence(fb_driver, socials["Facebook"])
                fb_driver.quit()
            except Exception as e:
                logging.warning(f"FB analysis failed: {str(e)[:50]}")
                try:
                    fb_driver.quit()
                except:
                    pass
        fb_email = get_best_email(fb_emails)
        
        insta_emails = []
        insta_email = ""
        insta_social_data = {'has_profile': False, 'followers': 0, 'posting_frequency': 'Unknown',
                            'last_post_days_ago': 999, 'posts_last_7_days': 0, 'posts_last_30_days': 0}
        if socials["Instagram"]:
            try:
                ig_driver = build_chrome(headless_mode, proxy)
                insta_social_data = social_analyzer.analyze_instagram_presence(ig_driver, socials["Instagram"])
                ig_driver.quit()
            except Exception as e:
                logging.warning(f"IG analysis failed: {str(e)[:50]}")
                try:
                    ig_driver.quit()
                except:
                    pass
        
        li_emails = []
        li_email = ""
        
        # Calculate overall social media score
        try:
            social_media_score = social_analyzer.calculate_social_score(fb_social_data, insta_social_data)
        except:
            social_media_score = {'total_score': 0, 'percentage': 0, 'assessment': 'Unknown', 'total_followers': 0}
        
        if website_email:
            final_email = website_email
        elif fb_email:
            final_email = fb_email
        elif li_email:
            final_email = li_email
        elif insta_email:
            final_email = insta_email
        elif maps_email:
            final_email = maps_email
        else:
            final_email = ""
        
        all_emails_found = list(set(website_emails + fb_emails + insta_emails + li_emails + list(maps_emails)))
        all_emails_str = ", ".join(all_emails_found)
        
        # 👤 FIND OWNER NAME & EMAIL USING HUNTER.IO
        owner_name = ""
        owner_email = ""
        owner_position = ""
        owner_confidence = 0
        owner_name_2 = ""
        owner_email_2 = ""
        owner_position_2 = ""
        owner_confidence_2 = 0
        owner_name_3 = ""
        owner_email_3 = ""
        owner_position_3 = ""
        owner_confidence_3 = 0
        owner_name_4 = ""
        owner_email_4 = ""
        owner_position_4 = ""
        owner_confidence_4 = 0
        
        if website and HUNTER_API_KEY:
            try:
                hunter_data = find_owner_email_hunter(website, HUNTER_API_KEY)
                owner_name = hunter_data.get('owner_name', '')
                owner_email = hunter_data.get('owner_email', '')
                owner_position = hunter_data.get('owner_position', '')
                owner_confidence = hunter_data.get('confidence', 0)
                owner_name_2 = hunter_data.get('owner_name_2', '')
                owner_email_2 = hunter_data.get('owner_email_2', '')
                owner_position_2 = hunter_data.get('owner_position_2', '')
                owner_confidence_2 = hunter_data.get('confidence_2', 0)
                owner_name_3 = hunter_data.get('owner_name_3', '')
                owner_email_3 = hunter_data.get('owner_email_3', '')
                owner_position_3 = hunter_data.get('owner_position_3', '')
                owner_confidence_3 = hunter_data.get('confidence_3', 0)
                owner_name_4 = hunter_data.get('owner_name_4', '')
                owner_email_4 = hunter_data.get('owner_email_4', '')
                owner_position_4 = hunter_data.get('owner_position_4', '')
                owner_confidence_4 = hunter_data.get('confidence_4', 0)
            except Exception as e:
                logging.warning(f"Hunter.io lookup failed: {str(e)[:50]}")
        
        all_phones = set([phone] + fb_phones) if phone else set(fb_phones)
        
        # 🕒 OPENING HOURS - DEEP EXTRACTION
        hours = ""
        try:
            # Method 1: From hours table
            hours_elem = driver.find_elements(By.CSS_SELECTOR, 'table.eK4R0e')
            if hours_elem: 
                hours = hours_elem[0].text.replace('\n', '; ')
        except: pass
        
        # Method 2: From aria-label
        if not hours:
            try:
                hours_button = driver.find_elements(By.XPATH, "//button[contains(@aria-label, 'Hours')] | //div[contains(@aria-label, 'Hours')]")
                if hours_button:
                    hours = hours_button[0].get_attribute('aria-label') or hours_button[0].text
            except: pass
        
        # Method 3: Click hours button and extract
        if not hours:
            try:
                hours_buttons = driver.find_elements(By.XPATH, "//button[contains(., 'Hours')] | //button[contains(., 'hours')]")
                if hours_buttons:
                    driver.execute_script("arguments[0].click();", hours_buttons[0])
                    time.sleep(0.3)
                    hours_table = driver.find_elements(By.CSS_SELECTOR, 'table.eK4R0e')
                    if hours_table:
                        hours = hours_table[0].text.replace('\n', '; ')
            except: pass
        
        # 📸 PHOTOS COUNT - ACCURATE EXTRACTION FOR STOREFRONT ANALYSIS
        photos_count = 0
        try:
            # Method 1: From photo button aria-label
            photo_buttons = driver.find_elements(By.CSS_SELECTOR, 'button[aria-label*="photo"], button[aria-label*="Photo"]')
            if photo_buttons:
                photo_text = photo_buttons[0].get_attribute('aria-label') or ''
                photo_match = re.search(r'(\d+)', photo_text)
                if photo_match:
                    photos_count = int(photo_match.group(1))
        except: pass
        
        # Method 2: From photos tab
        if photos_count == 0:
            try:
                photo_tabs = driver.find_elements(By.XPATH, "//button[contains(., 'Photos')] | //button[contains(., 'photos')]")
                if photo_tabs:
                    tab_text = photo_tabs[0].text or photo_tabs[0].get_attribute('aria-label') or ''
                    photo_match = re.search(r'(\d+)', tab_text)
                    if photo_match:
                        photos_count = int(photo_match.group(1))
            except: pass
        
        # Method 3: Count visible photo elements
        if photos_count == 0:
            try:
                photo_elements = driver.find_elements(By.XPATH, "//img[contains(@src, 'googleusercontent')] | //img[contains(@src, 'maps')]")
                photos_count = len(photo_elements) if photo_elements else 0
            except: pass
        
        # 🏆 NEARBY COMPETITOR ANALYSIS (10KM RADIUS)
        nearby_competitors = []
        competitor_analysis = {
            'position': 'Unknown', 'rank': 0, 'total_competitors': 0,
            'avg_competitor_rating': 0, 'avg_competitor_reviews': 0,
            'competitive_score': 0, 'competitive_advantage': 'Not analyzed',
            'market_opportunity': 'Unknown', 'stronger_competitors': 0,
            'weaker_competitors': 0, 'competitive_threats': []
        }
        competitor_report = "Not analyzed"
        
        if address and category:
            try:
                logging.info(f"[{name}] Analyzing nearby competitors...")
                comp_driver = build_chrome(headless_mode, proxy)
                nearby_competitors = nearby_analyzer.scrape_nearby_competitors(
                    comp_driver, name, category, address, max_competitors=8
                )
                comp_driver.quit()
                if nearby_competitors:
                    competitor_analysis = nearby_analyzer.analyze_competitive_position(
                        {'name': name, 'rating': rating_val, 'reviews': reviews_val},
                        nearby_competitors
                    )
                    competitor_report = nearby_analyzer.generate_competitor_report(nearby_competitors)
            except Exception as e:
                logging.warning(f"Competitor analysis failed: {str(e)[:50]}")
                try:
                    comp_driver.quit()
                except:
                    pass
        
        # Get website HTML for analysis
        website_html = ""
        if website:
            try:
                r = requests.get(website, timeout=5, headers={"User-Agent": random.choice(USER_AGENTS)})
                if r.status_code == 200:
                    website_html = r.text
            except:
                pass
        
        # 🍽️ SCRAPE SERVICE OPTIONS FROM GBP - ULTRA ACCURATE
        has_dine_in = "Unknown"
        has_delivery = "Unknown"
        has_takeout = "Unknown"
        has_curbside_pickup = "Unknown"
        
        try:
            # Deep scroll to load ALL sections
            for _ in range(8):
                driver.execute_script("window.scrollBy(0, 600);")
                time.sleep(0.1)
            
            # Get fresh HTML
            html = driver.page_source
            html_lower = html.lower()
            
            # Method 1: Check service options with aria-labels (MOST ACCURATE)
            service_elements = driver.find_elements(By.XPATH, 
                "//div[contains(@aria-label, 'ervice')] | "
                "//div[contains(@aria-label, 'Dine')] | "
                "//div[contains(@aria-label, 'Delivery')] | "
                "//div[contains(@aria-label, 'Takeout')] | "
                "//div[contains(@aria-label, 'Takeaway')] | "
                "//div[contains(@aria-label, 'Curbside')] | "
                "//button[contains(@aria-label, 'Dine')] | "
                "//button[contains(@aria-label, 'Delivery')] | "
                "//button[contains(@aria-label, 'Takeout')]"
            )
            
            for elem in service_elements:
                label = (elem.get_attribute('aria-label') or '').lower()
                text = (elem.text or '').lower()
                combined = label + ' ' + text
                
                # Dine-in detection
                if 'dine' in combined and 'in' in combined:
                    if 'no dine' in combined or 'dine-in: no' in combined:
                        has_dine_in = "No"
                    elif any(x in combined for x in ['yes', 'offers', 'available', 'dine-in: yes']):
                        has_dine_in = "Yes"
                    elif has_dine_in == "Unknown":
                        has_dine_in = "Yes"  # If mentioned, assume yes
                
                # Delivery detection
                if 'delivery' in combined:
                    if 'no delivery' in combined or 'delivery: no' in combined:
                        has_delivery = "No"
                    elif any(x in combined for x in ['yes', 'offers', 'available', 'delivery: yes']):
                        has_delivery = "Yes"
                    elif has_delivery == "Unknown":
                        has_delivery = "Yes"
                
                # Takeout detection
                if any(x in combined for x in ['takeout', 'takeaway', 'take-out', 'take out']):
                    if 'no takeout' in combined or 'takeout: no' in combined:
                        has_takeout = "No"
                    elif any(x in combined for x in ['yes', 'offers', 'available', 'takeout: yes']):
                        has_takeout = "Yes"
                    elif has_takeout == "Unknown":
                        has_takeout = "Yes"
                
                # Curbside detection
                if 'curbside' in combined:
                    if 'no curbside' in combined:
                        has_curbside_pickup = "No"
                    else:
                        has_curbside_pickup = "Yes"
            
            # Method 2: Check service badges/chips
            badges = driver.find_elements(By.XPATH, 
                "//span[contains(@class, 'service')] | "
                "//div[contains(@class, 'chip')] | "
                "//div[contains(@class, 'badge')]"
            )
            for badge in badges:
                txt = (badge.text or '').lower()
                if 'dine' in txt and has_dine_in == "Unknown": has_dine_in = "Yes"
                if 'delivery' in txt and has_delivery == "Unknown": has_delivery = "Yes"
                if 'takeout' in txt and has_takeout == "Unknown": has_takeout = "Yes"
                if 'curbside' in txt and has_curbside_pickup == "Unknown": has_curbside_pickup = "Yes"
            
            # Method 3: Parse structured data from HTML
            if has_dine_in == "Unknown":
                if re.search(r'"servesCuisine"|"dineIn"\s*:\s*true', html_lower):
                    has_dine_in = "Yes"
                elif re.search(r'dine[\s-]?in[^<]{0,50}(yes|available|✓)', html_lower):
                    has_dine_in = "Yes"
                elif re.search(r'dine[\s-]?in[^<]{0,50}(no|unavailable|✗)', html_lower):
                    has_dine_in = "No"
            
            if has_delivery == "Unknown":
                if re.search(r'"hasDelivery"|"delivery"\s*:\s*true', html_lower):
                    has_delivery = "Yes"
                elif re.search(r'delivery[^<]{0,50}(yes|available|✓)', html_lower) or 'delivers' in html_lower:
                    has_delivery = "Yes"
                elif re.search(r'delivery[^<]{0,50}(no|unavailable|✗)', html_lower):
                    has_delivery = "No"
            
            if has_takeout == "Unknown":
                if re.search(r'"takeout"|"takeaway"\s*:\s*true', html_lower):
                    has_takeout = "Yes"
                elif re.search(r'take[\s-]?out[^<]{0,50}(yes|available|✓)', html_lower):
                    has_takeout = "Yes"
                elif re.search(r'take[\s-]?out[^<]{0,50}(no|unavailable|✗)', html_lower):
                    has_takeout = "No"
            
            if has_curbside_pickup == "Unknown":
                if 'curbside' in html_lower:
                    if re.search(r'curbside[^<]{0,50}(yes|available|✓)', html_lower):
                        has_curbside_pickup = "Yes"
                    elif re.search(r'curbside[^<]{0,50}(no|unavailable)', html_lower):
                        has_curbside_pickup = "No"
                    else:
                        has_curbside_pickup = "Yes"
        except Exception as e:
            logging.warning(f"Service scraping error: {str(e)[:50]}")
        
        # 🏢 SCRAPE AMENITIES & ATTRIBUTES FROM GBP - DEEP EXTRACTION
        amenities = {
            'WiFi': 'Unknown', 'Parking': 'Unknown', 'Wheelchair_Accessible': 'Unknown',
            'Outdoor_Seating': 'Unknown', 'Reservations': 'Unknown', 'Good_for_Kids': 'Unknown',
            'Good_for_Groups': 'Unknown', 'Restroom': 'Unknown', 'Accepts_Credit_Cards': 'Unknown'
        }
        
        try:
            # Scroll to amenities section
            for _ in range(10):
                driver.execute_script("window.scrollBy(0, 700);")
                time.sleep(0.08)
            
            # Click "See all" or "More" buttons for amenities
            try:
                more_buttons = driver.find_elements(By.XPATH, 
                    "//button[contains(., 'amenities')] | "
                    "//button[contains(., 'Amenities')] | "
                    "//button[contains(@aria-label, 'amenities')] | "
                    "//button[contains(., 'See all')]"
                )
                for btn in more_buttons[:3]:
                    try:
                        driver.execute_script("arguments[0].click();", btn)
                        time.sleep(0.3)
                    except: pass
            except: pass
            
            # Extract from ALL amenity-related elements
            amenity_elements = driver.find_elements(By.XPATH, 
                "//div[contains(@aria-label, 'Amenities')] | "
                "//div[contains(@aria-label, 'Accessibility')] | "
                "//div[contains(@aria-label, 'Offerings')] | "
                "//div[contains(@aria-label, 'Payments')] | "
                "//div[contains(@aria-label, 'Planning')] | "
                "//span[contains(., 'Wi-Fi')] | "
                "//span[contains(., 'Parking')] | "
                "//span[contains(., 'Wheelchair')] | "
                "//span[contains(., 'Outdoor')] | "
                "//span[contains(., 'Reservation')] | "
                "//span[contains(., 'Kids')] | "
                "//span[contains(., 'Groups')] | "
                "//span[contains(., 'Restroom')] | "
                "//span[contains(., 'Credit')]"
            )
            
            amenity_text = ' '.join([
                (elem.get_attribute('aria-label') or '') + ' ' + (elem.text or '')
                for elem in amenity_elements
            ]).lower()
            
            # Also check page source
            html_lower = driver.page_source.lower()
            combined_text = amenity_text + ' ' + html_lower
            
            # WiFi detection
            if re.search(r'(free\s*wi-?fi|wifi|wi-fi)', combined_text):
                if 'no wifi' in combined_text or 'no wi-fi' in combined_text:
                    amenities['WiFi'] = 'No'
                else:
                    amenities['WiFi'] = 'Yes'
            
            # Parking detection
            if re.search(r'parking', combined_text):
                if 'no parking' in combined_text:
                    amenities['Parking'] = 'No'
                else:
                    amenities['Parking'] = 'Yes'
            
            # Wheelchair accessible
            if re.search(r'wheelchair|accessible|accessibility', combined_text):
                if 'not accessible' in combined_text or 'no wheelchair' in combined_text:
                    amenities['Wheelchair_Accessible'] = 'No'
                else:
                    amenities['Wheelchair_Accessible'] = 'Yes'
            
            # Outdoor seating
            if re.search(r'outdoor\s*seating|outdoor\s*dining', combined_text):
                if 'no outdoor' in combined_text:
                    amenities['Outdoor_Seating'] = 'No'
                else:
                    amenities['Outdoor_Seating'] = 'Yes'
            
            # Reservations
            if re.search(r'reservations?|booking|book\s*a\s*table', combined_text):
                if 'no reservation' in combined_text:
                    amenities['Reservations'] = 'No'
                else:
                    amenities['Reservations'] = 'Yes'
            
            # Good for kids
            if re.search(r'kid|child|family', combined_text):
                if 'not good for kids' in combined_text:
                    amenities['Good_for_Kids'] = 'No'
                else:
                    amenities['Good_for_Kids'] = 'Yes'
            
            # Good for groups
            if re.search(r'group|party|large\s*group', combined_text):
                if 'not good for groups' in combined_text:
                    amenities['Good_for_Groups'] = 'No'
                else:
                    amenities['Good_for_Groups'] = 'Yes'
            
            # Restroom
            if re.search(r'restroom|bathroom|toilet', combined_text):
                amenities['Restroom'] = 'Yes'
            
            # Credit cards
            if re.search(r'credit\s*card|debit\s*card|card\s*payment|accepts\s*cards', combined_text):
                if 'cash only' in combined_text or 'no cards' in combined_text:
                    amenities['Accepts_Credit_Cards'] = 'No'
                else:
                    amenities['Accepts_Credit_Cards'] = 'Yes'
        except Exception as e:
            logging.warning(f"Amenity scraping error: {str(e)[:50]}")
        
        # 🚚 DETECT DELIVERY SERVICES - REAL ONLY (FROM WEBSITE + MAPS)
        delivery_services = []
        delivery_patterns = {
            'Zomato': r'zomato\.com|order.*zomato|zomato.*order',
            'Swiggy': r'swiggy\.com|order.*swiggy|swiggy.*order',
            'UberEats': r'ubereats\.com|uber\s*eats|order.*uber',
            'DoorDash': r'doordash\.com|door\s*dash|order.*doordash',
            'GrubHub': r'grubhub\.com|grub\s*hub|order.*grubhub',
            'Postmates': r'postmates\.com|order.*postmates',
        }
        
        # Only check if website exists
        if website_html:
            combined_text = (html + website_html).lower()
            for service, pattern in delivery_patterns.items():
                if re.search(pattern, combined_text, re.IGNORECASE):
                    delivery_services.append(service)
        
        delivery_services_str = ", ".join(delivery_services) if delivery_services else "Not Detected"
        
        # 💳 DETECT POS SYSTEM - REAL ONLY (FROM WEBSITE ONLY)
        pos_system = "Unknown"
        pos_model = "Unknown"
        pos_status = "No"
        pos_reason = "Not visible on website"
        pos_features = "N/A"
        
        # ONLY check website HTML - POS systems are ONLY visible on their own website
        if website_html:
            pos_patterns = {
                'Square': r'squareup\.com|square\.site|square\s*pos|powered\s*by\s*square',
                'Toast': r'toasttab\.com|toast\s*pos|powered\s*by\s*toast',
                'Clover': r'clover\.com|clover\s*pos|powered\s*by\s*clover',
                'Lightspeed': r'lightspeedhq\.com|lightspeed\s*pos',
                'TouchBistro': r'touchbistro\.com|touch\s*bistro',
                'Revel': r'revelsystems\.com|revel\s*systems',
                'Shopify POS': r'shopify\.com.*pos|shopify\s*pos',
                'NCR Aloha': r'ncr\.com|aloha\s*pos',
            }
            
            for pos, pattern in pos_patterns.items():
                if re.search(pattern, website_html.lower(), re.IGNORECASE):
                    pos_system = pos
                    pos_model = pos
                    pos_status = "Yes"
                    pos_reason = f"Uses {pos}"
                    
                    # Detect features based on POS system
                    features = []
                    if pos == 'Square':
                        features = ['Online Ordering', 'Payment Processing', 'Appointments']
                    elif pos == 'Toast':
                        features = ['Online Ordering', 'Reservations', 'Delivery Management']
                    elif pos == 'Clover':
                        features = ['Payment Processing', 'Inventory', 'Employee Management']
                    elif pos == 'Shopify POS':
                        features = ['E-commerce', 'Inventory', 'Online Store']
                    elif pos == 'Lightspeed':
                        features = ['Inventory', 'Multi-location', 'Reporting']
                    elif pos == 'TouchBistro':
                        features = ['Table Management', 'Menu Management', 'Staff Management']
                    elif pos == 'Revel':
                        features = ['Inventory', 'Employee Management', 'Reporting']
                    elif pos == 'NCR Aloha':
                        features = ['Table Service', 'Kitchen Display', 'Reporting']
                    else:
                        features = ['Payment Processing']
                    
                    pos_features = ', '.join(features)
                    break
        
        # If no POS detected, provide reason
        if pos_status == "No":
            if not website:
                pos_reason = "No website to check"
            elif not website_html:
                pos_reason = "Website not accessible"
            else:
                pos_reason = "Not advertised on website (likely uses manual/basic system)"
        
        has_pos = pos_status
        
        # 🌐 DETECT WEBSITE PLATFORM & TYPE - REAL DETECTION ONLY
        website_platform = "No Website"
        website_type = "No Website"
        website_tech = "No Website"
        website_quality_desc = "No Website"
        
        if website and website_html:
            # Platform detection - STRICT PATTERNS
            if 'wp-content' in website_html or 'wp-includes' in website_html or '/wp-json/' in website_html:
                website_platform = "WordPress"
                website_type = "Dynamic CMS"
                website_quality_desc = "Professional CMS"
            elif 'wix.com' in website_html or 'wixsite.com' in website_html or 'static.wixstatic.com' in website_html:
                website_platform = "Wix"
                website_type = "Drag & Drop Builder"
                website_quality_desc = "Website Builder"
            elif 'squarespace' in website_html.lower() or 'sqsp.com' in website_html:
                website_platform = "Squarespace"
                website_type = "Drag & Drop Builder"
                website_quality_desc = "Website Builder"
            elif 'shopify' in website_html.lower() or 'myshopify.com' in website_html or 'cdn.shopify.com' in website_html:
                website_platform = "Shopify"
                website_type = "E-commerce Platform"
                website_quality_desc = "E-commerce Store"
            elif '__next' in website_html or '_next/static' in website_html:
                website_platform = "Next.js (React)"
                website_type = "Modern Framework"
                website_quality_desc = "Custom Built"
            elif 'nuxt' in website_html.lower() or '_nuxt/' in website_html:
                website_platform = "Nuxt (Vue)"
                website_type = "Modern Framework"
                website_quality_desc = "Custom Built"
            elif re.search(r'react', website_html.lower()) and re.search(r'<div\s+id=["\']root["\']', website_html):
                website_platform = "React"
                website_type = "JavaScript Framework"
                website_quality_desc = "Custom Built"
            elif re.search(r'<html[^>]*>', website_html) and '<script' not in website_html[:5000]:
                website_platform = "Static HTML"
                website_type = "Static Website"
                website_quality_desc = "Basic Static"
            else:
                website_platform = "Custom/Unknown"
                website_type = "Dynamic" if '<script' in website_html.lower() else "Static"
                website_quality_desc = "Custom Built"
            
            # Technology detection - ONLY if clearly present
            tech_stack = []
            if 'jquery.min.js' in website_html or 'jquery.js' in website_html: 
                tech_stack.append('jQuery')
            if 'bootstrap.min.css' in website_html or 'bootstrap.css' in website_html: 
                tech_stack.append('Bootstrap')
            if 'tailwind' in website_html.lower() and 'tailwind.css' in website_html.lower(): 
                tech_stack.append('Tailwind')
            if 'angular.min.js' in website_html or 'angular.js' in website_html: 
                tech_stack.append('Angular')
            
            website_tech = ', '.join(tech_stack) if tech_stack else website_platform
        
        has_website_status = "Yes" if website else "No"
        # 🔥 STOREFRONT ANALYSIS - REAL DETECTION ONLY
        # Storefront exists ONLY if: address exists AND (photos > 5 OR rating exists)
        has_physical_storefront = "Unknown"
        storefront_quality = "Unknown"
        storefront_score = 0
        storefront_description = "Unknown"
        
        if address and (photos_count > 5 or rating_val > 0):
            has_physical_storefront = "Yes"
            # Calculate real score
            if photos_count > 50:
                storefront_score = 90
                storefront_quality = "Excellent"
            elif photos_count > 20:
                storefront_score = 75
                storefront_quality = "Good"
            elif photos_count > 5:
                storefront_score = 50
                storefront_quality = "Average"
            else:
                storefront_score = 30
                storefront_quality = "Poor"
            
            storefront_description = f"{storefront_quality} - {photos_count} photos, {rating_val}★ rating"
        elif address:
            has_physical_storefront = "Likely"
            storefront_quality = "Unverified"
            storefront_score = 20
            storefront_description = "Address exists but no photos/reviews"
        else:
            has_physical_storefront = "No"
            storefront_quality = "No Storefront"
            storefront_score = 0
            storefront_description = "No physical location detected"
        
        storefront_analysis = {
            'quality': storefront_quality,
            'score': storefront_score,
            'description': storefront_description
        }
        
        has_menu = 'menu' in website_html.lower() if website_html else False
        has_contact_page = 'contact' in website_html.lower() if website_html else False
        mobile_friendly = 'viewport' in website_html.lower() if website_html else False
        has_online_ordering = bool(re.search(r'order\s*online|online\s*order', website_html.lower())) if website_html else False
        
        website_analysis = rest_intel.analyze_website_quality(
            website, has_menu, has_contact_page, has_online_ordering, mobile_friendly
        )
        
        social_analysis = rest_intel.analyze_social_presence(
            socials["Facebook"], socials["Instagram"], socials["Twitter"], socials["LinkedIn"]
        )
        
        gbp_analysis = rest_intel.analyze_gbp_presence(
            photos_count > 0, bool(hours), bool(phone), bool(website), bool(address), reviews_val
        )
        
        # 📊 NEEDS ANALYSIS - BASED ON REAL DATA ONLY
        needs_analysis = rest_intel.identify_needs(
            delivery_services, pos_system, has_online_ordering,
            website_analysis['score'], social_analysis['score'], gbp_analysis['score'],
            category, has_dine_in, has_delivery, has_takeout
        )
        
        # Override needs based on REAL detection
        if pos_system == "Not Detected":
            needs_analysis['needs']['POS System'] = {
                'priority': 'HIGH',
                'reason': 'No POS system detected on website',
                'impact': 'Manual operations, slower service',
                'solution': 'Modern Cloud POS (Square/Toast/Clover)'
            }
        
        if website_platform == "No Website":
            needs_analysis['needs']['Website'] = {
                'priority': 'CRITICAL',
                'reason': 'No website detected',
                'impact': 'Zero online presence',
                'solution': 'Professional website with online ordering'
            }
        elif website_platform in ["Static HTML", "Custom/Unknown"]:
            needs_analysis['needs']['Website Upgrade'] = {
                'priority': 'MEDIUM',
                'reason': f'Basic {website_platform} website',
                'impact': 'Limited functionality',
                'solution': 'Upgrade to WordPress or modern CMS'
            }
        
        if has_physical_storefront == "No":
            needs_analysis['needs']['Storefront'] = {
                'priority': 'HIGH',
                'reason': 'No physical storefront detected',
                'impact': 'No customer trust',
                'solution': 'Add photos and verify location'
            }
        
        budget_tier = "High" if reviews_val > 100 else "Medium" if reviews_val > 30 else "Low"
        urgency = "High" if needs_analysis['critical_count'] > 0 else "Medium" if needs_analysis['high_count'] >= 2 else "Low"
        product_recommendations = rest_intel.recommend_products(needs_analysis, budget_tier, urgency)
        
        infrastructure_score = 0
        if delivery_services: infrastructure_score += 30
        if pos_system != "Unknown": infrastructure_score += 30
        if has_online_ordering: infrastructure_score += 40
        
        top_3_products = product_recommendations.get('top_3', [])
        all_products = product_recommendations.get('products', {})
        
        top_3_detailed = []
        buy_probs = []
        for prod_name in top_3_products[:3]:
            if prod_name in all_products:
                top_3_detailed.append(prod_name)
                buy_probs.append(float(all_products[prod_name]['buy_probability'].rstrip('%')))
            else:
                top_3_detailed.append("N/A")
                buy_probs.append(0)
        while len(top_3_detailed) < 3:
            top_3_detailed.append("N/A")
            buy_probs.append(0)
        
        needs_list = [f"{n} ({i['priority']})" for n, i in needs_analysis.get('needs', {}).items()]
        what_they_need = " | ".join(needs_list) if needs_list else "All systems operational"
        what_we_provide = " | ".join(top_3_products) if top_3_products else "Consultation"
        
        lead_score = (contact_quality * 0.3) + (gbp_analysis['score'] * 0.2) + ((website_analysis['score'] + social_analysis['score']) / 2 * 0.2) + (infrastructure_score * 0.3)
        lead_tier = "Hot" if lead_score >= 75 else "Warm" if lead_score >= 55 else "Cold" if lead_score >= 35 else "Ice"
        sales_strategy_data = rest_intel.generate_sales_strategy(needs_analysis, product_recommendations, lead_tier, bool(final_email), bool(phone))
        
        # Simple scoring
        has_website = 1 if website else 0
        has_phone = 1 if phone else 0
        has_email = 1 if final_email else 0
        has_social = 1 if any([socials["Facebook"], socials["Instagram"], socials["Twitter"], socials["LinkedIn"]]) else 0
        
        contact_quality = (has_email * 40) + (has_phone * 30) + (has_website * 20) + (has_social * 10)
        business_size = "Large" if reviews_val > 100 else "Medium" if reviews_val > 20 else "Small"
        
        return {
            "Search Query": search_query_used, "Category": category, "Zipcode": zipcode,
            "City": city, "State": state, "Business Name": name, "Address": address, 
            "Phone": phone, "Facebook Phone": ", ".join(fb_phones), "All Phones": ", ".join(sorted(all_phones)),
            "Website": website, "Website Platform": website_platform,
            "Website Type": website_type,
            "Website Technology": website_tech,
            "Website Quality Description": website_quality_desc,
            "Facebook": socials["Facebook"], "Instagram": socials["Instagram"],
            "Twitter": socials["Twitter"], "LinkedIn": socials["LinkedIn"],
            "Google Maps Email": maps_email, "All Website Emails": all_website_emails, "Website Email": website_email, 
            "Facebook Email": fb_email, "Instagram Email": insta_email, "LinkedIn Email": li_email,
            "All Emails Found": all_emails_str, "Total Emails": len(all_emails_found),
            "Final Email": final_email,
            "Email Source": "Website" if website_email and final_email == website_email else "Facebook" if fb_email and final_email == fb_email else "LinkedIn" if li_email and final_email == li_email else "Instagram" if insta_email and final_email == insta_email else "Maps" if maps_email and final_email == maps_email else "None",
            
            # === 👤 OWNER INFORMATION (HUNTER.IO) ===
            "👤 Owner Name": owner_name if owner_name else "Not Found",
            "📧 Owner Email": owner_email if owner_email else "Not Found",
            "👤 Owner Position": owner_position if owner_position else "Not Found",
            "👤 Owner Email Confidence": f"{owner_confidence}%" if owner_confidence else "N/A",
            
            "👤 Owner Name 2": owner_name_2 if owner_name_2 else "Not Found",
            "📧 Owner Email 2": owner_email_2 if owner_email_2 else "Not Found",
            "👤 Owner Position 2": owner_position_2 if owner_position_2 else "Not Found",
            "👤 Owner Email Confidence 2": f"{owner_confidence_2}%" if owner_confidence_2 else "N/A",
            
            "👤 Owner Name 3": owner_name_3 if owner_name_3 else "Not Found",
            "📧 Owner Email 3": owner_email_3 if owner_email_3 else "Not Found",
            "👤 Owner Position 3": owner_position_3 if owner_position_3 else "Not Found",
            "👤 Owner Email Confidence 3": f"{owner_confidence_3}%" if owner_confidence_3 else "N/A",
            
            "👤 Owner Name 4": owner_name_4 if owner_name_4 else "Not Found",
            "📧 Owner Email 4": owner_email_4 if owner_email_4 else "Not Found",
            "👤 Owner Position 4": owner_position_4 if owner_position_4 else "Not Found",
            "👤 Owner Email Confidence 4": f"{owner_confidence_4}%" if owner_confidence_4 else "N/A",
            
            "Maps URL": gmaps_url,
            "Place ID": place_id, "Closure Status": closure_status, "Rating": rating, "Reviews Count": reviews,
            "Newest Review Date": newest_review_date,
            "Price Range": price, "Business Type": category, "Opening Hours": hours,
            "Business Size": business_size, "Contact Quality": f"{contact_quality}%",
            "Photos Count": photos_count,
            "Storefront Quality": storefront_quality,
            "Storefront Description": storefront_description,
            "Has Physical Storefront": has_physical_storefront,
            
            # === ✅ WHAT THEY HAVE (REAL DETECTION ONLY) ===
            "✅ Has Storefront": has_physical_storefront,
            "Storefront Quality": storefront_quality,
            "Storefront Score": f"{storefront_score}%",
            "Storefront Description": storefront_description,
            "Has Physical Storefront": has_physical_storefront,
            
            "✅ Has POS System": has_pos,
            "POS System Name": pos_system,
            "POS Model": pos_model,
            "POS Status": pos_status,
            
            "✅ Has Website": has_website_status,
            "Website Platform": website_platform,
            "Website Type": website_type,
            "Website Technology": website_tech,
            "Website Quality Description": website_quality_desc,
            
            "✅ Has POS System": "Yes" if pos_system != "Unknown" else "No",
            "POS System Name": pos_system,
            
            "✅ Has Website": "Yes" if website else "No",
            "Website Quality": website_analysis['quality'],
            "Website Score": f"{website_analysis['score']}%",
            
            "✅ Has Social Media": "Yes" if has_social else "No",
            "Social Media Quality": social_analysis['quality'],
            "Social Media Score": f"{social_analysis['score']}%",
            
            "✅ Has Online Ordering": "Yes" if has_online_ordering else "No",
            
            "✅ GBP Presence": gbp_analysis['quality'],
            "GBP Score": f"{gbp_analysis['score']}%",
            
            # === 🍽️ SERVICE OPTIONS ===
            "🍽️ Dine-in Available": has_dine_in,
            "🚚 Delivery Available": has_delivery,
            "📦 Takeout Available": has_takeout,
            "🚗 Curbside Pickup Available": has_curbside_pickup,
            
            # === DELIVERY SERVICES (REAL ONLY) ===
            "Delivery Platforms": delivery_services_str,
            "Delivery Platform Count": len(delivery_services) if delivery_services else 0,
            
            # === 🏢 AMENITIES & ATTRIBUTES ===
            "📶 Has WiFi": amenities['WiFi'],
            "🏎️ Has Parking": amenities['Parking'],
            "♿ Wheelchair Accessible": amenities['Wheelchair_Accessible'],
            "🌳 Outdoor Seating": amenities['Outdoor_Seating'],
            "📞 Accepts Reservations": amenities['Reservations'],
            "👶 Good for Kids": amenities['Good_for_Kids'],
            "👥 Good for Groups": amenities['Good_for_Groups'],
            "🚻 Has Restroom": amenities['Restroom'],
            "💳 Accepts Credit Cards": amenities['Accepts_Credit_Cards'],
            
            # === ❌ WHAT THEY NEED ===
            "❌ Needs Storefront Upgrade": "Yes" if storefront_analysis['score'] < 60 else "No",
            "❌ Needs POS System": "Yes" if pos_system == "Unknown" else "No",
            "❌ Needs Website": "Yes" if not website or website_analysis['score'] < 50 else "No",
            "❌ Needs Social Media": "Yes" if not has_social or social_analysis['score'] < 40 else "No",
            "❌ Needs Online Ordering": "Yes" if not has_online_ordering else "No",
            "❌ Needs Delivery Integration": "Yes" if has_delivery == "Yes" and len(delivery_services) < 2 else "No",
            "❌ Needs GBP Optimization": "Yes" if gbp_analysis['score'] < 70 else "No",
            
            # === 💰 WHAT WE PROVIDE ===
            "💰 Product 1": top_3_detailed[0],
            "💰 Product 1 Price": f"${all_products[top_3_detailed[0]]['price']}" if top_3_detailed[0] in all_products else "N/A",
            "💰 Product 1 Buy Probability": f"{round(buy_probs[0], 1)}%",
            "💰 Product 1 Priority": all_products[top_3_detailed[0]]['priority'] if top_3_detailed[0] in all_products else "N/A",
            
            "💰 Product 2": top_3_detailed[1],
            "💰 Product 2 Price": f"${all_products[top_3_detailed[1]]['price']}" if top_3_detailed[1] in all_products else "N/A",
            "💰 Product 2 Buy Probability": f"{round(buy_probs[1], 1)}%",
            "💰 Product 2 Priority": all_products[top_3_detailed[1]]['priority'] if top_3_detailed[1] in all_products else "N/A",
            
            "💰 Product 3": top_3_detailed[2],
            "💰 Product 3 Price": f"${all_products[top_3_detailed[2]]['price']}" if top_3_detailed[2] in all_products else "N/A",
            "💰 Product 3 Buy Probability": f"{round(buy_probs[2], 1)}%",
            "💰 Product 3 Priority": all_products[top_3_detailed[2]]['priority'] if top_3_detailed[2] in all_products else "N/A",
            
            # === 📊 SUMMARY & PITCH ===
            "📊 Infrastructure Score": f"{infrastructure_score}%",
            "📊 Total Needs Count": needs_analysis.get('total_needs', 0),
            "📊 Critical Needs": needs_analysis.get('critical_count', 0),
            "📊 High Priority Needs": needs_analysis.get('high_count', 0),
            "📊 Service Summary": needs_analysis.get('service_summary', 'Unknown'),
            "📊 What They Need": what_they_need,
            "📊 What We Provide": what_we_provide,
            "📊 Lead Tier": lead_tier,
            "📊 Lead Score": round(lead_score, 1),
            "📊 Sales Strategy": sales_strategy_data.get('main_pitch', 'Consultative'),
            "📊 Outreach Method": sales_strategy_data.get('outreach_method', 'Email'),
            "📊 Win Probability": sales_strategy_data.get('win_probability', '50%'),
            
            # === POS SYSTEM (REAL DETECTION) ===
            "POS System": pos_system,
            "Has POS": has_pos,
            "POS Model": pos_model,
            "POS Status": f"{pos_status} ({pos_reason})",
            "POS Features": pos_features,
            "POS Detection Reason": pos_reason,
            # === SOCIAL MEDIA ANALYSIS ===
            "📱 Facebook Followers": fb_social_data.get('followers', 0) if fb_social_data else 0,
            "📱 Facebook Posting Frequency": fb_social_data.get('posting_frequency', 'Unknown') if fb_social_data else 'No Page',
            "📱 Facebook Last Post": fb_social_data.get('last_post_date', 'Unknown') if fb_social_data else 'N/A',
            "📱 Facebook Last Post Days Ago": fb_social_data.get('last_post_days_ago', 999) if fb_social_data else 999,
            "📱 Facebook Posts Last 7 Days": fb_social_data.get('posts_last_7_days', 0) if fb_social_data else 0,
            "📱 Facebook Posts Last 30 Days": fb_social_data.get('posts_last_30_days', 0) if fb_social_data else 0,
            "📱 Facebook Engagement": fb_social_data.get('engagement_level', 'None') if fb_social_data else 'None',
            "📱 Facebook Response Rate": fb_social_data.get('response_rate', 'Unknown') if fb_social_data else 'N/A',
            "📱 Facebook Verified": "Yes" if fb_social_data.get('page_verified', False) else "No",
            
            "📸 Instagram Followers": insta_social_data.get('followers', 0) if insta_social_data else 0,
            "📸 Instagram Posts Count": insta_social_data.get('posts_count', 0) if insta_social_data else 0,
            "📸 Instagram Last Post": insta_social_data.get('last_post_date', 'Unknown') if insta_social_data else 'N/A',
            "📸 Instagram Last Post Days Ago": insta_social_data.get('last_post_days_ago', 999) if insta_social_data else 999,
            "📸 Instagram Posts Last 7 Days": insta_social_data.get('posts_last_7_days', 0) if insta_social_data else 0,
            "📸 Instagram Posts Last 30 Days": insta_social_data.get('posts_last_30_days', 0) if insta_social_data else 0,
            "📸 Instagram Posting Frequency": insta_social_data.get('posting_frequency', 'Unknown') if insta_social_data else 'No Profile',
            "📸 Instagram Engagement": insta_social_data.get('engagement_level', 'None') if insta_social_data else 'None',
            "📸 Instagram Verified": "Yes" if insta_social_data.get('verified', False) else "No",
            "📸 Instagram Bio Contact": "Yes" if insta_social_data.get('bio_has_contact', False) else "No",
            
            "📊 Social Media Total Score": social_media_score.get('total_score', 0),
            "📊 Social Media Percentage": f"{social_media_score.get('percentage', 0)}%",
            "📊 Social Media Assessment": social_media_score.get('assessment', 'Unknown'),
            "📊 Total Social Followers": social_media_score.get('total_followers', 0),
            
            # === NEARBY COMPETITOR ANALYSIS (10KM) ===
            "🏆 Competitors Within 10km": competitor_analysis.get('total_competitors', 0),
            "🏆 Market Position": competitor_analysis.get('position', 'Unknown'),
            "🏆 Competitive Rank": f"#{competitor_analysis.get('rank', 0)} of {competitor_analysis.get('total_competitors', 0) + 1}",
            "🏆 Competitive Score": competitor_analysis.get('competitive_score', 0),
            "🏆 Avg Competitor Rating": competitor_analysis.get('avg_competitor_rating', 0),
            "🏆 Avg Competitor Reviews": competitor_analysis.get('avg_competitor_reviews', 0),
            "🏆 Competitive Advantage": competitor_analysis.get('competitive_advantage', 'Unknown'),
            "🏆 Market Opportunity": competitor_analysis.get('market_opportunity', 'Unknown'),
            "🏆 Stronger Competitors": competitor_analysis.get('stronger_competitors', 0),
            "🏆 Weaker Competitors": competitor_analysis.get('weaker_competitors', 0),
            "🏆 Top Threats": " | ".join(competitor_analysis.get('competitive_threats', [])) if competitor_analysis.get('competitive_threats') else "None",
            "🏆 Competitor Report": competitor_report
        }
    except Exception as e:
        return {"Maps URL": gmaps_url, "Status": f"ERROR: {str(e)[:50]}", "Business Name": "", "Lead Tier": "Unknown"}
    finally:
        driver.quit()

def scrape_details(session_id, config):
    session = get_session(session_id)
    links = session["collected_links"]
    update_status(session_id, f"Scraping {len(links)} businesses...", total_to_scrape=len(links))
    results = []
    max_workers = min(4, max(2, len(links) // 10))
    total_timeout = max(600, len(links) * 40)
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        futures = {pool.submit(scrape_business_entry, url, query, zipc, config.get("scrape_timeout", 15), config.get("headless_mode", True), config.get("proxy")): (url, query, zipc) for url, query, zipc in links}
        try:
            for i, fut in enumerate(as_completed(futures, timeout=total_timeout)):
                if session["stop_scraping_flag"]: break
                try:
                    res = fut.result(timeout=36)
                    if res: results.append(res)
                except Exception as e:
                    print(f"⚠️ Skipped: {str(e)[:30]}")
                update_status(session_id, f"Scraped {len(results)}/{len(links)}", detail_progress=(i+1)/len(links), scraped_count=len(results))
        except Exception as e:
            print(f"⚠️ {len(futures) - len(results)} futures unfinished")
    with session["lock"]: session["results_df"] = pd.DataFrame(results)

def scraping_worker(session_id, config):
    session = get_session(session_id)
    try:
        with session["lock"]: 
            session["scraping_active"] = True
            session["stop_scraping_flag"] = False
            session["results_df"] = pd.DataFrame()
            session["collected_links"] = []
            session["link_count"] = 0
            session["scraped_count"] = 0
            session["total_to_scrape"] = 0
        update_status(session_id, "Collecting links...")
        collect_gmaps_links(session_id, config)
        if not session["stop_scraping_flag"] and session["collected_links"]:
            update_status(session_id, "Scraping details...")
            scrape_details(session_id, config)
        update_status(session_id, "Complete! Ready for next job.")
    except Exception as e: 
        update_status(session_id, f"Complete! Ready for next job.")
    finally:
        with session["lock"]: 
            session["scraping_active"] = False
            session["stop_scraping_flag"] = False

@app.route("/")
def index(): return render_template("index.html")

@app.route("/start-scraping", methods=["POST"])
def start_scraping():
    session_id = request.headers.get('X-Session-ID', 'default')
    
    with SESSIONS_LOCK:
        active_count = sum(1 for s in SESSIONS.values() if s["scraping_active"])
        if active_count >= MAX_CONCURRENT_SESSIONS:
            return jsonify({"status": "error", "message": f"Maximum {MAX_CONCURRENT_SESSIONS} concurrent sessions reached. Please wait."}), 200
    
    session = get_session(session_id)
    with session["lock"]:
        if session["scraping_active"]: 
            return jsonify({"status": "error", "message": "Your session is already scraping. Please wait."}), 200
    
    threading.Thread(target=scraping_worker, args=(session_id, request.json or {}), daemon=True).start()
    return jsonify({"status": "success", "message": "Started"})

@app.route("/status")
def status():
    session_id = request.headers.get('X-Session-ID', 'default')
    session = get_session(session_id)
    with session["lock"]: return jsonify({k: v for k, v in session.items() if k not in ["results_df", "lock"]})

@app.route("/stop-scraping", methods=["POST"])
def stop_scraping():
    session_id = request.headers.get('X-Session-ID', 'default')
    session = get_session(session_id)
    with session["lock"]: 
        session["stop_scraping_flag"] = True
        session["scraping_active"] = False
    return jsonify({"status": "success", "message": "Stopped"})

@app.route("/get-results")
def get_results():
    session_id = request.headers.get('X-Session-ID', 'default')
    session = get_session(session_id)
    with session["lock"]: return jsonify(session["results_df"].to_dict(orient="records") if not session["results_df"].empty else [])

@app.route("/download-csv")
def download_csv():
    session_id = request.headers.get('X-Session-ID') or request.args.get('session', 'default')
    session = get_session(session_id)
    with session["lock"]:
        if not session["results_df"].empty:
            buf = io.BytesIO()
            session["results_df"].to_csv(buf, index=False, encoding="utf-8-sig")
            buf.seek(0)
            return send_file(buf, as_attachment=True, download_name=f"scraped_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv", mimetype="text/csv")
    return "No data", 404

@app.route("/download-excel")
def download_excel():
    session_id = request.headers.get('X-Session-ID') or request.args.get('session', 'default')
    session = get_session(session_id)
    with session["lock"]:
        if not session["results_df"].empty:
            buf = io.BytesIO()
            with pd.ExcelWriter(buf, engine="xlsxwriter") as writer:
                df = session["results_df"]
                df.to_excel(writer, index=False, sheet_name="Leads", startrow=1)
                
                workbook = writer.book
                worksheet = writer.sheets["Leads"]
                
                # Formats
                header_format = workbook.add_format({
                    'bold': True, 'bg_color': '#4472C4', 'font_color': 'white',
                    'border': 1, 'align': 'center', 'valign': 'vcenter'
                })
                hot_format = workbook.add_format({'bg_color': '#FF6B6B', 'font_color': 'white', 'bold': True})
                warm_format = workbook.add_format({'bg_color': '#FFA500', 'font_color': 'white'})
                cold_format = workbook.add_format({'bg_color': '#87CEEB'})
                email_format = workbook.add_format({'bg_color': '#90EE90'})
                product_format = workbook.add_format({'bg_color': '#FFD700'})
                
                # Title
                title_format = workbook.add_format({
                    'bold': True, 'font_size': 16, 'bg_color': '#1F4E78',
                    'font_color': 'white', 'align': 'center'
                })
                worksheet.merge_range('A1:Z1', f'Lead Intelligence Report - {datetime.now().strftime("%Y-%m-%d %H:%M")}', title_format)
                
                # Headers
                for col_num, value in enumerate(df.columns.values):
                    worksheet.write(1, col_num, value, header_format)
                
                # Column widths
                worksheet.set_column('A:A', 20)  # Search Query
                worksheet.set_column('B:B', 15)  # Category
                worksheet.set_column('C:G', 18)  # Location & Name
                worksheet.set_column('H:H', 15)  # Phone
                worksheet.set_column('I:P', 25)  # Emails & Social
                worksheet.set_column('Q:Z', 20)  # Analytics
                worksheet.set_column('AA:AZ', 18)  # Products
                
                # Conditional formatting for Lead Tier
                if 'Lead Tier' in df.columns:
                    tier_col = df.columns.get_loc('Lead Tier') + 1
                    for row in range(2, len(df) + 2):
                        cell_val = df.iloc[row-2]['Lead Tier'] if row-2 < len(df) else ''
                        if cell_val == 'Hot':
                            worksheet.write(row, tier_col, cell_val, hot_format)
                        elif cell_val == 'Warm':
                            worksheet.write(row, tier_col, cell_val, warm_format)
                        elif cell_val == 'Cold':
                            worksheet.write(row, tier_col, cell_val, cold_format)
                
                # Highlight emails
                if 'Final Email' in df.columns:
                    email_col = df.columns.get_loc('Final Email') + 1
                    for row in range(2, len(df) + 2):
                        if row-2 < len(df) and df.iloc[row-2]['Final Email']:
                            val = df.iloc[row-2]["Final Email"]; worksheet.write(row, email_col, str(val) if pd.notna(val) else "", email_format)
                
                # Highlight top products
                for prod_col in ['Top Product 1', 'Top Product 2', 'Top Product 3']:
                    if prod_col in df.columns:
                        col_idx = df.columns.get_loc(prod_col) + 1
                        for row in range(2, len(df) + 2):
                            if row-2 < len(df):
                                worksheet.write(row, col_idx, df.iloc[row-2][prod_col], product_format)
                
                # Freeze panes
                worksheet.freeze_panes(2, 0)
                
                # Auto filter
                worksheet.autofilter(1, 0, len(df) + 1, len(df.columns) - 1)
                
            buf.seek(0)
            return send_file(buf, as_attachment=True, download_name=f"ProMax_Leads_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx", mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet")
    return "No data", 404

@app.route("/save-progress", methods=["POST"])
def save_progress_route(): return jsonify({"status": "success", "message": "Saved"})

@app.route("/load-progress", methods=["POST"])
def load_progress_route(): return jsonify({"status": "success", "message": "Loaded"})

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001, debug=True)
