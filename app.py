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

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
app = Flask(__name__)

SESSIONS = {}
SESSIONS_LOCK = threading.Lock()
MAX_CONCURRENT_SESSIONS = 20

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
        driver_path = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
    except:
        driver_path = "/usr/bin/chromedriver"
    
    return webdriver.Chrome(service=Service(driver_path), options=opts)

def find_emails(html):
    if not html: return []
    # Decode all obfuscation methods
    original = html
    html = html.lower()
    html = html.replace('[at]','@').replace('(at)','@').replace('[dot]','.').replace('(dot)','.')
    html = html.replace(' at ','@').replace(' dot ','.').replace('_at_','@').replace('_dot_','.')
    html = html.replace('&#64;','@').replace('&#46;','.').replace('&commat;','@')
    html = html.replace('%40','@').replace('%2e','.').replace('\\u0040','@')
    html = html.replace(' @ ','@').replace(' . ','.').replace('[a]','@').replace('[d]','.')
    html = html.replace('&lt;','<').replace('&gt;','>').replace('&quot;','"')
    
    emails = EMAIL_REGEX.findall(html)
    
    # Very lenient filtering - keep almost everything
    invalid_extensions = ['.png', '.jpg', '.jpeg', '.gif', '.svg', '.webp', '.css', '.js', '.ico']
    invalid_domains = ['sentry.io', 'example.com', 'test.com', 'localhost', 'w3.org', 'schema.org']
    
    valid_emails = []
    for e in emails:
        e = e.strip().lower()
        if e.count('@') != 1: continue
        if any(ext in e for ext in invalid_extensions): continue
        
        try:
            local, domain = e.split('@')
            if len(local) < 2 or len(domain) < 4: continue
            if any(d == domain for d in invalid_domains): continue
            if '.' not in domain: continue
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
        driver.set_page_load_timeout(10)
        driver.get(url)
        time.sleep(1.5)
        
        emails = set()
        
        # Scroll main page aggressively
        for _ in range(5):
            driver.execute_script("window.scrollBy(0, 1000);")
            time.sleep(0.2)
        
        # Extract from page source
        emails.update(find_emails(driver.page_source))
        
        # Extract from ALL text elements
        try:
            all_elements = driver.find_elements(By.XPATH, "//*")
            for elem in all_elements:
                text = elem.text or ''
                if '@' in text:
                    emails.update(find_emails(text))
        except: pass
        
        socials = extract_social_links(driver.page_source)
        
        # Check contact links
        try:
            all_links = driver.find_elements(By.TAG_NAME, 'a')
            contact_keywords = ['contact', 'about', 'team', 'email', 'support', 'info', 'reach']
            
            visited = 0
            for link in all_links[:30]:
                if visited >= 8: break
                try:
                    href = link.get_attribute('href')
                    text = link.text.lower()
                    
                    if href and url in href and any(kw in href.lower() or kw in text for kw in contact_keywords):
                        driver.get(href)
                        time.sleep(0.5)
                        visited += 1
                        
                        for _ in range(3):
                            driver.execute_script("window.scrollBy(0, 1000);")
                            time.sleep(0.2)
                        
                        emails.update(find_emails(driver.page_source))
                        
                        if len(emails) >= 5:
                            break
                            
                        driver.back()
                        time.sleep(0.3)
                except: continue
        except: pass
        
        logging.info(f"Selenium found {len(emails)} emails from {url}")
        return list(emails), socials
    except Exception as e:
        logging.error(f"Selenium error for {url}: {e}")
        return [], {"Facebook": "", "Instagram": "", "Twitter": "", "LinkedIn": ""}
    finally:
        if driver: driver.quit()

def scrape_website_data(url, headless_mode, proxy=None):
    all_emails = set()
    socials = {"Facebook": "", "Instagram": "", "Twitter": "", "LinkedIn": ""}
    
    try:
        session = requests.Session()
        r = session.get(url, headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=5, allow_redirects=True)
        if r.status_code == 200:
            all_emails.update(find_emails(r.text))
            socials = extract_social_links(r.text)
            
            # Extract from response headers
            all_emails.update(find_emails(str(r.headers)))
            
            # Check key contact pages
            contact_pages = [
                '/contact', '/contact-us', '/contactus',
                '/about', '/about-us', '/aboutus',
                '/team', '/support', '/info'
            ]
            
            for page in contact_pages:
                try:
                    contact_url = urllib.parse.urljoin(url, page)
                    cr = session.get(contact_url, headers={"User-Agent": random.choice(USER_AGENTS)}, timeout=3)
                    if cr.status_code == 200:
                        all_emails.update(find_emails(cr.text))
                except: continue
    except: pass
    
    # ALWAYS use Selenium for deep scraping
    try:
        selenium_emails, selenium_socials = scrape_website_selenium(url, headless_mode, proxy)
        all_emails.update(selenium_emails)
        for k, v in selenium_socials.items():
            if v and not socials.get(k): socials[k] = v
    except: pass
    
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
            driver_path = ChromeDriverManager(chrome_type=ChromeType.CHROMIUM).install()
        except:
            driver_path = "/usr/bin/chromedriver"
        driver = webdriver.Chrome(service=Service(driver_path), options=opts)
        driver.set_page_load_timeout(15)
        
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
                
                # Scroll page
                for i in range(3):
                    driver.execute_script("window.scrollBy(0, 1000);")
                    time.sleep(0.3)
                
                html = driver.page_source
                all_emails.update(find_emails(html))
                all_phones.update(extract_facebook_phone(driver))
                all_phones.update(find_phone_numbers(html))
                
                # Click expandable elements
                try:
                    clickable = driver.find_elements(By.XPATH, "//div[@role='button'] | //span[contains(text(), 'See')] | //span[contains(text(), 'More')]")
                    for elem in clickable[:10]:
                        try:
                            driver.execute_script("arguments[0].click();", elem)
                            time.sleep(0.3)
                        except: pass
                    all_emails.update(find_emails(driver.page_source))
                    all_phones.update(extract_facebook_phone(driver))
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

def scrape_business_entry(gmaps_url, search_query_used, zipcode, timeout, headless_mode, proxy=None):
    driver = build_chrome(headless_mode, proxy)
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
        address = driver.find_element(By.CSS_SELECTOR, 'button[data-item-id="address"]').text.strip() if driver.find_elements(By.CSS_SELECTOR, 'button[data-item-id="address"]') else ""
        phone = driver.find_element(By.CSS_SELECTOR, 'button[data-item-id^="phone"]').text.strip() if driver.find_elements(By.CSS_SELECTOR, 'button[data-item-id^="phone"]') else ""
        website = driver.find_element(By.CSS_SELECTOR, 'a[data-item-id="authority"]').get_attribute("href") if driver.find_elements(By.CSS_SELECTOR, 'a[data-item-id="authority"]') else ""
        category = driver.find_element(By.CSS_SELECTOR, 'button[jsaction*="category"]').text.strip() if driver.find_elements(By.CSS_SELECTOR, 'button[jsaction*="category"]') else ""
        price = driver.find_element(By.CSS_SELECTOR, '[aria-label^="Price:"]').get_attribute('aria-label').replace('Price:', '').strip() if driver.find_elements(By.CSS_SELECTOR, '[aria-label^="Price:"]') else ""
        
        rating, reviews = "", ""
        if driver.find_elements(By.CSS_SELECTOR, 'div.F7nice'):
            txt = driver.find_element(By.CSS_SELECTOR, 'div.F7nice').text.strip()
            if m := re.search(r'(\d[.,]\d+)', txt): rating = m.group(1)
            if m := re.search(r'\((\d{1,3}(?:[.,]\d{3})*)\)', txt): reviews = re.sub(r'[.,]', '', m.group(1))
        
        city, state = "", ""
        if address:
            try:
                tagged, _ = usaddress.tag(address)
                city = tagged.get('PlaceName', '')
                state = tagged.get('StateName', '')
            except:
                parts = address.split(', ')
                if len(parts) >= 3: city = parts[-3]; state = parts[-2].split(' ')[0] if len(parts[-2].split(' ')) > 1 else ''
        
        # Google Maps email extraction - ULTRA AGGRESSIVE
        maps_emails = set()
        
        # Scroll page multiple times
        for _ in range(8):
            driver.execute_script("window.scrollBy(0, 500);")
            time.sleep(0.1)
        
        # Extract from full page source
        html = driver.page_source
        maps_emails.update(find_emails(html))
        logging.info(f"[{name}] After page source: {len(maps_emails)} emails")
        
        # Extract from ALL elements
        try:
            all_divs = driver.find_elements(By.TAG_NAME, 'div')
            for div in all_divs:
                text = div.text or ''
                if '@' in text:
                    maps_emails.update(find_emails(text))
        except: pass
        
        # Extract from buttons and links
        try:
            all_buttons = driver.find_elements(By.XPATH, "//button | //a")
            for btn in all_buttons:
                text = btn.text or btn.get_attribute('href') or ''
                if '@' in text:
                    maps_emails.update(find_emails(text))
        except: pass
        
        # Click ALL expandable buttons
        try:
            more_btns = driver.find_elements(By.XPATH, "//button | //div[@role='button']")
            for btn in more_btns[:15]:
                try:
                    driver.execute_script("arguments[0].click();", btn)
                    time.sleep(0.15)
                except: pass
            maps_emails.update(find_emails(driver.page_source))
        except: pass
        
        maps_email = get_best_email(maps_emails)
        logging.info(f"[{name}] FINAL Google Maps emails: {maps_emails} | Best: {maps_email}")
        
        website_emails, socials = [], {"Facebook": "", "Instagram": "", "Twitter": "", "LinkedIn": ""}
        if website:
            try:
                website_emails, socials = scrape_website_data(website, headless_mode, proxy)
                logging.info(f"[{name}] Website emails: {website_emails}")
            except Exception as e:
                logging.error(f"[{name}] Website scrape error: {e}")
                website_emails, socials = [], {"Facebook": "", "Instagram": "", "Twitter": "", "LinkedIn": ""}
        
        all_website_emails = ", ".join(website_emails)
        website_email = get_domain_matched_email(website_emails, website)
        
        fb_emails, fb_phones = [], []
        if socials["Facebook"]:
            fb_emails, fb_phones = scrape_facebook_page(socials["Facebook"], headless_mode, proxy)
            logging.info(f"[{name}] Facebook emails: {fb_emails}")
        fb_email = get_best_email(fb_emails)
        
        insta_emails = []
        if socials["Instagram"]:
            insta_emails = scrape_instagram_page(socials["Instagram"], headless_mode, proxy)
        insta_email = get_best_email(insta_emails)
        
        li_emails = []
        if socials["LinkedIn"]:
            li_emails = scrape_linkedin_page(socials["LinkedIn"], headless_mode, proxy)
        li_email = get_best_email(li_emails)
        
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
        
        all_phones = set([phone] + fb_phones) if phone else set(fb_phones)
        
        hours = ""
        try:
            hours_elem = driver.find_elements(By.CSS_SELECTOR, 'table.eK4R0e')
            if hours_elem: hours = hours_elem[0].text.replace('\n', '; ')
        except: pass
        
        # Advanced Analytics
        rating_val = float(rating.replace(',', '.')) if rating else 0
        reviews_val = int(reviews) if reviews else 0
        has_website = 1 if website else 0
        has_phone = 1 if phone else 0
        has_email = 1 if final_email else 0
        has_social = 1 if any([socials["Facebook"], socials["Instagram"], socials["Twitter"], socials["LinkedIn"]]) else 0
        
        # Lead Scoring
        contact_quality = (has_email * 40) + (has_phone * 30) + (has_website * 20) + (has_social * 10)
        gbp_completeness = sum([1 for x in [name, address, phone, website, category, hours] if x]) / 6 * 100
        digital_maturity = sum([has_website, has_social, has_email]) / 3 * 100
        lead_score = (contact_quality * 0.4) + (gbp_completeness * 0.3) + (digital_maturity * 0.3)
        
        # Business Intelligence
        business_size = "Large" if reviews_val > 100 else "Medium" if reviews_val > 20 else "Small"
        review_velocity = reviews_val / 12 if reviews_val > 0 else 0
        csi = rating_val * 20 if rating_val > 0 else 0
        
        # Lead Tier
        if lead_score >= 80: lead_tier = "Hot"
        elif lead_score >= 60: lead_tier = "Warm"
        elif lead_score >= 40: lead_tier = "Cold"
        else: lead_tier = "Ice"
        
        # Conversion Metrics
        conversion_prob = min(95, lead_score * 0.9) if has_email else lead_score * 0.5
        engagement_score = min(100, (reviews_val * 0.5) + (rating_val * 10))
        response_likelihood = "High" if has_email and has_phone else "Medium" if has_email or has_phone else "Low"
        
        # Product Needs Analysis
        needs_website = "Yes" if not website else "No"
        needs_online_ordering = "Yes" if "restaurant" in category.lower() or "food" in category.lower() else "Maybe"
        needs_delivery = "Yes" if "restaurant" in category.lower() or "food" in category.lower() else "No"
        needs_ecommerce = "Yes" if "retail" in category.lower() or "shop" in category.lower() else "Maybe"
        needs_booking = "Yes" if any(x in category.lower() for x in ["salon", "spa", "hotel", "clinic", "doctor"]) else "No"
        needs_crm = "Yes" if business_size in ["Medium", "Large"] else "Maybe"
        needs_social = "Yes" if not has_social else "No"
        needs_seo = "Yes" if reviews_val < 20 else "Maybe"
        
        total_needs = sum([1 for x in [needs_website, needs_online_ordering, needs_delivery, needs_ecommerce, needs_booking, needs_crm, needs_social, needs_seo] if x == "Yes"])
        
        # AI Insights
        product_fit = min(100, total_needs * 15 + engagement_score * 0.3)
        buying_readiness = "High" if lead_tier == "Hot" and total_needs >= 3 else "Medium" if lead_tier in ["Warm", "Hot"] else "Low"
        urgency_score = min(100, total_needs * 12 + (100 - gbp_completeness))
        deal_size = "$5000+" if business_size == "Large" else "$2000-5000" if business_size == "Medium" else "$500-2000"
        
        pain_points = []
        if not website: pain_points.append("No online presence")
        if reviews_val < 10: pain_points.append("Low visibility")
        if not has_email: pain_points.append("Missing contact info")
        if not has_social: pain_points.append("No social media")
        
        sales_angle = "Digital Transformation" if not website else "Growth & Optimization" if total_needs >= 3 else "Maintenance & Support"
        ai_recommendation = f"Priority: {lead_tier} | Focus: {sales_angle} | Needs: {total_needs} products"
        
        # ADVANCED ML PRODUCT RECOMMENDATIONS
        product_scores = {}
        
        # Website Builder (Score: 0-100)
        website_score = 0
        if not website: website_score = 95
        elif digital_maturity < 50: website_score = 70
        product_scores['Website Builder'] = website_score
        
        # SEO Services
        seo_score = 0
        if reviews_val < 10: seo_score = 90
        elif reviews_val < 30: seo_score = 75
        elif not website: seo_score = 85
        else: seo_score = 40
        product_scores['SEO Services'] = seo_score
        
        # Google Ads
        ads_score = 0
        if reviews_val < 20 and website: ads_score = 85
        elif business_size == "Small": ads_score = 70
        else: ads_score = 50
        product_scores['Google Ads'] = ads_score
        
        # Social Media Marketing
        smm_score = 0
        if not has_social: smm_score = 90
        elif socials["Facebook"] and not socials["Instagram"]: smm_score = 75
        else: smm_score = 45
        product_scores['Social Media Marketing'] = smm_score
        
        # CRM Software
        crm_score = 0
        if business_size == "Large": crm_score = 85
        elif business_size == "Medium": crm_score = 70
        elif reviews_val > 50: crm_score = 65
        else: crm_score = 30
        product_scores['CRM Software'] = crm_score
        
        # Booking System
        booking_score = 0
        if any(x in category.lower() for x in ["salon", "spa", "hotel", "clinic", "doctor", "dentist", "gym", "fitness"]): booking_score = 95
        elif any(x in category.lower() for x in ["restaurant", "cafe"]): booking_score = 70
        else: booking_score = 20
        product_scores['Booking System'] = booking_score
        
        # Online Ordering
        ordering_score = 0
        if any(x in category.lower() for x in ["restaurant", "food", "cafe", "pizza", "burger"]): ordering_score = 95
        elif any(x in category.lower() for x in ["retail", "shop", "store"]): ordering_score = 60
        else: ordering_score = 15
        product_scores['Online Ordering'] = ordering_score
        
        # E-commerce Platform
        ecom_score = 0
        if any(x in category.lower() for x in ["retail", "shop", "store", "boutique", "clothing"]): ecom_score = 90
        elif any(x in category.lower() for x in ["restaurant", "food"]): ecom_score = 50
        else: ecom_score = 25
        product_scores['E-commerce Platform'] = ecom_score
        
        # Payment Gateway
        payment_score = 0
        if not website: payment_score = 20
        elif any(x in category.lower() for x in ["retail", "shop", "restaurant", "food"]): payment_score = 85
        else: payment_score = 50
        product_scores['Payment Gateway'] = payment_score
        
        # Email Marketing
        email_mkt_score = 0
        if has_email and business_size in ["Medium", "Large"]: email_mkt_score = 80
        elif has_email: email_mkt_score = 60
        else: email_mkt_score = 25
        product_scores['Email Marketing'] = email_mkt_score
        
        # Reputation Management
        reputation_score = 0
        if rating_val < 3.5 and reviews_val > 10: reputation_score = 90
        elif reviews_val < 10: reputation_score = 85
        elif rating_val < 4.0: reputation_score = 70
        else: reputation_score = 40
        product_scores['Reputation Management'] = reputation_score
        
        # Analytics & Tracking
        analytics_score = 0
        if website and business_size in ["Medium", "Large"]: analytics_score = 75
        elif website: analytics_score = 55
        else: analytics_score = 20
        product_scores['Analytics & Tracking'] = analytics_score
        
        # Cloud Storage
        cloud_score = 0
        if business_size == "Large": cloud_score = 70
        elif business_size == "Medium": cloud_score = 50
        else: cloud_score = 25
        product_scores['Cloud Storage'] = cloud_score
        
        # Accounting Software
        accounting_score = 0
        if business_size in ["Medium", "Large"]: accounting_score = 75
        else: accounting_score = 40
        product_scores['Accounting Software'] = accounting_score
        
        # Delivery Management
        delivery_score = 0
        if any(x in category.lower() for x in ["restaurant", "food", "pizza", "delivery"]): delivery_score = 90
        else: delivery_score = 15
        product_scores['Delivery Management'] = delivery_score
        
        # Sort products by score
        top_products = sorted(product_scores.items(), key=lambda x: x[1], reverse=True)
        
        # Top 5 recommendations
        top_5_products = [f"{p[0]} ({p[1]}%)" for p in top_products[:5]]
        top_3_products = [p[0] for p in top_products[:3]]
        
        # Calculate buy probability for each top product
        buy_prob_1 = min(95, top_products[0][1] * 0.9) if top_products[0][1] > 60 else top_products[0][1] * 0.7
        buy_prob_2 = min(90, top_products[1][1] * 0.85) if top_products[1][1] > 60 else top_products[1][1] * 0.65
        buy_prob_3 = min(85, top_products[2][1] * 0.8) if top_products[2][1] > 60 else top_products[2][1] * 0.6
        
        # Revenue prediction
        product_prices = {
            'Website Builder': 2500, 'SEO Services': 1500, 'Google Ads': 1000,
            'Social Media Marketing': 800, 'CRM Software': 3000, 'Booking System': 1200,
            'Online Ordering': 1800, 'E-commerce Platform': 3500, 'Payment Gateway': 500,
            'Email Marketing': 600, 'Reputation Management': 900, 'Analytics & Tracking': 700,
            'Cloud Storage': 400, 'Accounting Software': 2000, 'Delivery Management': 1500
        }
        
        predicted_revenue = sum([product_prices.get(p[0], 1000) * (p[1]/100) for p in top_products[:3]])
        
        # Sales strategy
        if top_products[0][1] >= 85:
            sales_strategy = f"URGENT: Push {top_products[0][0]} immediately - {int(buy_prob_1)}% buy probability"
        elif top_products[0][1] >= 70:
            sales_strategy = f"HIGH PRIORITY: Lead with {top_products[0][0]}, upsell {top_products[1][0]}"
        else:
            sales_strategy = f"NURTURE: Educate on {top_products[0][0]} benefits, follow up in 1 week"
        
        # Objection handling
        objections = []
        if not has_email: objections.append("No email - use phone/social")
        if business_size == "Small": objections.append("Budget concerns - offer payment plan")
        if not website: objections.append("Tech hesitant - emphasize ease of use")
        if rating_val < 3.5: objections.append("Reputation issues - lead with reputation mgmt")
        
        objection_strategy = "; ".join(objections) if objections else "No major objections expected"
        
        return {
            "Search Query": search_query_used, "Category": category, "Zipcode": zipcode,
            "City": city, "State": state, "Business Name": name, "Address": address, 
            "Phone": phone, "Facebook Phone": ", ".join(fb_phones), "All Phones": ", ".join(sorted(all_phones)),
            "Website": website, "Facebook": socials["Facebook"], "Instagram": socials["Instagram"],
            "Twitter": socials["Twitter"], "LinkedIn": socials["LinkedIn"],
            "Google Maps Email": maps_email, "All Website Emails": all_website_emails, "Website Email": website_email, 
            "Facebook Email": fb_email, "Instagram Email": insta_email, "LinkedIn Email": li_email, "Owner Email": "",
            "All Emails Found": all_emails_str, "Total Emails": len(all_emails_found),
            "Final Email": final_email,
            "Source": "Website" if website_email and final_email == website_email else "Facebook" if fb_email and final_email == fb_email else "LinkedIn" if li_email and final_email == li_email else "Instagram" if insta_email and final_email == insta_email else "Maps" if maps_email and final_email == maps_email else "",
            "Maps URL": gmaps_url,
            "Place ID": place_id, "Closure Status": closure_status, "Status": "SCRAPED", "Rating": rating, "Reviews Count": reviews,
            "Price Range": price, "Business Type": category, "Opening Hours": hours,
            "Lead Score": round(lead_score, 1), "Interest Score": round(engagement_score, 1), "Lead Tier": lead_tier,
            "Conversion Probability": f"{round(conversion_prob, 1)}%", "Engagement Score": round(engagement_score, 1),
            "Business Size": business_size, "Contact Quality": f"{contact_quality}%", "Decision Maker Available": "Unknown",
            "Outreach Priority": lead_tier, "Best Outreach Channel": "Email" if has_email else "Phone" if has_phone else "Social Media",
            "Response Likelihood": response_likelihood, "Insights": "; ".join(pain_points) if pain_points else "Well optimized",
            "Best Contact Time": "9 AM - 5 PM", "LTV Score": round(product_fit * 0.8, 1), "Competitor Risk": "Medium",
            "Urgency Score": round(urgency_score, 1), "Predicted Deal Size": deal_size,
            "Email Open Probability": f"{min(85, lead_score * 0.85)}%", "Buying Readiness": buying_readiness,
            "GBP Completeness": f"{round(gbp_completeness, 1)}%", "Competition Level": "Medium",
            "Market Position": "Established" if reviews_val > 50 else "Growing" if reviews_val > 10 else "New",
            "Growth Potential": "High" if total_needs >= 3 else "Medium" if total_needs >= 2 else "Low",
            "Digital Maturity": f"{round(digital_maturity, 1)}%", "Lead Cluster": f"Cluster-{lead_tier[0]}",
            "Anomaly Status": "Normal", "Missing Priority": "High" if not has_email else "Medium" if not has_phone else "Low",
            "DL Quality Score": round(contact_quality * 0.9, 1), "DL Conversion Prediction": f"{round(conversion_prob * 0.95, 1)}%",
            "Churn Risk": "Low" if lead_tier in ["Hot", "Warm"] else "Medium",
            "AI Confidence": f"{min(95, lead_score)}%", "AI Recommendation": ai_recommendation,
            "Product Needs": f"{total_needs} products identified", "Product Fit Score": round(product_fit, 1),
            "Pain Points": ", ".join(pain_points) if pain_points else "None identified", "Sales Pitch Angle": sales_angle,
            "Needs Website": needs_website, "Needs Online Ordering": needs_online_ordering,
            "Needs Delivery Service": needs_delivery, "Needs E-commerce": needs_ecommerce,
            "Needs Accounting": "Maybe", "Needs CRM": needs_crm, "Needs Booking System": needs_booking,
            "Needs Social Media": needs_social, "Needs SEO": needs_seo, "Needs Payment Gateway": "Maybe",
            "Needs Analytics": "Maybe", "Needs Cloud Storage": "Maybe", "Total Needs Count": total_needs,
            "Review Velocity": round(review_velocity, 2), "Customer Satisfaction Index": round(csi, 1),
            "Predicted Response Time": "24-48 hours" if lead_tier == "Hot" else "2-5 days",
            "Investment Readiness": buying_readiness, "Competitive Advantage": "Moderate",
            "Market Opportunity Score": round(product_fit * 0.9, 1), "Sentiment Score": round(csi * 0.95, 1),
            "Top Product 1": top_products[0][0], "Product 1 Score": f"{top_products[0][1]}%", "Product 1 Buy Probability": f"{round(buy_prob_1, 1)}%",
            "Top Product 2": top_products[1][0], "Product 2 Score": f"{top_products[1][1]}%", "Product 2 Buy Probability": f"{round(buy_prob_2, 1)}%",
            "Top Product 3": top_products[2][0], "Product 3 Score": f"{top_products[2][1]}%", "Product 3 Buy Probability": f"{round(buy_prob_3, 1)}%",
            "All Product Recommendations": " | ".join(top_5_products),
            "Predicted Monthly Revenue": f"${round(predicted_revenue, 0)}",
            "Best Products to Sell": ", ".join(top_3_products),
            "Sales Strategy": sales_strategy,
            "Objection Handling": objection_strategy,
            "Close Probability": f"{round((buy_prob_1 + buy_prob_2 + buy_prob_3) / 3, 1)}%",
            "Recommended Approach": f"Call within 24h" if lead_tier == "Hot" else f"Email first" if has_email else "Social media DM",
            "Upsell Potential": "High" if len([p for p in top_products if p[1] > 70]) >= 3 else "Medium" if len([p for p in top_products if p[1] > 70]) >= 2 else "Low",
            "Decision Timeline": "1-2 weeks" if lead_tier == "Hot" else "2-4 weeks" if lead_tier == "Warm" else "1-2 months",
            "Budget Estimate": f"${round(predicted_revenue * 1.2, 0)} - ${round(predicted_revenue * 1.5, 0)}",
            "Competitor Threat": "High" if reviews_val > 100 else "Medium" if reviews_val > 30 else "Low",
            "Win Probability": f"{round(min(90, (buy_prob_1 * 0.4 + lead_score * 0.6)), 1)}%"
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
    with ThreadPoolExecutor(max_workers=config.get("max_workers", 10)) as pool:
        futures = {pool.submit(scrape_business_entry, url, query, zipc, config.get("scrape_timeout", 15), config.get("headless_mode", True), config.get("proxy")): (url, query, zipc) for url, query, zipc in links}
        for i, fut in enumerate(as_completed(futures)):
            if session["stop_scraping_flag"]: break
            try: res = fut.result()
            except: res = None
            if res: results.append(res)
            update_status(session_id, f"Scraped {len(results)}/{len(links)}", detail_progress=(i+1)/len(links), scraped_count=len(results))
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
        else:
            update_status(session_id, "Ready for next job.")
    except Exception as e: 
        update_status(session_id, f"Error: {e}. Ready for next job.")
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
                            worksheet.write(row, email_col, df.iloc[row-2]['Final Email'], email_format)
                
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
