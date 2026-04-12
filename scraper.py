import requests
from bs4 import BeautifulSoup
import json
import time
import re
from urllib.parse import urljoin
from typing import List, Dict, Optional
from config import HEADERS, CATEGORY_URLS


def get_page(url: str, max_retries: int = 3) -> Optional[BeautifulSoup]:
    """Fetch a page and return BeautifulSoup object"""
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=HEADERS, timeout=30)
            if response.status_code == 200:
                return BeautifulSoup(response.text, 'html.parser')
            elif response.status_code == 404:
                return None
        except Exception as e:
            print(f"Attempt {attempt + 1} failed for {url}: {e}")
            time.sleep(2)
    return None


def extract_products_from_collection(collection_url: str) -> List[Dict]:
    """Extract all product links from a collection page with pagination"""
    products = []
    page = 1
    
    while True:
        if page == 1:
            url = collection_url
        else:
            url = f"{collection_url}?page={page}"
        
        print(f"Scraping page {page}: {url}")
        soup = get_page(url)
        
        if soup is None:
            print(f"Page {page} not found, stopping")
            break
        
        product_links = soup.find_all('a', href=re.compile(r'/en/products/[^/]+$'))
        
        if not product_links:
            print(f"No products found on page {page}, stopping")
            break
        
        for link in product_links:
            href = link.get('href', '')
            product_url = urljoin('https://supraw.com', href)
            if product_url not in [p['product_url'] for p in products]:
                products.append({
                    'product_url': product_url,
                    'title': link.get_text(strip=True) if link.get_text(strip=True) else None
                })
        
        print(f"Found {len(product_links)} products on page {page}")
        page += 1
        time.sleep(1)
    
    return products


def parse_category_from_url(url: str) -> str:
    """Extract category name from collection URL"""
    match = re.search(r'/collections/([^/?]+)', url)
    if match:
        category = match.group(1).replace('-', ' ').replace('_', ' ').title()
        return category
    return ""


def extract_product_details(product_url: str) -> Optional[Dict]:
    """Extract all details from a product page"""
    soup = get_page(product_url)
    if soup is None:
        return None
    
    details = {
        'product_url': product_url,
        'title': None,
        'price': None,
        'sale': None,
        'description': None,
        'image_url': None,
        'additional_images': [],
        'sizes': [],
        'category': None,
        'gender': 'unisex'
    }
    
    scripts = soup.find_all('script', type='application/ld+json')
    for script in scripts:
        try:
            data = json.loads(script.string)
            
            if isinstance(data, dict):
                if data.get('@type') == 'ProductGroup' or data.get('@type') == 'Product':
                    details['title'] = data.get('name')
                    details['description'] = data.get('description')
                    
                    brand = data.get('brand')
                    if isinstance(brand, dict):
                        details['brand'] = brand.get('name')
                    
                    category = data.get('category')
                    if category:
                        details['category'] = category
                    
                    variants = data.get('hasVariant', [])
                    if isinstance(variants, list) and variants:
                        first_variant = variants[0] if isinstance(variants[0], dict) else {}
                        
                        offers = first_variant.get('offers', {})
                        if offers:
                            if isinstance(offers, list):
                                offers = offers[0] if offers else {}
                            if isinstance(offers, dict):
                                details['price'] = offers.get('price') or offers.get('highPrice') or offers.get('lowPrice')
                                details['sale'] = offers.get('salePrice') or details['price']
                                currency = offers.get('priceCurrency', 'EUR')
                                if details['price']:
                                    details['price'] = f"{details['price']} {currency}"
                                if details['sale']:
                                    details['sale'] = f"{details['sale']} {currency}"
                        
                        images = first_variant.get('image')
                        if images:
                            if isinstance(images, list):
                                if images:
                                    details['image_url'] = urljoin('https://supraw.com', images[0].replace('&width=4000', '&width=1200'))
                                    for img in images[1:]:
                                        details['additional_images'].append(urljoin('https://supraw.com', img.replace('&width=4000', '&width=1200')))
                            elif isinstance(images, str):
                                details['image_url'] = urljoin('https://supraw.com', images.replace('&width=4000', '&width=1200'))
                    
                    if not details['image_url']:
                        images = data.get('image')
                        if images:
                            if isinstance(images, list):
                                if images:
                                    details['image_url'] = urljoin('https://supraw.com', images[0].replace('&width=4000', '&width=1200'))
                                    for img in images[1:]:
                                        details['additional_images'].append(urljoin('https://supraw.com', img.replace('&width=4000', '&width=1200')))
                            elif isinstance(images, str):
                                details['image_url'] = urljoin('https://supraw.com', images.replace('&width=4000', '&width=1200'))
                    
                    if not details['price']:
                        offers = data.get('offers')
                        if offers:
                            if isinstance(offers, list):
                                offers = offers[0] if offers else {}
                            if isinstance(offers, dict):
                                details['price'] = offers.get('price') or offers.get('highPrice') or offers.get('lowPrice')
                                details['sale'] = offers.get('salePrice') or details['price']
                                currency = offers.get('priceCurrency', 'EUR')
                                if details['price']:
                                    details['price'] = f"{details['price']} {currency}"
                                if details['sale']:
                                    details['sale'] = f"{details['sale']} {currency}"
        except (json.JSONDecodeError, AttributeError, TypeError) as e:
            continue
    
    if not details['title']:
        title_elem = soup.find('h1')
        if title_elem:
            details['title'] = title_elem.get_text(strip=True)
    
    if not details['description']:
        desc_elem = soup.find('div', {'id': 'product-description'})
        if desc_elem:
            details['description'] = desc_elem.get_text(strip=True)
    
    if not details['description']:
        desc_elem = soup.find('div', {'class': 'product__description'})
        if desc_elem:
            details['description'] = desc_elem.get_text(strip=True)
    
    if not details['image_url']:
        image_container = soup.find('media-slider') or soup.find('div', {'class': 'product__media'})
        if image_container:
            images = image_container.find_all('img')
            if images:
                first_img = images[0].get('src') or images[0].get('data-src')
                if first_img:
                    details['image_url'] = urljoin('https://supraw.com', first_img.replace('&width=4000', '&width=1200'))
                
                for img in images[1:]:
                    src = img.get('src') or img.get('data-src')
                    if src:
                        details['additional_images'].append(
                            urljoin('https://supraw.com', src.replace('&width=4000', '&width=1200'))
                        )
    
    if not details['image_url']:
        main_img = soup.find('img', {'id': 'main-image'})
        if main_img:
            src = main_img.get('src') or main_img.get('data-src')
            if src:
                details['image_url'] = urljoin('https://supraw.com', src.replace('&width=4000', '&width=1200'))
    
    size_buttons = soup.find_all('button', {'class': 'button-size'})
    if size_buttons:
        details['sizes'] = [btn.get_text(strip=True) for btn in size_buttons]
    
    if not details['sizes']:
        size_select = soup.find('select', {'class': 'select-size'})
        if size_select:
            options = size_select.find_all('option')
            details['sizes'] = [opt.get_text(strip=True) for opt in options if opt.get('value')]
    
    size_labels = soup.find_all('label', {'class': 'size-label'})
    if not details['sizes'] and size_labels:
        details['sizes'] = [label.get_text(strip=True) for label in size_labels]
    
    product_id = product_url.split('/products/')[-1].split('?')[0] if '/products/' in product_url else None
    details['id'] = product_id
    
    if not details['sale']:
        details['sale'] = details['price']
    
    return details


def scrape_all_products() -> List[Dict]:
    """Main function to scrape all products from all categories"""
    all_products = []
    
    for category_url in CATEGORY_URLS:
        category_name = parse_category_from_url(category_url)
        print(f"\n{'='*50}")
        print(f"Scraping category: {category_name}")
        print(f"{'='*50}")
        
        products = extract_products_from_collection(category_url)
        
        for product in products:
            product['category'] = category_name
        
        all_products.extend(products)
        print(f"Found {len(products)} products in {category_name}")
    
    print(f"\nTotal products to scrape: {len(all_products)}")
    return all_products


if __name__ == "__main__":
    products = scrape_all_products()
    print(f"\nTotal products scraped from collections: {len(products)}")
