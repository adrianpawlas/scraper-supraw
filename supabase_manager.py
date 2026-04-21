from supabase import create_client, Client
from typing import Dict, List, Optional
import json
import time
from datetime import datetime, timedelta
from config import SUPABASE_URL, SUPABASE_KEY, SCRAPER_SOURCE, BRAND


class SupabaseManager:
    def __init__(self):
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.source = SCRAPER_SOURCE
        self.brand = BRAND
        self.batch_size = 50
    
    def check_existing_product(self, product_url: str) -> Optional[Dict]:
        """Check if product already exists in database"""
        response = self.client.table('products').select('*').eq('source', self.source).eq('product_url', product_url).execute()
        
        if response.data and len(response.data) > 0:
            return response.data[0]
        return None
    
    def check_existing_products_batch(self, product_urls: List[str]) -> Dict[str, Dict]:
        """Check multiple products at once - returns dict of url -> existing data"""
        if not product_urls:
            return {}
        
        response = self.client.table('products').select('*').eq('source', self.source).in_('product_url', product_urls).execute()
        
        result = {}
        for item in response.data:
            result[item['product_url']] = item
        return result
    
    def get_all_product_urls(self) -> List[str]:
        """Get all product URLs for this source - used for stale detection"""
        response = self.client.table('products').select('product_url').eq('source', self.source).execute()
        return [item['product_url'] for item in response.data]
    
    def get_stale_product_urls(self, seen_urls: List[str], consecutive_runs: int = 2) -> List[str]:
        """Get products marked as stale (not seen in X consecutive runs)
        Note: Uses metadata JSON to track stale_count since column may not exist"""
        seen_set = set(seen_urls)
        
        response = self.client.table('products').select('product_url, metadata').eq('source', self.source).execute()
        
        stale_urls = []
        for item in response.data:
            product_url = item['product_url']
            metadata = item.get('metadata')
            
            stale_count = 0
            if metadata:
                try:
                    meta = json.loads(metadata) if isinstance(metadata, str) else metadata
                    stale_count = meta.get('stale_count', 0)
                except:
                    pass
            
            if product_url not in seen_set:
                new_count = stale_count + 1
                if new_count >= consecutive_runs:
                    stale_urls.append(product_url)
            
            new_metadata = meta if metadata else {}
            if not isinstance(new_metadata, dict):
                new_metadata = {}
            new_metadata['stale_count'] = stale_count + 1 if product_url not in seen_set else 0
            
            try:
                self.client.table('products').update({'metadata': json.dumps(new_metadata)}).eq('source', self.source).eq('product_url', product_url).execute()
            except:
                pass
        
        return stale_urls
    
    def mark_products_seen(self, product_urls: List[str]) -> None:
        """Reset stale_count for products seen in this run - uses metadata"""
        if not product_urls:
            return
        
        for url in product_urls:
            try:
                response = self.client.table('products').select('metadata').eq('source', self.source).eq('product_url', url).execute()
                if response.data:
                    metadata = response.data[0].get('metadata')
                    meta = {}
                    if metadata:
                        try:
                            meta = json.loads(metadata) if isinstance(metadata, str) else metadata
                        except:
                            pass
                    if not isinstance(meta, dict):
                        meta = {}
                    meta['stale_count'] = 0
                    meta['last_seen'] = datetime.utcnow().isoformat()
                    self.client.table('products').update({'metadata': json.dumps(meta)}).eq('source', self.source).eq('product_url', url).execute()
            except:
                pass
    
    def increment_stale_count(self, product_urls: List[str]) -> None:
        """Increment stale_count for products not seen in this run - uses metadata"""
        if not product_urls:
            return
        
        seen_set = set(product_urls)
        
        try:
            response = self.client.table('products').select('product_url, metadata').eq('source', self.source).execute()
            
            for item in response.data:
                product_url = item['product_url']
                
                if product_url not in seen_set:
                    metadata = item.get('metadata')
                    meta = {}
                    if metadata:
                        try:
                            meta = json.loads(metadata) if isinstance(metadata, str) else metadata
                        except:
                            pass
                    if not isinstance(meta, dict):
                        meta = {}
                    
                    current_count = meta.get('stale_count', 0)
                    meta['stale_count'] = current_count + 1
                    
                    try:
                        self.client.table('products').update({'metadata': json.dumps(meta)}).eq('source', self.source).eq('product_url', product_url).execute()
                    except:
                        pass
        except:
            pass
    
    def parse_price(self, price_text: str) -> str:
        """Convert price to format 'amountCURRENCY'"""
        if not price_text:
            return ""
        
        import re
        price_text = price_text.strip()
        
        patterns = [
            (r'([\d.,]+)\s*€', 'EUR'),
            (r'([\d.,]+)\s*Kč', 'CZK'),
            (r'([\d.,]+)\s*£', 'GBP'),
            (r'([\d.,]+)\s*\$', 'USD'),
            (r'([\d.,]+)\s*kr\.', 'SEK'),
            (r'([\d.,]+)\s*CHF', 'CHF'),
            (r'([\d.,]+)\s*PLN', 'PLN'),
        ]
        
        prices = []
        for pattern, currency in patterns:
            matches = re.findall(pattern, price_text, re.IGNORECASE)
            for match in matches:
                amount = match.replace(',', '.')
                try:
                    float(amount)
                    prices.append(f"{amount}{currency}")
                except:
                    pass
        
        return ','.join(prices) if prices else price_text
    
    def format_additional_images(self, images: List[str]) -> str:
        """Format additional images as comma-separated string"""
        if not images:
            return ""
        return " , ".join(images)
    
    def parse_category(self, category: str) -> str:
        """Parse and format category - separate with commas"""
        if not category:
            return ""
        
        category = category.strip()
        separators = ['&', ' and ', '/']
        parts = [category]
        
        for sep in separators:
            new_parts = []
            for part in parts:
                new_parts.extend(part.split(sep))
            parts = new_parts
        
        categories = [part.strip() for part in parts if part.strip()]
        return ', '.join(categories)
    
    def normalize_image_url(self, url: str) -> str:
        """Normalize image URL for comparison - remove width params and normalize domain/path"""
        if not url:
            return ""
        
        url = url.split('?')[0]
        
        url = url.replace('https://cdn.shopify.com/s/files/', 'https://supraw.com/cdn/shop/files/')
        url = url.replace('https://cdn.shopify.com/files/', 'https://supraw.com/cdn/shop/files/')
        
        import re
        match = re.search(r'files/([^/]+)$', url)
        if match:
            filename = match.group(1)
            return f"https://supraw.com/cdn/shop/files/{filename}"
        
        return url
    
    def compare_products(self, scraped: Dict, existing: Dict) -> bool:
        """Compare scraped data against existing - return True if changed"""
        fields_to_check = ['title', 'price', 'sale', 'description']
        
        for field in fields_to_check:
            scraped_val = scraped.get(field)
            existing_val = existing.get(field)
            
            if scraped_val != existing_val:
                return True
        
        scraped_sizes = ', '.join(scraped.get('sizes', [])) if scraped.get('sizes') else None
        existing_size = existing.get('size')
        
        if scraped_sizes != existing_size:
            return True
        
        scraped_img = self.normalize_image_url(scraped.get('image_url', ''))
        existing_img = self.normalize_image_url(existing.get('image_url', ''))
        
        if scraped_img != existing_img:
            return True
        
        return False
    
    def prepare_product_data(self, product_details: Dict, regenerate_embeddings: bool = False) -> Dict:
        """Prepare product data for database insertion"""
        product_id = product_details.get('id', product_details.get('product_url', '').split('/products/')[-1])
        
        if not product_id:
            product_id = str(hash(product_details.get('product_url', '')))[:16]
        
        price_raw = product_details.get('price', '')
        sale_raw = product_details.get('sale', '')
        
        price = self.parse_price(price_raw)
        sale = self.parse_price(sale_raw) if sale_raw else price
        
        additional_images = product_details.get('additional_images', [])
        additional_images_str = self.format_additional_images(additional_images)
        
        metadata = {
            'name': product_details.get('title'),
            'description': product_details.get('description'),
            'sizes': product_details.get('sizes'),
            'price': price_raw,
            'sale': sale_raw,
            'category': product_details.get('category'),
            'gender': product_details.get('gender'),
            'stale_count': 0,
            'last_seen': datetime.utcnow().isoformat(),
        }
        
        category = self.parse_category(product_details.get('category', ''))
        
        data = {
            'id': product_id,
            'source': self.source,
            'product_url': product_details.get('product_url'),
            'affiliate_url': None,
            'image_url': product_details.get('image_url', ''),
            'brand': self.brand,
            'title': product_details.get('title', ''),
            'description': product_details.get('description'),
            'category': category,
            'gender': product_details.get('gender', 'unisex'),
            'metadata': json.dumps(metadata),
            'size': ', '.join(product_details.get('sizes', [])) if product_details.get('sizes') else None,
            'second_hand': False,
            'country': 'Czechia',
            'price': price,
            'sale': sale,
            'additional_images': additional_images_str,
            'created_at': datetime.utcnow().isoformat(),
        }
        
        if regenerate_embeddings and product_details.get('image_embedding'):
            if isinstance(product_details['image_embedding'], list):
                data['image_embedding'] = json.dumps(product_details['image_embedding'])
            else:
                data['image_embedding'] = product_details['image_embedding']
        
        if regenerate_embeddings and product_details.get('info_embedding'):
            if isinstance(product_details['info_embedding'], list):
                data['info_embedding'] = json.dumps(product_details['info_embedding'])
            else:
                data['info_embedding'] = product_details['info_embedding']
        
        return data
    
    def batch_insert(self, products: List[Dict], max_retries: int = 3) -> tuple:
        """Insert/update products in batches with retry logic"""
        if not products:
            return 0, []
        
        failed = []
        
        for i in range(0, len(products), self.batch_size):
            batch = products[i:i + self.batch_size]
            retry_count = 0
            
            while retry_count < max_retries:
                try:
                    response = self.client.table('products').upsert(batch, on_conflict='source, product_url').execute()
                    
                    if response.data:
                        break
                    retry_count += 1
                except Exception as e:
                    retry_count += 1
                    if retry_count >= max_retries:
                        failed.extend([p.get('product_url', 'unknown') for p in batch])
                        self.log_failed_products(batch, str(e))
                    else:
                        time.sleep(1)
        
        return len(products) - len(failed), failed
    
    def log_failed_products(self, products: List[Dict], error: str) -> None:
        """Log failed products to file"""
        import os
        from datetime import datetime
        
        log_dir = os.path.join(os.path.dirname(__file__), 'logs')
        os.makedirs(log_dir, exist_ok=True)
        
        log_file = os.path.join(log_dir, 'failed_products.log')
        
        timestamp = datetime.now().isoformat()
        with open(log_file, 'a') as f:
            f.write(f"\n--- Failed at {timestamp} ---\n")
            f.write(f"Error: {error}\n")
            for p in products:
                f.write(f"Product: {p.get('product_url', 'unknown')}\n")
    
    def delete_products(self, product_urls: List[str]) -> int:
        """Delete products by URLs"""
        if not product_urls:
            return 0
        
        deleted = 0
        for url in product_urls:
            try:
                self.client.table('products').delete().eq('source', self.source).eq('product_url', url).execute()
                deleted += 1
            except Exception as e:
                print(f"Failed to delete {url}: {e}")
        
        return deleted
    
    def process_products_batch(self, products_with_details: List[Dict], existing_products: Dict[str, Dict]) -> tuple:
        """Process products and determine what to insert/update/skip"""
        new_products = []
        products_to_update = []
        unchanged_products = []
        
        for product in products_with_details:
            product_url = product.get('product_url')
            existing = existing_products.get(product_url)
            
            needs_embedding = False
            should_skip = False
            
            if existing is None:
                needs_embedding = True
                new_products.append(product)
            else:
                image_changed = product.get('image_url') != existing.get('image_url')
                
                if image_changed:
                    needs_embedding = True
                
                has_changes = self.compare_products(product, existing)
                
                if has_changes:
                    products_to_update.append(product)
                else:
                    unchanged_products.append(product)
                    should_skip = True
            
            product['needs_embedding'] = needs_embedding
            product['should_skip'] = should_skip
        
        return new_products, products_to_update, unchanged_products


def main():
    manager = SupabaseManager()
    
    test_product = {
        'id': 'test-product-1',
        'product_url': 'https://supraw.com/en/products/test-product',
        'image_url': 'https://supraw.com/cdn/shop/files/test.jpg',
        'title': 'Test Product',
        'description': 'Test description',
        'category': 'Test Category',
        'gender': 'unisex',
        'price': '100 EUR',
        'sale': '80 EUR',
        'sizes': ['S', 'M', 'L'],
        'additional_images': ['https://example.com/img1.jpg', 'https://example.com/img2.jpg'],
    }
    
    data = manager.prepare_product_data(test_product)
    print("Prepared product data:")
    for key, value in data.items():
        if key not in ['metadata']:
            print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
