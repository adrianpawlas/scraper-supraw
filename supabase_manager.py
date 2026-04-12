from supabase import create_client, Client
from typing import Dict, List, Optional
import json
from datetime import datetime
from config import SUPABASE_URL, SUPABASE_KEY, SCRAPER_SOURCE, BRAND


class SupabaseManager:
    def __init__(self):
        self.client: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        self.source = SCRAPER_SOURCE
        self.brand = BRAND
    
    def check_existing_product(self, product_url: str) -> Optional[Dict]:
        """Check if product already exists in database"""
        response = self.client.table('products').select('*').eq('source', self.source).eq('product_url', product_url).execute()
        
        if response.data and len(response.data) > 0:
            return response.data[0]
        return None
    
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
            (r'([\d.,]+)\s*Kč', 'CZK'),
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
        
        categories = []
        for part in parts:
            part = part.strip()
            if part:
                categories.append(part)
        
        return ', '.join(categories)
    
    def prepare_product_data(self, product_details: Dict) -> Dict:
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
        }
        
        category = self.parse_category(product_details.get('category', ''))
        
        image_embedding = product_details.get('image_embedding')
        info_embedding = product_details.get('info_embedding')
        
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
        
        if image_embedding:
            if isinstance(image_embedding, list):
                data['image_embedding'] = json.dumps(image_embedding)
            else:
                data['image_embedding'] = image_embedding
        
        if info_embedding:
            if isinstance(info_embedding, list):
                data['info_embedding'] = json.dumps(info_embedding)
            else:
                data['info_embedding'] = info_embedding
        
        return data
    
    def insert_product(self, product_data: Dict) -> bool:
        """Insert a single product into the database"""
        try:
            response = self.client.table('products').insert(product_data).execute()
            return True
        except Exception as e:
            error_str = str(e)
            if 'duplicate key' in error_str.lower() or 'unique constraint' in error_str.lower():
                print(f"Product already exists: {product_data.get('title')}")
            else:
                print(f"Error inserting product {product_data.get('title')}: {e}")
            return False
    
    def update_product(self, product_url: str, product_data: Dict) -> bool:
        """Update an existing product"""
        try:
            response = self.client.table('products').update(product_data).eq('source', self.source).eq('product_url', product_url).execute()
            return True
        except Exception as e:
            print(f"Error updating product: {e}")
            return False
    
    def upsert_product(self, product_details: Dict) -> bool:
        """Insert or update a product"""
        existing = self.check_existing_product(product_details.get('product_url', ''))
        
        product_data = self.prepare_product_data(product_details)
        
        if existing:
            print(f"Product already exists, updating: {product_data.get('title')}")
            return self.update_product(product_details.get('product_url', ''), product_data)
        else:
            print(f"Inserting new product: {product_data.get('title')}")
            return self.insert_product(product_data)
    
    def batch_insert(self, products: List[Dict]) -> int:
        """Insert multiple products"""
        success_count = 0
        
        for product in products:
            if self.upsert_product(product):
                success_count += 1
        
        return success_count


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
        if key != 'metadata':
            print(f"  {key}: {value}")


if __name__ == "__main__":
    main()
