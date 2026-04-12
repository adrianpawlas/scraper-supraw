import time
import json
from datetime import datetime
from typing import Dict, List
from config import CATEGORY_URLS
from scraper import extract_products_from_collection, extract_product_details, parse_category_from_url
from embeddings import EmbeddingGenerator
from supabase_manager import SupabaseManager


class SuprawScraper:
    def __init__(self):
        self.embedding_generator = EmbeddingGenerator()
        self.supabase_manager = SupabaseManager()
        self.stats = {
            'total_products': 0,
            'successful': 0,
            'failed': 0,
            'skipped': 0,
        }
    
    def scrape_all_collections(self) -> List[Dict]:
        """Scrape all products from all configured collections"""
        all_products = []
        
        for category_url in CATEGORY_URLS:
            category_name = parse_category_from_url(category_url)
            print(f"\n{'='*60}")
            print(f"COLLECTION: {category_name}")
            print(f"{'='*60}")
            
            products = extract_products_from_collection(category_url)
            
            for product in products:
                product['category'] = category_name
            
            all_products.extend(products)
            print(f"Found {len(products)} products in {category_name}")
        
        self.stats['total_products'] = len(all_products)
        print(f"\n{'='*60}")
        print(f"TOTAL PRODUCTS TO PROCESS: {len(all_products)}")
        print(f"{'='*60}\n")
        
        return all_products
    
    def process_product(self, product_info: Dict) -> bool:
        """Process a single product: get details, generate embeddings, insert to DB"""
        product_url = product_info.get('product_url')
        category = product_info.get('category')
        
        print(f"\nProcessing: {product_url}")
        
        existing = self.supabase_manager.check_existing_product(product_url)
        if existing:
            print(f"  Product already in database, skipping")
            self.stats['skipped'] += 1
            return True
        
        details = extract_product_details(product_url)
        
        if not details or not details.get('image_url'):
            print(f"  Failed to get product details or image")
            self.stats['failed'] += 1
            return False
        
        details['category'] = category
        
        print(f"  Title: {details.get('title')}")
        print(f"  Price: {details.get('price')}")
        print(f"  Image: {details.get('image_url')[:50]}...")
        
        print(f"  Generating image embedding...")
        image_embedding = self.embedding_generator.generate_image_embedding(
            details.get('image_url', '')
        )
        
        print(f"  Generating info embedding...")
        info_text = self.embedding_generator.create_combined_info_text(details)
        info_embedding = self.embedding_generator.generate_text_embedding(info_text)
        
        product_data = self.supabase_manager.prepare_product_data(details)
        
        product_data['image_embedding'] = json.dumps(image_embedding)
        product_data['info_embedding'] = json.dumps(info_embedding)
        
        success = self.supabase_manager.insert_product(product_data)
        
        if success:
            self.stats['successful'] += 1
            print(f"  Successfully inserted to database")
        else:
            self.stats['failed'] += 1
        
        return success
    
    def run(self):
        """Main run method"""
        print("="*60)
        print("SUPRAW SCRAPER STARTED")
        print(f"Start time: {datetime.now().isoformat()}")
        print("="*60)
        
        product_urls = self.scrape_all_collections()
        
        print(f"\nProcessing {len(product_urls)} products...")
        
        for i, product in enumerate(product_urls):
            print(f"\n[{i+1}/{len(product_urls)}]")
            
            try:
                self.process_product(product)
            except Exception as e:
                print(f"  Error processing product: {e}")
                self.stats['failed'] += 1
            
            time.sleep(1)
        
        print(f"\n{'='*60}")
        print("SCRAPING COMPLETE")
        print(f"{'='*60}")
        print(f"Total products: {self.stats['total_products']}")
        print(f"Successful: {self.stats['successful']}")
        print(f"Failed: {self.stats['failed']}")
        print(f"Skipped: {self.stats['skipped']}")
        print(f"End time: {datetime.now().isoformat()}")


def main():
    scraper = SuprawScraper()
    scraper.run()


if __name__ == "__main__":
    main()
