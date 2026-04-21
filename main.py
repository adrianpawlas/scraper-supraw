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
            'new': 0,
            'updated': 0,
            'unchanged': 0,
            'deleted': 0,
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
    
    def process_product(self, product_info: Dict, existing_products: Dict[str, Dict]) -> bool:
        """Process a single product: get details, generate embeddings, prepare for batch"""
        product_url = product_info.get('product_url')
        category = product_info.get('category')
        
        existing = existing_products.get(product_url)
        
        details = extract_product_details(product_url)
        
        if not details or not details.get('image_url'):
            return None
        
        details['category'] = category
        
        needs_embedding = False
        has_changes = False
        
        if existing is None:
            needs_embedding = True
            has_changes = True
        else:
            existing_url = self.supabase_manager.normalize_image_url(existing.get('image_url', ''))
            scraped_url = self.supabase_manager.normalize_image_url(details.get('image_url', ''))
            image_changed = scraped_url != existing_url
            if image_changed:
                needs_embedding = True
            
            has_changes = self.supabase_manager.compare_products(details, existing)
        
        if existing and not has_changes:
            self.stats['unchanged'] += 1
            return None
        
        if needs_embedding and existing is not None:
            print(f"  Regenerating embeddings for changed product: {details.get('title')}")
        
        if needs_embedding:
            print(f"  Generating image embedding...")
            time.sleep(0.5)
            image_embedding = self.embedding_generator.generate_image_embedding(
                details.get('image_url', '')
            )
            details['image_embedding'] = image_embedding
            
            print(f"  Generating info embedding...")
            time.sleep(0.5)
            info_text = self.embedding_generator.create_combined_info_text(details)
            info_embedding = self.embedding_generator.generate_text_embedding(info_text)
            details['info_embedding'] = info_embedding
        
        if existing is None:
            self.stats['new'] += 1
        else:
            self.stats['updated'] += 1
        
        product_data = self.supabase_manager.prepare_product_data(details, regenerate_embeddings=needs_embedding)
        
        return product_data
    
    def run(self):
        """Main run method"""
        print("="*60)
        print("SUPRAW SCRAPER STARTED")
        print(f"Start time: {datetime.now().isoformat()}")
        print("="*60)
        
        product_urls = self.scrape_all_collections()
        
        scraped_urls = [p.get('product_url') for p in product_urls]
        
        print(f"\nChecking existing products...")
        existing_products = self.supabase_manager.check_existing_products_batch(scraped_urls)
        print(f"Found {len(existing_products)} existing products")
        
        print(f"\nProcessing {len(product_urls)} products...")
        
        batch_data = []
        
        for i, product in enumerate(product_urls):
            print(f"\n[{i+1}/{len(product_urls)}] {product.get('product_url')}")
            
            try:
                result = self.process_product(product, existing_products)
                
                if isinstance(result, dict):
                    batch_data.append(result)
            except Exception as e:
                print(f"  Error processing product: {e}")
        
        if batch_data:
            print(f"\nInserting {len(batch_data)} products in batches...")
            inserted, failed = self.supabase_manager.batch_insert(batch_data)
            print(f"Successfully inserted/updated {inserted} products")
            if failed:
                print(f"Failed to insert {len(failed)} products")
        
        print(f"\nCleaning up stale products...")
        stale_products = self.supabase_manager.get_stale_product_urls(scraped_urls, consecutive_runs=2)
        
        if stale_products:
            deleted = self.supabase_manager.delete_products(stale_products)
            self.stats['deleted'] = deleted
            print(f"Deleted {deleted} stale products")
        else:
            print("No stale products found")
        
        self.supabase_manager.increment_stale_count(scraped_urls)
        self.supabase_manager.mark_products_seen(scraped_urls)
        
        print(f"\n{'='*60}")
        print("SCRAPING COMPLETE")
        print(f"{'='*60}")
        print(f"Total products scraped: {self.stats.get('total_products', 0)}")
        print(f"New products added: {self.stats['new']}")
        print(f"Products updated: {self.stats['updated']}")
        print(f"Products unchanged (skipped): {self.stats['unchanged']}")
        print(f"Stale products deleted: {self.stats['deleted']}")
        print(f"End time: {datetime.now().isoformat()}")


def main():
    scraper = SuprawScraper()
    scraper.run()


if __name__ == "__main__":
    main()
