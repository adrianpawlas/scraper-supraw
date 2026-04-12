import torch
from transformers import AutoModel, AutoProcessor
from PIL import Image
import requests
from io import BytesIO
from typing import List, Union
import numpy as np
from config import EMBEDDING_MODEL, EMBEDDING_DIMENSION


class EmbeddingGenerator:
    def __init__(self):
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        print(f"Loading embedding model: {EMBEDDING_MODEL}")
        print(f"Using device: {self.device}")
        
        self.model = AutoModel.from_pretrained(EMBEDDING_MODEL)
        self.processor = AutoProcessor.from_pretrained(EMBEDDING_MODEL)
        self.model.to(self.device)
        self.model.eval()
    
    def load_image_from_url(self, url: str) -> Image.Image:
        """Load image from URL"""
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return Image.open(BytesIO(response.content)).convert('RGB')
    
    def generate_image_embedding(self, image_url: str) -> List[float]:
        """Generate 768-dim embedding from image URL using vision model"""
        try:
            image = self.load_image_from_url(image_url)
            
            inputs = self.processor(images=image, return_tensors="pt")
            pixel_values = inputs['pixel_values'].to(self.device)
            
            with torch.no_grad():
                vision_outputs = self.model.vision_model(pixel_values=pixel_values)
                embedding = vision_outputs.pooler_output
            
            embedding = embedding.squeeze().cpu().numpy()
            
            if embedding.ndim == 0:
                embedding = np.array([embedding.item()])
            
            return embedding.tolist()
        
        except Exception as e:
            print(f"Error generating image embedding for {image_url}: {e}")
            return [0.0] * EMBEDDING_DIMENSION
    
    def generate_text_embedding(self, text: str) -> List[float]:
        """Generate 768-dim embedding from text using text model"""
        try:
            inputs = self.processor(text=text, return_tensors="pt", padding=True, truncation=True)
            input_ids = inputs['input_ids'].to(self.device)
            
            with torch.no_grad():
                text_outputs = self.model.text_model(input_ids=input_ids)
                embedding = text_outputs.pooler_output
            
            embedding = embedding.squeeze().cpu().numpy()
            
            if embedding.ndim == 0:
                embedding = np.array([embedding.item()])
            
            return embedding.tolist()
        
        except Exception as e:
            print(f"Error generating text embedding: {e}")
            return [0.0] * EMBEDDING_DIMENSION
    
    def batch_generate_image_embeddings(self, image_urls: List[str]) -> List[List[float]]:
        """Generate embeddings for multiple images"""
        embeddings = []
        for url in image_urls:
            emb = self.generate_image_embedding(url)
            embeddings.append(emb)
        return embeddings
    
    def create_combined_info_text(self, product_data: dict) -> str:
        """Create combined text for info embedding"""
        parts = []
        
        if product_data.get('title'):
            parts.append(f"Title: {product_data['title']}")
        
        if product_data.get('price'):
            parts.append(f"Price: {product_data['price']}")
        
        if product_data.get('category'):
            parts.append(f"Category: {product_data['category']}")
        
        if product_data.get('gender'):
            parts.append(f"Gender: {product_data['gender']}")
        
        if product_data.get('description'):
            parts.append(f"Description: {product_data['description']}")
        
        if product_data.get('sizes'):
            sizes_str = ", ".join(product_data['sizes'])
            parts.append(f"Sizes: {sizes_str}")
        
        if product_data.get('metadata'):
            parts.append(f"Metadata: {product_data['metadata']}")
        
        return " | ".join(parts)


def main():
    generator = EmbeddingGenerator()
    
    test_image_url = "https://supraw.com/cdn/shop/files/IMG_1037_2.jpg?v=1772995431&width=1200"
    print(f"Testing image embedding for: {test_image_url}")
    img_emb = generator.generate_image_embedding(test_image_url)
    print(f"Image embedding dimension: {len(img_emb)}")
    print(f"First 5 values: {img_emb[:5]}")
    
    test_text = "FOLDED DENIM PANT Price 3,107.00 Kč Category Pants Unisex"
    print(f"\nTesting text embedding for: {test_text}")
    text_emb = generator.generate_text_embedding(test_text)
    print(f"Text embedding dimension: {len(text_emb)}")
    print(f"First 5 values: {text_emb[:5]}")


if __name__ == "__main__":
    main()
