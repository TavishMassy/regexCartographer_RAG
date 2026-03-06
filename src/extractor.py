import os
import fitz
import docx
import shutil
import pymupdf4llm  
import pandas as pd
from pathlib import Path
from rich.progress import track

class Cartographer:
    def __init__(self) -> None:
        self.supported = {'.pdf', '.docx', '.txt'}

    def _extract_pdf_chunks(self, file_path, link) -> list:
        """PDFs get 'shredded' page-by-page. Metadata appended to content."""
        source_name = Path(file_path).name
        try:
            pages = pymupdf4llm.to_markdown(file_path, page_chunks=True)
            chunks = []
            for page in pages:
                page_num = page.get("metadata", {}).get("page", 0) + 1
                text = page.get("text", "").strip()
                
                # Append metadata directly to the text block
                content_with_meta = f"{text}\n\n--- Metadata ---\nPage: {page_num}\nSource: {source_name}\nLink: {link}"
                
                chunks.append({
                    "source": source_name,
                    "content": content_with_meta
                })
            return chunks
        except Exception:
            # Fallback to standard PyMuPDF if markdown fails
            doc = fitz.open(file_path)
            return [{
                "source": source_name,
                "content": f"{page.get_text().strip()}\n\n--- Metadata ---\nPage: {i+1}\nSource: {source_name}\nLink: {link}"
            } for i, page in enumerate(doc)]

    def _extract_docx_chunks(self, file_path, link) -> list:
        """DOCX files are split by paragraphs."""
        source_name = Path(file_path).name
        doc = docx.Document(file_path)
        chunks = []
        paragraphs = [p.text for p in doc.paragraphs if len(p.text.strip()) > 20]
        
        for i in range(0, len(paragraphs), 5):
            combined_text = "\n\n".join(paragraphs[i:i+5])
            content_with_meta = f"{combined_text}\n\n--- Metadata ---\nSource: {source_name}\nLink: {link}"
            
            chunks.append({
                "source": source_name,
                "content": content_with_meta
            })
        return chunks

    def _extract_txt_chunks(self, file_path, link) -> list:
        """TXT files are split by character count."""
        source_name = Path(file_path).name
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            text = f.read()
            size = 1000
            chunks = [text[i:i+size] for i in range(0, len(text), size)]
            
            return [{
                "source": source_name,
                "content": f"{c}\n\n--- Metadata ---\nSource: {source_name}\nLink: {link}"
            } for c in chunks]

    def run(self, source_folder, output_file, quarantine_folder) -> pd.DataFrame:
        all_data = []
        folder = Path(source_folder)
        files = [(f, input(f'Link for {f.name}: ')) for f in folder.iterdir() if f.suffix.lower() in self.supported]
        
        print(f"⚔️  The Arsenal: Shredding {len(files)} policy documents...")

        for f, link in track(files, description="[cyan]Processing..."):
            ext = f.suffix.lower()
            
            if ext == '.pdf':
                chunks = self._extract_pdf_chunks(f, link)
            elif ext == '.docx':
                chunks = self._extract_docx_chunks(f, link)
            elif ext == '.txt':
                chunks = self._extract_txt_chunks(f, link)
            else:
                # Fixed quarantine copy logic
                q_path = Path(quarantine_folder)
                q_path.mkdir(parents=True, exist_ok=True)
                shutil.copy(f, q_path / f.name)
                continue # Skip extending all_data for quarantined files
            
            all_data.extend(chunks)

        df = pd.DataFrame(all_data)
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_parquet(output_file, index=False)
        print(f"✅ Mission Accomplished: {len(df)} chunks ready for The Vault.")
        return df

if __name__ == "__main__":
    cartographer = Cartographer()
    # Note: Corrected 'quarentine' to 'quarantine' spelling in the folder name below
    cartographer.run("data/files", "data/result/data.parquet", "data/quarantine")