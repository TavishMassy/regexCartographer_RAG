import os
import fitz
import docx
import hashlib
import pymupdf4llm  
import pandas as pd
from pathlib import Path
from rich.progress import track

class PolicyCartographer:
    def __init__(self) -> None:
        self.supported = {'.pdf', '.docx', '.txt'}

    def get_hash(self, file_path) -> str:
        """Creates a unique ID based on file CONTENT."""
        hasher = hashlib.md5()
        with open(file_path, 'rb') as f:
            buf = f.read(65536)
            while len(buf) > 0:
                hasher.update(buf)
                buf = f.read(65536)
        return hasher.hexdigest()

    def _extract_pdf_chunks(self, file_path, file_id) -> list:
        """PDFs get 'shredded' page-by-page for high-precision RAG."""
        try:
            # page_chunks=True allows us to keep the Page Number for citations
            pages = pymupdf4llm.to_markdown(file_path, page_chunks=True)
            chunks = []
            for page in pages:
                page_num = page.get("metadata", {}).get("page", 0) + 1
                chunks.append({
                    "chunk_id": f"{file_id}_p{page_num}",
                    "content": page.get("text", "").strip(),
                    "metadata": {"page": page_num, "source": Path(file_path).name}
                })
            return chunks
        except Exception:
            # Fallback to standard PyMuPDF if markdown fails
            doc = fitz.open(file_path)
            return [{
                "chunk_id": f"{file_id}_p{i+1}",
                "content": page.get_text(),
                "metadata": {"page": i+1, "source": Path(file_path).name}
            } for i, page in enumerate(doc)]

    def _extract_docx_chunks(self, file_path, file_id) -> list:
        """DOCX files are split by paragraphs to maintain context."""
        doc = docx.Document(file_path)
        chunks = []
        # Grouping every 5 paragraphs to create a 'chunk'
        paragraphs = [p.text for p in doc.paragraphs if len(p.text.strip()) > 20]
        for i in range(0, len(paragraphs), 5):
            combined_text = "\n\n".join(paragraphs[i:i+5])
            chunks.append({
                "chunk_id": f"{file_id}_c{i//5}",
                "content": combined_text,
                "metadata": {"page": "N/A (Docx)", "source": Path(file_path).name}
            })
        return chunks

    def _extract_txt_chunks(self, file_path, file_id) -> list:
        """TXT files are split by character count (approx 1000 chars)."""
        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
            text = f.read()
            # Simple chunking for flat text
            size = 1000
            chunks = [text[i:i+size] for i in range(0, len(text), size)]
            return [{
                "chunk_id": f"{file_id}_t{i}",
                "content": c,
                "metadata": {"page": "N/A (Txt)", "source": Path(file_path).name}
            } for i, c in enumerate(chunks)]

    def run(self, source_folder, output_file) -> pd.DataFrame:
        all_data = []
        folder = Path(source_folder)
        files = [f for f in folder.iterdir() if f.suffix.lower() in self.supported]
        
        print(f"⚔️  The Arsenal: Shredding {len(files)} policy documents...")

        for f in track(files, description="[cyan]Processing..."):
            file_id = self.get_hash(f)
            ext = f.suffix.lower()
            
            if ext == '.pdf':
                chunks = self._extract_pdf_chunks(f, file_id)
            elif ext == '.docx':
                chunks = self._extract_docx_chunks(f, file_id)
            else: # .txt
                chunks = self._extract_txt_chunks(f, file_id)
            
            all_data.extend(chunks)

        df = pd.DataFrame(all_data)
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_parquet(output_file, index=False)
        print(f"✅ Mission Accomplished: {len(df)} chunks ready for The Vault.")
        return df

if __name__ == "__main__":
    cartographer = PolicyCartographer()
    cartographer.run("data/files", "data/result/data.parquet")