import json
import pandas as pd
from rich.table import Table
from rich.console import Console

console = Console()

def export_parquet_to_json(input_file, output_file):
    try:
        # Load the processed data from your extraction step
        df = pd.read_parquet(input_file)
        
        # Convert the DataFrame to a list of dictionaries for clean JSON output
        # 'records' format is ideal for n8n ingestion
        data_list = df.to_dict(orient='records')
        
        # Export to JSON
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(data_list, f, ensure_ascii=False, indent=4)
        
        console.print(f"✅ [bold green]JSON Audit File Created:[/bold green] [yellow]{output_file}[/yellow]")
        
        # Display a preview of the structured data
        table = Table(title="Vault Data Preview (Top 3 Chunks)")
        table.add_column("Content Snippet", style="green")

        for row in data_list[:3]:
            # Extract page from metadata safely
            content = row.get('content', '')[:75].replace('\n', ' ') + "..."
            table.add_row(content)

        console.print(table)
        
    except Exception as e:
        console.print(f"[bold red]❌ Inspection Failed:[/bold red] {e}")

if __name__ == "__main__":
    # Ensure these paths match your local project structure
    INPUT_PATH = "data/result/data.parquet"
    OUTPUT_PATH = "data/result/audit_vault_data.json"
    export_parquet_to_json(INPUT_PATH, OUTPUT_PATH)