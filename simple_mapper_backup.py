import json
import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from fuzzywuzzy import fuzz
import yaml
import sys
import os

# Add src to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', '..', 'src'))

logger = logging.getLogger(__name__)

@dataclass
class MappingResult:
    cnops_code: str
    original_name: str
    dci1: str
    rxcui: Optional[str] = None
    rxnorm_name: Optional[str] = None
    confidence_score: float = 0.0
    mapping_method: str = "none"
    validation_notes: List[str] = field(default_factory=list)
    alternative_matches: List[Dict] = field(default_factory=list)

# Simple test mapper without config files
class SimpleCNOPSMapper:
    def __init__(self):
        # Hardcoded config to avoid BOM issues
        self.base_url = "https://rxnav.nlm.nih.gov/REST"
        self.rate_limit = 0.2
        self.timeout = 30
        self.retries = 3
        
        import requests
        self.session = requests.Session()
        self.session.headers.update({'User-Agent': 'CNOPS-RxNorm-Mapper/1.0'})
        
        # Load translation dictionaries
        self.ingredient_translations = {
            "ACIDE ACETYLSALICYLIQUE": "aspirin",
            "PARACETAMOL": "acetaminophen",
            "DICLOFENAC": "diclofenac",
            "CETIRIZINE": "cetirizine",
            "FLUCONAZOLE": "fluconazole",
            "LANSOPRAZOLE": "lansoprazole"
        }
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        import time
        import requests
        for attempt in range(self.retries):
            try:
                time.sleep(self.rate_limit)
                url = f"{self.base_url}/{endpoint}"
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                print(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt == self.retries - 1:
                    return None
                time.sleep(2 ** attempt)
        return None
    
    def search_by_name(self, name: str) -> Optional[str]:
        params = {'name': name}
        result = self._make_request('rxcui.json', params)
        if result and result.get('idGroup', {}).get('rxnormId'):
            return result['idGroup']['rxnormId'][0]
        return None
    
    def map_single_drug(self, cnops_record: Dict) -> MappingResult:
        result = MappingResult(
            cnops_code=cnops_record.get('CODE', ''),
            original_name=cnops_record.get('NOM', ''),
            dci1=cnops_record.get('DCI1', '')
        )
        
        if not result.dci1:
            result.validation_notes.append("No DCI1 ingredient specified")
            return result
        
        # Try direct lookup
        rxcui = self.search_by_name(result.dci1)
        if rxcui:
            result.rxcui = rxcui
            result.rxnorm_name = result.dci1
            result.mapping_method = "direct_exact"
            result.confidence_score = 0.9
            result.validation_notes.append("HIGH confidence mapping")
            return result
        
        # Try translation
        if result.dci1.upper() in self.ingredient_translations:
            translated = self.ingredient_translations[result.dci1.upper()]
            rxcui = self.search_by_name(translated)
            if rxcui:
                result.rxcui = rxcui
                result.rxnorm_name = translated
                result.mapping_method = "translated_exact"
                result.confidence_score = 0.8
                result.validation_notes.append(f"Used translation: {result.dci1} -> {translated}")
                result.validation_notes.append("HIGH confidence mapping")
                return result
        
        result.validation_notes.append("No mapping found")
        return result
    
    def process_file(self, input_path: str, output_path: str = None) -> pd.DataFrame:
        print(f"Loading data from {input_path}")
        df = pd.read_excel(input_path)
        print(f"Processing {len(df)} records...")
        
        results = []
        for idx, row in df.iterrows():
            if idx % 100 == 0:
                print(f"Processed {idx}/{len(df)} records")
            
            try:
                mapping_result = self.map_single_drug(row.to_dict())
                results.append({
                    'CNOPS_CODE': mapping_result.cnops_code,
                    'ORIGINAL_NAME': mapping_result.original_name,
                    'DCI1': mapping_result.dci1,
                    'RXCUI': mapping_result.rxcui,
                    'RXNORM_NAME': mapping_result.rxnorm_name,
                    'CONFIDENCE_SCORE': mapping_result.confidence_score,
                    'MAPPING_METHOD': mapping_result.mapping_method,
                    'VALIDATION_NOTES': '; '.join(mapping_result.validation_notes)
                })
            except Exception as e:
                print(f"Error processing record {idx}: {e}")
                results.append({
                    'CNOPS_CODE': row.get('CODE', ''),
                    'ORIGINAL_NAME': row.get('NOM', ''),
                    'DCI1': row.get('DCI1', ''),
                    'RXCUI': None,
                    'RXNORM_NAME': None,
                    'CONFIDENCE_SCORE': 0.0,
                    'MAPPING_METHOD': 'error',
                    'VALIDATION_NOTES': f'Error: {str(e)}'
                })
        
        results_df = pd.DataFrame(results)
        
        if output_path:
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            results_df.to_excel(output_path, index=False)
            print(f"Results saved to {output_path}")
        
        # Print summary
        total = len(results_df)
        mapped = len(results_df[results_df['RXCUI'].notna()])
        print(f"\nSUMMARY: {mapped}/{total} records mapped ({mapped/total*100:.1f}%)")
        
        return results_df

def main():
    import sys
    
    input_file = "data/input/refdesmedicamentscnops.xlsx"
    output_file = "data/output/cnops_rxnorm_mappings.xlsx"
    
    if len(sys.argv) > 1:
        input_file = sys.argv[1]
    
    try:
        mapper = SimpleCNOPSMapper()
        results_df = mapper.process_file(input_file, output_file)
        print("Mapping completed successfully!")
        return 0
    except Exception as e:
        print(f"Mapping failed: {e}")
        import traceback
        traceback.print_exc()
        return 1

if __name__ == "__main__":
    exit(main())
