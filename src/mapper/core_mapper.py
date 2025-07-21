import json
import pandas as pd
import logging
from typing import Dict, List, Optional, Tuple
from dataclasses import dataclass, field
from fuzzywuzzy import fuzz
import yaml
from .api_client import RxNormAPIClient

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

class CNOPSToRxNormMapper:
    def __init__(self, config_path: str = "config/mapping_config.yaml"):
        # Load configuration
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Initialize API client
        self.api_client = RxNormAPIClient()
        
        # Load translation dictionaries
        self.ingredient_translations = self._load_json_dict(
            "data/dictionaries/ingredient_translations.json"
        )
        self.dose_form_translations = self._load_json_dict(
            "data/dictionaries/dose_form_translations.json"
        )
        
        # Configuration shortcuts
        self.confidence_thresholds = self.config['mapping']['confidence_thresholds']
        self.validation_config = self.config['mapping']['validation']
    
    def _load_json_dict(self, path: str) -> Dict[str, str]:
        '''Load JSON dictionary file'''
        try:
            with open(path, 'r', encoding='utf-8') as f:
                return json.load(f)
        except FileNotFoundError:
            logger.warning(f"Dictionary file not found: {path}")
            return {}
    
    def map_single_drug(self, cnops_record: Dict) -> MappingResult:
        '''Map a single CNOPS drug record to RxNorm'''
        result = MappingResult(
            cnops_code=cnops_record.get('CODE', ''),
            original_name=cnops_record.get('NOM', ''),
            dci1=cnops_record.get('DCI1', '')
        )
        
        if not result.dci1:
            result.validation_notes.append("No DCI1 ingredient specified")
            return result
        
        # Strategy 1: Direct lookup
        rxcui = self.api_client.search_by_name(result.dci1)
        if rxcui:
            result.rxcui = rxcui
            result.rxnorm_name = result.dci1
            result.mapping_method = "direct_exact"
            result.confidence_score = 0.9
            self._enhance_with_products(result, cnops_record)
            self._validate_mapping(result, cnops_record)
            return result
        
        # Strategy 2: Translation-based lookup
        if result.dci1.upper() in self.ingredient_translations:
            translated = self.ingredient_translations[result.dci1.upper()]
            rxcui = self.api_client.search_by_name(translated)
            if rxcui:
                result.rxcui = rxcui
                result.rxnorm_name = translated
                result.mapping_method = "translated_exact"
                result.confidence_score = 0.8
                result.validation_notes.append(f"Used translation: {result.dci1} -> {translated}")
                self._enhance_with_products(result, cnops_record)
                self._validate_mapping(result, cnops_record)
                return result
        
        # Strategy 3: Fuzzy matching
        matches = self.api_client.approximate_search(result.dci1, max_entries=5)
        if matches:
            best_match = matches[0]
            if best_match['score'] >= 80:  # High similarity threshold
                result.rxcui = best_match['rxcui']
                result.rxnorm_name = best_match['term']
                result.mapping_method = "fuzzy_high"
                result.confidence_score = 0.6
                result.alternative_matches = matches[1:]
                result.validation_notes.append(f"Fuzzy match score: {best_match['score']}")
                self._enhance_with_products(result, cnops_record)
        
        self._validate_mapping(result, cnops_record)
        return result
    
    def _enhance_with_products(self, result: MappingResult, cnops_record: Dict):
        '''Enhance mapping with specific drug products'''
        if not result.rxcui:
            return
        
        products = self.api_client.get_related_concepts(result.rxcui, 'SCD+SBD')
        if not products:
            return
        
        # Try to find best matching product
        target_strength = f"{cnops_record.get('DOSAGE1', '')} {cnops_record.get('UNITE_DOSAGE1', '')}"
        target_form = cnops_record.get('FORME', '')
        
        best_product = self._find_best_product_match(products, target_strength, target_form)
        if best_product:
            result.rxcui = best_product['rxcui']
            result.rxnorm_name = best_product['name']
            result.confidence_score += 0.1  # Bonus for specific product
            result.validation_notes.append("Enhanced with specific product")
    
    def _find_best_product_match(self, products: List[Dict], target_strength: str, target_form: str) -> Optional[Dict]:
        '''Find best matching product'''
        if not products:
            return None
        
        scored_products = []
        
        for product in products:
            score = 0
            product_name = product.get('name', '').upper()
            
            # Score based on strength
            if target_strength.strip() and target_strength.upper() in product_name:
                score += 50
            
            # Score based on dose form
            translated_form = self.dose_form_translations.get(target_form, target_form)
            if translated_form.upper() in product_name:
                score += 30
            
            scored_products.append((score, product))
        
        scored_products.sort(reverse=True)
        if scored_products and scored_products[0][0] > 0:
            return scored_products[0][1]
        
        # Return first SCD if available
        for product in products:
            if product.get('tty') == 'SCD':
                return product
        
        return products[0] if products else None
    
    def _validate_mapping(self, result: MappingResult, cnops_record: Dict):
        '''Validate mapping and adjust confidence'''
        if not result.rxcui:
            result.confidence_score = 0.0
            result.validation_notes.append("No RxNorm mapping found")
            return
        
        # Check name similarity
        if result.rxnorm_name and result.dci1:
            similarity = fuzz.ratio(result.dci1.upper(), result.rxnorm_name.upper())
            if similarity < self.validation_config['name_similarity_threshold']:
                result.confidence_score *= self.validation_config['form_mismatch_penalty']
                result.validation_notes.append(f"Low name similarity: {similarity}%")
        
        # Check for combination drugs
        if "/" in result.dci1 and "/" not in (result.rxnorm_name or ""):
            result.confidence_score *= self.validation_config['combination_drug_penalty']
            result.validation_notes.append("Possible combination drug mismatch")
        
        # Categorize confidence
        if result.confidence_score >= self.confidence_thresholds['high']:
            result.validation_notes.append("HIGH confidence mapping")
        elif result.confidence_score >= self.confidence_thresholds['medium']:
            result.validation_notes.append("MEDIUM confidence mapping")
        elif result.confidence_score >= self.confidence_thresholds['low']:
            result.validation_notes.append("LOW confidence - review recommended")
        else:
            result.validation_notes.append("VERY LOW confidence - manual review required")
    
    def process_file(self, input_path: str, output_path: str = None) -> pd.DataFrame:
        '''Process entire CNOPS Excel file'''
        logger.info(f"Loading data from {input_path}")
        df = pd.read_excel(input_path)
        
        logger.info(f"Processing {len(df)} records...")
        
        results = []
        for idx, row in df.iterrows():
            if idx % 100 == 0:
                logger.info(f"Processed {idx}/{len(df)} records")
            
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
                    'VALIDATION_NOTES': '; '.join(mapping_result.validation_notes),
                    'ALTERNATIVES_COUNT': len(mapping_result.alternative_matches)
                })
            except Exception as e:
                logger.error(f"Error processing record {idx}: {e}")
                results.append({
                    'CNOPS_CODE': row.get('CODE', ''),
                    'ORIGINAL_NAME': row.get('NOM', ''),
                    'DCI1': row.get('DCI1', ''),
                    'RXCUI': None,
                    'RXNORM_NAME': None,
                    'CONFIDENCE_SCORE': 0.0,
                    'MAPPING_METHOD': 'error',
                    'VALIDATION_NOTES': f'Error: {str(e)}',
                    'ALTERNATIVES_COUNT': 0
                })
        
        results_df = pd.DataFrame(results)
        
        if output_path:
            results_df.to_excel(output_path, index=False)
            logger.info(f"Results saved to {output_path}")
        
        self._print_summary(results_df)
        return results_df
    
    def _print_summary(self, df: pd.DataFrame):
        '''Print mapping summary statistics'''
        total = len(df)
        mapped = len(df[df['RXCUI'].notna()])
        high_conf = len(df[df['CONFIDENCE_SCORE'] >= self.confidence_thresholds['high']])
        med_conf = len(df[(df['CONFIDENCE_SCORE'] >= self.confidence_thresholds['medium']) & 
                         (df['CONFIDENCE_SCORE'] < self.confidence_thresholds['high'])])
        low_conf = len(df[df['CONFIDENCE_SCORE'] < self.confidence_thresholds['medium']])
        
        print("\n" + "="*60)
        print("CNOPS TO RXNORM MAPPING SUMMARY")
        print("="*60)
        print(f"Total records: {total}")
        print(f"Successfully mapped: {mapped} ({mapped/total*100:.1f}%)")
        print(f"High confidence (>={self.confidence_thresholds['high']}): {high_conf} ({high_conf/total*100:.1f}%)")
        print(f"Medium confidence (>={self.confidence_thresholds['medium']}): {med_conf} ({med_conf/total*100:.1f}%)")
        print(f"Low confidence (<{self.confidence_thresholds['medium']}): {low_conf} ({low_conf/total*100:.1f}%)")
        print("="*60)
