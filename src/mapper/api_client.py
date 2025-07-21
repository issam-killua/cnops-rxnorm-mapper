import requests
import time
import logging
from typing import Dict, List, Optional
import yaml

logger = logging.getLogger(__name__)

class RxNormAPIClient:
    def __init__(self, config_path: str = "config/api_config.yaml"):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.base_url = self.config['rxnorm']['base_url']
        self.rate_limit = self.config['rxnorm']['rate_limit']
        self.timeout = self.config['rxnorm']['timeout']
        self.retries = self.config['rxnorm']['retries']
        
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'CNOPS-RxNorm-Mapper/1.0'
        })
    
    def _make_request(self, endpoint: str, params: Dict = None) -> Optional[Dict]:
        '''Make API request with rate limiting and error handling'''
        for attempt in range(self.retries):
            try:
                time.sleep(self.rate_limit)
                url = f"{self.base_url}/{endpoint}"
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                return response.json()
            except requests.exceptions.RequestException as e:
                logger.warning(f"Request failed (attempt {attempt + 1}): {e}")
                if attempt == self.retries - 1:
                    logger.error(f"All retries failed for {endpoint}")
                    return None
                time.sleep(2 ** attempt)  # Exponential backoff
        return None
    
    def search_by_name(self, name: str) -> Optional[str]:
        '''Search for exact drug name match'''
        params = {'name': name}
        result = self._make_request('rxcui.json', params)
        
        if result and result.get('idGroup', {}).get('rxnormId'):
            return result['idGroup']['rxnormId'][0]
        return None
    
    def approximate_search(self, term: str, max_entries: int = 10) -> List[Dict]:
        '''Fuzzy search for drug terms'''
        params = {'term': term, 'maxEntries': max_entries}
        result = self._make_request('approximateTerm.json', params)
        
        matches = []
        if result and result.get('approximateGroup', {}).get('candidate'):
            for candidate in result['approximateGroup']['candidate']:
                matches.append({
                    'rxcui': candidate.get('rxcui'),
                    'term': candidate.get('term'),
                    'score': candidate.get('score', 0)
                })
        
        return matches
    
    def get_related_concepts(self, rxcui: str, tty: str = None) -> List[Dict]:
        '''Get related concepts for an RXCUI'''
        endpoint = f'rxcui/{rxcui}/related.json'
        params = {'tty': tty} if tty else None
        result = self._make_request(endpoint, params)
        
        concepts = []
        if result and result.get('relatedGroup'):
            for group in result['relatedGroup'].get('conceptGroup', []):
                if group.get('conceptProperties'):
                    for concept in group['conceptProperties']:
                        concepts.append({
                            'rxcui': concept.get('rxcui'),
                            'name': concept.get('name'),
                            'tty': concept.get('tty'),
                            'synonym': concept.get('synonym')
                        })
        
        return concepts
