#!/usr/bin/env python3
# scripts/utils/soc_code_mapper.py
"""
SOC Code Standardization and Mapping Utilities
Handles Standard Occupational Classification (SOC) code normalization and O*NET mappings
"""
import re
import logging
from typing import Optional, Dict, List

logger = logging.getLogger(__name__)

class SOCCodeMapper:
    """
    Utility class for handling SOC code standardization and O*NET mappings.
    """
    
    def __init__(self):
        # Initialize SOC group mappings (major 2-digit groups)
        self.soc_major_groups = {
            "11": "Management Occupations",
            "13": "Business and Financial Operations Occupations", 
            "15": "Computer and Mathematical Occupations",
            "17": "Architecture and Engineering Occupations",
            "19": "Life, Physical, and Social Science Occupations",
            "21": "Community and Social Service Occupations",
            "23": "Legal Occupations",
            "25": "Educational Instruction and Library Occupations",
            "27": "Arts, Design, Entertainment, Sports, and Media Occupations",
            "29": "Healthcare Practitioners and Technical Occupations",
            "31": "Healthcare Support Occupations",
            "33": "Protective Service Occupations",
            "35": "Food Preparation and Serving Related Occupations",
            "37": "Building and Grounds Cleaning and Maintenance Occupations",
            "39": "Personal Care and Service Occupations",
            "41": "Sales and Related Occupations",
            "43": "Office and Administrative Support Occupations",
            "45": "Farming, Fishing, and Forestry Occupations",
            "47": "Construction and Extraction Occupations",
            "49": "Installation, Maintenance, and Repair Occupations",
            "51": "Production Occupations",
            "53": "Transportation and Material Moving Occupations",
            "55": "Military Specific Occupations"
        }
        
        # Common SOC code patterns and their standardized formats
        self.soc_patterns = [
            # Pattern: XX-XXXX (standard format)
            (r'^(\d{2})-(\d{4})$', r'\1-\2'),
            # Pattern: XX-XXXX.XX (with decimal)
            (r'^(\d{2})-(\d{4})\.\d{2}$', r'\1-\2'),
            # Pattern: XXXXXX (6 digits)
            (r'^(\d{2})(\d{4})$', r'\1-\2'),
            # Pattern: XX.XXXX (dot separator)
            (r'^(\d{2})\.(\d{4})$', r'\1-\2'),
            # Pattern: XX XXXX (space separator)
            (r'^(\d{2})\s+(\d{4})$', r'\1-\2'),
        ]

    def standardize_soc_code(self, soc_code: str) -> Optional[str]:
        """
        Standardize SOC codes to 6-digit format (XX-XXXX).
        Handle various input formats: 15-1252, 151252, 15-1252.00, etc.
        
        Args:
            soc_code: Input SOC code in any format
            
        Returns:
            Standardized SOC code in XX-XXXX format, or None if invalid
        """
        if not soc_code:
            return None
        
        # Clean input: remove extra whitespace
        cleaned = str(soc_code).strip()
        
        # Try each pattern
        for pattern, replacement in self.soc_patterns:
            match = re.match(pattern, cleaned)
            if match:
                standardized = re.sub(pattern, replacement, cleaned)
                # Validate the result
                if self._validate_soc_code(standardized):
                    return standardized
        
        logger.warning(f"Could not standardize SOC code: {soc_code}")
        return None

    def _validate_soc_code(self, soc_code: str) -> bool:
        """
        Validate that a SOC code follows the correct format and has a valid major group.
        
        Args:
            soc_code: SOC code to validate
            
        Returns:
            True if valid, False otherwise
        """
        if not soc_code or len(soc_code) != 7:
            return False
        
        # Check format: XX-XXXX
        if not re.match(r'^\d{2}-\d{4}$', soc_code):
            return False
        
        # Check if major group exists
        major_group = soc_code[:2]
        return major_group in self.soc_major_groups

    def get_major_group(self, soc_code: str) -> Optional[str]:
        """
        Get the major occupational group for a SOC code.
        
        Args:
            soc_code: Standardized SOC code
            
        Returns:
            Major group description or None if invalid
        """
        standardized = self.standardize_soc_code(soc_code)
        if not standardized:
            return None
        
        major_group = standardized[:2]
        return self.soc_major_groups.get(major_group)

    def get_ai_susceptibility_defaults(self, soc_code: str) -> Dict[str, float]:
        """
        Get default AI automation/augmentation rates based on SOC major group.
        These serve as fallbacks when specific occupation data isn't available.
        
        Args:
            soc_code: SOC code to get defaults for
            
        Returns:
            Dictionary with automation and augmentation rates
        """
        standardized = self.standardize_soc_code(soc_code)
        if not standardized:
            return {"automation": 0.45, "augmentation": 0.35}  # Overall defaults
        
        major_group = standardized[:2]
        
        # Research-based defaults by major occupational group
        # These are informed by automation potential studies and AI capability research
        soc_group_defaults = {
            "11": {"automation": 0.15, "augmentation": 0.60},  # Management
            "13": {"automation": 0.35, "augmentation": 0.70},  # Business/Financial
            "15": {"automation": 0.25, "augmentation": 0.80},  # Computer/Math
            "17": {"automation": 0.30, "augmentation": 0.65},  # Architecture/Engineering
            "19": {"automation": 0.20, "augmentation": 0.75},  # Life/Physical Sciences
            "21": {"automation": 0.40, "augmentation": 0.50},  # Community/Social Services
            "23": {"automation": 0.25, "augmentation": 0.60},  # Legal
            "25": {"automation": 0.20, "augmentation": 0.70},  # Education
            "27": {"automation": 0.15, "augmentation": 0.65},  # Arts/Media
            "29": {"automation": 0.30, "augmentation": 0.70},  # Healthcare Practitioners
            "31": {"automation": 0.45, "augmentation": 0.40},  # Healthcare Support
            "33": {"automation": 0.50, "augmentation": 0.35},  # Protective Service
            "35": {"automation": 0.60, "augmentation": 0.25},  # Food Preparation
            "37": {"automation": 0.55, "augmentation": 0.30},  # Building Maintenance
            "39": {"automation": 0.50, "augmentation": 0.40},  # Personal Care
            "41": {"automation": 0.65, "augmentation": 0.30},  # Sales
            "43": {"automation": 0.70, "augmentation": 0.25},  # Office/Administrative
            "45": {"automation": 0.60, "augmentation": 0.20},  # Farming/Fishing
            "47": {"automation": 0.45, "augmentation": 0.35},  # Construction
            "49": {"automation": 0.50, "augmentation": 0.40},  # Installation/Maintenance
            "51": {"automation": 0.75, "augmentation": 0.20},  # Production
            "53": {"automation": 0.80, "augmentation": 0.15},  # Transportation
            "55": {"automation": 0.35, "augmentation": 0.45},  # Military
        }
        
        return soc_group_defaults.get(major_group, {"automation": 0.45, "augmentation": 0.35})

    def map_industry_to_naics(self, industry_name: str) -> Optional[str]:
        """
        Map friendly industry names to NAICS codes for BLS data collection.
        
        Args:
            industry_name: Human-readable industry name
            
        Returns:
            NAICS code or None if not found
        """
        industry_naics_mapping = {
            "Information": "51",
            "Professional and Business Services": "54", 
            "Financial Activities": "52",
            "Education and Health Services": "62",
            "Manufacturing": "31-33",
            "Trade, Transportation, and Utilities": "44-45",
            "Construction": "23",
            "Leisure and Hospitality": "72",
            "Mining and Logging": "21",
            "Other Services": "81",
            "Government": "92",
            "Agriculture": "11"
        }
        
        # Try exact match first
        if industry_name in industry_naics_mapping:
            return industry_naics_mapping[industry_name]
        
        # Try case-insensitive match
        for industry, naics in industry_naics_mapping.items():
            if industry.lower() == industry_name.lower():
                return naics
        
        # Try partial matching
        for industry, naics in industry_naics_mapping.items():
            if any(word.lower() in industry_name.lower() for word in industry.split()):
                logger.info(f"Partial match: '{industry_name}' -> '{industry}' (NAICS: {naics})")
                return naics
        
        logger.warning(f"No NAICS mapping found for industry: {industry_name}")
        return None

    def get_bls_series_template(self, naics_code: str, soc_code: str) -> str:
        """
        Generate BLS OEWS series ID template for industry×occupation data.
        
        Args:
            naics_code: NAICS industry code
            soc_code: SOC occupation code
            
        Returns:
            BLS series ID template
        """
        # BLS OEWS series format: OEUS + Area + Industry + Occupation + DataType
        # OEUS000000 (National) + INDUSTRY + OCCUPATION + 01 (Employment)
        
        # Standardize SOC code and remove dash
        standardized_soc = self.standardize_soc_code(soc_code)
        if not standardized_soc:
            return None
        
        soc_numeric = standardized_soc.replace("-", "")
        
        # Format NAICS code to 6 digits (pad with zeros)
        if naics_code == "31-33":  # Special case for Manufacturing
            naics_formatted = "310000"
        elif naics_code == "44-45":  # Special case for Retail Trade
            naics_formatted = "440000"  
        elif naics_code == "48-49":  # Special case for Transportation
            naics_formatted = "480000"
        else:
            naics_formatted = f"{naics_code}0000"[:6]
        
        # Build series ID
        series_id = f"OEUS000000{naics_formatted}{soc_numeric}01"
        
        return series_id

    def batch_standardize_soc_codes(self, soc_codes: List[str]) -> Dict[str, Optional[str]]:
        """
        Standardize a batch of SOC codes efficiently.
        
        Args:
            soc_codes: List of SOC codes to standardize
            
        Returns:
            Dictionary mapping original codes to standardized codes
        """
        results = {}
        
        for soc_code in soc_codes:
            results[soc_code] = self.standardize_soc_code(soc_code)
        
        # Log summary
        successful = sum(1 for v in results.values() if v is not None)
        logger.info(f"Standardized {successful}/{len(soc_codes)} SOC codes successfully")
        
        return results

    def validate_occupation_data(self, occupation_data: Dict) -> Dict[str, any]:
        """
        Validate occupation data structure and SOC codes.
        
        Args:
            occupation_data: Dictionary of occupation data with SOC codes as keys
            
        Returns:
            Validation report
        """
        validation_report = {
            "total_occupations": len(occupation_data),
            "valid_soc_codes": 0,
            "invalid_soc_codes": [],
            "missing_fields": [],
            "warnings": []
        }
        
        required_fields = ["automation_rate", "augmentation_rate"]
        
        for soc_code, data in occupation_data.items():
            # Validate SOC code
            standardized = self.standardize_soc_code(soc_code)
            if standardized:
                validation_report["valid_soc_codes"] += 1
            else:
                validation_report["invalid_soc_codes"].append(soc_code)
            
            # Check required fields
            for field in required_fields:
                if field not in data:
                    validation_report["missing_fields"].append(f"{soc_code}: missing {field}")
                elif not isinstance(data[field], (int, float)):
                    validation_report["warnings"].append(f"{soc_code}: {field} is not numeric")
                elif not (0 <= data[field] <= 1):
                    validation_report["warnings"].append(f"{soc_code}: {field} outside valid range [0,1]")
        
        validation_report["validation_passed"] = (
            len(validation_report["invalid_soc_codes"]) == 0 and
            len(validation_report["missing_fields"]) == 0
        )
        
        return validation_report


def main():
    """Example usage and testing."""
    mapper = SOCCodeMapper()
    
    # Test SOC code standardization
    test_codes = [
        "15-1252",
        "151252", 
        "15-1252.00",
        "43.3031",
        "43 3031",
        "invalid",
        None
    ]
    
    print("SOC Code Standardization Test:")
    for code in test_codes:
        standardized = mapper.standardize_soc_code(code)
        major_group = mapper.get_major_group(code)
        defaults = mapper.get_ai_susceptibility_defaults(code)
        
        print(f"  {code} -> {standardized} ({major_group})")
        if standardized:
            print(f"    Defaults: Auto={defaults['automation']:.2f}, Aug={defaults['augmentation']:.2f}")

    # Test industry mapping
    print("\nIndustry NAICS Mapping Test:")
    test_industries = ["Information", "Healthcare", "Technology", "Unknown Industry"]
    for industry in test_industries:
        naics = mapper.map_industry_to_naics(industry)
        print(f"  {industry} -> NAICS {naics}")


if __name__ == "__main__":
    main()