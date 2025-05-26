import json
import pytest
import sys
import os
import numpy as np
from datetime import datetime

# Add the scripts directory to the path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'scripts'))

# Import our modules
from analysis.calculate_ai_impact import AIImpactCalculator
from analysis.project_impact import AIImpactProjector
from analysis.confidence_intervals import ConfidenceIntervalCalculator

class TestProjectionRealism:
    """Test suite for ensuring non-linear behavior in AI impact projections."""

    def test_transformation_rate_non_linear(self):
        """Test that transformation rate shows realistic variation"""
        # Create test employment data
        test_employment_data = {
            "industries": {
                "Information": {"current": 3000000, "previous": 2800000},
                "Manufacturing": {"current": 12000000, "previous": 12100000},
                "Healthcare": {"current": 20000000, "previous": 19500000}
            }
        }
        
        test_job_data = None  # Will use defaults
        
        # Initialize calculator
        calculator = AIImpactCalculator()
        
        # Calculate impact
        result = calculator.calculate_net_impact(test_employment_data, test_job_data)
        
        # Verify transformation rate exists and makes sense
        assert "transformation_rate" in result
        assert isinstance(result["transformation_rate"], (int, float))
        assert 0 <= result["transformation_rate"] <= 2.0  # Reasonable bounds
        
        # Verify transformation by industry exists
        assert "transformation_by_industry" in result
        assert len(result["transformation_by_industry"]) > 0
        
        # Check that different industries have different transformation rates
        transformation_values = list(result["transformation_by_industry"].values())
        assert len(set(transformation_values)) > 1, "All industries have identical transformation rates"

    def test_confidence_intervals_realistic(self):
        """Test that confidence decreases realistically over time"""
        calculator = ConfidenceIntervalCalculator()
        
        # Test confidence by timeframe calculation
        confidence_by_year = calculator.calculate_confidence_by_timeframe(5)
        
        # Verify confidence decreases over time
        years = sorted(confidence_by_year.keys())
        confidences = [confidence_by_year[year] for year in years]
        
        # Check that confidence generally decreases
        for i in range(1, len(confidences)):
            assert confidences[i] <= confidences[i-1] + 0.05, f"Confidence increased from year {years[i-1]} to {years[i]}"
        
        # Check bounds
        for confidence in confidences:
            assert 0.3 <= confidence <= 0.95, f"Confidence {confidence} outside reasonable bounds"
        
        # Check non-linear decline (not strictly linear)
        if len(confidences) >= 3:
            # Calculate differences between consecutive years
            diffs = [confidences[i] - confidences[i-1] for i in range(1, len(confidences))]
            # Verify the differences aren't all identical (would indicate linear decline)
            diff_variance = np.var(diffs)
            assert diff_variance > 0.001, "Confidence decline appears too linear"

    def test_industry_variation(self):
        """Test that different industries show different adoption patterns"""
        projector = AIImpactProjector()
        
        test_industries = ["Technology", "Healthcare", "Government", "Manufacturing"]
        
        adoption_patterns = {}
        for industry in test_industries:
            # Test adoption projection for each industry
            adoptions = projector.calculate_adoption_projection(0.3, 5, industry)
            adoption_patterns[industry] = adoptions
        
        # Verify each industry has a unique pattern
        for i, industry1 in enumerate(test_industries):
            for industry2 in test_industries[i+1:]:
                pattern1 = adoption_patterns[industry1]
                pattern2 = adoption_patterns[industry2]
                
                # Calculate correlation between patterns
                correlation = np.corrcoef(pattern1, pattern2)[0, 1]
                assert correlation < 0.95, f"{industry1} and {industry2} have nearly identical adoption patterns"
        
        # Verify patterns are non-linear
        for industry, pattern in adoption_patterns.items():
            if len(pattern) >= 3:
                # Check for non-linear progression
                differences = [pattern[i] - pattern[i-1] for i in range(1, len(pattern))]
                diff_variance = np.var(differences)
                assert diff_variance > 0.0001, f"{industry} adoption pattern appears too linear"

    def test_projection_non_linearity(self):
        """Test that projections show non-linear patterns"""
        projector = AIImpactProjector()
        
        # Create mock current impact data
        current_impact = {
            "date": "2025-05",
            "total_impact": -0.06,
            "by_industry": {
                "Technology": {"impact": -0.12},
                "Healthcare": {"impact": -0.02},
                "Manufacturing": {"impact": -0.10}
            },
            "components": {
                "displacement_effect": -0.12,
                "creation_effect": 0.04,
                "market_maturity": 0.5,
                "demand_effect": 0.02
            }
        }
        
        # Test component evolution
        component_projections = projector.project_component_evolution(current_impact)
        
        # Verify non-linear evolution for each component
        for component_name, values in component_projections.items():
            if len(values) >= 3:
                # Calculate year-over-year changes
                changes = [values[i] - values[i-1] for i in range(1, len(values))]
                
                # Check that changes aren't all identical (linear)
                change_variance = np.var(changes)
                assert change_variance > 0.0001, f"{component_name} shows linear progression"
                
                # Verify reasonable bounds
                for value in values:
                    if component_name == "market_maturity":
                        assert 0 <= value <= 1.0, f"Market maturity {value} outside bounds"
                    else:
                        assert -1.0 <= value <= 1.0, f"{component_name} value {value} outside reasonable bounds"

    def test_s_curve_properties(self):
        """Test that S-curve calculations have proper mathematical properties"""
        projector = AIImpactProjector()
        
        # Test S-curve for different starting points
        starting_adoptions = [0.1, 0.3, 0.5, 0.7]
        
        for start_adoption in starting_adoptions:
            adoptions = projector.calculate_adoption_projection(start_adoption, 5, "Technology")
            
            # Verify monotonic increase (S-curve should always increase)
            for i in range(1, len(adoptions)):
                assert adoptions[i] >= adoptions[i-1], f"Adoption decreased from year {i-1} to {i}"
            
            # Verify bounded growth (shouldn't exceed ceiling)
            ceiling = projector.get_sector_param("Technology", "adoption_ceiling", default=0.8)
            for adoption in adoptions:
                assert adoption <= ceiling * 1.1, f"Adoption {adoption} exceeded ceiling {ceiling}"
            
            # Verify S-curve shape (acceleration should change)
            if len(adoptions) >= 4:
                accelerations = []
                for i in range(2, len(adoptions)):
                    # Second derivative approximation
                    accel = (adoptions[i] - adoptions[i-1]) - (adoptions[i-1] - adoptions[i-2])
                    accelerations.append(accel)
                
                # S-curve should show varying acceleration
                accel_range = max(accelerations) - min(accelerations)
                assert accel_range > 0.001, "S-curve shows constant acceleration (linear behavior)"

    def test_data_quality_validation(self):
        """Test that data quality indicators are properly calculated"""
        calculator = AIImpactCalculator()
        
        # Test with good data
        good_employment_data = {
            "industries": {f"Industry_{i}": {"current": 1000000} for i in range(10)}
        }
        good_job_data = {"test": "data"}
        
        completeness = calculator.assess_data_completeness(good_employment_data, good_job_data)
        assert completeness >= 0.7, "Good data should have high completeness score"
        
        # Test with poor data
        poor_employment_data = {"industries": {"Single": {"current": 1000}}}
        poor_job_data = None
        
        poor_completeness = calculator.assess_data_completeness(poor_employment_data, poor_job_data)
        assert poor_completeness <= 0.5, "Poor data should have low completeness score"
        
        # Test with mixed data
        mixed_completeness = calculator.assess_data_completeness(good_employment_data, None)
        assert 0.4 <= mixed_completeness <= 0.8, "Mixed data should have medium completeness score"

    def test_realistic_bounds(self):
        """Test that all calculated values are within realistic bounds"""
        calculator = AIImpactCalculator()
        
        # Create realistic test data
        test_employment = {
            "industries": {
                "Information": {"current": 3000000},
                "Healthcare": {"current": 20000000},
                "Manufacturing": {"current": 12000000},
                "Total Nonfarm": {"current": 150000000}
            }
        }
        
        result = calculator.calculate_net_impact(test_employment, None)
        
        # Check overall impact bounds
        assert -0.5 <= result["total_impact"] <= 0.3, f"Total impact {result['total_impact']} outside realistic bounds"
        
        # Check transformation rate bounds  
        assert 0 <= result["transformation_rate"] <= 2.0, f"Transformation rate {result['transformation_rate']} outside bounds"
        
        # Check industry-specific impacts
        for industry, data in result["by_industry"].items():
            impact = data["impact"]
            assert -0.8 <= impact <= 0.5, f"{industry} impact {impact} outside realistic bounds"
        
        # Check component bounds
        components = result["components"]
        assert -0.8 <= components["displacement_effect"] <= 0, "Displacement effect outside bounds"
        assert 0 <= components["creation_effect"] <= 0.5, "Creation effect outside bounds"
        assert 0 <= components["market_maturity"] <= 1.0, "Market maturity outside bounds"

if __name__ == "__main__":
    # Run basic tests if executed directly
    test_suite = TestProjectionRealism()
    
    print("Running projection realism tests...")
    
    try:
        test_suite.test_transformation_rate_non_linear()
        print("✓ Transformation rate test passed")
    except Exception as e:
        print(f"✗ Transformation rate test failed: {e}")
    
    try:
        test_suite.test_confidence_intervals_realistic()
        print("✓ Confidence intervals test passed")
    except Exception as e:
        print(f"✗ Confidence intervals test failed: {e}")
    
    try:
        test_suite.test_industry_variation()
        print("✓ Industry variation test passed")
    except Exception as e:
        print(f"✗ Industry variation test failed: {e}")
    
    try:
        test_suite.test_projection_non_linearity()
        print("✓ Projection non-linearity test passed")
    except Exception as e:
        print(f"✗ Projection non-linearity test failed: {e}")
    
    try:
        test_suite.test_s_curve_properties()
        print("✓ S-curve properties test passed")
    except Exception as e:
        print(f"✗ S-curve properties test failed: {e}")
    
    try:
        test_suite.test_data_quality_validation()
        print("✓ Data quality validation test passed")
    except Exception as e:
        print(f"✗ Data quality validation test failed: {e}")
    
    try:
        test_suite.test_realistic_bounds()
        print("✓ Realistic bounds test passed")
    except Exception as e:
        print(f"✗ Realistic bounds test failed: {e}")
    
    print("Test suite completed!")