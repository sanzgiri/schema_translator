"""
Tests for Chainlit UI components

Note: These are unit tests for UI helper functions.
Full UI testing requires running the Chainlit app and using browser automation.
"""

import pytest
from unittest.mock import Mock, MagicMock
from app import (
    format_result_table,
    format_statistics,
    format_debug_info
)
from schema_translator.models import HarmonizedRow, HarmonizedResult


class TestResultFormatting:
    """Test result table formatting."""
    
    def test_format_empty_result(self):
        """Test formatting with no results."""
        result = HarmonizedResult(
            results=[],
            total_count=0,
            customers_queried=["customer_a"],
            customers_succeeded=[],
            customers_failed=["customer_a"],
            execution_time_ms=1.0
        )
        
        output = format_result_table(result)
        assert output == "*No results found*"
    
    def test_format_single_row(self):
        """Test formatting with single row."""
        row = HarmonizedRow(
            customer_id="customer_a",
            data={
                "contract_id": "C001",
                "status": "active",
                "value": "10000"
            }
        )
        
        result = HarmonizedResult(
            results=[row],
            total_count=1,
            customers_queried=["customer_a"],
            customers_succeeded=["customer_a"],
            customers_failed=[],
            execution_time_ms=1.0
        )
        
        output = format_result_table(result)
        
        # Check table structure
        assert "| contract_id | status | value |" in output
        assert "|---|---|---|" in output
        assert "| C001 | active | 10000 |" in output
    
    def test_format_multiple_rows(self):
        """Test formatting with multiple rows."""
        rows = [
            HarmonizedRow(
                customer_id="customer_a",
                data={"id": "1", "name": "Test1"}
            ),
            HarmonizedRow(
                customer_id="customer_b",
                data={"id": "2", "name": "Test2"}
            )
        ]
        
        result = HarmonizedResult(
            results=rows,
            total_count=2,
            customers_queried=["customer_a", "customer_b"],
            customers_succeeded=["customer_a", "customer_b"],
            customers_failed=[],
            execution_time_ms=1.0
        )
        
        output = format_result_table(result)
        
        assert "| 1 | Test1 |" in output
        assert "| 2 | Test2 |" in output
    
    def test_format_truncates_large_results(self):
        """Test that large results are truncated."""
        rows = [
            HarmonizedRow(
                customer_id="customer_a",
                data={"id": str(i)}
            )
            for i in range(100)
        ]
        
        result = HarmonizedResult(
            results=rows,
            total_count=100,
            customers_queried=["customer_a"],
            customers_succeeded=["customer_a"],
            customers_failed=[],
            execution_time_ms=1.0
        )
        
        output = format_result_table(result)
        
        # Should show "... and 50 more rows"
        assert "50 more rows" in output


class TestStatisticsFormatting:
    """Test statistics formatting."""
    
    def test_format_successful_stats(self):
        """Test formatting successful query stats."""
        stats = {
            'success_rate': 100.0,
            'total_rows': 10,
            'customers_queried': ['customer_a', 'customer_b'],
            'customers_succeeded': ['customer_a', 'customer_b'],
            'customers_failed': [],
            'execution_time_ms': 1.23
        }
        
        output = format_statistics(stats)
        
        assert "100.0%" in output
        assert "10" in output
        assert "2" in output  # customers queried
        assert "1.23ms" in output
    
    def test_format_partial_success_stats(self):
        """Test formatting with some failures."""
        stats = {
            'success_rate': 50.0,
            'total_rows': 5,
            'customers_queried': ['customer_a', 'customer_b'],
            'customers_succeeded': ['customer_a'],
            'customers_failed': ['customer_b'],
            'execution_time_ms': 2.5
        }
        
        output = format_statistics(stats)
        
        assert "50.0%" in output
        assert "customer_b" in output  # failed customer shown


class TestDebugFormatting:
    """Test debug information formatting."""
    
    def test_format_basic_debug_info(self):
        """Test formatting basic debug information."""
        debug = {
            'semantic_plan': {
                'intent': 'find_contracts',
                'projections': ['contract_id', 'status'],
                'filters': [],
                'aggregations': []
            },
            'sql_queries': {
                'customer_a': 'SELECT c.id, c.status FROM contracts c LIMIT 10'
            }
        }
        
        output = format_debug_info(debug)
        
        assert "find_contracts" in output
        assert "contract_id" in output
        assert "status" in output
        assert "SELECT" in output
    
    def test_format_debug_with_filters(self):
        """Test formatting debug info with filters."""
        debug = {
            'semantic_plan': {
                'intent': 'find_contracts',
                'projections': ['contract_id'],
                'filters': [
                    {'concept': 'status', 'operator': '==', 'value': 'active'}
                ],
                'aggregations': []
            },
            'sql_queries': {
                'customer_a': 'SELECT c.id FROM contracts c WHERE c.status = "active"'
            }
        }
        
        output = format_debug_info(debug)
        
        assert "status" in output
        assert "==" in output
        assert "active" in output
    
    def test_format_debug_with_aggregations(self):
        """Test formatting debug info with aggregations."""
        debug = {
            'semantic_plan': {
                'intent': 'count_contracts',
                'projections': [],
                'filters': [],
                'aggregations': ['COUNT']
            },
            'sql_queries': {
                'customer_a': 'SELECT COUNT(*) FROM contracts'
            }
        }
        
        output = format_debug_info(debug)
        
        assert "COUNT" in output


class TestUIIntegration:
    """Integration tests for UI components."""
    
    def test_end_to_end_result_display(self):
        """Test complete result display flow."""
        # Create sample result
        rows = [
            HarmonizedRow(
                customer_id="customer_a",
                data={
                    "contract_id": "C001",
                    "status": "active",
                    "value": "10000"
                }
            )
        ]
        
        result = HarmonizedResult(
            results=rows,
            total_count=1,
            customers_queried=["customer_a"],
            customers_succeeded=["customer_a"],
            customers_failed=[],
            execution_time_ms=1.0
        )
        
        # Format table
        table = format_result_table(result)
        assert "C001" in table
        
        # Format stats
        stats = {
            'success_rate': 100.0,
            'total_rows': 1,
            'customers_queried': ['customer_a'],
            'customers_succeeded': ['customer_a'],
            'customers_failed': [],
            'execution_time_ms': 1.0
        }
        stats_output = format_statistics(stats)
        assert "100.0%" in stats_output
        
        # Both should be valid markdown
        assert "|" in table
        assert "**" in stats_output


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
