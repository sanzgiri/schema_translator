"""
Feedback Loop for learning from user interactions

This module collects and analyzes user feedback to improve query understanding
and schema mapping over time.
"""

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timedelta, timezone
from collections import defaultdict, Counter
import json
import logging
from pathlib import Path

from schema_translator.models import (
    QueryFeedback,
    SemanticQueryPlan,
    QueryIntent
)

logger = logging.getLogger(__name__)


class FeedbackLoop:
    """Collects and analyzes user feedback to improve the system."""
    
    def __init__(self, feedback_file: Optional[Path] = None):
        """Initialize feedback loop.
        
        Args:
            feedback_file: Path to store feedback (default: data/feedback.jsonl)
        """
        self.feedback_file = feedback_file or Path("data/feedback.jsonl")
        self.feedback_file.parent.mkdir(parents=True, exist_ok=True)
        
        # In-memory cache
        self.feedback_cache: List[QueryFeedback] = []
        self.query_patterns: Dict[str, int] = defaultdict(int)
        self.failure_patterns: Dict[str, List[str]] = defaultdict(list)
        
        # Load existing feedback
        self._load_feedback()
        logger.info(f"FeedbackLoop initialized with {len(self.feedback_cache)} feedback entries")
    
    def submit_feedback(
        self,
        query_text: str,
        semantic_plan: SemanticQueryPlan,
        feedback_type: str,
        feedback_text: Optional[str] = None,
        correct_result: Optional[Any] = None
    ) -> QueryFeedback:
        """Submit user feedback on a query result.
        
        Args:
            query_text: Original natural language query
            semantic_plan: Semantic plan that was used
            feedback_type: Type of feedback (good, incorrect, missing)
            feedback_text: Optional user comment
            correct_result: What the correct result should be
            
        Returns:
            QueryFeedback object
        """
        feedback = QueryFeedback(
            query_text=query_text,
            semantic_plan=semantic_plan,
            feedback_type=feedback_type,
            feedback_text=feedback_text,
            correct_result=correct_result
        )
        
        # Store in cache
        self.feedback_cache.append(feedback)
        
        # Update patterns
        if feedback_type == "incorrect" or feedback_type == "missing":
            self.failure_patterns[feedback_type].append(query_text)
        
        # Track query patterns
        intent_str = str(semantic_plan.intent)
        self.query_patterns[intent_str] += 1
        
        # Persist to disk
        self._save_feedback(feedback)
        
        logger.info(f"Feedback received: {feedback_type} for query '{query_text}'")
        return feedback
    
    def get_feedback_summary(
        self,
        days: int = 30
    ) -> Dict[str, Any]:
        """Get summary of feedback received.
        
        Args:
            days: Number of days to include in summary
            
        Returns:
            Summary statistics
        """
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        recent_feedback = [
            f for f in self.feedback_cache
            if f.timestamp >= cutoff_date
        ]
        
        if not recent_feedback:
            return {
                "total_feedback": 0,
                "period_days": days,
                "feedback_types": {},
                "most_problematic_queries": []
            }
        
        # Count by type
        type_counts = Counter(f.feedback_type for f in recent_feedback)
        
        # Find most problematic queries (incorrect/missing)
        problem_queries = [
            f.query_text for f in recent_feedback
            if f.feedback_type in ["incorrect", "missing"]
        ]
        problem_query_counts = Counter(problem_queries)
        
        return {
            "total_feedback": len(recent_feedback),
            "period_days": days,
            "feedback_types": dict(type_counts),
            "most_problematic_queries": problem_query_counts.most_common(10),
            "success_rate": (type_counts.get("good", 0) / len(recent_feedback) * 100
                            if recent_feedback else 0)
        }
    
    def analyze_failure_patterns(self) -> Dict[str, Any]:
        """Analyze patterns in failed queries.
        
        Returns:
            Analysis of common failure patterns
        """
        if not self.failure_patterns["incorrect"] and not self.failure_patterns["missing"]:
            return {
                "total_failures": 0,
                "common_issues": [],
                "suggested_improvements": []
            }
        
        all_failures = (
            self.failure_patterns["incorrect"] +
            self.failure_patterns["missing"]
        )
        
        # Count failure frequency
        failure_counts = Counter(all_failures)
        
        # Analyze common terms in failed queries
        all_words = []
        for query in all_failures:
            all_words.extend(query.lower().split())
        
        word_counts = Counter(all_words)
        # Remove common words
        common_words = {"the", "a", "an", "and", "or", "but", "in", "on", "at", "to", "for"}
        common_terms = [
            (word, count) for word, count in word_counts.most_common(20)
            if word not in common_words and len(word) > 2
        ]
        
        # Generate suggestions
        suggestions = []
        if common_terms:
            suggestions.append(
                f"Consider mapping concepts for: {', '.join(word for word, _ in common_terms[:5])}"
            )
        
        if failure_counts.most_common(1):
            most_common = failure_counts.most_common(1)[0]
            suggestions.append(
                f"Query '{most_common[0]}' failed {most_common[1]} times - needs attention"
            )
        
        return {
            "total_failures": len(all_failures),
            "unique_failures": len(failure_counts),
            "most_common_failures": failure_counts.most_common(5),
            "common_terms": common_terms[:10],
            "suggested_improvements": suggestions
        }
    
    def get_query_patterns(self, top_n: int = 10) -> List[Tuple[str, int]]:
        """Get most common query patterns.
        
        Args:
            top_n: Number of top patterns to return
            
        Returns:
            List of (intent, count) tuples
        """
        return sorted(
            self.query_patterns.items(),
            key=lambda x: x[1],
            reverse=True
        )[:top_n]
    
    def suggest_new_concepts(
        self,
        min_occurrences: int = 3
    ) -> List[Dict[str, Any]]:
        """Suggest new concepts to add based on failed queries.
        
        Args:
            min_occurrences: Minimum times a term must appear
            
        Returns:
            List of suggested concepts with context
        """
        # Analyze words in failed queries
        all_failures = (
            self.failure_patterns["incorrect"] +
            self.failure_patterns["missing"]
        )
        
        if not all_failures:
            return []
        
        # Extract potential concept names
        all_words = []
        for query in all_failures:
            words = query.lower().split()
            all_words.extend(words)
        
        word_counts = Counter(all_words)
        
        # Filter to meaningful terms
        common_words = {
            "the", "a", "an", "and", "or", "but", "in", "on", "at", "to",
            "for", "with", "show", "find", "get", "list", "all", "me"
        }
        
        suggestions = []
        for word, count in word_counts.most_common(50):
            if (count >= min_occurrences and
                word not in common_words and
                len(word) > 2):
                
                # Find example queries containing this term
                examples = [
                    q for q in all_failures[:5]
                    if word in q.lower()
                ]
                
                suggestions.append({
                    "term": word,
                    "occurrences": count,
                    "example_queries": examples[:3]
                })
        
        return suggestions[:10]
    
    def get_improvement_recommendations(self) -> Dict[str, Any]:
        """Get comprehensive improvement recommendations.
        
        Returns:
            Recommendations for system improvements
        """
        feedback_summary = self.get_feedback_summary(days=30)
        failure_analysis = self.analyze_failure_patterns()
        concept_suggestions = self.suggest_new_concepts(min_occurrences=2)
        query_patterns = self.get_query_patterns(top_n=10)
        
        recommendations = {
            "overall_health": "good" if feedback_summary.get("success_rate", 0) > 80 else "needs_improvement",
            "feedback_summary": feedback_summary,
            "failure_analysis": failure_analysis,
            "new_concept_suggestions": concept_suggestions,
            "popular_query_patterns": query_patterns,
            "action_items": []
        }
        
        # Generate action items
        if feedback_summary.get("success_rate", 0) < 80:
            recommendations["action_items"].append(
                "Success rate below 80% - review failed queries and improve mappings"
            )
        
        if len(concept_suggestions) > 0:
            recommendations["action_items"].append(
                f"Add {len(concept_suggestions)} new concepts based on user queries"
            )
        
        if failure_analysis.get("total_failures", 0) > 10:
            recommendations["action_items"].append(
                "High failure count - focus on most common failure patterns"
            )
        
        return recommendations
    
    def _load_feedback(self):
        """Load feedback from disk."""
        if not self.feedback_file.exists():
            return
        
        try:
            with open(self.feedback_file, 'r') as f:
                for line in f:
                    if line.strip():
                        data = json.loads(line)
                        # Reconstruct feedback object
                        feedback = QueryFeedback(**data)
                        self.feedback_cache.append(feedback)
                        
                        # Update patterns
                        intent_str = str(feedback.semantic_plan.intent)
                        self.query_patterns[intent_str] += 1
                        
                        if feedback.feedback_type in ["incorrect", "missing"]:
                            self.failure_patterns[feedback.feedback_type].append(
                                feedback.query_text
                            )
        except Exception as e:
            logger.error(f"Error loading feedback: {e}", exc_info=True)
    
    def _save_feedback(self, feedback: QueryFeedback):
        """Save single feedback entry to disk.
        
        Args:
            feedback: Feedback to save
        """
        try:
            # Convert to dict for JSON serialization
            data = feedback.model_dump(mode='json')
            
            with open(self.feedback_file, 'a') as f:
                f.write(json.dumps(data) + '\n')
        except Exception as e:
            logger.error(f"Error saving feedback: {e}", exc_info=True)
    
    def export_feedback(
        self,
        output_file: Path,
        days: Optional[int] = None
    ) -> int:
        """Export feedback to a file.
        
        Args:
            output_file: Path to export file
            days: Optional number of days to include (None = all)
            
        Returns:
            Number of feedback entries exported
        """
        feedback_to_export = self.feedback_cache
        
        if days:
            cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
            feedback_to_export = [
                f for f in feedback_to_export
                if f.timestamp >= cutoff_date
            ]
        
        output_file.parent.mkdir(parents=True, exist_ok=True)
        
        with open(output_file, 'w') as f:
            for feedback in feedback_to_export:
                data = feedback.model_dump(mode='json')
                f.write(json.dumps(data, indent=2) + '\n')
        
        logger.info(f"Exported {len(feedback_to_export)} feedback entries to {output_file}")
        return len(feedback_to_export)
    
    def clear_old_feedback(self, days: int = 90) -> int:
        """Remove feedback older than specified days.
        
        Args:
            days: Keep feedback newer than this many days
            
        Returns:
            Number of entries removed
        """
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        old_count = len(self.feedback_cache)
        self.feedback_cache = [
            f for f in self.feedback_cache
            if f.timestamp >= cutoff_date
        ]
        removed = old_count - len(self.feedback_cache)
        
        # Rebuild patterns
        self.query_patterns.clear()
        self.failure_patterns.clear()
        for feedback in self.feedback_cache:
            intent_str = str(feedback.semantic_plan.intent)
            self.query_patterns[intent_str] += 1
            if feedback.feedback_type in ["incorrect", "missing"]:
                self.failure_patterns[feedback.feedback_type].append(
                    feedback.query_text
                )
        
        # Rewrite file
        if removed > 0:
            self.feedback_file.unlink(missing_ok=True)
            for feedback in self.feedback_cache:
                self._save_feedback(feedback)
        
        logger.info(f"Removed {removed} old feedback entries")
        return removed
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get overall feedback statistics.
        
        Returns:
            Statistics dictionary
        """
        if not self.feedback_cache:
            return {
                "total_feedback": 0,
                "feedback_by_type": {},
                "average_age_days": 0,
                "oldest_feedback": None,
                "newest_feedback": None
            }
        
        type_counts = Counter(f.feedback_type for f in self.feedback_cache)
        
        now = datetime.now(timezone.utc)
        ages = [(now - f.timestamp).days for f in self.feedback_cache]
        
        return {
            "total_feedback": len(self.feedback_cache),
            "feedback_by_type": dict(type_counts),
            "average_age_days": sum(ages) / len(ages) if ages else 0,
            "oldest_feedback": min(f.timestamp for f in self.feedback_cache),
            "newest_feedback": max(f.timestamp for f in self.feedback_cache),
            "unique_queries": len(set(f.query_text for f in self.feedback_cache))
        }
