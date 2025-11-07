"""Configuration management for Schema Translator."""

import os
from pathlib import Path
from typing import Optional

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Config(BaseSettings):
    """Application configuration loaded from environment variables."""
    
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore"
    )
    
    # Anthropic API Configuration
    anthropic_api_key: str = Field(
        ...,
        description="Anthropic API key for Claude access"
    )
    
    # LLM Configuration
    model_name: str = Field(
        default="claude-sonnet-4-20250514",
        description="Claude model to use"
    )
    max_tokens: int = Field(
        default=4096,
        ge=1,
        le=8192,
        description="Maximum tokens for LLM responses"
    )
    temperature: float = Field(
        default=0.0,
        ge=0.0,
        le=1.0,
        description="Temperature for LLM responses"
    )
    
    # Database Configuration
    database_dir: Path = Field(
        default=Path("./databases"),
        description="Directory containing customer databases"
    )
    
    # Knowledge Graph Configuration
    knowledge_graph_path: Path = Field(
        default=Path("./knowledge_graph.json"),
        description="Path to knowledge graph JSON file"
    )
    
    # Query History Configuration
    query_history_path: Path = Field(
        default=Path("./query_history.json"),
        description="Path to query history JSON file"
    )
    
    # Logging Configuration
    log_level: str = Field(
        default="INFO",
        description="Logging level"
    )
    
    @field_validator("database_dir", "knowledge_graph_path", "query_history_path", mode="before")
    @classmethod
    def convert_to_path(cls, v) -> Path:
        """Convert string paths to Path objects."""
        if isinstance(v, str):
            return Path(v)
        return v
    
    @field_validator("log_level")
    @classmethod
    def validate_log_level(cls, v: str) -> str:
        """Validate log level is acceptable."""
        valid_levels = ["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]
        v_upper = v.upper()
        if v_upper not in valid_levels:
            raise ValueError(f"log_level must be one of {valid_levels}")
        return v_upper
    
    def validate_config(self) -> None:
        """Validate configuration on startup."""
        # Check API key is set
        if not self.anthropic_api_key or self.anthropic_api_key == "your_api_key_here":
            raise ValueError(
                "ANTHROPIC_API_KEY must be set in environment or .env file"
            )
        
        # Create database directory if it doesn't exist
        self.database_dir.mkdir(parents=True, exist_ok=True)
        
        # Ensure parent directories exist for other paths
        self.knowledge_graph_path.parent.mkdir(parents=True, exist_ok=True)
        self.query_history_path.parent.mkdir(parents=True, exist_ok=True)
    
    def get_database_path(self, customer_id: str) -> Path:
        """Get path to a specific customer database.
        
        Args:
            customer_id: Customer identifier (e.g., 'a', 'b', 'customer_a')
            
        Returns:
            Path to the customer's SQLite database
        """
        # Normalize customer_id (handle both 'a' and 'customer_a' formats)
        customer_id = customer_id.lower()
        if not customer_id.startswith("customer_"):
            customer_id = f"customer_{customer_id}"
        
        return self.database_dir / f"{customer_id}.db"
    
    def __repr__(self) -> str:
        """Safe repr that doesn't expose API key."""
        return (
            f"Config(model_name='{self.model_name}', "
            f"database_dir='{self.database_dir}', "
            f"log_level='{self.log_level}')"
        )


# Global config instance
_config: Optional[Config] = None


def get_config() -> Config:
    """Get or create the global configuration instance.
    
    Returns:
        Config: The global configuration object
        
    Raises:
        ValueError: If configuration is invalid
    """
    global _config
    if _config is None:
        _config = Config()
        _config.validate_config()
    return _config


def reload_config() -> Config:
    """Reload configuration from environment.
    
    Useful for testing or when environment variables change.
    
    Returns:
        Config: The newly loaded configuration object
    """
    global _config
    _config = Config()
    _config.validate_config()
    return _config
