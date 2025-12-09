
import functools
from src.feature_store.client import FeatureStore

# Singleton Client for registration
fs_client = FeatureStore()

# Global map: Feature Name -> Function Object
# This allows the Engine to look up the code for "rsi_14"
function_map = {}

def register_feature(name: str, description: str, category: str = "technical", tags: list = None, group: str = "technical"):
    """
    Decorator to register a feature function.
    1. Updates the JSON Catalog via FeatureStore client.
    2. Adds the function to the global function_map for execution.
    """
    def decorator(func):
        # 1. Register Metadata in Catalog
        fs_client.register_feature(
            name=name,
            description=description,
            category=category,
            group=group,
            tags=tags or []
        )
        
        # 2. Register Function for Execution
        function_map[name] = func
        
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            return func(*args, **kwargs)
        return wrapper
    return decorator
