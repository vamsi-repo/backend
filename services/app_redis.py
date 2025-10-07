"""
Enterprise Redis Caching Service for Data Sync AI
Handles all caching operations with enterprise-level error handling and monitoring
"""
import redis
import json
import logging
import pickle
from typing import Any, Optional, Dict, List
from datetime import datetime, timedelta
from functools import wraps
import hashlib

logger = logging.getLogger('data_sync_ai.redis')

class AppRedis:
    """Enterprise Redis service for Data Sync AI caching"""
    
    def __init__(self, host='localhost', port=6379, db=0, password=None, cache_ttl=3600):
        """Initialize Redis connection with enterprise configuration"""
        try:
            self.redis_client = redis.Redis(
                host=host,
                port=port,
                db=db,
                password=password,
                decode_responses=False,  # Handle binary data
                socket_connect_timeout=5,
                socket_timeout=5,
                retry_on_timeout=True,
                health_check_interval=30
            )
            
            # Test connection
            self.redis_client.ping()
            self.cache_ttl = cache_ttl
            self.is_connected = True
            
            logger.info(f"Redis connection established: {host}:{port}/{db}")
            
        except Exception as e:
            logger.error(f"Redis connection failed: {str(e)}")
            self.redis_client = None
            self.is_connected = False
    
    def _generate_cache_key(self, prefix: str, identifier: str, **kwargs) -> str:
        """Generate consistent cache keys"""
        key_parts = [prefix, identifier]
        
        # Add additional parameters to key
        for key, value in kwargs.items():
            key_parts.append(f"{key}:{value}")
        
        cache_key = ":".join(key_parts)
        
        # Hash long keys to prevent Redis key length issues
        if len(cache_key) > 200:
            cache_key = f"{prefix}:hash:{hashlib.sha256(cache_key.encode()).hexdigest()}"
        
        return cache_key
    
    def set_cache(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """Set cache value with TTL"""
        if not self.is_connected:
            logger.warning("Redis not connected, skipping cache set")
            return False
        
        try:
            ttl = ttl or self.cache_ttl
            
            # Serialize data
            if isinstance(value, (dict, list)):
                serialized_value = json.dumps(value, default=str)
            else:
                serialized_value = pickle.dumps(value)
            
            result = self.redis_client.setex(key, ttl, serialized_value)
            logger.debug(f"Cache set: {key} (TTL: {ttl}s)")
            return result
            
        except Exception as e:
            logger.error(f"Cache set failed for key {key}: {str(e)}")
            return False
    
    def get_cache(self, key: str) -> Optional[Any]:
        """Get cache value"""
        if not self.is_connected:
            logger.warning("Redis not connected, skipping cache get")
            return None
        
        try:
            value = self.redis_client.get(key)
            if value is None:
                return None
            
            # Try JSON first, then pickle
            try:
                return json.loads(value)
            except (json.JSONDecodeError, TypeError):
                return pickle.loads(value)
                
        except Exception as e:
            logger.error(f"Cache get failed for key {key}: {str(e)}")
            return None
    
    def delete_cache(self, key: str) -> bool:
        """Delete cache entry"""
        if not self.is_connected:
            return False
        
        try:
            result = self.redis_client.delete(key)
            logger.debug(f"Cache deleted: {key}")
            return bool(result)
            
        except Exception as e:
            logger.error(f"Cache delete failed for key {key}: {str(e)}")
            return False
    
    def delete_pattern(self, pattern: str) -> int:
        """Delete multiple keys matching pattern"""
        if not self.is_connected:
            return 0
        
        try:
            keys = self.redis_client.keys(pattern)
            if keys:
                deleted = self.redis_client.delete(*keys)
                logger.debug(f"Deleted {deleted} keys matching pattern: {pattern}")
                return deleted
            return 0
            
        except Exception as e:
            logger.error(f"Pattern delete failed for {pattern}: {str(e)}")
            return 0
    
    # Enterprise-specific caching methods
    
    def cache_template_data(self, template_id: int, template_data: Dict, user_id: int, tenant_id: Optional[int] = None) -> bool:
        """Cache template data with multi-tenant support"""
        cache_key = self._generate_cache_key("template", str(template_id), user=user_id, tenant=tenant_id or 'global')
        return self.set_cache(cache_key, template_data, ttl=7200)  # 2 hours for templates
    
    def get_cached_template(self, template_id: int, user_id: int, tenant_id: Optional[int] = None) -> Optional[Dict]:
        """Get cached template data"""
        cache_key = self._generate_cache_key("template", str(template_id), user=user_id, tenant=tenant_id or 'global')
        return self.get_cache(cache_key)
    
    def cache_validation_rules(self, template_id: int, rules_data: List[Dict], tenant_id: Optional[int] = None) -> bool:
        """Cache validation rules for template"""
        cache_key = self._generate_cache_key("rules", str(template_id), tenant=tenant_id or 'global')
        return self.set_cache(cache_key, rules_data, ttl=3600)  # 1 hour for rules
    
    def get_cached_validation_rules(self, template_id: int, tenant_id: Optional[int] = None) -> Optional[List[Dict]]:
        """Get cached validation rules"""
        cache_key = self._generate_cache_key("rules", str(template_id), tenant=tenant_id or 'global')
        return self.get_cache(cache_key)
    
    def cache_user_templates_list(self, user_id: int, templates_list: List[Dict], tenant_id: Optional[int] = None) -> bool:
        """Cache user's templates list"""
        cache_key = self._generate_cache_key("user_templates", str(user_id), tenant=tenant_id or 'global')
        return self.set_cache(cache_key, templates_list, ttl=1800)  # 30 minutes for lists
    
    def get_cached_user_templates_list(self, user_id: int, tenant_id: Optional[int] = None) -> Optional[List[Dict]]:
        """Get cached user templates list"""
        cache_key = self._generate_cache_key("user_templates", str(user_id), tenant=tenant_id or 'global')
        return self.get_cache(cache_key)
    
    def cache_validation_history(self, template_id: int, history_data: Dict, user_id: int) -> bool:
        """Cache validation history for template"""
        cache_key = self._generate_cache_key("validation_history", str(template_id), user=user_id)
        return self.set_cache(cache_key, history_data, ttl=1800)  # 30 minutes
    
    def get_cached_validation_history(self, template_id: int, user_id: int) -> Optional[Dict]:
        """Get cached validation history"""
        cache_key = self._generate_cache_key("validation_history", str(template_id), user=user_id)
        return self.get_cache(cache_key)
    
    def cache_duckdb_analysis(self, file_hash: str, analysis_data: Dict) -> bool:
        """Cache DuckDB file analysis results"""
        cache_key = self._generate_cache_key("duckdb_analysis", file_hash)
        return self.set_cache(cache_key, analysis_data, ttl=14400)  # 4 hours
    
    def get_cached_duckdb_analysis(self, file_hash: str) -> Optional[Dict]:
        """Get cached DuckDB analysis"""
        cache_key = self._generate_cache_key("duckdb_analysis", file_hash)
        return self.get_cache(cache_key)
    
    def invalidate_template_cache(self, template_id: int, user_id: int, tenant_id: Optional[int] = None):
        """Invalidate all cache entries for a template"""
        patterns = [
            f"template:{template_id}:*",
            f"rules:{template_id}:*", 
            f"user_templates:{user_id}:*",
            f"validation_history:{template_id}:*"
        ]
        
        total_deleted = 0
        for pattern in patterns:
            total_deleted += self.delete_pattern(pattern)
        
        logger.info(f"Invalidated {total_deleted} cache entries for template {template_id}")
    
    def get_cache_stats(self) -> Dict:
        """Get Redis cache statistics"""
        if not self.is_connected:
            return {"status": "disconnected"}
        
        try:
            info = self.redis_client.info()
            return {
                "status": "connected",
                "used_memory": info.get('used_memory_human', 'N/A'),
                "connected_clients": info.get('connected_clients', 0),
                "total_commands_processed": info.get('total_commands_processed', 0),
                "keyspace_hits": info.get('keyspace_hits', 0),
                "keyspace_misses": info.get('keyspace_misses', 0),
                "hit_rate": round(info.get('keyspace_hits', 0) / max(1, info.get('keyspace_hits', 0) + info.get('keyspace_misses', 0)) * 100, 2)
            }
        except Exception as e:
            logger.error(f"Failed to get cache stats: {str(e)}")
            return {"status": "error", "error": str(e)}

def cache_result(cache_key_func=None, ttl=3600):
    """Decorator for caching function results"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            # Get cache instance (assuming it's passed as first argument or available globally)
            cache = None
            if args and hasattr(args[0], 'cache'):
                cache = args[0].cache
            
            if not cache or not cache.is_connected:
                return func(*args, **kwargs)
            
            # Generate cache key
            if cache_key_func:
                cache_key = cache_key_func(*args, **kwargs)
            else:
                cache_key = f"{func.__name__}:{hash(str(args) + str(kwargs))}"
            
            # Try to get from cache
            cached_result = cache.get_cache(cache_key)
            if cached_result is not None:
                logger.debug(f"Cache hit for {func.__name__}")
                return cached_result
            
            # Execute function and cache result
            result = func(*args, **kwargs)
            if result is not None:
                cache.set_cache(cache_key, result, ttl=ttl)
                logger.debug(f"Cached result for {func.__name__}")
            
            return result
        return wrapper
    return decorator

# Global cache instance (will be initialized by app factory)
redis_cache = None

def init_redis(config):
    """Initialize global Redis cache instance"""
    global redis_cache
    redis_cache = AppRedis(
        host=config.REDIS_HOST,
        port=config.REDIS_PORT,
        db=config.REDIS_DB,
        password=config.REDIS_PASSWORD,
        cache_ttl=config.REDIS_CACHE_TTL
    )
    return redis_cache
