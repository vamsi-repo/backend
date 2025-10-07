"""
Updated Enterprise Configuration for Data Sync AI
Using default Redis settings as requested
"""
import os
from dotenv import load_dotenv
import logging

load_dotenv()

class Config:
    """Main configuration class for Data Sync AI"""
    
    # Flask Configuration
    SECRET_KEY = os.environ.get('SECRET_KEY', 'data-sync-ai-super-secret-key-change-in-production')
    DEBUG = os.environ.get('DEBUG', 'True').lower() in ['true', '1', 'yes']
    
    # SQL Fabric Configuration
    FABRIC_TENANT_ID = os.environ.get('FABRIC_TENANT_ID', '12345678-1234-1234-1234-123456789012')
    FABRIC_CLIENT_ID = os.environ.get('FABRIC_CLIENT_ID', '87654321-4321-4321-4321-210987654321')
    FABRIC_CLIENT_SECRET = os.environ.get('FABRIC_CLIENT_SECRET', 'your-fabric-client-secret-here')
    FABRIC_SERVER = os.environ.get('FABRIC_SERVER', 'your-fabric-server.database.windows.net')
    FABRIC_DATABASE = os.environ.get('FABRIC_DATABASE', 'DataSyncAI')
    FABRIC_ONELAKE_URL = os.environ.get('FABRIC_ONELAKE_URL', 'https://onelake.dfs.fabric.microsoft.com/your-workspace')
    
    # DuckDB Configuration
    DUCKDB_MEMORY_LIMIT = os.environ.get('DUCKDB_MEMORY_LIMIT', '4GB')
    DUCKDB_THREADS = int(os.environ.get('DUCKDB_THREADS', '4'))
    
    # Redis Configuration (Default local Redis)
    REDIS_HOST = os.environ.get('REDIS_HOST', 'localhost')
    REDIS_PORT = int(os.environ.get('REDIS_PORT', '6379'))
    REDIS_DB = int(os.environ.get('REDIS_DB', '0'))
    REDIS_PASSWORD = os.environ.get('REDIS_PASSWORD', None)
    REDIS_CACHE_TTL = int(os.environ.get('REDIS_CACHE_TTL', '3600'))  # 1 hour
    
    # File Upload Configuration
    UPLOAD_FOLDER = os.environ.get('UPLOAD_FOLDER', 'uploads')
    MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size
    ALLOWED_EXTENSIONS = {'xlsx', 'xls', 'csv', 'txt', 'dat'}
    
    # Lakehouse Configuration
    LAKEHOUSE_ORIGINAL_PATH = 'original_files'
    LAKEHOUSE_CORRECTED_PATH = 'corrected_files'
    LAKEHOUSE_PROCESSED_PATH = 'processed_files'
    
    # Session Configuration
    SESSION_TYPE = 'filesystem'
    SESSION_PERMANENT = False
    PERMANENT_SESSION_LIFETIME = 86400  # 24 hours
    
    # Logging Configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s'

class DevelopmentConfig(Config):
    """Development configuration"""
    DEBUG = True
    LOG_LEVEL = 'DEBUG'

class ProductionConfig(Config):
    """Production configuration"""
    DEBUG = False
    LOG_LEVEL = 'INFO'

# Configuration mapping
config_map = {
    'development': DevelopmentConfig,
    'production': ProductionConfig,
    'default': DevelopmentConfig
}

def get_config():
    """Get configuration based on environment"""
    env = os.environ.get('FLASK_ENV', 'default')
    return config_map.get(env, DevelopmentConfig)

def setup_logging():
    """Setup enterprise-level logging"""
    config = get_config()
    
    # Create logs directory if it doesn't exist
    logs_dir = os.path.join(os.path.dirname(__file__), 'logs')
    os.makedirs(logs_dir, exist_ok=True)
    
    # Configure logging
    logging.basicConfig(
        level=getattr(logging, config.LOG_LEVEL),
        format=config.LOG_FORMAT,
        handlers=[
            logging.FileHandler(os.path.join(logs_dir, 'data_sync_ai.log')),
            logging.StreamHandler()
        ]
    )
    
    # Create specialized loggers
    loggers = {
        'fabric': logging.getLogger('data_sync_ai.fabric'),
        'duckdb': logging.getLogger('data_sync_ai.duckdb'),
        'redis': logging.getLogger('data_sync_ai.redis'),
        'validation': logging.getLogger('data_sync_ai.validation'),
        'auth': logging.getLogger('data_sync_ai.auth'),
        'api': logging.getLogger('data_sync_ai.api')
    }
    
    return loggers
