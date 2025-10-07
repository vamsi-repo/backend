"""
Enterprise MS Fabric Lakehouse Service for Data Sync AI
Handles file storage operations in MS Fabric Lakehouse following your exact workflows:
- Store original files during data validation
- Store corrected files after processing  
- Manage file metadata and access
"""
import logging
import os
import shutil
from typing import Dict, List, Optional, Any, Tuple, BinaryIO
from datetime import datetime
import pandas as pd
import json
from azure.identity import ClientSecretCredential
from azure.storage.filedatalake import DataLakeServiceClient, FileSystemClient
from io import BytesIO, StringIO
from .app_redis import AppRedis

logger = logging.getLogger('data_sync_ai.lakehouse')

class LakehouseManager:
    """
    Enterprise MS Fabric Lakehouse service for file operations
    Manages original and corrected files according to your workflows
    """
    
    def __init__(self, onelake_url: str, tenant_id: str, client_id: str, 
                 client_secret: str, cache: AppRedis = None):
        """Initialize Lakehouse connection"""
        try:
            # Azure authentication
            self.credential = ClientSecretCredential(
                tenant_id=tenant_id,
                client_id=client_id,
                client_secret=client_secret
            )
            
            self.onelake_url = onelake_url.rstrip('/')
            self.cache = cache
            
            # Initialize Data Lake service client
            self.service_client = DataLakeServiceClient(
                account_url=self.onelake_url,
                credential=self.credential
            )
            
            # Test connection
            self._test_connection()
            
            # File system paths
            self.original_path = 'original_files'
            self.corrected_path = 'corrected_files' 
            self.processed_path = 'processed_files'
            
            logger.info(f"Lakehouse connection established: {self.onelake_url}")
            
        except Exception as e:
            logger.error(f"Lakehouse initialization failed: {str(e)}")
            raise
    
    def _test_connection(self):
        """Test Lakehouse connection"""
        try:
            # List file systems to test connection
            file_systems = list(self.service_client.list_file_systems())
            logger.info(f"Lakehouse connection test successful. Found {len(file_systems)} file systems")
        except Exception as e:
            logger.error(f"Lakehouse connection test failed: {str(e)}")
            raise
    
    def _ensure_file_system_exists(self, file_system_name: str) -> FileSystemClient:
        """Ensure file system exists and return client"""
        try:
            file_system_client = self.service_client.get_file_system_client(file_system_name)
            
            # Try to get properties (this will fail if file system doesn't exist)
            try:
                file_system_client.get_file_system_properties()
                logger.debug(f"File system exists: {file_system_name}")
            except Exception:
                # Create file system if it doesn't exist
                file_system_client.create_file_system()
                logger.info(f"Created file system: {file_system_name}")
            
            return file_system_client
            
        except Exception as e:
            logger.error(f"Failed to ensure file system {file_system_name}: {str(e)}")
            raise
    
    def _generate_lakehouse_path(self, file_type: str, template_id: int, user_id: int, 
                                filename: str, tenant_id: Optional[int] = None) -> str:
        """Generate consistent Lakehouse path structure with *_sl suffix"""
        # Sanitize filename and add *_sl suffix
        name, ext = os.path.splitext(filename)
        safe_name = self._sanitize_filename(name)
        safe_filename = f"{safe_name}*_sl{ext}"
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        # Build path components
        base_path = {
            'original': self.original_path,
            'corrected': self.corrected_path,
            'processed': self.processed_path
        }.get(file_type, 'misc')
        
        # Include tenant in path if provided
        if tenant_id:
            path = f"{base_path}/tenant_{tenant_id}/user_{user_id}/template_{template_id}/{timestamp}_{safe_filename}"
        else:
            path = f"{base_path}/user_{user_id}/template_{template_id}/{timestamp}_{safe_filename}"
        
        return path
    
    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for Lakehouse storage"""
        # Remove or replace invalid characters
        invalid_chars = '<>:"/\\|?*'
        for char in invalid_chars:
            filename = filename.replace(char, '_')
        
        # Limit length and ensure it has an extension
        name, ext = os.path.splitext(filename)
        if len(name) > 200:
            name = name[:200]
        
        return f"{name}{ext}" if ext else f"{name}.data"
    
    # ORIGINAL FILE STORAGE (Data Validation Workflow Step 1)
    
    def store_original_file(self, file_path: str, template_id: int, user_id: int, 
                          tenant_id: Optional[int] = None, metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Store original file in Lakehouse during data validation workflow
        Returns file metadata including Lakehouse path
        """
        logger.info(f"Storing original file: {file_path} for template {template_id}")
        
        try:
            filename = os.path.basename(file_path)
            file_size = os.path.getsize(file_path)
            
            # Generate Lakehouse path
            lakehouse_path = self._generate_lakehouse_path('original', template_id, user_id, filename, tenant_id)
            
            # Ensure file system exists
            file_system_client = self._ensure_file_system_exists('datasyncai')
            
            # Create directory structure
            directory_path = os.path.dirname(lakehouse_path)
            if directory_path:
                directory_client = file_system_client.get_directory_client(directory_path)
                directory_client.create_directory()
            
            # Upload file
            file_client = file_system_client.get_file_client(lakehouse_path)
            
            with open(file_path, 'rb') as file_data:
                file_client.upload_data(
                    file_data.read(),
                    overwrite=True,
                    content_settings={
                        'content_type': self._get_content_type(filename)
                    }
                )
            
            # Prepare file metadata
            file_metadata = {
                'lakehouse_path': lakehouse_path,
                'original_filename': filename,
                'file_size_bytes': file_size,
                'upload_timestamp': datetime.now().isoformat(),
                'file_type': 'original',
                'template_id': template_id,
                'user_id': user_id,
                'tenant_id': tenant_id,
                'content_type': self._get_content_type(filename),
                'metadata': metadata or {}
            }
            
            # Cache file metadata
            if self.cache:
                cache_key = f"lakehouse:original:{template_id}:{user_id}"
                self.cache.set_cache(cache_key, file_metadata, ttl=7200)  # 2 hours
            
            logger.info(f"Original file stored successfully: {lakehouse_path}")
            return file_metadata
            
        except Exception as e:
            logger.error(f"Failed to store original file {file_path}: {str(e)}")
            raise
    
    def store_original_file_from_bytes(self, file_data: bytes, filename: str, template_id: int, 
                                     user_id: int, tenant_id: Optional[int] = None, 
                                     metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """Store original file from bytes data"""
        logger.info(f"Storing original file from bytes: {filename} for template {template_id}")
        
        try:
            # Generate Lakehouse path
            lakehouse_path = self._generate_lakehouse_path('original', template_id, user_id, filename, tenant_id)
            
            # Ensure file system exists
            file_system_client = self._ensure_file_system_exists('datasyncai')
            
            # Create directory structure
            directory_path = os.path.dirname(lakehouse_path)
            if directory_path:
                directory_client = file_system_client.get_directory_client(directory_path)
                directory_client.create_directory()
            
            # Upload file
            file_client = file_system_client.get_file_client(lakehouse_path)
            file_client.upload_data(
                file_data,
                overwrite=True,
                content_settings={
                    'content_type': self._get_content_type(filename)
                }
            )
            
            # Prepare file metadata
            file_metadata = {
                'lakehouse_path': lakehouse_path,
                'original_filename': filename,
                'file_size_bytes': len(file_data),
                'upload_timestamp': datetime.now().isoformat(),
                'file_type': 'original',
                'template_id': template_id,
                'user_id': user_id,
                'tenant_id': tenant_id,
                'content_type': self._get_content_type(filename),
                'metadata': metadata or {}
            }
            
            # Cache file metadata
            if self.cache:
                cache_key = f"lakehouse:original:{template_id}:{user_id}"
                self.cache.set_cache(cache_key, file_metadata, ttl=7200)
            
            logger.info(f"Original file stored successfully from bytes: {lakehouse_path}")
            return file_metadata
            
        except Exception as e:
            logger.error(f"Failed to store original file from bytes: {str(e)}")
            raise
    
    # CORRECTED FILE STORAGE (Data Validation Workflow Final Steps)
    
    def store_corrected_file(self, df_corrected: pd.DataFrame, original_filename: str, 
                           template_id: int, user_id: int, tenant_id: Optional[int] = None,
                           metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """
        Store corrected file after validation processing
        Takes corrected DataFrame and stores as Excel/CSV based on original format
        """
        logger.info(f"Storing corrected file for template {template_id}")
        
        try:
            # Determine output format based on original filename and add *_sl suffix
            name, ext = os.path.splitext(original_filename)
            corrected_filename = f"{name}_corrected*_sl{ext}"
            
            # Convert DataFrame to bytes based on format
            if ext.lower() in ['.xlsx', '.xls']:
                # Store as Excel
                buffer = BytesIO()
                with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                    df_corrected.to_excel(writer, index=False, sheet_name='Corrected_Data')
                file_bytes = buffer.getvalue()
            else:
                # Store as CSV
                buffer = StringIO()
                df_corrected.to_csv(buffer, index=False)
                file_bytes = buffer.getvalue().encode('utf-8')
            
            # Generate Lakehouse path
            lakehouse_path = self._generate_lakehouse_path('corrected', template_id, user_id, corrected_filename, tenant_id)
            
            # Ensure file system exists
            file_system_client = self._ensure_file_system_exists('datasyncai')
            
            # Create directory structure
            directory_path = os.path.dirname(lakehouse_path)
            if directory_path:
                directory_client = file_system_client.get_directory_client(directory_path)
                directory_client.create_directory()
            
            # Upload corrected file
            file_client = file_system_client.get_file_client(lakehouse_path)
            file_client.upload_data(
                file_bytes,
                overwrite=True,
                content_settings={
                    'content_type': self._get_content_type(corrected_filename)
                }
            )
            
            # Prepare file metadata
            file_metadata = {
                'lakehouse_path': lakehouse_path,
                'original_filename': corrected_filename,
                'file_size_bytes': len(file_bytes),
                'upload_timestamp': datetime.now().isoformat(),
                'file_type': 'corrected',
                'template_id': template_id,
                'user_id': user_id,
                'tenant_id': tenant_id,
                'content_type': self._get_content_type(corrected_filename),
                'rows_corrected': len(df_corrected),
                'columns_count': len(df_corrected.columns),
                'metadata': metadata or {}
            }
            
            # Cache file metadata
            if self.cache:
                cache_key = f"lakehouse:corrected:{template_id}:{user_id}"
                self.cache.set_cache(cache_key, file_metadata, ttl=7200)
            
            logger.info(f"Corrected file stored successfully: {lakehouse_path}")
            return file_metadata
            
        except Exception as e:
            logger.error(f"Failed to store corrected file: {str(e)}")
            raise
    
    def store_processed_file(self, file_data: bytes, filename: str, template_id: int,
                           user_id: int, tenant_id: Optional[int] = None,
                           metadata: Optional[Dict] = None) -> Dict[str, Any]:
        """Store processed file (intermediate processing files)"""
        logger.info(f"Storing processed file: {filename} for template {template_id}")
        
        try:
            # Generate Lakehouse path
            lakehouse_path = self._generate_lakehouse_path('processed', template_id, user_id, filename, tenant_id)
            
            # Ensure file system exists
            file_system_client = self._ensure_file_system_exists('datasyncai')
            
            # Create directory structure
            directory_path = os.path.dirname(lakehouse_path)
            if directory_path:
                directory_client = file_system_client.get_directory_client(directory_path)
                directory_client.create_directory()
            
            # Upload file
            file_client = file_system_client.get_file_client(lakehouse_path)
            file_client.upload_data(
                file_data,
                overwrite=True,
                content_settings={
                    'content_type': self._get_content_type(filename)
                }
            )
            
            # Prepare file metadata
            file_metadata = {
                'lakehouse_path': lakehouse_path,
                'original_filename': filename,
                'file_size_bytes': len(file_data),
                'upload_timestamp': datetime.now().isoformat(),
                'file_type': 'processed',
                'template_id': template_id,
                'user_id': user_id,
                'tenant_id': tenant_id,
                'content_type': self._get_content_type(filename),
                'metadata': metadata or {}
            }
            
            logger.info(f"Processed file stored successfully: {lakehouse_path}")
            return file_metadata
            
        except Exception as e:
            logger.error(f"Failed to store processed file: {str(e)}")
            raise
    
    # FILE RETRIEVAL METHODS
    
    def download_file(self, lakehouse_path: str) -> Tuple[bytes, Dict]:
        """Download file from Lakehouse and return bytes and metadata"""
        logger.info(f"Downloading file: {lakehouse_path}")
        
        try:
            file_system_client = self._ensure_file_system_exists('datasyncai')
            file_client = file_system_client.get_file_client(lakehouse_path)
            
            # Download file
            download = file_client.download_file()
            file_bytes = download.readall()
            
            # Get file properties
            properties = file_client.get_file_properties()
            
            metadata = {
                'content_type': properties.content_settings.content_type,
                'last_modified': properties.last_modified,
                'size': properties.size,
                'etag': properties.etag
            }
            
            logger.info(f"File downloaded successfully: {len(file_bytes)} bytes")
            return file_bytes, metadata
            
        except Exception as e:
            logger.error(f"Failed to download file {lakehouse_path}: {str(e)}")
            raise
    
    def get_file_metadata(self, lakehouse_path: str) -> Dict:
        """Get file metadata without downloading"""
        try:
            file_system_client = self._ensure_file_system_exists('datasyncai')
            file_client = file_system_client.get_file_client(lakehouse_path)
            
            properties = file_client.get_file_properties()
            
            return {
                'lakehouse_path': lakehouse_path,
                'content_type': properties.content_settings.content_type if properties.content_settings else None,
                'last_modified': properties.last_modified.isoformat() if properties.last_modified else None,
                'size': properties.size,
                'etag': properties.etag,
                'exists': True
            }
            
        except Exception as e:
            logger.warning(f"File metadata not found for {lakehouse_path}: {str(e)}")
            return {'lakehouse_path': lakehouse_path, 'exists': False, 'error': str(e)}
    
    def list_template_files(self, template_id: int, user_id: int, file_type: Optional[str] = None,
                          tenant_id: Optional[int] = None) -> List[Dict]:
        """List all files for a specific template"""
        logger.info(f"Listing files for template {template_id}, user {user_id}")
        
        try:
            file_system_client = self._ensure_file_system_exists('datasyncai')
            
            # Build search path
            if tenant_id:
                search_paths = [f"{file_type or 'original'}/tenant_{tenant_id}/user_{user_id}/template_{template_id}",
                              f"{file_type or 'corrected'}/tenant_{tenant_id}/user_{user_id}/template_{template_id}",
                              f"{file_type or 'processed'}/tenant_{tenant_id}/user_{user_id}/template_{template_id}"]
            else:
                search_paths = [f"{file_type or 'original'}/user_{user_id}/template_{template_id}",
                              f"{file_type or 'corrected'}/user_{user_id}/template_{template_id}",
                              f"{file_type or 'processed'}/user_{user_id}/template_{template_id}"]
            
            all_files = []
            
            for search_path in search_paths:
                try:
                    paths = file_system_client.get_paths(path=search_path, recursive=True)
                    
                    for path in paths:
                        if not path.is_directory:
                            file_info = {
                                'lakehouse_path': path.name,
                                'filename': os.path.basename(path.name),
                                'last_modified': path.last_modified.isoformat() if path.last_modified else None,
                                'size': path.content_length,
                                'file_type': self._determine_file_type_from_path(path.name),
                                'etag': path.etag
                            }
                            all_files.append(file_info)
                            
                except Exception as path_error:
                    # Path might not exist, continue with other paths
                    logger.debug(f"Search path not found: {search_path}")
                    continue
            
            logger.info(f"Found {len(all_files)} files for template {template_id}")
            return all_files
            
        except Exception as e:
            logger.error(f"Failed to list template files: {str(e)}")
            return []
    
    def delete_file(self, lakehouse_path: str) -> bool:
        """Delete file from Lakehouse"""
        logger.info(f"Deleting file: {lakehouse_path}")
        
        try:
            file_system_client = self._ensure_file_system_exists('datasyncai')
            file_client = file_system_client.get_file_client(lakehouse_path)
            
            file_client.delete_file()
            
            # Clear cache if exists
            if self.cache:
                # Clear related cache entries
                cache_patterns = [f"*{lakehouse_path}*", f"*lakehouse*"]
                for pattern in cache_patterns:
                    self.cache.delete_pattern(pattern)
            
            logger.info(f"File deleted successfully: {lakehouse_path}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to delete file {lakehouse_path}: {str(e)}")
            return False
    
    def cleanup_template_files(self, template_id: int, user_id: int, tenant_id: Optional[int] = None,
                             older_than_days: int = 30) -> int:
        """Cleanup old files for a template"""
        logger.info(f"Cleaning up files for template {template_id} older than {older_than_days} days")
        
        try:
            cutoff_date = datetime.now().timestamp() - (older_than_days * 24 * 60 * 60)
            files = self.list_template_files(template_id, user_id, tenant_id=tenant_id)
            
            deleted_count = 0
            for file_info in files:
                if file_info['last_modified']:
                    file_date = datetime.fromisoformat(file_info['last_modified'].replace('Z', '+00:00'))
                    if file_date.timestamp() < cutoff_date:
                        if self.delete_file(file_info['lakehouse_path']):
                            deleted_count += 1
            
            logger.info(f"Cleaned up {deleted_count} old files for template {template_id}")
            return deleted_count
            
        except Exception as e:
            logger.error(f"File cleanup failed: {str(e)}")
            return 0
    
    # UTILITY METHODS
    
    def _get_content_type(self, filename: str) -> str:
        """Determine content type from filename"""
        ext = os.path.splitext(filename)[1].lower()
        
        content_types = {
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
            '.xls': 'application/vnd.ms-excel',
            '.csv': 'text/csv',
            '.txt': 'text/plain',
            '.dat': 'application/octet-stream',
            '.json': 'application/json'
        }
        
        return content_types.get(ext, 'application/octet-stream')
    
    def _determine_file_type_from_path(self, lakehouse_path: str) -> str:
        """Determine file type from Lakehouse path"""
        if '/original_files/' in lakehouse_path:
            return 'original'
        elif '/corrected_files/' in lakehouse_path:
            return 'corrected'
        elif '/processed_files/' in lakehouse_path:
            return 'processed'
        else:
            return 'unknown'
    
    def get_storage_stats(self) -> Dict:
        """Get Lakehouse storage statistics"""
        try:
            file_system_client = self._ensure_file_system_exists('datasyncai')
            
            stats = {
                'original_files_count': 0,
                'corrected_files_count': 0,
                'processed_files_count': 0,
                'total_size_bytes': 0,
                'last_updated': datetime.now().isoformat()
            }
            
            # Count files in each category
            for file_type in ['original_files', 'corrected_files', 'processed_files']:
                try:
                    paths = file_system_client.get_paths(path=file_type, recursive=True)
                    
                    for path in paths:
                        if not path.is_directory:
                            stats[f'{file_type}_count'] += 1
                            stats['total_size_bytes'] += path.content_length or 0
                            
                except Exception:
                    # Path might not exist
                    continue
            
            return stats
            
        except Exception as e:
            logger.error(f"Failed to get storage stats: {str(e)}")
            return {'error': str(e)}
    
    def health_check(self) -> Dict:
        """Perform health check on Lakehouse service"""
        try:
            # Test connection
            file_systems = list(self.service_client.list_file_systems())
            
            # Test file operations
            test_data = b"health_check_test"
            test_path = f"health_check/test_{int(datetime.now().timestamp())}.txt"
            
            file_system_client = self._ensure_file_system_exists('datasyncai')
            file_client = file_system_client.get_file_client(test_path)
            
            # Test upload
            file_client.upload_data(test_data, overwrite=True)
            
            # Test download
            download = file_client.download_file()
            downloaded_data = download.readall()
            
            # Test delete
            file_client.delete_file()
            
            # Verify operations
            upload_success = True
            download_success = downloaded_data == test_data
            delete_success = True
            
            return {
                'status': 'healthy',
                'connection': True,
                'file_systems_count': len(file_systems),
                'upload_test': upload_success,
                'download_test': download_success,
                'delete_test': delete_success,
                'timestamp': datetime.now().isoformat()
            }
            
        except Exception as e:
            return {
                'status': 'unhealthy',
                'error': str(e),
                'timestamp': datetime.now().isoformat()
            }
