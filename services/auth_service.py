"""
Enterprise Authentication and Authorization Service for Data Sync AI
Handles multi-tenant authentication, role-based access, and user management
"""
import logging
import bcrypt
import json
from typing import Dict, List, Optional, Any, Tuple
from datetime import datetime, timedelta
from functools import wraps
from flask import session, request, jsonify
from .fabric_service import CachedFabricService
from .app_redis import AppRedis

logger = logging.getLogger('data_sync_ai.auth')

class AuthService:
    """Enterprise authentication service with multi-tenant support"""
    
    def __init__(self, fabric_service: CachedFabricService, cache: AppRedis = None):
        """Initialize auth service"""
        self.fabric_service = fabric_service
        self.cache = cache
        
        # Role hierarchy
        self.role_hierarchy = {
            'ADMIN': 3,
            'TENANT': 2,
            'USER': 1
        }
        
        logger.info("Auth service initialized")
    
    def authenticate_user(self, email: str, password: str) -> Dict[str, Any]:
        """Authenticate user credentials"""
        logger.info(f"Authentication attempt for: {email}")
        
        try:
            # Check for default admin
            if email == "admin" and password == "admin":
                return self._create_auth_response({
                    'id': 1,
                    'email': 'admin@datasyncai.com',
                    'first_name': 'System',
                    'last_name': 'Administrator',
                    'role': 'ADMIN',
                    'status': 'APPROVED'
                })
            
            # Check cache first
            cache_key = f"user:auth:{email.lower()}"
            cached_user = None
            if self.cache:
                cached_user = self.cache.get_cache(cache_key)
            
            if not cached_user:
                # Query database
                user_data = self.fabric_service.execute_query(
                    "SELECT * FROM login_details WHERE LOWER(email) = ?",
                    (email.lower(),),
                    fetch='one'
                )
                
                if not user_data:
                    logger.warning(f"User not found: {email}")
                    return {'success': False, 'message': 'Invalid credentials'}
                
                # Convert to dict (handle different cursor types)
                if hasattr(user_data, '_asdict'):
                    user_dict = user_data._asdict()
                else:
                    # For pyodbc Row objects
                    columns = [desc[0] for desc in user_data.cursor.description]
                    user_dict = dict(zip(columns, user_data))
                
                # Cache user data (without password)
                user_cache_data = {k: v for k, v in user_dict.items() if k != 'password'}
                if self.cache:
                    self.cache.set_cache(cache_key, user_cache_data, ttl=1800)  # 30 minutes
                
                cached_user = user_cache_data
                password_hash = user_dict.get('password')
            else:
                # Get password from database for cached users
                password_data = self.fabric_service.execute_query(
                    "SELECT password FROM login_details WHERE LOWER(email) = ?",
                    (email.lower(),),
                    fetch='one'
                )
                password_hash = password_data[0] if password_data else None
            
            # Verify password
            if not password_hash or not bcrypt.checkpw(password.encode('utf-8'), password_hash.encode('utf-8')):
                logger.warning(f"Invalid password for: {email}")
                return {'success': False, 'message': 'Invalid credentials'}
            
            # Check user status
            if cached_user.get('status') != 'APPROVED':
                logger.warning(f"User not approved: {email}")
                return {'success': False, 'message': 'Account pending approval'}
            
            logger.info(f"Authentication successful for: {email}")
            return self._create_auth_response(cached_user)
            
        except Exception as e:
            logger.error(f"Authentication error for {email}: {str(e)}")
            return {'success': False, 'message': 'Authentication failed'}
    
    def register_user(self, user_data: Dict[str, str]) -> Dict[str, Any]:
        """Register new user with domain-based tenant assignment"""
        logger.info(f"Registration attempt for: {user_data.get('email')}")
        
        try:
            # Validate required fields
            required_fields = ['first_name', 'last_name', 'email', 'mobile', 'password', 'confirm_password']
            for field in required_fields:
                if not user_data.get(field):
                    return {'success': False, 'message': f'{field} is required'}
            
            # Validate password match
            if user_data['password'] != user_data['confirm_password']:
                return {'success': False, 'message': 'Passwords do not match'}
            
            email = user_data['email'].lower()
            domain = email.split('@')[1] if '@' in email else None
            
            if not domain:
                return {'success': False, 'message': 'Invalid email format'}
            
            # Check if user already exists
            existing_user = self.fabric_service.execute_query(
                "SELECT id FROM login_details WHERE LOWER(email) = ?",
                (email,),
                fetch='one'
            )
            
            if existing_user:
                return {'success': False, 'message': 'User already exists'}
            
            # Find tenant for domain
            tenant_info = self._find_domain_tenant(domain)
            
            # Hash password
            hashed_password = bcrypt.hashpw(user_data['password'].encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            
            # Insert user
            user_id = self.fabric_service.execute_query(
                """
                INSERT INTO login_details (first_name, last_name, email, mobile, password, 
                                         role, domain, tenant_id, status)
                OUTPUT INSERTED.id
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    user_data['first_name'],
                    user_data['last_name'],
                    email,
                    user_data['mobile'],
                    hashed_password,
                    'USER',  # Default role
                    domain,
                    tenant_info['tenant_id'] if tenant_info else None,
                    'PENDING' if tenant_info else 'APPROVED'  # Auto-approve if no tenant
                ),
                fetch='one'
            )[0]
            
            # Clear related caches
            if self.cache:
                self.cache.delete_pattern(f"*{domain}*")
                self.cache.delete_pattern("*users*")
            
            logger.info(f"User registered successfully: {email}")
            return {
                'success': True,
                'message': 'Registration successful' + (' - Pending approval' if tenant_info else ''),
                'user': {'email': email, 'id': user_id}
            }
            
        except Exception as e:
            logger.error(f"Registration error: {str(e)}")
            return {'success': False, 'message': 'Registration failed'}
    
    def _find_domain_tenant(self, domain: str) -> Optional[Dict]:
        """Find tenant responsible for domain"""
        try:
            tenant_data = self.fabric_service.execute_query(
                "SELECT id as tenant_id, email FROM login_details WHERE role = 'TENANT' AND domain = ?",
                (domain,),
                fetch='one'
            )
            
            if tenant_data:
                return {
                    'tenant_id': tenant_data[0],
                    'tenant_email': tenant_data[1]
                }
            
            return None
            
        except Exception as e:
            logger.error(f"Error finding domain tenant: {str(e)}")
            return None
    
    def _create_auth_response(self, user_data: Dict) -> Dict[str, Any]:
        """Create standardized authentication response"""
        return {
            'success': True,
            'message': 'Login successful',
            'user': {
                'id': user_data['id'],
                'email': user_data['email'],
                'first_name': user_data.get('first_name'),
                'last_name': user_data.get('last_name'),
                'role': user_data.get('role', 'USER'),
                'tenant_id': user_data.get('tenant_id')
            }
        }
    
    def create_session(self, user_data: Dict):
        """Create user session"""
        try:
            session['loggedin'] = True
            session['user_id'] = user_data['id']
            session['user_email'] = user_data['email']
            session['user_role'] = user_data.get('role', 'USER')
            session['tenant_id'] = user_data.get('tenant_id')
            session.permanent = True
            
            logger.info(f"Session created for user: {user_data['email']}")
            
        except Exception as e:
            logger.error(f"Session creation failed: {str(e)}")
            raise
    
    def clear_session(self):
        """Clear user session"""
        try:
            user_email = session.get('user_email', 'unknown')
            session.clear()
            logger.info(f"Session cleared for user: {user_email}")
            
        except Exception as e:
            logger.error(f"Session clear failed: {str(e)}")
    
    def get_user_info(self, user_id: int) -> Optional[Dict]:
        """Get user information"""
        try:
            cache_key = f"user:info:{user_id}"
            
            if self.cache:
                cached_info = self.cache.get_cache(cache_key)
                if cached_info:
                    return cached_info
            
            user_data = self.fabric_service.execute_query(
                "SELECT id, first_name, last_name, email, role, domain, tenant_id, status FROM login_details WHERE id = ?",
                (user_id,),
                fetch='one'
            )
            
            if user_data:
                # Convert to dict
                columns = ['id', 'first_name', 'last_name', 'email', 'role', 'domain', 'tenant_id', 'status']
                user_dict = dict(zip(columns, user_data))
                
                if self.cache:
                    self.cache.set_cache(cache_key, user_dict, ttl=1800)
                
                return user_dict
            
            return None
            
        except Exception as e:
            logger.error(f"Error getting user info: {str(e)}")
            return None
    
    def update_user_role(self, user_id: int, new_role: str, admin_id: int) -> bool:
        """Update user role (admin/tenant only)"""
        try:
            if new_role not in self.role_hierarchy:
                return False
            
            self.fabric_service.execute_query(
                "UPDATE login_details SET role = ?, approved_by = ? WHERE id = ?",
                (new_role, admin_id, user_id)
            )
            
            # Clear caches
            if self.cache:
                self.cache.delete_pattern(f"*user:{user_id}*")
                self.cache.delete_pattern("*users*")
            
            logger.info(f"User role updated: {user_id} -> {new_role}")
            return True
            
        except Exception as e:
            logger.error(f"Role update failed: {str(e)}")
            return False
    
    def approve_user(self, user_id: int, approver_id: int) -> bool:
        """Approve pending user"""
        try:
            self.fabric_service.execute_query(
                "UPDATE login_details SET status = 'APPROVED', approved_by = ? WHERE id = ? AND status = 'PENDING'",
                (approver_id, user_id)
            )
            
            # Clear caches
            if self.cache:
                self.cache.delete_pattern(f"*user:{user_id}*")
            
            logger.info(f"User approved: {user_id} by {approver_id}")
            return True
            
        except Exception as e:
            logger.error(f"User approval failed: {str(e)}")
            return False
    
    def get_users_for_tenant(self, tenant_id: int) -> List[Dict]:
        """Get users under tenant management"""
        try:
            users = self.fabric_service.execute_query(
                """
                SELECT id, first_name, last_name, email, role, status, created_at
                FROM login_details 
                WHERE tenant_id = ? OR (role = 'TENANT' AND id = ?)
                ORDER BY created_at DESC
                """,
                (tenant_id, tenant_id),
                fetch='all'
            )
            
            # Convert to list of dicts
            user_list = []
            if users:
                columns = ['id', 'first_name', 'last_name', 'email', 'role', 'status', 'created_at']
                for user_row in users:
                    user_dict = dict(zip(columns, user_row))
                    user_list.append(user_dict)
            
            return user_list
            
        except Exception as e:
            logger.error(f"Error getting tenant users: {str(e)}")
            return []
    
    def get_pending_users(self) -> List[Dict]:
        """Get all pending users (admin only)"""
        try:
            users = self.fabric_service.execute_query(
                """
                SELECT id, first_name, last_name, email, domain, created_at
                FROM login_details 
                WHERE status = 'PENDING'
                ORDER BY created_at ASC
                """,
                fetch='all'
            )
            
            # Convert to list of dicts
            user_list = []
            if users:
                columns = ['id', 'first_name', 'last_name', 'email', 'domain', 'created_at']
                for user_row in users:
                    user_dict = dict(zip(columns, user_row))
                    user_list.append(user_dict)
            
            return user_list
            
        except Exception as e:
            logger.error(f"Error getting pending users: {str(e)}")
            return []
    
    def reset_password(self, email: str, new_password: str, confirm_password: str) -> Dict[str, Any]:
        """Reset user password"""
        try:
            if new_password != confirm_password:
                return {'success': False, 'message': 'Passwords do not match'}
            
            # Check if user exists
            user_exists = self.fabric_service.execute_query(
                "SELECT id FROM login_details WHERE LOWER(email) = ?",
                (email.lower(),),
                fetch='one'
            )
            
            if not user_exists:
                return {'success': False, 'message': 'Email not found'}
            
            # Hash new password
            hashed_password = bcrypt.hashpw(new_password.encode('utf-8'), bcrypt.gensalt()).decode('utf-8')
            
            # Update password
            self.fabric_service.execute_query(
                "UPDATE login_details SET password = ? WHERE LOWER(email) = ?",
                (hashed_password, email.lower())
            )
            
            # Clear user cache
            if self.cache:
                self.cache.delete_pattern(f"*{email.lower()}*")
            
            logger.info(f"Password reset successful for: {email}")
            return {'success': True, 'message': 'Password reset successful'}
            
        except Exception as e:
            logger.error(f"Password reset failed: {str(e)}")
            return {'success': False, 'message': 'Password reset failed'}

# Decorators for authorization
def login_required(f):
    """Decorator to require authentication"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'loggedin' not in session or 'user_id' not in session:
            logger.warning(f"Unauthorized access to {f.__name__}: session missing")
            return jsonify({'success': False, 'message': 'Authentication required'}), 401
        return f(*args, **kwargs)
    return decorated_function

def role_required(required_roles):
    """Decorator to require specific roles"""
    if isinstance(required_roles, str):
        required_roles = [required_roles]
    
    def decorator(f):
        @wraps(f)
        def decorated_function(*args, **kwargs):
            if 'loggedin' not in session or 'user_role' not in session:
                logger.warning(f"Unauthorized access to {f.__name__}: session missing")
                return jsonify({'success': False, 'message': 'Authentication required'}), 401
            
            user_role = session.get('user_role')
            if user_role not in required_roles:
                logger.warning(f"Insufficient permissions for {f.__name__}: user has {user_role}, requires {required_roles}")
                return jsonify({'success': False, 'message': 'Insufficient permissions'}), 403
            
            return f(*args, **kwargs)
        return decorated_function
    return decorator

def tenant_access_required(f):
    """Decorator to require tenant-level access"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'loggedin' not in session:
            return jsonify({'success': False, 'message': 'Authentication required'}), 401
        
        user_role = session.get('user_role')
        if user_role not in ['ADMIN', 'TENANT']:
            return jsonify({'success': False, 'message': 'Tenant access required'}), 403
        
        return f(*args, **kwargs)
    return decorated_function

def admin_required(f):
    """Decorator to require admin access"""
    @wraps(f)
    def decorated_function(*args, **kwargs):
        if 'loggedin' not in session or session.get('user_role') != 'ADMIN':
            logger.warning(f"Admin access denied for {f.__name__}")
            return jsonify({'success': False, 'message': 'Admin access required'}), 403
        return f(*args, **kwargs)
    return decorated_function

def validate_tenant_access(template_id: int, user_id: int, user_role: str, tenant_id: Optional[int] = None) -> bool:
    """Validate if user has access to template based on tenant rules"""
    try:
        # Admin has access to everything
        if user_role == 'ADMIN':
            return True
        
        # For tenant/user roles, check template ownership
        # This would need to be implemented based on your specific tenant isolation requirements
        
        return True  # Simplified for now
        
    except Exception as e:
        logger.error(f"Tenant access validation failed: {str(e)}")
        return False
