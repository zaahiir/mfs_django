#utils.py

from django.contrib.auth import get_user_model
from django.forms import model_to_dict
from django.utils.timezone import now
from django.db.models import Model
from decimal import Decimal
from rest_framework_simplejwt.tokens import RefreshToken
from ipware import get_client_ip
from .models import ActivityLog
import datetime

User = get_user_model()


def get_tokens_for_user(user_data):
    """Generate JWT tokens for a user with custom claims"""
    user, _ = User.objects.get_or_create(username=user_data.get('username'))
    refresh = RefreshToken.for_user(user)

    # Add custom claims
    refresh['user_type'] = user_data.get('user_type')
    refresh['custom_user_id'] = user_data.get('id')

    return {
        'refresh': str(refresh),
        'access': str(refresh.access_token),
    }


class ActivityLogger:

    @staticmethod
    def serialize_date(obj):
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        raise TypeError(f"Type {type(obj)} not serializable")

    @staticmethod
    def handle_file_upload(file):
        """Handle file upload objects by extracting relevant information"""
        if hasattr(file, 'name') and hasattr(file, 'size'):
            return {
                'filename': file.name,
                'size': file.size,
                'content_type': getattr(file, 'content_type', None)
            }
        return str(file)

    @staticmethod
    def normalize_value(value):
        """Convert complex data types to simple JSON-serializable values"""
        if isinstance(value, Model):
            if hasattr(value, 'amcName'):
                return value.amcName
            elif hasattr(value, 'arnNumber'):
                return value.arnNumber
            return value.id
        elif isinstance(value, Decimal):
            return str(value)
        elif isinstance(value, (datetime.date, datetime.datetime)):
            return ActivityLogger.serialize_date(value)
        elif isinstance(value, (list, tuple)):
            return [ActivityLogger.normalize_value(item) for item in value]
        elif isinstance(value, dict):
            return {k: ActivityLogger.normalize_value(v) for k, v in value.items()}
        elif hasattr(value, '_name') and hasattr(value, 'file'):  # InMemoryUploadedFile check
            return ActivityLogger.handle_file_upload(value)
        return value

    @staticmethod
    def clean_request_data(request_data):
        """Clean request data by handling file uploads and other complex types"""
        if isinstance(request_data, dict):
            cleaned_data = {}
            for key, value in request_data.items():
                if key == 'courierFile' and hasattr(value, '_name'):
                    cleaned_data[key] = ActivityLogger.handle_file_upload(value)
                else:
                    cleaned_data[key] = ActivityLogger.normalize_value(value)
            return cleaned_data
        return request_data

    @staticmethod
    def serialize_model_instance(instance):
        """Serialize a model instance to clean, normalized format"""
        if instance is None:
            return None

        try:
            data = model_to_dict(instance)
            normalized_data = {
                key: ActivityLogger.normalize_value(value)
                for key, value in data.items()
            }
            return normalized_data
        except Exception as e:
            print(f"Serialization error: {str(e)}")
            return None

    @staticmethod
    def prepare_details(request, details=None, instance=None):
        """Prepare details in the exact format needed"""
        normalized_details = {
            'path': request.path,
            'method': request.method,
            'timestamp': ActivityLogger.serialize_date(now()),
        }

        if details and 'new_data' in details:
            data = details['new_data']
            if isinstance(data, str):
                try:
                    import ast
                    data = ast.literal_eval(data)
                except:
                    data = {'raw_data': data}

            # Clean the request data
            normalized_details['data'] = ActivityLogger.clean_request_data(data)

        elif instance:
            if hasattr(instance.__class__, 'get_serializer'):
                serializer_class = instance.__class__.get_serializer()
                normalized_data = serializer_class(instance).data
            else:
                normalized_data = model_to_dict(instance)
                normalized_data = ActivityLogger.clean_request_data(normalized_data)

            normalized_details['data'] = normalized_data

        return normalized_details

    @staticmethod
    def log_activity(request, action, entity_type=None, entity_id=None, details=None, instance=None,
                     previous_data=None):
        """Log activity with proper handling of file uploads"""
        try:
            client_ip, _ = get_client_ip(request)
            if not client_ip:
                client_ip = '0.0.0.0'

            username = request.user.username if request.user.is_authenticated else 'Anonymous'

            # Clean and normalize the details
            normalized_details = ActivityLogger.prepare_details(request, details, instance)

            # Clean previous data if it exists
            if previous_data:
                previous_data = ActivityLogger.clean_request_data(previous_data)

            activity_log = ActivityLog(
                user=request.user if request.user.is_authenticated else None,
                username=username,
                action=action,
                entity_type=entity_type,
                entity_id=str(entity_id) if entity_id is not None else None,
                details=normalized_details,
                previous_data=previous_data,
                ip_address=client_ip
            )
            activity_log.save()

            return activity_log

        except Exception as e:
            print(f"Error logging activity: {str(e)}")
            import traceback
            traceback.print_exc()
            return None

    @staticmethod
    def log_crud(request, action, model_name, instance_id, details=None, instance=None):
        """Log CRUD operations with normalized data"""
        return ActivityLogger.log_activity(
            request=request,
            action=action,
            entity_type=model_name,
            entity_id=instance_id,
            details=details,
            instance=instance
        )

    @staticmethod
    def log_auth(request, action):
        """Log authentication-related activities"""
        return ActivityLogger.log_activity(
            request=request,
            action=action,
            entity_type='Authentication'
        )


def get_client_info(request):
    """Extract client information from request"""
    client_ip, _ = get_client_ip(request)
    return {
        'ip_address': client_ip or '0.0.0.0',
        'user_agent': request.META.get('HTTP_USER_AGENT', 'Unknown'),
        'method': request.method,
        'path': request.path
    }
