import base64
import traceback
from django.core.exceptions import ValidationError
from django.core.files.base import ContentFile
from django.core.serializers.json import DjangoJSONEncoder
from django.db import transaction
from rest_framework import viewsets, status
from rest_framework.decorators import action
from rest_framework.response import Response
from django.db import connection
from django.contrib.auth import authenticate
from rest_framework.permissions import IsAuthenticated
from django.db.models import Q, Model
from rest_framework_simplejwt.tokens import RefreshToken, TokenError
import urllib.parse
from .serializers import *
from django.http import JsonResponse
from datetime import datetime, timedelta, date
from .utils import get_tokens_for_user, ActivityLogger
from django.core.management import call_command
from django.db.models import Q
from django.core.paginator import Paginator
import json
from django.core.files.storage import default_storage

logger = logging.getLogger(__name__)


class CustomJSONEncoder(DjangoJSONEncoder):
    class CustomJSONEncoder(json.JSONEncoder):
        def default(self, obj):
            if isinstance(obj, (date, datetime)):
                return obj.isoformat()
            return super().default(obj)


class UserViewSet(viewsets.ViewSet):
    @action(detail=False, methods=['post'])
    def login(self, request):
        username = request.data.get('username')
        password = request.data.get('password')
        dob = request.data.get('dob')

        if not username:
            return Response({"detail": "Username is required"}, status=status.HTTP_400_BAD_REQUEST)

        # Authenticate superuser
        user = authenticate(username=username, password=password)
        if user and user.is_superuser:
            ActivityLogger.log_auth(request, 'LOGIN')
            tokens = get_tokens_for_user({
                'username': user.username,
                'id': user.id,
                'user_type': 'superuser'
            })
            return Response({
                **tokens,
                'user_type': 'superuser',
                'user_id': user.id,
                'username': user.username,
                'email': user.email,
            }, status=status.HTTP_200_OK)

        # Check Employee credentials
        employee = EmployeeModel.objects.filter(employeeEmail=username).first()
        if employee:
            if not password:
                return Response({"detail": "Password is required for employee login"},
                                status=status.HTTP_400_BAD_REQUEST)

            if employee.check_password(password):
                tokens = get_tokens_for_user({
                    'username': employee.employeeEmail,
                    'id': employee.id,
                    'user_type': 'employee'
                })
                return Response({
                    **tokens,
                    'user_type': 'employee',
                    'user_id': employee.id,
                    'name': employee.employeeName,
                    'email': employee.employeeEmail,
                    'user_type_id': employee.employeeUserType.id if employee.employeeUserType else None,
                    'user_type_name': employee.employeeUserType.userTypeName if employee.employeeUserType else None,
                }, status=status.HTTP_200_OK)
            else:
                return Response({"detail": "Invalid password for employee"}, status=status.HTTP_401_UNAUTHORIZED)

        # Check Client credentials
        if not dob:
            return Response({"detail": "Date of Birth is required for client login"},
                            status=status.HTTP_400_BAD_REQUEST)

        try:
            dob_date = datetime.strptime(dob, '%Y-%m-%d').date()
        except ValueError:
            return Response({"detail": "Invalid date format. Use YYYY-MM-DD."}, status=status.HTTP_400_BAD_REQUEST)

        client = ClientModel.objects.filter(clientPanNo=username, clientDateOfBirth=dob_date).first()
        if client:
            tokens = get_tokens_for_user({
                'username': client.clientPanNo,
                'id': client.id,
                'user_type': 'client'
            })
            return Response({
                **tokens,
                'user_type': 'client',
                'user_id': client.id,
                'name': client.clientName,
                'email': client.clientEmail,
            }, status=status.HTTP_200_OK)

        return Response({"detail": "No user found with provided credentials"}, status=status.HTTP_401_UNAUTHORIZED)

    @action(detail=False, methods=['get'])
    def profile(self, request):
        user = request.user
        if not user.is_authenticated:
            return Response({'detail': 'Authentication credentials were not provided.'},
                            status=status.HTTP_401_UNAUTHORIZED)

        if user.is_superuser:
            return Response({
                'user_type': 'superuser',
                'user_id': user.id,
                'username': user.username,
                'name': user.username,  # Use username as name for superusers
                'email': user.email,
            }, status=status.HTTP_200_OK)

        try:
            employee = EmployeeModel.objects.get(employeeEmail=user.email)
            serializer = EmployeeModelSerializers(employee, context={'request': request})
            return Response({
                'user_type': 'employee',
                **serializer.data
            }, status=status.HTTP_200_OK)
        except EmployeeModel.DoesNotExist:
            pass

        try:
            client = ClientModel.objects.get(clientEmail=user.email)
            return Response({
                'user_type': 'client',
                'user_id': client.id,
                'name': client.clientName,
                'email': client.clientEmail,
                'pan_no': client.clientPanNo,
                'date_of_birth': client.clientDateOfBirth,
            }, status=status.HTTP_200_OK)
        except ClientModel.DoesNotExist:
            pass

        return Response({'detail': 'Profile not found'}, status=status.HTTP_404_NOT_FOUND)

    @action(detail=False, methods=['post'])
    def logout(self, request):
        try:
            ActivityLogger.log_auth(request, 'LOGOUT')
            refresh_token = request.data.get("refresh_token")
            if not refresh_token:
                raise ValidationError("Refresh token is required")

            token = RefreshToken(refresh_token)
            token.blacklist()
            return Response({"detail": "Successfully logged out."}, status=status.HTTP_200_OK)
        except TokenError:
            return Response({"detail": "Invalid token"}, status=status.HTTP_400_BAD_REQUEST)
        except ValidationError as e:
            return Response({"detail": str(e)}, status=status.HTTP_400_BAD_REQUEST)
        except Exception as e:
            return Response({"detail": "An error occurred during logout"}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class UserTypeViewSet(viewsets.ModelViewSet):
    queryset = UserTypeModel.objects.filter(hideStatus=0)
    serializer_class = UserTypeModelSerializers
    permission_classes = [IsAuthenticated]  # Ensure only authenticated users can access these views

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = UserTypeModelSerializers(UserTypeModel.objects.filter(hideStatus=0).order_by('-id'),
                                                      many=True)
            else:
                serializer = UserTypeModelSerializers(UserTypeModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                                                      many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All  Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = UserTypeModelSerializers(data=request.data)
            else:
                serializer = UserTypeModelSerializers(instance=UserTypeModel.objects.get(id=pk), data=request.data)
            if serializer.is_valid():
                serializer.save()
                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            UserTypeModel.objects.filter(id=pk).update(hideStatus=1)
            response = {'code': 1, 'message': "Done Successfully"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class CountryViewSet(viewsets.ModelViewSet):
    queryset = CountryModel.objects.filter(hideStatus=0)
    serializer_class = CountryModelSerializers
    permission_classes = [IsAuthenticated]

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                Country = CountryModel.objects.filter(hideStatus=0).order_by('-id')
            else:
                Country = CountryModel.objects.filter(hideStatus=0, id=pk).order_by('-id')
            serializer = CountryModelSerializers(Country, many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = CountryModelSerializers(data=request.data)
            else:
                instance = CountryModel.objects.get(id=pk)
                serializer = CountryModelSerializers(instance=instance, data=request.data)
            if serializer.is_valid():
                serializer.save()
                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            CountryModel.objects.filter(id=pk).update(hideStatus=1)
            response = {'code': 1, 'message': "Done Successfully"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class StateViewSet(viewsets.ModelViewSet):
    queryset = StateModel.objects.filter(hideStatus=0)
    serializer_class = StateModelSerializers
    permission_classes = [IsAuthenticated]

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                states = StateModel.objects.filter(hideStatus=0).order_by('-id')
            else:
                states = StateModel.objects.filter(hideStatus=0, id=pk).order_by('-id')
            serializer = StateModelSerializers(states, many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = StateModelSerializers(data=request.data)
            else:
                instance = StateModel.objects.get(id=pk)
                serializer = StateModelSerializers(instance=instance, data=request.data)
            if serializer.is_valid():
                serializer.save()
                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            StateModel.objects.filter(id=pk).update(hideStatus=1)
            response = {'code': 1, 'message': "Done Successfully"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class ModeViewSet(viewsets.ModelViewSet):
    queryset = ModeModel.objects.filter(hideStatus=0)
    serializer_class = ModeModelSerializers
    permission_classes = [IsAuthenticated]

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                modes = ModeModel.objects.filter(hideStatus=0).order_by('-id')
            else:
                modes = ModeModel.objects.filter(hideStatus=0, id=pk).order_by('-id')
            serializer = ModeModelSerializers(modes, many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = ModeModelSerializers(data=request.data)
            else:
                instance = ModeModel.objects.get(id=pk)
                serializer = ModeModelSerializers(instance=instance, data=request.data)
            if serializer.is_valid():
                serializer.save()
                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            ModeModel.objects.filter(id=pk).update(hideStatus=1)
            response = {'code': 1, 'message': "Done Successfully"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class IssueTypeViewSet(viewsets.ModelViewSet):
    queryset = IssueTypeModel.objects.filter(hideStatus=0)
    serializer_class = IssueTypeModelSerializers
    permission_classes = [IsAuthenticated]

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = IssueTypeModelSerializers(IssueTypeModel.objects.filter(hideStatus=0).order_by('-id'),
                                                       many=True)
            else:
                serializer = IssueTypeModelSerializers(
                    IssueTypeModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                    many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All  Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = IssueTypeModelSerializers(data=request.data)
            else:
                serializer = IssueTypeModelSerializers(instance=IssueTypeModel.objects.get(id=pk), data=request.data)
            if serializer.is_valid():
                serializer.save()
                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            IssueTypeModel.objects.filter(id=pk).update(hideStatus=1)
            response = {'code': 1, 'message': "Done Successfully"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class FormTypeViewSet(viewsets.ModelViewSet):
    queryset = FormTypeModel.objects.filter(hideStatus=0)
    serializer_class = FormTypeModelSerializers
    permission_classes = [IsAuthenticated]

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = FormTypeModelSerializers(FormTypeModel.objects.filter(hideStatus=0).order_by('-id'),
                                                      many=True)
            else:
                serializer = FormTypeModelSerializers(
                    FormTypeModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                    many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All  Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = FormTypeModelSerializers(data=request.data)
            else:
                serializer = FormTypeModelSerializers(instance=FormTypeModel.objects.get(id=pk), data=request.data)
            if serializer.is_valid():
                serializer.save()
                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            FormTypeModel.objects.filter(id=pk).update(hideStatus=1)
            response = {'code': 1, 'message': "Done Successfully"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class GstTypeViewSet(viewsets.ModelViewSet):
    queryset = GstTypeModel.objects.filter(hideStatus=0)
    serializer_class = GstTypeModelSerializers
    permission_classes = [IsAuthenticated]

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = GstTypeModelSerializers(GstTypeModel.objects.filter(hideStatus=0).order_by('-id'),
                                                     many=True)
            else:
                serializer = GstTypeModelSerializers(
                    GstTypeModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                    many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All  Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = GstTypeModelSerializers(data=request.data)
            else:
                serializer = GstTypeModelSerializers(instance=GstTypeModel.objects.get(id=pk), data=request.data)
            if serializer.is_valid():
                serializer.save()
                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            GstTypeModel.objects.filter(id=pk).update(hideStatus=1)
            response = {'code': 1, 'message': "Done Successfully"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class FileTypeViewSet(viewsets.ModelViewSet):
    queryset = FileTypeModel.objects.filter(hideStatus=0)
    serializer_class = FileTypeModelSerializers
    permission_classes = [IsAuthenticated]

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = FileTypeModelSerializers(FileTypeModel.objects.filter(hideStatus=0).order_by('-id'),
                                                      many=True)
            else:
                serializer = FileTypeModelSerializers(
                    FileTypeModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                    many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All  Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = FileTypeModelSerializers(data=request.data)
            else:
                serializer = FileTypeModelSerializers(instance=FileTypeModel.objects.get(id=pk), data=request.data)
            if serializer.is_valid():
                serializer.save()
                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            FileTypeModel.objects.filter(id=pk).update(hideStatus=1)
            response = {'code': 1, 'message': "Done Successfully"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class GenderViewSet(viewsets.ModelViewSet):
    queryset = GenderModel.objects.filter(hideStatus=0)
    serializer_class = GenderModelSerializers
    permission_classes = [IsAuthenticated]

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = GenderModelSerializers(GenderModel.objects.filter(hideStatus=0).order_by('-id'),
                                                    many=True)
            else:
                serializer = GenderModelSerializers(GenderModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                                                    many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All  Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = GenderModelSerializers(data=request.data)
            else:
                serializer = GenderModelSerializers(instance=GenderModel.objects.get(id=pk), data=request.data)
            if serializer.is_valid():
                serializer.save()
                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            GenderModel.objects.filter(id=pk).update(hideStatus=1)
            response = {'code': 1, 'message': "Done Successfully"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class MaritalStatusViewSet(viewsets.ModelViewSet):
    queryset = MaritalStatusModel.objects.filter(hideStatus=0)
    serializer_class = MaritalStatusModelSerializers
    permission_classes = [IsAuthenticated]

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = MaritalStatusModelSerializers(
                    MaritalStatusModel.objects.filter(hideStatus=0).order_by('-id'),
                    many=True)
            else:
                serializer = MaritalStatusModelSerializers(
                    MaritalStatusModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                    many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All  Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = MaritalStatusModelSerializers(data=request.data)
            else:
                serializer = MaritalStatusModelSerializers(instance=MaritalStatusModel.objects.get(id=pk),
                                                           data=request.data)
            if serializer.is_valid():
                serializer.save()
                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            MaritalStatusModel.objects.filter(id=pk).update(hideStatus=1)
            response = {'code': 1, 'message': "Done Successfully"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class PoliticallyExposedPersonViewSet(viewsets.ModelViewSet):
    queryset = PoliticallyExposedPersonModel.objects.filter(hideStatus=0)
    serializer_class = PoliticallyExposedPersonModelSerializers
    permission_classes = [IsAuthenticated]

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = PoliticallyExposedPersonModelSerializers(
                    PoliticallyExposedPersonModel.objects.filter(hideStatus=0).order_by('-id'),
                    many=True)
            else:
                serializer = PoliticallyExposedPersonModelSerializers(
                    PoliticallyExposedPersonModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                    many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All  Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = PoliticallyExposedPersonModelSerializers(data=request.data)
            else:
                serializer = PoliticallyExposedPersonModelSerializers(
                    instance=PoliticallyExposedPersonModel.objects.get(id=pk), data=request.data)
            if serializer.is_valid():
                serializer.save()
                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            PoliticallyExposedPersonModel.objects.filter(id=pk).update(hideStatus=1)
            response = {'code': 1, 'message': "Done Successfully"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class BankNameViewSet(viewsets.ModelViewSet):
    queryset = BankNameModel.objects.filter(hideStatus=0)
    serializer_class = BankNameModelSerializers
    permission_classes = [IsAuthenticated]

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = BankNameModelSerializers(BankNameModel.objects.filter(hideStatus=0).order_by('-id'),
                                                      many=True)
            else:
                serializer = BankNameModelSerializers(BankNameModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                                                      many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All  Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = BankNameModelSerializers(data=request.data)
            else:
                serializer = BankNameModelSerializers(instance=BankNameModel.objects.get(id=pk), data=request.data)
            if serializer.is_valid():
                serializer.save()
                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            BankNameModel.objects.filter(id=pk).update(hideStatus=1)
            response = {'code': 1, 'message': "Done Successfully"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class RelationshipViewSet(viewsets.ModelViewSet):
    queryset = RelationshipModel.objects.filter(hideStatus=0)
    serializer_class = RelationshipModelSerializers
    permission_classes = [IsAuthenticated]

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = RelationshipModelSerializers(
                    RelationshipModel.objects.filter(hideStatus=0).order_by('-id'),
                    many=True)
            else:
                serializer = RelationshipModelSerializers(
                    RelationshipModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                    many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All  Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = RelationshipModelSerializers(data=request.data)
            else:
                serializer = RelationshipModelSerializers(instance=RelationshipModel.objects.get(id=pk),
                                                          data=request.data)
            if serializer.is_valid():
                serializer.save()
                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            RelationshipModel.objects.filter(id=pk).update(hideStatus=1)
            response = {'code': 1, 'message': "Done Successfully"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class AccountTypeViewSet(viewsets.ModelViewSet):
    queryset = AccountTypeModel.objects.filter(hideStatus=0)
    serializer_class = AccountTypeModelSerializers
    permission_classes = [IsAuthenticated]

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = AccountTypeModelSerializers(
                    AccountTypeModel.objects.filter(hideStatus=0).order_by('-id'),
                    many=True)
            else:
                serializer = AccountTypeModelSerializers(
                    AccountTypeModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                    many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All  Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = AccountTypeModelSerializers(data=request.data)
            else:
                serializer = AccountTypeModelSerializers(instance=AccountTypeModel.objects.get(id=pk),
                                                         data=request.data)
            if serializer.is_valid():
                serializer.save()
                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            AccountTypeModel.objects.filter(id=pk).update(hideStatus=1)
            response = {'code': 1, 'message': "Done Successfully"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class AccountPreferenceViewSet(viewsets.ModelViewSet):
    queryset = AccountPreferenceModel.objects.filter(hideStatus=0)
    serializer_class = AccountPreferenceModelSerializers
    permission_classes = [IsAuthenticated]

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = AccountPreferenceModelSerializers(
                    AccountPreferenceModel.objects.filter(hideStatus=0).order_by('-id'),
                    many=True)
            else:
                serializer = AccountPreferenceModelSerializers(
                    AccountPreferenceModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                    many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All  Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = AccountPreferenceModelSerializers(data=request.data)
            else:
                serializer = AccountPreferenceModelSerializers(instance=AccountPreferenceModel.objects.get(id=pk),
                                                               data=request.data)
            if serializer.is_valid():
                serializer.save()
                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            AccountPreferenceModel.objects.filter(id=pk).update(hideStatus=1)
            response = {'code': 1, 'message': "Done Successfully"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


#End of Masters


class ArnEntryViewSet(viewsets.ModelViewSet):
    queryset = ArnEntryModel.objects.filter(hideStatus=0)
    serializer_class = ArnEntryModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    @action(detail=False, methods=['GET'])
    def countries(self, request):
        countries = CountryModel.objects.filter(hideStatus=0).values('id', 'countryCode', 'countryName', 'dailCode')
        country_data = [
            {
                "id": country['id'],
                "code": country['countryCode'],
                "name": country['countryName'],
                "dial_code": country['dailCode']
            }
            for country in countries
        ]
        return Response(country_data)

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = ArnEntryModelSerializers(ArnEntryModel.objects.filter(hideStatus=0).order_by('-id'),
                                                      many=True)
            else:
                serializer = ArnEntryModelSerializers(ArnEntryModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                                                      many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All  Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            data = request.data.copy()

            if pk == "0":
                # Create operation
                serializer = ArnEntryModelSerializers(data=request.data)
                if serializer.is_valid():
                    instance = serializer.save()
                    # Log the create activity
                    ActivityLogger.log_activity(
                        request=request,
                        action='CREATE',
                        entity_type='ArnEntry',
                        entity_id=instance.id,
                        details={'new_data': serializer.data}
                    )
                    response = {'code': 1, 'message': "Done Successfully"}
                else:
                    print("Serializer errors:", serializer.errors)
                    response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
            else:
                # Update operation
                try:
                    instance = ArnEntryModel.objects.get(id=pk)
                    # Capture previous data before update
                    previous_data = self.get_previous_data(instance)

                    serializer = ArnEntryModelSerializers(instance=instance, data=request.data)
                    if serializer.is_valid():
                        # Log before saving the changes
                        ActivityLogger.log_activity(
                            request=request,
                            action='UPDATE',
                            entity_type='ArnEntry',
                            entity_id=pk,
                            details={'new_data': serializer.validated_data},
                            previous_data=previous_data
                        )
                        # Save the changes
                        serializer.save()
                        response = {'code': 1, 'message': "Done Successfully"}
                    else:
                        print("Serializer errors:", serializer.errors)
                        response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
                except ArnEntryModel.DoesNotExist:
                    response = {'code': 0, 'message': "ArnEntry not found"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = ArnEntryModel.objects.get(id=pk)
                # Capture previous data before deletion
                previous_data = self.get_previous_data(instance)

                # Log the deletion with the captured data
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='ArnEntry',
                    entity_id=pk,
                    previous_data=previous_data
                )

                # Soft delete the instance
                instance.hideStatus = 1
                instance.save()

                response = {'code': 1, 'message': "Done Successfully"}
            except ArnEntryModel.DoesNotExist:
                response = {'code': 0, 'message': "ArnEntry not found"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class AmcEntryViewSet(viewsets.ModelViewSet):
    queryset = AmcEntryModel.objects.filter(hideStatus=0)
    serializer_class = AmcEntryModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    @action(detail=False, methods=['GET'])
    def countries(self, request):
        countries = CountryModel.objects.filter(hideStatus=0).values('id', 'countryCode', 'countryName', 'dailCode')
        country_data = [
            {
                "id": country['id'],
                "code": country['countryCode'],
                "name": country['countryName'],
                "dial_code": country['dailCode']
            }
            for country in countries
        ]
        return Response(country_data)

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = AmcEntryModelSerializers(AmcEntryModel.objects.filter(hideStatus=0).order_by('-id'),
                                                      many=True)
            else:
                serializer = AmcEntryModelSerializers(AmcEntryModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                                                      many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All  Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                # Create operation
                serializer = AmcEntryModelSerializers(data=request.data)
                if serializer.is_valid():
                    instance = serializer.save()
                    # Log the create activity
                    ActivityLogger.log_activity(
                        request=request,
                        action='CREATE',
                        entity_type='AmcEntry',
                        entity_id=instance.id,
                        details={'new_data': serializer.data}
                    )
                    response = {'code': 1, 'message': "Done Successfully"}
                else:
                    print("Serializer errors:", serializer.errors)
                    response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
            else:
                # Update operation
                try:
                    instance = AmcEntryModel.objects.get(id=pk)
                    # Capture previous data before update
                    previous_data = self.get_previous_data(instance)

                    serializer = AmcEntryModelSerializers(instance=instance, data=request.data)
                    if serializer.is_valid():
                        # Log before saving the changes
                        ActivityLogger.log_activity(
                            request=request,
                            action='UPDATE',
                            entity_type='AmcEntry',
                            entity_id=pk,
                            details={'new_data': serializer.validated_data},
                            previous_data=previous_data
                        )
                        # Save the changes
                        serializer.save()
                        response = {'code': 1, 'message': "Done Successfully"}
                    else:
                        print("Serializer errors:", serializer.errors)
                        response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
                except AmcEntryModel.DoesNotExist:
                    response = {'code': 0, 'message': "AMC not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = AmcEntryModel.objects.get(id=pk)
                # Capture previous data before deletion
                previous_data = self.get_previous_data(instance)

                # Log the deletion with the captured data
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='AmcEntry',
                    entity_id=pk,
                    previous_data=previous_data
                )

                # Soft delete the instance
                instance.hideStatus = 1
                instance.save()

                response = {'code': 1, 'message': "Done Successfully"}
            except AmcEntryModel.DoesNotExist:
                response = {'code': 0, 'message': "AMC not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class FundViewSet(viewsets.ModelViewSet):
    queryset = FundModel.objects.filter(hideStatus=0)
    serializer_class = FundModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    @action(detail=False, methods=['GET'])
    def by_amc(self, request):
        amc_id = request.query_params.get('amcId')
        page = int(request.query_params.get('page', 1))
        page_size = int(request.query_params.get('pageSize', 50))
        search = request.query_params.get('search', '')

        if not amc_id:
            return Response({'code': 0, 'message': 'AMC ID is required'})

        funds = FundModel.objects.filter(hideStatus=0, fundAmcName_id=amc_id)

        if search:
            funds = funds.filter(fundName__icontains=search)

        funds = funds.order_by('fundName')

        paginator = Paginator(funds, page_size)

        try:
            funds_page = paginator.page(page)
        except Exception:
            return Response({'code': 0, 'message': 'Invalid page number'})

        serializer = FundModelSerializers(funds_page, many=True)

        return Response({
            'code': 1,
            'data': serializer.data,
            'message': 'Funds retrieved successfully',
            'total_pages': paginator.num_pages,
            'current_page': page,
            'total_items': paginator.count
        })

    @action(detail=False, methods=['GET'])
    def paginated_funds(self, request):
        page = int(request.query_params.get('page', 1))
        page_size = int(request.query_params.get('page_size', 100))
        search = request.query_params.get('search', '')
        amc_id = request.query_params.get('amc_id')

        queryset = self.get_queryset()

        if amc_id:
            queryset = queryset.filter(fundAmcName_id=amc_id)

        if search:
            queryset = queryset.filter(Q(fundName__icontains=search) | Q(schemeCode__icontains=search))

        start = (page - 1) * page_size
        end = start + page_size

        funds = queryset[start:end]
        total_count = queryset.count()

        serializer = self.get_serializer(funds, many=True)

        return Response({
            'results': serializer.data,
            'total_count': total_count,
            'page': page,
            'page_size': page_size
        })

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                serializer = FundModelSerializers(FundModel.objects.filter(hideStatus=0).order_by('-id'),
                                                  many=True)
            else:
                serializer = FundModelSerializers(FundModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                                                  many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All  Retried"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                # Create operation
                serializer = FundModelSerializers(data=request.data)
                if serializer.is_valid():
                    instance = serializer.save()
                    # Log the create activity
                    ActivityLogger.log_activity(
                        request=request,
                        action='CREATE',
                        entity_type='FundEntry',
                        entity_id=instance.id,
                        details={'new_data': serializer.data}
                    )
                    response = {'code': 1, 'message': "Done Successfully"}
                else:
                    print("Serializer errors:", serializer.errors)
                    response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
            else:
                # Update operation
                try:
                    instance = FundModel.objects.get(id=pk)
                    # Capture previous data before update
                    previous_data = self.get_previous_data(instance)

                    serializer = FundModelSerializers(instance=instance, data=request.data)
                    if serializer.is_valid():
                        # Log before saving the changes
                        ActivityLogger.log_activity(
                            request=request,
                            action='UPDATE',
                            entity_type='FundEntry',
                            entity_id=pk,
                            details={'new_data': serializer.validated_data},
                            previous_data=previous_data
                        )
                        # Save the changes
                        serializer.save()
                        response = {'code': 1, 'message': "Done Successfully"}
                    else:
                        print("Serializer errors:", serializer.errors)
                        response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
                except FundModel.DoesNotExist:
                    response = {'code': 0, 'message': "Fund not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = FundModel.objects.get(id=pk)
                # Capture previous data before deletion
                previous_data = self.get_previous_data(instance)

                # Log the deletion with the captured data
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='FundEntry',
                    entity_id=pk,
                    previous_data=previous_data
                )

                # Soft delete the instance
                instance.hideStatus = 1
                instance.save()

                response = {'code': 1, 'message': "Done Successfully"}
            except FundModel.DoesNotExist:
                response = {'code': 0, 'message': "Fund not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class AumEntryViewSet(viewsets.ModelViewSet):
    queryset = AumEntryModel.objects.filter(hideStatus=0)
    serializer_class = AumEntryModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    @action(detail=False, methods=['GET'])
    def listing(self, request):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

        page_size = int(request.query_params.get('page_size', 10))
        page = int(request.query_params.get('page', 1))
        search = request.query_params.get('search', '')

        queryset = self.get_queryset().select_related(
            'aumArnNumber',
            'aumAmcName',
        )

        if search:
            queryset = queryset.filter(
                Q(aumArnNumber__arnNumber__icontains=search) |
                Q(aumAmcName__amcName__icontains=search) |
                Q(aumInvoiceNumber__icontains=search) |
                Q(aumAmount__icontains=search) |
                Q(aumMonth__icontains=search)
            )

        total_count = queryset.count()
        total_pages = (total_count + page_size - 1) // page_size

        start = (page - 1) * page_size
        end = start + page_size

        queryset = queryset.order_by('-id')[start:end]

        serializer = self.get_serializer(queryset, many=True)

        data = {
            'code': 1,
            'data': serializer.data,
            'message': "Retrieved Successfully",
            'total_count': total_count,
            'total_pages': total_pages,
            'current_page': page
        }

        return Response(data)

    @action(detail=False, methods=['GET'])
    def total_count(self, request):
        search = request.query_params.get('search', '')
        queryset = self.get_queryset()

        if search:
            queryset = queryset.filter(
                Q(aumArnNumber__arnNumber__icontains=search) |
                Q(aumAmcName__amcName__icontains=search) |
                Q(aumInvoiceNumber__icontains=search) |
                Q(aumAmount__icontains=search) |
                Q(aumMonth__icontains=search)
            )

        total_count = queryset.count()
        return Response({'total_count': total_count})

    @action(detail=True, methods=['GET'])
    def list_for_update(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = AumEntryModel.objects.get(id=pk)
                serializer = AumEntryModelSerializers(instance)
                return Response({'code': 1, 'data': serializer.data, 'message': "Retrieved Successfully"})
            except AumEntryModel.DoesNotExist:
                return Response({'code': 0, 'message': "NAV not found"}, status=404)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=401)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            data = request.data.copy()

            # Validate aumMonth format
            if 'aumMonth' in data:
                try:
                    datetime.strptime(data['aumMonth'], '%Y-%m')
                except ValueError:
                    return Response({'code': 0, 'message': "Invalid aumMonth format. Use YYYY-MM."})

            if pk == "0":
                # Create operation
                serializer = self.get_serializer(data=request.data)
                if serializer.is_valid():
                    instance = serializer.save()
                    ActivityLogger.log_activity(
                        request=request,
                        action='CREATE',
                        entity_type='AumEntry',
                        entity_id=instance.id,
                        details={'new_data': serializer.data}
                    )
                    response = {'code': 1, 'message': "Done Successfully"}
                else:
                    print("Serializer errors:", serializer.errors)
                    response = {'code': 0, 'message': "Unable to Process Request"}
            else:
                # Update operation
                try:
                    instance = AumEntryModel.objects.get(id=pk)
                    # Capture previous data before update
                    previous_data = self.get_previous_data(instance)

                    serializer = self.get_serializer(instance, data=request.data)
                    if serializer.is_valid():
                        # Log before saving the changes
                        ActivityLogger.log_activity(
                            request=request,
                            action='UPDATE',
                            entity_type='AumEntry',
                            entity_id=pk,
                            details={'new_data': serializer.validated_data},
                            previous_data=previous_data
                        )
                        # Save the changes
                        serializer.save()
                        response = {'code': 1, 'message': "Done Successfully"}
                    else:
                        print("Serializer errors:", serializer.errors)
                        response = {'code': 0, 'message': "Unable to Process Request"}
                except AumEntryModel.DoesNotExist:
                    response = {'code': 0, 'message': "AumEntry not found"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = AumEntryModel.objects.get(id=pk)
                # Capture the data before deletion
                previous_data = self.get_previous_data(instance)

                # Log the deletion with the captured data
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='AumEntry',
                    entity_id=pk,
                    previous_data=previous_data
                )

                # Soft delete the instance
                instance.hideStatus = 1
                instance.save()

                response = {'code': 1, 'message': "Done Successfully"}
            except AumEntryModel.DoesNotExist:
                response = {'code': 0, 'message': "AumEntry not found"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class CommissionEntryViewSet(viewsets.ModelViewSet):
    queryset = CommissionEntryModel.objects.filter(hideStatus=0)
    serializer_class = CommissionEntryModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    @action(detail=False, methods=['GET'])
    def listing(self, request):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

        page_size = int(request.query_params.get('page_size', 10))
        page = int(request.query_params.get('page', 1))
        search = request.query_params.get('search', '')

        queryset = self.get_queryset().select_related(
            'commissionArnNumber',
            'commissionAmcName',
        )

        if search:
            queryset = queryset.filter(
                Q(commissionArnNumber__arnNumber__icontains=search) |
                Q(commissionAmcName__amcName__icontains=search) |
                Q(commissionAmount__icontains=search) |
                Q(commissionMonth__icontains=search)
            )

        total_count = queryset.count()
        total_pages = (total_count + page_size - 1) // page_size

        start = (page - 1) * page_size
        end = start + page_size

        queryset = queryset.order_by('-id')[start:end]

        serializer = self.get_serializer(queryset, many=True)

        data = {
            'code': 1,
            'data': serializer.data,
            'message': "Retrieved Successfully",
            'total_count': total_count,
            'total_pages': total_pages,
            'current_page': page
        }

        return Response(data)

    @action(detail=False, methods=['GET'])
    def total_count(self, request):
        search = request.query_params.get('search', '')
        queryset = self.get_queryset()

        if search:
            queryset = queryset.filter(
                Q(commissionArnNumber__arnNumber__icontains=search) |
                Q(commissionAmcName__amcName__icontains=search) |
                Q(commissionAmount__icontains=search) |
                Q(commissionMonth__icontains=search)
            )

        total_count = queryset.count()
        return Response({'total_count': total_count})

    @action(detail=True, methods=['GET'])
    def list_for_update(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = CommissionEntryModel.objects.get(id=pk)
                serializer = CommissionEntryModelSerializers(instance)
                return Response({'code': 1, 'data': serializer.data, 'message': "Retrieved Successfully"})
            except CommissionEntryModel.DoesNotExist:
                return Response({'code': 0, 'message': "NAV not found"}, status=404)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=401)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            data = request.data.copy()

            # Ensure aumMonth is in YYYY-MM format
            if 'aumMonth' in data:
                try:
                    # Validate the format of aumMonth
                    datetime.strptime(data['aumMonth'], '%Y-%m')
                except ValueError:
                    return Response({'code': 0, 'message': "Invalid aumMonth format. Use YYYY-MM."})

            if pk == "0":
                # Create operation
                serializer = CommissionEntryModelSerializers(data=data)
                if serializer.is_valid():
                    instance = serializer.save()

                    # Log the create activity
                    ActivityLogger.log_activity(
                        request=request,
                        action='CREATE',
                        entity_type='CommissionEntry',
                        entity_id=instance.id,
                        details={'new_data': serializer.data}
                    )
                    response = {'code': 1, 'message': "Done Successfully"}
                else:
                    response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
            else:
                # Update operation
                try:
                    instance = CommissionEntryModel.objects.get(id=pk)
                    # Capture previous data before update
                    previous_data = self.get_previous_data(instance)

                    serializer = CommissionEntryModelSerializers(instance=instance, data=data)
                    if serializer.is_valid():
                        # Log before saving the changes
                        ActivityLogger.log_activity(
                            request=request,
                            action='UPDATE',
                            entity_type='CommissionEntry',
                            entity_id=pk,
                            details={'new_data': serializer.validated_data},
                            previous_data=previous_data
                        )
                        # Save the changes
                        serializer.save()
                        response = {'code': 1, 'message': "Done Successfully"}
                    else:
                        response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
                except CommissionEntryModel.DoesNotExist:
                    response = {'code': 0, 'message': "Commission Entry not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = CommissionEntryModel.objects.get(id=pk)
                # Capture previous data before deletion
                previous_data = self.get_previous_data(instance)

                # Log the deletion with the captured data
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='CommissionEntry',
                    entity_id=pk,
                    previous_data=previous_data
                )

                # Soft delete the instance
                instance.hideStatus = 1
                instance.save()

                response = {'code': 1, 'message': "Done Successfully"}
            except CommissionEntryModel.DoesNotExist:
                response = {'code': 0, 'message': "Commission Entry not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class AumYoyGrowthEntryViewSet(viewsets.ModelViewSet):
    queryset = AumYoyGrowthEntryModel.objects.filter(hideStatus=0)
    serializer_class = AumYoyGrowthEntryModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    @action(detail=False, methods=['GET'])
    def listing(self, request):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

        page_size = int(request.query_params.get('page_size', 10))
        page = int(request.query_params.get('page', 1))
        search = request.query_params.get('search', '')

        queryset = self.get_queryset().select_related(
            'aumYoyGrowthAmcName',
        )

        if search:
            queryset = queryset.filter(
                Q(aumYoyGrowthAmcName__amcName__icontains=search) |
                Q(aumYoyGrowthAmount__icontains=search) |
                Q(aumYoyGrowthDate__icontains=search)
            )

        total_count = queryset.count()
        total_pages = (total_count + page_size - 1) // page_size

        start = (page - 1) * page_size
        end = start + page_size

        queryset = queryset.order_by('-id')[start:end]

        serializer = self.get_serializer(queryset, many=True)

        data = {
            'code': 1,
            'data': serializer.data,
            'message': "Retrieved Successfully",
            'total_count': total_count,
            'total_pages': total_pages,
            'current_page': page
        }

        return Response(data)

    @action(detail=False, methods=['GET'])
    def total_count(self, request):
        search = request.query_params.get('search', '')
        queryset = self.get_queryset()

        if search:
            queryset = queryset.filter(
                Q(aumYoyGrowthAmcName__amcName__icontains=search) |
                Q(aumYoyGrowthAmount__icontains=search) |
                Q(aumYoyGrowthDate__icontains=search)
            )

        total_count = queryset.count()
        return Response({'total_count': total_count})

    @action(detail=True, methods=['GET'])
    def list_for_update(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = AumYoyGrowthEntryModel.objects.get(id=pk)
                serializer = AumYoyGrowthEntryModelSerializers(instance)
                return Response({'code': 1, 'data': serializer.data, 'message': "Retrieved Successfully"})
            except AumYoyGrowthEntryModel.DoesNotExist:
                return Response({'code': 0, 'message': "NAV not found"}, status=404)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=401)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            data = request.data.copy()

            if pk == "0":
                # Create operation
                serializer = AumYoyGrowthEntryModelSerializers(data=data)
                if serializer.is_valid():
                    instance = serializer.save()

                    # Log the create activity
                    ActivityLogger.log_activity(
                        request=request,
                        action='CREATE',
                        entity_type='AumYoyGrowthEntry',
                        entity_id=instance.id,
                        details={'new_data': serializer.data}
                    )
                    response = {'code': 1, 'message': "Done Successfully"}
                else:
                    response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
            else:
                # Update operation
                try:
                    instance = AumYoyGrowthEntryModel.objects.get(id=pk)
                    # Capture previous data before update
                    previous_data = self.get_previous_data(instance)

                    serializer = AumYoyGrowthEntryModelSerializers(instance=instance, data=data)
                    if serializer.is_valid():
                        # Log before saving the changes
                        ActivityLogger.log_activity(
                            request=request,
                            action='UPDATE',
                            entity_type='AumYoyGrowthEntry',
                            entity_id=pk,
                            details={'new_data': serializer.validated_data},
                            previous_data=previous_data
                        )
                        # Save the changes
                        serializer.save()
                        response = {'code': 1, 'message': "Done Successfully"}
                    else:
                        response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
                except AumYoyGrowthEntryModel.DoesNotExist:
                    response = {'code': 0, 'message': "AUM YOY Growth Entry not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = AumYoyGrowthEntryModel.objects.get(id=pk)
                # Capture previous data before deletion
                previous_data = self.get_previous_data(instance)

                # Log the deletion with the captured data
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='AumYoyGrowthEntry',
                    entity_id=pk,
                    previous_data=previous_data
                )

                # Soft delete the instance
                instance.hideStatus = 1
                instance.save()

                response = {'code': 1, 'message': "Done Successfully"}
            except AumYoyGrowthEntryModel.DoesNotExist:
                response = {'code': 0, 'message': "AUM YOY Growth Entry not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class IndustryAumEntryViewSet(viewsets.ModelViewSet):
    queryset = IndustryAumEntryModel.objects.filter(hideStatus=0)
    serializer_class = IndustryAumEntryModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    @action(detail=False, methods=['GET'])
    def listing(self, request):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

        page_size = int(request.query_params.get('page_size', 10))
        page = int(request.query_params.get('page', 1))
        search = request.query_params.get('search', '')

        queryset = self.get_queryset().select_related(
            'industryAumMode',
        )

        if search:
            queryset = queryset.filter(
                Q(industryAumMode__modeName__icontains=search) |
                Q(industryName__icontains=search) |
                Q(industryAumDate__icontains=search) |
                Q(industryAumMode__icontains=search)
            )

        total_count = queryset.count()
        total_pages = (total_count + page_size - 1) // page_size

        start = (page - 1) * page_size
        end = start + page_size

        queryset = queryset.order_by('-id')[start:end]

        serializer = self.get_serializer(queryset, many=True)

        data = {
            'code': 1,
            'data': serializer.data,
            'message': "Retrieved Successfully",
            'total_count': total_count,
            'total_pages': total_pages,
            'current_page': page
        }

        return Response(data)

    @action(detail=False, methods=['GET'])
    def total_count(self, request):
        search = request.query_params.get('search', '')
        queryset = self.get_queryset()

        if search:
            queryset = queryset.filter(
                Q(industryName__icontains=search) |
                Q(industryAumDate__icontains=search) |
                Q(industryAumAmount__icontains=search) |
                Q(industryAumMode__icontains=search)
            )

        total_count = queryset.count()
        return Response({'total_count': total_count})

    @action(detail=True, methods=['GET'])
    def list_for_update(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = IndustryAumEntryModel.objects.get(id=pk)
                serializer = IndustryAumEntryModelSerializers(instance)
                return Response({'code': 1, 'data': serializer.data, 'message': "Retrieved Successfully"})
            except IndustryAumEntryModel.DoesNotExist:
                return Response({'code': 0, 'message': "NAV not found"}, status=404)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=401)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            data = request.data.copy()

            if pk == "0":
                # Create operation
                serializer = IndustryAumEntryModelSerializers(data=data)
                if serializer.is_valid():
                    instance = serializer.save()

                    # Log the create activity
                    ActivityLogger.log_activity(
                        request=request,
                        action='CREATE',
                        entity_type='IndustryAumEntry',
                        entity_id=instance.id,
                        details={'new_data': serializer.data}
                    )
                    response = {'code': 1, 'message': "Done Successfully"}
                else:
                    response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
            else:
                # Update operation
                try:
                    instance = IndustryAumEntryModel.objects.get(id=pk)
                    # Capture previous data before update
                    previous_data = self.get_previous_data(instance)

                    serializer = IndustryAumEntryModelSerializers(instance=instance, data=data)
                    if serializer.is_valid():
                        # Log before saving the changes
                        ActivityLogger.log_activity(
                            request=request,
                            action='UPDATE',
                            entity_type='IndustryAumEntry',
                            entity_id=pk,
                            details={'new_data': serializer.validated_data},
                            previous_data=previous_data
                        )
                        # Save the changes
                        serializer.save()
                        response = {'code': 1, 'message': "Done Successfully"}
                    else:
                        response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
                except IndustryAumEntryModel.DoesNotExist:
                    response = {'code': 0, 'message': "Industry AUM Entry not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = IndustryAumEntryModel.objects.get(id=pk)
                # Capture previous data before deletion
                previous_data = self.get_previous_data(instance)

                # Log the deletion with the captured data
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='IndustryAumEntry',
                    entity_id=pk,
                    previous_data=previous_data
                )

                # Soft delete the instance
                instance.hideStatus = 1
                instance.save()

                response = {'code': 1, 'message': "Done Successfully"}
            except IndustryAumEntryModel.DoesNotExist:
                response = {'code': 0, 'message': "Industry AUM Entry not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class GstEntryViewSet(viewsets.ModelViewSet):
    queryset = GstEntryModel.objects.filter(hideStatus=0)
    serializer_class = GstEntryModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    @action(detail=False, methods=['GET'])
    def listing(self, request):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

        page_size = int(request.query_params.get('page_size', 10))
        page = int(request.query_params.get('page', 1))
        search = request.query_params.get('search', '')

        queryset = self.get_queryset().select_related(
            'gstAmcName',
        )

        if search:
            queryset = queryset.filter(
                Q(gstAmcName__amcName__icontains=search) |
                Q(gstInvoiceDate__icontains=search) |
                Q(gstInvoiceNumber__icontains=search) |
                Q(gstTotalValue__icontains=search) |
                Q(gstTaxableValue__icontains=search) |
                Q(gstIGst__icontains=search) |
                Q(gstSGst__icontains=search) |
                Q(gstCGst__icontains=search)
            )

        total_count = queryset.count()
        total_pages = (total_count + page_size - 1) // page_size

        start = (page - 1) * page_size
        end = start + page_size

        queryset = queryset.order_by('-id')[start:end]

        serializer = self.get_serializer(queryset, many=True)

        data = {
            'code': 1,
            'data': serializer.data,
            'message': "Retrieved Successfully",
            'total_count': total_count,
            'total_pages': total_pages,
            'current_page': page
        }

        return Response(data)

    @action(detail=False, methods=['GET'])
    def total_count(self, request):
        search = request.query_params.get('search', '')
        queryset = self.get_queryset()

        if search:
            queryset = queryset.filter(
                Q(gstAmcName__amcName__icontains=search) |
                Q(gstInvoiceDate__icontains=search) |
                Q(gstInvoiceNumber__icontains=search) |
                Q(gstTotalValue__icontains=search) |
                Q(gstTaxableValue__icontains=search) |
                Q(gstIGst__icontains=search) |
                Q(gstSGst__icontains=search) |
                Q(gstCGst__icontains=search)
            )

        total_count = queryset.count()
        return Response({'total_count': total_count})

    @action(detail=True, methods=['GET'])
    def list_for_update(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = GstEntryModel.objects.get(id=pk)
                serializer = GstEntryModelSerializers(instance)
                return Response({'code': 1, 'data': serializer.data, 'message': "Retrieved Successfully"})
            except GstEntryModel.DoesNotExist:
                return Response({'code': 0, 'message': "NAV not found"}, status=404)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=401)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            data = request.data.copy()

            if pk == "0":
                # Create operation
                serializer = GstEntryModelSerializers(data=data)
                if serializer.is_valid():
                    instance = serializer.save()

                    # Log the create activity
                    ActivityLogger.log_activity(
                        request=request,
                        action='CREATE',
                        entity_type='GstEntry',
                        entity_id=instance.id,
                        details={'new_data': serializer.data}
                    )
                    response = {'code': 1, 'message': "Done Successfully"}
                else:
                    response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
            else:
                # Update operation
                try:
                    instance = GstEntryModel.objects.get(id=pk)
                    # Capture previous data before update
                    previous_data = self.get_previous_data(instance)

                    serializer = GstEntryModelSerializers(instance=instance, data=data)
                    if serializer.is_valid():
                        # Log the update before saving
                        ActivityLogger.log_activity(
                            request=request,
                            action='UPDATE',
                            entity_type='GstEntry',
                            entity_id=pk,
                            details={'new_data': serializer.validated_data},
                            previous_data=previous_data
                        )
                        # Save the updated instance
                        serializer.save()
                        response = {'code': 1, 'message': "Done Successfully"}
                    else:
                        response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
                except GstEntryModel.DoesNotExist:
                    response = {'code': 0, 'message': "GST Entry not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = GstEntryModel.objects.get(id=pk)
                # Capture previous data before deletion
                previous_data = self.get_previous_data(instance)

                # Log the deletion with the captured data
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='GstEntry',
                    entity_id=pk,
                    previous_data=previous_data
                )

                # Soft delete the instance
                instance.hideStatus = 1
                instance.save()

                response = {'code': 1, 'message': "Done Successfully"}
            except GstEntryModel.DoesNotExist:
                response = {'code': 0, 'message': "GST Entry not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class IssueViewSet(viewsets.ModelViewSet):
    queryset = IssueModel.objects.filter(hideStatus=0)
    serializer_class = IssueModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    def calculate_resolution_date(self, start_date, estimated_days):
        current_date = start_date
        working_days = 0
        while working_days < estimated_days:
            current_date += timedelta(days=1)
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                working_days += 1
        return current_date

    @action(detail=False, methods=['GET'])
    def listing(self, request):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

        page_size = int(request.query_params.get('page_size', 10))
        page = int(request.query_params.get('page', 1))
        search = request.query_params.get('search', '')

        queryset = self.get_queryset().select_related(
            'issueType',
            'issueClientName',
        )

        if search:
            queryset = queryset.filter(
                Q(issueType__issueTypeName__icontains=search) |
                Q(issueClientName__clientName__icontains=search) |
                Q(issueDate__icontains=search) |
                Q(issueResolutionDate__icontains=search) |
                Q(issueDescription__icontains=search)
            )

        total_count = queryset.count()
        total_pages = (total_count + page_size - 1) // page_size

        start = (page - 1) * page_size
        end = start + page_size

        queryset = queryset.order_by('-id')[start:end]

        serializer = self.get_serializer(queryset, many=True)

        data = {
            'code': 1,
            'data': serializer.data,
            'message': "Retrieved Successfully",
            'total_count': total_count,
            'total_pages': total_pages,
            'current_page': page
        }

        return Response(data)

    @action(detail=False, methods=['GET'])
    def total_count(self, request):
        search = request.query_params.get('search', '')
        queryset = self.get_queryset()

        if search:
            queryset = queryset.filter(
                Q(issueType__issueTypeName__icontains=search) |
                Q(issueClientName__clientName__icontains=search) |
                Q(issueDate__icontains=search) |
                Q(issueResolutionDate__icontains=search) |
                Q(issueDescription__icontains=search)
            )

        total_count = queryset.count()
        return Response({'total_count': total_count})

    @action(detail=True, methods=['GET'])
    def list_for_update(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = IssueModel.objects.get(id=pk)
                serializer = IssueModelSerializers(instance)
                return Response({'code': 1, 'data': serializer.data, 'message': "Retrieved Successfully"})
            except IssueModel.DoesNotExist:
                return Response({'code': 0, 'message': "NAV not found"}, status=404)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=401)

    @action(detail=True, methods=['POST'])
    @transaction.atomic
    def processing(self, request, pk=None):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': 'Token is invalid'})

        data = request.data
        try:
            client = ClientModel.objects.get(id=data['issueClientName'])
            issue_type = IssueTypeModel.objects.get(id=data['issueType'])

            issue_date = datetime.strptime(data['issueDate'], '%Y-%m-%d').date()

            if pk == '0':  # Create new issue
                issue_resolution_date = self.calculate_resolution_date(
                    start_date=issue_date,
                    estimated_days=issue_type.estimatedIssueDay
                )

                # Create IssueModel
                issue = IssueModel.objects.create(
                    issueClientName=client,
                    issueType=issue_type,
                    issueDate=issue_date,
                    issueResolutionDate=issue_resolution_date,
                    issueDescription=data['issueDescription'],
                    hideStatus=0
                )

                # Log the create action
                ActivityLogger.log_activity(
                    request=request,
                    action='CREATE',
                    entity_type='Issue',
                    entity_id=issue.id,
                    details={'new_data': data}
                )

                message = 'Issue created successfully'
            else:  # Update existing issue
                issue = IssueModel.objects.get(id=pk)
                issue_resolution_date = datetime.strptime(data['issueResolutionDate'], '%Y-%m-%d').date()

                previous_data = self.get_previous_data(issue)

                # Update IssueModel
                issue.issueClientName = client
                issue.issueType = issue_type
                issue.issueDate = issue_date
                issue.issueResolutionDate = issue_resolution_date
                issue.issueDescription = data['issueDescription']
                issue.save()

                # Log the update action
                ActivityLogger.log_activity(
                    request=request,
                    action='UPDATE',
                    entity_type='Issue',
                    entity_id=pk,
                    details={'new_data': data},
                    previous_data=previous_data
                )

                # If there's an associated DailyEntryModel, update it
                if issue.issueDailyEntry:
                    daily_entry = issue.issueDailyEntry
                    daily_entry.dailyEntryIssueType = issue_type
                    daily_entry.save()
                    message = 'Issue and associated daily entry updated successfully'
                else:
                    message = 'Issue updated successfully'

            return Response({
                'code': 1,
                'message': message,
                'issue_id': issue.id,
                'issue_resolution_date': issue_resolution_date.strftime('%Y-%m-%d')
            })
        except Exception as e:
            transaction.set_rollback(True)
            return Response({
                'code': 0,
                'message': f'Failed to process issue: {str(e)}'
            }, status=status.HTTP_400_BAD_REQUEST)

    @action(detail=True, methods=['GET'])
    @transaction.atomic
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                # Get the issue instance
                issue = IssueModel.objects.get(id=pk)
                previous_data = self.get_previous_data(issue)

                # Update the issue's hideStatus (soft delete)
                issue.hideStatus = 1
                issue.save()

                # Log the delete action
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='Issue',
                    entity_id=pk,
                    previous_data=previous_data
                )

                # If there's an associated DailyEntryModel, update it
                if issue.issueDailyEntry:
                    daily_entry = issue.issueDailyEntry
                    daily_entry.dailyEntryIssueType = None
                    daily_entry.save()

                # Update DailyEntryModel instances related to the issue
                DailyEntryModel.objects.filter(dailyEntryIssueType=issue.issueType).update(dailyEntryIssueType=None)

                return Response({'code': 1, 'message': "Issue deleted successfully, daily entries updated"})
            except IssueModel.DoesNotExist:
                return Response({'code': 0, 'message': "Issue not found"}, status=status.HTTP_404_NOT_FOUND)
            except Exception as e:
                transaction.set_rollback(True)
                return Response({'code': 0, 'message': f"An error occurred: {str(e)}"},
                                status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)


class StatementViewSet(viewsets.ModelViewSet):
    queryset = StatementModel.objects.filter(hideStatus=0)
    serializer_class = StatementModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    @action(detail=False, methods=['GET'])
    def listing(self, request):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

        page_size = int(request.query_params.get('page_size', 10))
        page = int(request.query_params.get('page', 1))
        search = request.query_params.get('search', '')

        queryset = self.get_queryset().select_related(
            'statementAmcName',
        )

        if search:
            queryset = queryset.filter(
                Q(statementAmcName__amcName__icontains=search) |
                Q(statementDate__icontains=search) |
                Q(statementInvestorName__icontains=search) |
                Q(statementInvestorPanNo__icontains=search) |
                Q(statementFundName__icontains=search)
            )

        total_count = queryset.count()
        total_pages = (total_count + page_size - 1) // page_size

        start = (page - 1) * page_size
        end = start + page_size

        queryset = queryset.order_by('-id')[start:end]

        serializer = self.get_serializer(queryset, many=True)

        data = {
            'code': 1,
            'data': serializer.data,
            'message': "Retrieved Successfully",
            'total_count': total_count,
            'total_pages': total_pages,
            'current_page': page
        }

        return Response(data)

    @action(detail=False, methods=['GET'])
    def total_count(self, request):
        search = request.query_params.get('search', '')
        queryset = self.get_queryset()

        if search:
            queryset = queryset.filter(
                Q(statementAmcName__amcName__icontains=search) |
                Q(statementDate__icontains=search) |
                Q(statementInvestorName__icontains=search) |
                Q(statementInvestorPanNo__icontains=search) |
                Q(statementFundName__icontains=search)
            )

        total_count = queryset.count()
        return Response({'total_count': total_count})

    @action(detail=True, methods=['GET'])
    def list_for_update(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = StatementModel.objects.get(id=pk)
                serializer = StatementModelSerializers(instance)
                return Response({'code': 1, 'data': serializer.data, 'message': "Retrieved Successfully"})
            except StatementModel.DoesNotExist:
                return Response({'code': 0, 'message': "NAV not found"}, status=404)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=401)

    @action(detail=True, methods=['POST'])
    @transaction.atomic
    def processing(self, request, pk=None):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': 'Token is invalid'}, status=status.HTTP_401_UNAUTHORIZED)

        try:
            if pk == "0":  # Creating a new statement
                serializer = StatementModelSerializers(data=request.data)
            else:  # Updating an existing statement
                statement = StatementModel.objects.get(id=pk)
                previous_data = self.get_previous_data(statement)
                serializer = StatementModelSerializers(instance=statement, data=request.data)

            if serializer.is_valid():
                statement = serializer.save()

                # Log the activity based on creation or update
                action_type = 'CREATE' if pk == "0" else 'UPDATE'
                ActivityLogger.log_activity(
                    request=request,
                    action=action_type,
                    entity_type='Statement',
                    entity_id=statement.id,
                    details={'new_data': request.data},
                    previous_data=previous_data if pk != "0" else None
                )

                response = {'code': 1, 'message': "Done Successfully"}
            else:
                response = {'code': 0, 'message': "Unable to Process Request"}
        except StatementModel.DoesNotExist:
            return Response({'code': 0, 'message': 'Statement not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            transaction.set_rollback(True)
            return Response({'code': 0, 'message': f'An error occurred: {str(e)}'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(response)

    @action(detail=True, methods=['GET'])
    @transaction.atomic
    def deletion(self, request, pk=None):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': 'Token is invalid'}, status=status.HTTP_401_UNAUTHORIZED)

        try:
            statement = StatementModel.objects.get(id=pk)
            previous_data = self.get_previous_data(statement)

            # Soft delete the statement (update hideStatus to 1)
            StatementModel.objects.filter(id=pk).update(hideStatus=1)

            # Log the delete action
            ActivityLogger.log_activity(
                request=request,
                action='DELETE',
                entity_type='Statement',
                entity_id=pk,
                previous_data=previous_data
            )

            response = {'code': 1, 'message': "Done Successfully"}
        except StatementModel.DoesNotExist:
            return Response({'code': 0, 'message': 'Statement not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            transaction.set_rollback(True)
            return Response({'code': 0, 'message': f'An error occurred: {str(e)}'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

        return Response(response)


class CourierViewSet(viewsets.ModelViewSet):
    queryset = CourierModel.objects.filter(hideStatus=0)
    serializer_class = CourierModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    @action(detail=False, methods=['GET'])
    def listing(self, request):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

        page_size = int(request.query_params.get('page_size', 10))
        page = int(request.query_params.get('page', 1))
        search = request.query_params.get('search', '')

        queryset = self.get_queryset().select_related(
            'courierClientName',
        )

        if search:
            queryset = queryset.filter(
                Q(courierClientName__clientName__icontains=search) |
                Q(courierClientAddress__icontains=search) |
                Q(courierMobileNumber__icontains=search) |
                Q(courierEmail__icontains=search)
            )

        total_count = queryset.count()
        total_pages = (total_count + page_size - 1) // page_size

        start = (page - 1) * page_size
        end = start + page_size

        queryset = queryset.order_by('-id')[start:end]

        serializer = self.get_serializer(queryset, many=True)

        data = {
            'code': 1,
            'data': serializer.data,
            'message': "Retrieved Successfully",
            'total_count': total_count,
            'total_pages': total_pages,
            'current_page': page
        }

        return Response(data)

    @action(detail=False, methods=['GET'])
    def total_count(self, request):
        search = request.query_params.get('search', '')
        queryset = self.get_queryset()

        if search:
            queryset = queryset.filter(
                Q(courierClientName__clientName__icontains=search) |
                Q(courierClientAddress__icontains=search) |
                Q(courierMobileNumber__icontains=search) |
                Q(courierEmail__icontains=search)
            )

        total_count = queryset.count()
        return Response({'total_count': total_count})

    @action(detail=True, methods=['GET'])
    def list_for_update(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = CourierModel.objects.get(id=pk)
                serializer = CourierModelSerializers(instance)
                return Response({'code': 1, 'data': serializer.data, 'message': "Retrieved Successfully"})
            except CourierModel.DoesNotExist:
                return Response({'code': 0, 'message': "NAV not found"}, status=404)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=401)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                with transaction.atomic():
                    if pk == "0":
                        serializer = CourierModelSerializers(data=request.data)
                        action_type = 'CREATE'
                        previous_data = None
                    else:
                        instance = CourierModel.objects.get(id=pk)
                        previous_data = self.get_previous_data(instance)
                        serializer = CourierModelSerializers(instance=instance, data=request.data, partial=True)
                        action_type = 'UPDATE'

                    if serializer.is_valid():
                        instance = serializer.save()

                        # Create a clean version of request.data for logging
                        clean_data = {
                            key: value for key, value in request.data.items()
                            if key != 'courierFile'  # Exclude the file from the log
                        }

                        if 'courierFile' in request.FILES:
                            files_info = []
                            for file in request.FILES.getlist('courierFile'):
                                files_info.append({
                                    'filename': file.name,
                                    'size': file.size,
                                    'content_type': file.content_type
                                })
                            clean_data['courierFile'] = files_info

                        # Log activity with clean data
                        ActivityLogger.log_activity(
                            request=request,
                            action=action_type,
                            entity_type='Courier',
                            entity_id=instance.id,
                            details={'new_data': clean_data},
                            previous_data=previous_data
                        )

                        response = {'code': 1, 'message': "Done Successfully"}
                    else:
                        response = {'code': 0, 'message': "Unable to Process Request", 'errors': serializer.errors}
            except ValidationError as e:
                response = {'code': 0, 'message': "File validation error", 'errors': str(e)}
            except Exception as e:
                response = {'code': 0, 'message': str(e)}
        else:
            response = {'code': 0, 'message': "Token is invalid"}

        return Response(response, status=status.HTTP_200_OK)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = CourierModel.objects.get(id=pk)
                previous_data = self.get_previous_data(instance)
                CourierModel.objects.filter(id=pk).update(hideStatus=1)

                # Log delete activity
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='Courier',
                    entity_id=pk,
                    previous_data=previous_data
                )

                response = {'code': 1, 'message': "Done Successfully"}
            except CourierModel.DoesNotExist:
                response = {'code': 0, 'message': "Courier not found"}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class CourierFileViewSet(viewsets.ModelViewSet):
    queryset = CourierFileModel.objects.filter(hideStatus=0)
    serializer_class = CourierFileModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            files = CourierFileModel.objects.filter(courier_id=pk, hideStatus=0)
            serializer = CourierFileModelSerializers(files, many=True)
            response = {'code': 1, 'data': serializer.data, 'message': "All Files Retrieved"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                file = CourierFileModel.objects.get(id=pk)
                file.hideStatus = 1
                file.save()
                response = {'code': 1, 'message': "File Deleted Successfully"}
            except CourierFileModel.DoesNotExist:
                response = {'code': 0, 'message': "File not found"}
            except Exception as e:
                response = {'code': 0, 'message': str(e)}
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class FormsViewSet(viewsets.ModelViewSet):
    queryset = FormsModel.objects.filter(hideStatus=0)
    serializer_class = FormsModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        serializer = self.get_serializer(instance, context={'request': self.request})
        return serializer.data

    @action(detail=False, methods=['GET'])
    def listing(self, request):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

        page_size = int(request.query_params.get('page_size', 10))
        page = int(request.query_params.get('page', 1))
        search = request.query_params.get('search', '')

        queryset = self.get_queryset().select_related(
            'formsAmcName',
            'formsType',
        )

        if search:
            queryset = queryset.filter(
                Q(formsAmcName__amcName__icontains=search) |
                Q(formsType__formTypeName__icontains=search) |
                Q(formsDescription__icontains=search)
            )

        total_count = queryset.count()
        total_pages = (total_count + page_size - 1) // page_size

        start = (page - 1) * page_size
        end = start + page_size

        queryset = queryset.order_by('-id')[start:end]

        serializer = self.get_serializer(queryset, many=True)

        data = {
            'code': 1,
            'data': serializer.data,
            'message': "Retrieved Successfully",
            'total_count': total_count,
            'total_pages': total_pages,
            'current_page': page
        }

        return Response(data)

    @action(detail=False, methods=['GET'])
    def total_count(self, request):
        search = request.query_params.get('search', '')
        queryset = self.get_queryset()

        if search:
            queryset = queryset.filter(
                Q(formsAmcName__amcName__icontains=search) |
                Q(formsType__formTypeName__icontains=search) |
                Q(formsDescription__icontains=search)
            )

        total_count = queryset.count()
        return Response({'total_count': total_count})

    @action(detail=True, methods=['GET'])
    def list_for_update(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = FormsModel.objects.get(id=pk)
                serializer = FormsModelSerializers(instance, context={'request': request})
                return Response({'code': 1, 'data': serializer.data, 'message': "Retrieved Successfully"})
            except FormsModel.DoesNotExist:
                return Response({'code': 0, 'message': "NAV not found"}, status=404)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=401)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                # Create operation
                serializer = FormsModelSerializers(data=request.data)
                if serializer.is_valid():
                    instance = serializer.save()
                    # Log the create activity
                    ActivityLogger.log_activity(
                        request=request,
                        action='CREATE',
                        entity_type='Forms',
                        entity_id=instance.id,
                        details={'new_data': serializer.data}
                    )
                    response = {'code': 1, 'message': "Done Successfully"}
                else:
                    print("Serializer errors:", serializer.errors)
                    response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
            else:
                # Update operation
                try:
                    instance = FormsModel.objects.get(id=pk)
                    # Capture previous data before update
                    previous_data = self.get_previous_data(instance)

                    serializer = FormsModelSerializers(instance=instance, data=request.data, partial=True)
                    if serializer.is_valid():
                        # Log before saving the changes
                        ActivityLogger.log_activity(
                            request=request,
                            action='UPDATE',
                            entity_type='Forms',
                            entity_id=pk,
                            details={'new_data': serializer.validated_data},
                            previous_data=previous_data
                        )
                        # Save the changes
                        serializer.save()
                        response = {'code': 1, 'message': "Done Successfully"}
                    else:
                        print("Serializer errors:", serializer.errors)
                        response = {'code': 0, 'message': "Unable to Process Request", 'error': serializer.errors}
                except FormsModel.DoesNotExist:
                    response = {'code': 0, 'message': "Form not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = FormsModel.objects.get(id=pk)
                # Capture previous data before deletion
                previous_data = self.get_previous_data(instance)

                # Log the deletion with the captured data
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='Forms',
                    entity_id=pk,
                    previous_data=previous_data
                )

                # Soft delete the instance
                instance.hideStatus = 1
                instance.save()

                response = {'code': 1, 'message': "Done Successfully"}
            except FormsModel.DoesNotExist:
                response = {'code': 0, 'message': "Form not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class MarketingViewSet(viewsets.ModelViewSet):
    queryset = MarketingModel.objects.filter(hideStatus=0)
    serializer_class = MarketingModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    def handle_file_upload(self, file, old_file=None):
        """Handle file upload and cleanup old file if exists"""
        if old_file:
            try:
                # Get the relative path from the full URL
                old_path = old_file.split('/media/')[-1]
                if default_storage.exists(old_path):
                    default_storage.delete(old_path)
            except Exception as e:
                print(f"Error deleting old file: {str(e)}")
        return file

    @action(detail=False, methods=['GET'])
    def listing(self, request):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

        page_size = int(request.query_params.get('page_size', 10))
        page = int(request.query_params.get('page', 1))
        search = request.query_params.get('search', '')

        queryset = self.get_queryset().select_related(
            'marketingAmcName',
            'marketingType',
        )

        if search:
            queryset = queryset.filter(
                Q(marketingAmcName__amcName__icontains=search) |
                Q(marketingType__fileTypeName__icontains=search) |
                Q(marketingDescription__icontains=search)
            )

        total_count = queryset.count()
        total_pages = (total_count + page_size - 1) // page_size

        start = (page - 1) * page_size
        end = start + page_size

        queryset = queryset.order_by('-id')[start:end]

        serializer = self.get_serializer(queryset, many=True, context={'request': request})
        data = serializer.data

        response_data = {
            'code': 1,
            'data': data,
            'message': "Retrieved Successfully",
            'total_count': total_count,
            'total_pages': total_pages,
            'current_page': page
        }

        return Response(response_data)

    @action(detail=False, methods=['GET'])
    def total_count(self, request):
        search = request.query_params.get('search', '')
        queryset = self.get_queryset()

        if search:
            queryset = queryset.filter(
                Q(marketingAmcName__amcName__icontains=search) |
                Q(marketingType__fileTypeName__icontains=search) |
                Q(marketingDescription__icontains=search)
            )

        total_count = queryset.count()
        return Response({'total_count': total_count})

    @action(detail=True, methods=['GET'])
    def list_for_update(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = MarketingModel.objects.get(id=pk)
                serializer = self.get_serializer(instance, context={'request': request})
                data = serializer.data

                return Response({'code': 1, 'data': data, 'message': "Retrieved Successfully"})
            except MarketingModel.DoesNotExist:
                return Response({'code': 0, 'message': "Marketing entry not found"}, status=404)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=401)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"},
                            status=status.HTTP_401_UNAUTHORIZED)

        try:
            with transaction.atomic():
                if pk == "0":
                    # Create operation
                    if not request.FILES.get('marketingFile'):
                        return Response(
                            {'code': 0, 'message': "Marketing file is required"},
                            status=status.HTTP_400_BAD_REQUEST
                        )

                    # Prepare data for serializer
                    data = {
                        'marketingAmcName': request.data.get('marketingAmcName'),
                        'marketingType': request.data.get('marketingType'),
                        'marketingDescription': request.data.get('marketingDescription'),
                        'marketingFile': request.FILES['marketingFile'],
                        'hideStatus': request.data.get('hideStatus', '0')
                    }

                    serializer = self.get_serializer(data=data)
                    if serializer.is_valid():
                        instance = serializer.save()
                        ActivityLogger.log_activity(
                            request=request,
                            action='CREATE',
                            entity_type='Marketing',
                            entity_id=instance.id,
                            details={'new_data': serializer.data}
                        )
                        return Response({'code': 1, 'message': "Created Successfully"})
                    return Response(
                        {'code': 0, 'message': "Validation Error", 'errors': serializer.errors},
                        status=status.HTTP_400_BAD_REQUEST
                    )
                else:
                    # Update operation
                    try:
                        instance = self.get_queryset().get(id=pk)
                    except MarketingModel.DoesNotExist:
                        return Response(
                            {'code': 0, 'message': "Marketing material not found"},
                            status=status.HTTP_404_NOT_FOUND
                        )

                    previous_data = self.get_previous_data(instance)

                    # Prepare update data
                    update_data = {
                        'marketingAmcName': request.data.get('marketingAmcName'),
                        'marketingType': request.data.get('marketingType'),
                        'marketingDescription': request.data.get('marketingDescription'),
                        'hideStatus': request.data.get('hideStatus', instance.hideStatus)
                    }

                    # Handle file update if new file is provided
                    if request.FILES.get('marketingFile'):
                        old_file = instance.marketingFile.url if instance.marketingFile else None
                        update_data['marketingFile'] = self.handle_file_upload(
                            request.FILES['marketingFile'],
                            old_file
                        )

                    serializer = self.get_serializer(
                        instance=instance,
                        data=update_data,
                        partial=True
                    )

                    if serializer.is_valid():
                        instance = serializer.save()
                        ActivityLogger.log_activity(
                            request=request,
                            action='UPDATE',
                            entity_type='Marketing',
                            entity_id=pk,
                            details={'new_data': serializer.validated_data},
                            previous_data=previous_data
                        )
                        return Response({'code': 1, 'message': "Updated Successfully"})
                    return Response(
                        {'code': 0, 'message': "Validation Error", 'errors': serializer.errors},
                        status=status.HTTP_400_BAD_REQUEST
                    )

        except Exception as e:
            return Response(
                {'code': 0, 'message': f"Processing Error: {str(e)}"},
                status=status.HTTP_500_INTERNAL_SERVER_ERROR
            )

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = MarketingModel.objects.get(id=pk)
                # Capture previous data before deletion
                previous_data = self.get_previous_data(instance)

                # Log the deletion with the captured data
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='Marketing',
                    entity_id=pk,
                    previous_data=previous_data
                )

                # Soft delete the instance
                instance.hideStatus = 1
                instance.save()

                response = {'code': 1, 'message': "Done Successfully"}
            except MarketingModel.DoesNotExist:
                response = {'code': 0, 'message': "Marketing material not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['GET'])
    def share_links(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                marketing = MarketingModel.objects.get(id=pk, hideStatus=0)
                file_url = request.build_absolute_uri(marketing.marketingFile.url)
                title = f"Check out this marketing material: {marketing.marketingType}"

                # WhatsApp sharing
                whatsapp_link = f"https://api.whatsapp.com/send?text={urllib.parse.quote(title + ' ' + file_url)}"

                # Telegram sharing
                telegram_link = f"https://t.me/share/url?url={urllib.parse.quote(file_url)}&text={urllib.parse.quote(title)}"

                # Facebook sharing
                facebook_link = f"https://www.facebook.com/sharer/sharer.php?u={urllib.parse.quote(file_url)}"

                response = {
                    'code': 1,
                    'data': {
                        'whatsapp': whatsapp_link,
                        'telegram': telegram_link,
                        'facebook': facebook_link,
                        'file_url': file_url,
                        'title': title,
                    },
                    'message': "Share links generated successfully"
                }
            except MarketingModel.DoesNotExist:
                response = {'code': 0, 'data': {}, 'message': "Marketing material not found"}
        else:
            response = {'code': 0, 'data': {}, 'message': "Unauthorized access"}

        return Response(response)


class TaskViewSet(viewsets.ModelViewSet):
    queryset = TaskModel.objects.filter(hideStatus=0)
    serializer_class = TaskModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    @action(detail=False, methods=['GET'])
    def listing(self, request):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

        page_size = int(request.query_params.get('page_size', 10))
        page = int(request.query_params.get('page', 1))
        search = request.query_params.get('search', '')

        queryset = self.get_queryset().select_related(
            'taskClient',
        )

        if search:
            queryset = queryset.filter(
                Q(taskClient__clientName__icontains=search) |
                Q(taskTitle__icontains=search) |
                Q(taskDate__icontains=search)
            )

        total_count = queryset.count()
        total_pages = (total_count + page_size - 1) // page_size

        start = (page - 1) * page_size
        end = start + page_size

        queryset = queryset.order_by('-id')[start:end]

        serializer = self.get_serializer(queryset, many=True)

        data = {
            'code': 1,
            'data': serializer.data,
            'message': "Retrieved Successfully",
            'total_count': total_count,
            'total_pages': total_pages,
            'current_page': page
        }

        return Response(data)

    @action(detail=False, methods=['GET'])
    def total_count(self, request):
        search = request.query_params.get('search', '')
        queryset = self.get_queryset()

        if search:
            queryset = queryset.filter(
                Q(taskClient__clientName__icontains=search) |
                Q(taskTitle__icontains=search) |
                Q(taskDate__icontains=search)
            )

        total_count = queryset.count()
        return Response({'total_count': total_count})

    @action(detail=True, methods=['GET'])
    def list_for_update(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = TaskModel.objects.get(id=pk)
                serializer = TaskModelSerializers(instance)
                return Response({'code': 1, 'data': serializer.data, 'message': "Retrieved Successfully"})
            except TaskModel.DoesNotExist:
                return Response({'code': 0, 'message': "NAV not found"}, status=404)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=401)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                # Creating a new task
                serializer = TaskModelSerializers(data=request.data)
                if serializer.is_valid():
                    instance = serializer.save()
                    # Log creation activity
                    ActivityLogger.log_activity(
                        request=request,
                        action='CREATE',
                        entity_type='Task',
                        entity_id=instance.id,
                        details={'new_data': serializer.data}
                    )
                    response = {'code': 1, 'message': "Done Successfully"}
                else:
                    response = {'code': 0, 'message': "Unable to Process Request", 'errors': serializer.errors}
            else:
                # Updating an existing task
                try:
                    instance = TaskModel.objects.get(id=pk)
                    previous_data = self.get_previous_data(instance)

                    serializer = TaskModelSerializers(instance=instance, data=request.data, partial=True)
                    if serializer.is_valid():
                        # Log update activity
                        ActivityLogger.log_activity(
                            request=request,
                            action='UPDATE',
                            entity_type='Task',
                            entity_id=pk,
                            details={'new_data': serializer.validated_data},
                            previous_data=previous_data
                        )
                        serializer.save()
                        response = {'code': 1, 'message': "Done Successfully"}
                    else:
                        response = {'code': 0, 'message': "Unable to Process Request", 'errors': serializer.errors}
                except TaskModel.DoesNotExist:
                    response = {'code': 0, 'message': "Task not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response, status=status.HTTP_200_OK)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = TaskModel.objects.get(id=pk)
                previous_data = self.get_previous_data(instance)

                # Log the deletion activity
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='Task',
                    entity_id=pk,
                    previous_data=previous_data
                )

                # Soft delete the instance
                instance.hideStatus = 1
                instance.save()
                response = {'code': 1, 'message': "Done Successfully"}
            except TaskModel.DoesNotExist:
                response = {'code': 0, 'message': "Task not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class EmployeeViewSet(viewsets.ModelViewSet):
    queryset = EmployeeModel.objects.filter(hideStatus=0)
    serializer_class = EmployeeModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    @action(detail=True, methods=['GET'])
    def listing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                queryset = EmployeeModel.objects.filter(hideStatus=0).order_by('-id')
            else:
                queryset = EmployeeModel.objects.filter(hideStatus=0, id=pk).order_by('-id')

            # Pass the request context to the serializer
            serializer = EmployeeModelSerializers(queryset, many=True, context={'request': request})
            response = {'code': 1, 'data': serializer.data, 'message': "All Retrieved"}
        else:
            response = {'code': 0, 'data': [], 'message': "Token is invalid"}
        return Response(response)

    @action(detail=True, methods=['POST'])
    def processing(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            if pk == "0":
                # Creating a new employee
                serializer = EmployeeModelSerializers(data=request.data)
                if serializer.is_valid():
                    # Save new instance first
                    employee_instance = serializer.save()

                    # Hash the password if provided
                    raw_password = request.data.get('employeePassword', None)
                    if raw_password:
                        employee_instance.set_password(raw_password)
                        employee_instance.save()  # Save updated instance with hashed password

                    # Log creation activity
                    ActivityLogger.log_activity(
                        request=request,
                        action='CREATE',
                        entity_type='Employee',
                        entity_id=employee_instance.id,
                        details={'new_data': serializer.data}
                    )

                    response = {'code': 1, 'message': "Done Successfully"}
                else:
                    response = {'code': 0, 'message': "Unable to Process Request", 'errors': serializer.errors}
            else:
                # Updating an existing employee
                try:
                    employee_instance = EmployeeModel.objects.get(id=pk)
                    previous_data = self.get_previous_data(employee_instance)

                    serializer = EmployeeModelSerializers(instance=employee_instance, data=request.data, partial=True)
                    if serializer.is_valid():
                        serializer.save()  # Save instance first

                        # Hash the password if provided
                        raw_password = request.data.get('employeePassword', None)
                        if raw_password:
                            employee_instance.set_password(raw_password)
                            employee_instance.save()  # Save updated instance with hashed password

                        # Log update activity
                        ActivityLogger.log_activity(
                            request=request,
                            action='UPDATE',
                            entity_type='Employee',
                            entity_id=pk,
                            details={'new_data': serializer.validated_data},
                            previous_data=previous_data
                        )

                        response = {'code': 1, 'message': "Done Successfully"}
                    else:
                        response = {'code': 0, 'message': "Unable to Process Request", 'errors': serializer.errors}
                except EmployeeModel.DoesNotExist:
                    return Response({'code': 0, 'message': "Employee not found"}, status=status.HTTP_404_NOT_FOUND)
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response, status=status.HTTP_200_OK)

    @action(detail=True, methods=['POST'])
    def update_password(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                employee = EmployeeModel.objects.get(id=pk)
                new_password = request.data.get('newPassword')
                if new_password:
                    previous_data = self.get_previous_data(employee)

                    employee.set_password(new_password)
                    employee.save()

                    # Log password update activity
                    ActivityLogger.log_activity(
                        request=request,
                        action='UPDATE_PASSWORD',
                        entity_type='Employee',
                        entity_id=pk,
                        details={'new_password': True},
                        previous_data=previous_data
                    )

                    return Response({'code': 1, 'message': "Password updated successfully"})
                else:
                    return Response({'code': 0, 'message': "New password is required"},
                                    status=status.HTTP_400_BAD_REQUEST)
            except EmployeeModel.DoesNotExist:
                return Response({'code': 0, 'message': "Employee not found"}, status=status.HTTP_404_NOT_FOUND)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                employee_instance = EmployeeModel.objects.get(id=pk)
                previous_data = self.get_previous_data(employee_instance)

                # Log the deletion activity
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='Employee',
                    entity_id=pk,
                    previous_data=previous_data
                )

                # Soft delete the instance
                employee_instance.hideStatus = 1
                employee_instance.save()
                response = {'code': 1, 'message': "Done Successfully"}
            except EmployeeModel.DoesNotExist:
                return Response({'code': 0, 'message': "Employee not found"}, status=status.HTTP_404_NOT_FOUND)
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)


class ClientViewSet(viewsets.ModelViewSet):
    queryset = ClientModel.objects.filter(hideStatus=0)
    serializer_class = ClientModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    @action(detail=False, methods=['GET'])
    def listing(self, request):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

        page_size = int(request.query_params.get('page_size', 10))
        page = int(request.query_params.get('page', 1))
        search = request.query_params.get('search', '')

        queryset = self.get_queryset()

        if search:
            queryset = queryset.filter(
                Q(clientName__icontains=search) |
                Q(clientEmail__icontains=search) |
                Q(clientPhone__icontains=search)
            )

        total_count = queryset.count()
        total_pages = (total_count + page_size - 1) // page_size

        start = (page - 1) * page_size
        end = start + page_size

        queryset = queryset.order_by('-id')[start:end]

        serializer = self.get_serializer(queryset, many=True)

        data = {
            'code': 1,
            'data': serializer.data,
            'message': "Retrieved Successfully",
            'total_count': total_count,
            'total_pages': total_pages,
            'current_page': page
        }

        return Response(data)

    @action(detail=False, methods=['GET'])
    def total_count(self, request):
        search = request.query_params.get('search', '')
        queryset = self.get_queryset()

        if search:
            queryset = queryset.filter(
                Q(clientName__icontains=search) |
                Q(clientEmail__icontains=search) |
                Q(clientPhone__icontains=search)
            )

        total_count = queryset.count()
        return Response({'total_count': total_count})

    @action(detail=True, methods=['GET'])
    def list_for_update(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = ClientModel.objects.get(id=pk)
                serializer = ClientModelSerializers(instance)
                return Response({'code': 1, 'data': serializer.data, 'message': "Retrieved Successfully"})
            except ClientModel.DoesNotExist:
                return Response({'code': 0, 'message': "NAV not found"}, status=404)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=401)

    @action(detail=False, methods=['GET'])
    def countries(self, request):
        countries = CountryModel.objects.filter(hideStatus=0).values('id', 'countryCode', 'countryName', 'dailCode')
        country_data = [
            {
                "id": country['id'],
                "code": country['countryCode'],
                "name": country['countryName'],
                "dial_code": country['dailCode']
            }
            for country in countries
        ]
        return Response(country_data)

    @action(detail=True, methods=['GET'])
    def listing_client(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                client = ClientModelSerializers(ClientModel.objects.filter(hideStatus=0, id=pk).order_by('-id'),
                                                many=True)
                client_family = ClientFamilyDetailModelSerializers(
                    ClientFamilyDetailModel.objects.filter(hideStatus=0, clientFamilyDetailId=pk).order_by('-id'),
                    many=True)
                client_children = ClientChildrenDetailModelSerializers(
                    ClientChildrenDetailModel.objects.filter(hideStatus=0, clientChildrenId=pk).order_by('-id'),
                    many=True)
                client_present_address = ClientPresentAddressModelSerializers(
                    ClientPresentAddressModel.objects.filter(hideStatus=0, clientPresentAddressId=pk).order_by('-id'),
                    many=True)
                client_permanent_address = ClientPermanentAddressModelSerializers(
                    ClientPermanentAddressModel.objects.filter(hideStatus=0, clientPermanentAddressId=pk).order_by(
                        '-id'), many=True)
                client_office_address = ClientOfficeAddressModelSerializers(
                    ClientOfficeAddressModel.objects.filter(hideStatus=0, clientOfficeAddressId=pk).order_by('-id'),
                    many=True)
                client_overseas_address = ClientOverseasAddressModelSerializers(
                    ClientOverseasAddressModel.objects.filter(hideStatus=0, clientOverseasAddressId=pk).order_by('-id'),
                    many=True)
                client_nominee = ClientNomineeModelSerializers(
                    ClientNomineeModel.objects.filter(hideStatus=0, clientNomineeId=pk).order_by('-id'), many=True)
                client_insurance = ClientInsuranceModelSerializers(
                    ClientInsuranceModel.objects.filter(hideStatus=0, clientInsuranceId=pk).order_by('-id'), many=True)
                client_medical_insurance = ClientMedicalInsuranceModelSerializers(
                    ClientMedicalInsuranceModel.objects.filter(hideStatus=0, clientMedicalInsuranceId=pk).order_by(
                        '-id'), many=True)
                client_term_insurance = ClientTermInsuranceModelSerializers(
                    ClientTermInsuranceModel.objects.filter(hideStatus=0, clientTermInsuranceId=pk).order_by('-id'),
                    many=True)
                client_upload_files = ClientUploadFileModelSerializers(
                    ClientUploadFileModel.objects.filter(hideStatus=0, clientUploadFileId=pk).order_by('-id'),
                    many=True)
                client_bank = ClientBankModelSerializers(
                    ClientBankModel.objects.filter(hideStatus=0, clientBankId=pk).order_by('-id'), many=True)
                client_tax = ClientTaxModelSerializers(
                    ClientTaxModel.objects.filter(hideStatus=0, clientTaxId=pk).order_by('-id'), many=True)
                client_attorney = ClientPowerOfAttorneyModelSerializers(
                    ClientPowerOfAttorneyModel.objects.filter(hideStatus=0, clientPowerOfAttorneyId=pk).order_by('-id'),
                    many=True)
                client_guardian = ClientGuardianModelSerializers(
                    ClientGuardianModel.objects.filter(hideStatus=0, clientGuardianId=pk).order_by('-id'),
                    many=True)

                combined_serializer = {
                    "client": client.data,
                    "family": client_family.data,
                    "children": client_children.data,
                    "present_address": client_present_address.data,
                    "permanent_address": client_permanent_address.data,
                    "office_address": client_office_address.data,
                    "overseas_address": client_overseas_address.data,
                    "nominee": client_nominee.data,
                    "insurance": client_insurance.data,
                    "medical_insurance": client_medical_insurance.data,
                    "term_insurance": client_term_insurance.data,
                    "upload_files": client_upload_files.data,
                    "bank": client_bank.data,
                    "tax": client_tax.data,
                    "attorney": client_attorney.data,
                    "guardian": client_guardian.data,
                }
                return JsonResponse({'code': 1, 'data': combined_serializer, 'message': "All Retrieved"},
                                    encoder=CustomJSONEncoder)
            except Exception as e:
                error_message = str(e)
                stack_trace = traceback.format_exc()
                return Response({
                    'code': 0,
                    'message': "An error occurred while retrieving client data",
                    'error': error_message,
                    'stack_trace': stack_trace
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            return Response({'code': 0, 'data': [], 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

    @action(detail=True, methods=['POST'])
    @transaction.atomic
    def processing(self, request, pk=None):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Authentication token missing"},
                            status=status.HTTP_401_UNAUTHORIZED)

        try:
            with transaction.atomic():
                # Store previous data if updating
                previous_data = None
                if pk != "0":
                    try:
                        client_instance = ClientModel.objects.get(id=pk)
                        previous_data = self._gather_previous_data(client_instance)
                    except ClientModel.DoesNotExist:
                        return Response({'code': 0, 'message': "Client not found"},
                                        status=status.HTTP_404_NOT_FOUND)

                # Process main client data
                client_data = request.data.get('clientJson', {})
                if pk == "0":
                    client_serializer = ClientModelSerializers(data=client_data)
                    action = 'CREATE'
                else:
                    client_serializer = ClientModelSerializers(instance=client_instance, data=client_data)
                    action = 'UPDATE'

                if client_serializer.is_valid():
                    client_instance = client_serializer.save()
                else:
                    logger.error(f"Client serializer errors: {client_serializer.errors}")
                    return Response({'code': 0, 'message': "Invalid client data",
                                     'errors': client_serializer.errors})

                # Process all related data
                self._process_family_details(request, client_instance)
                self._process_children_details(request, client_instance)
                self._process_addresses(request, client_instance)
                self._process_nominees(request, client_instance)
                self._process_insurance_policies(request, client_instance)
                self._process_file_uploads(request, client_instance)
                self._process_bank_details(request, client_instance)
                self._process_tax_details(request, client_instance)
                self._process_guardian_details(request, client_instance)
                self._process_attorney_details(request, client_instance)

                # Gather new data after all processing
                new_data = self._gather_previous_data(client_instance)

                # Log the activity
                ActivityLogger.log_activity(
                    request=request,
                    action=action,
                    entity_type='Client',
                    entity_id=client_instance.id,
                    details={'new_data': new_data},
                    previous_data=previous_data
                )

                return Response({'code': 1, 'message': "Client data processed successfully"},
                                status=status.HTTP_200_OK)

        except Exception as e:
            logger.error(f"Error processing client data: {e}")
            return Response({'code': 0, 'message': f"An error occurred: {str(e)}"},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    def _process_family_details(self, request, client_instance):
        """Process family details with error handling"""
        family_data = request.data.get('familyJson', {})
        family_instance, created = ClientFamilyDetailModel.objects.get_or_create(
            clientFamilyDetailId=client_instance)
        family_serializer = ClientFamilyDetailModelSerializers(
            instance=family_instance,
            data=family_data
        )
        if family_serializer.is_valid():
            family_serializer.save()
        else:
            raise ValidationError(family_serializer.errors)

    def _process_children_details(self, request, client_instance):
        """Process children details with error handling"""
        children_data = request.data.get('childrenJson', [])
        existing_children = ClientChildrenDetailModel.objects.filter(
            clientChildrenId=client_instance)
        existing_children_ids = set(existing_children.values_list('id', flat=True))
        processed_children_ids = set()

        for child_data in children_data:
            child_id = child_data.get('id')
            if child_id:
                child_instance = existing_children.filter(id=child_id).first()
                if child_instance:
                    child_serializer = ClientChildrenDetailModelSerializers(
                        instance=child_instance,
                        data=child_data
                    )
                    processed_children_ids.add(child_id)
                else:
                    child_serializer = ClientChildrenDetailModelSerializers(data=child_data)
            else:
                child_serializer = ClientChildrenDetailModelSerializers(data=child_data)

            if child_serializer.is_valid():
                child_serializer.save(clientChildrenId=client_instance)
            else:
                raise ValidationError(child_serializer.errors)

        # Delete children that were not in the submitted data
        children_to_delete = existing_children_ids - processed_children_ids
        ClientChildrenDetailModel.objects.filter(id__in=children_to_delete).delete()

    def _process_addresses(self, request, client_instance):
        """Process all address types with error handling"""
        addresses_types = [
            ('presentAddressJson', ClientPresentAddressModelSerializers, 'clientPresentAddressId'),
            ('permanentAddressJson', ClientPermanentAddressModelSerializers, 'clientPermanentAddressId'),
            ('officeAddressJson', ClientOfficeAddressModelSerializers, 'clientOfficeAddressId'),
            ('overseasAddressJson', ClientOverseasAddressModelSerializers, 'clientOverseasAddressId'),
        ]

        for address_key, serializer_class, client_field in addresses_types:
            address_data = request.data.get(address_key, {})
            if address_data:
                address_instance, created = serializer_class.Meta.model.objects.get_or_create(
                    **{client_field: client_instance})
                address_serializer = serializer_class(
                    instance=address_instance,
                    data=address_data
                )
                if address_serializer.is_valid():
                    address_serializer.save(**{client_field: client_instance})
                else:
                    raise ValidationError(address_serializer.errors)

    def _process_nominees(self, request, client_instance):
        """Process nominee details with error handling"""
        nominee_data = request.data.get('nomineeJson', [])
        ClientNomineeModel.objects.filter(clientNomineeId=client_instance).delete()

        for nominee in nominee_data:
            nominee_serializer = ClientNomineeModelSerializers(data=nominee)
            if nominee_serializer.is_valid():
                nominee_serializer.save(clientNomineeId=client_instance)
            else:
                raise ValidationError(nominee_serializer.errors)

    def _process_insurance_policies(self, request, client_instance):
        """Process all insurance types with error handling"""
        insurance_types = [
            ('insuranceJson', ClientInsuranceModelSerializers, 'clientInsuranceId'),
            ('medicalInsuranceJson', ClientMedicalInsuranceModelSerializers, 'clientMedicalInsuranceId'),
            ('termInsuranceJson', ClientTermInsuranceModelSerializers, 'clientTermInsuranceId'),
        ]

        for insurance_key, serializer_class, client_field in insurance_types:
            insurance_data = request.data.get(insurance_key, [])
            serializer_class.Meta.model.objects.filter(**{client_field: client_instance}).delete()

            for policy in insurance_data:
                policy[client_field] = client_instance.id
                policy_serializer = serializer_class(data=policy)
                if policy_serializer.is_valid():
                    policy_serializer.save()
                else:
                    raise ValidationError(policy_serializer.errors)

    def _process_file_uploads(self, request, client_instance):
        """Process file uploads with error handling"""
        upload_files_data = request.data.get('uploadFilesJson', {})
        file_instance, created = ClientUploadFileModel.objects.get_or_create(
            clientUploadFileId=client_instance)

        if upload_files_data:
            for field_name, file_data in upload_files_data.items():
                if file_data:
                    if isinstance(file_data, str) and file_data.startswith('data:'):
                        format, imgstr = file_data.split(';base64,')
                        ext = format.split('/')[-1]
                        data = ContentFile(base64.b64decode(imgstr),
                                           name=f'{field_name}.{ext}')
                    elif isinstance(file_data, dict) and 'name' in file_data and 'content' in file_data:
                        ext = file_data['name'].split('.')[-1]
                        data = ContentFile(base64.b64decode(file_data['content']),
                                           name=file_data['name'])
                    else:
                        logger.warning(f"Unexpected file data format for {field_name}")
                        continue
                    setattr(file_instance, field_name, data)
            file_instance.save()

    def _process_bank_details(self, request, client_instance):
        """Process bank details with error handling"""
        bank_data = request.data.get('bankJson', [])
        ClientBankModel.objects.filter(clientBankId=client_instance).delete()

        for bank in bank_data:
            bank_serializer = ClientBankModelSerializers(data=bank)
            if bank_serializer.is_valid():
                bank_serializer.save(clientBankId=client_instance)
            else:
                raise ValidationError(bank_serializer.errors)

    def _process_tax_details(self, request, client_instance):
        """Process tax details with error handling"""
        tax_data = request.data.get('taxJson', {})
        tax_instance, created = ClientTaxModel.objects.get_or_create(
            clientTaxId=client_instance)
        tax_serializer = ClientTaxModelSerializers(
            instance=tax_instance,
            data=tax_data
        )
        if tax_serializer.is_valid():
            tax_serializer.save(clientTaxId=client_instance)
        else:
            raise ValidationError(tax_serializer.errors)

    def _process_guardian_details(self, request, client_instance):
        """Process guardian details with error handling"""
        guardian_data = request.data.get('guardianJSON', {})
        guardian_instance, created = ClientGuardianModel.objects.get_or_create(
            clientGuardianId=client_instance)
        guardian_serializer = ClientGuardianModelSerializers(
            instance=guardian_instance,
            data=guardian_data
        )
        if guardian_serializer.is_valid():
            guardian_serializer.save(clientGuardianId=client_instance)
        else:
            raise ValidationError(guardian_serializer.errors)

    def _process_attorney_details(self, request, client_instance):
        """Process attorney details with error handling"""
        attorney_data = request.data.get('attorneyJson', {})
        attorney_instance, created = ClientPowerOfAttorneyModel.objects.get_or_create(
            clientPowerOfAttorneyId=client_instance)
        attorney_serializer = ClientPowerOfAttorneyModelSerializers(
            instance=attorney_instance,
            data=attorney_data
        )
        if attorney_serializer.is_valid():
            attorney_serializer.save(clientPowerOfAttorneyId=client_instance)
        else:
            raise ValidationError(attorney_serializer.errors)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        """
        Soft delete a client and all related data by setting hideStatus to '1'
        """
        try:
            user = request.user
            if not user.is_authenticated:
                return Response({'code': 0, 'message': "Unauthorized access"}, status=401)

            with transaction.atomic():
                try:
                    client = ClientModel.objects.get(id=pk)
                except ClientModel.DoesNotExist:
                    return Response({'code': 0, 'message': "Client not found"}, status=404)

                # Store data before deletion for activity log
                previous_data = self._gather_previous_data(client)

                # Perform soft deletion by updating hideStatus
                self._perform_soft_deletion(client)

                # Log the deletion activity
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='Client',
                    entity_id=client.id,
                    details={'status_change': 'Hidden'},
                    instance=client,
                    previous_data=previous_data
                )

                return Response({
                    'code': 1,
                    'message': "Client and all related data hidden successfully"
                })

        except Exception as e:
            import traceback
            return Response({
                'code': 0,
                'message': f"An error occurred: {str(e)}",
                'trace': traceback.format_exc()
            }, status=500)

    def _gather_previous_data(self, client):
        """
        Gather all related data for the client
        """

        def get_single_instance_data(model, filter_kwargs, serializer_class):
            instance = model.objects.filter(**filter_kwargs).first()
            return serializer_class(instance).data if instance else None

        def get_multiple_instances_data(model, filter_kwargs, serializer_class):
            instances = model.objects.filter(**filter_kwargs)
            return serializer_class(instances, many=True).data if instances.exists() else []

        return {
            'client': ClientModelSerializers(client).data,
            'family': get_single_instance_data(
                ClientFamilyDetailModel,
                {'clientFamilyDetailId': client},
                ClientFamilyDetailModelSerializers
            ),
            'children': get_multiple_instances_data(
                ClientChildrenDetailModel,
                {'clientChildrenId': client},
                ClientChildrenDetailModelSerializers
            ),
            'present_address': get_single_instance_data(
                ClientPresentAddressModel,
                {'clientPresentAddressId': client},
                ClientPresentAddressModelSerializers
            ),
            'permanent_address': get_single_instance_data(
                ClientPermanentAddressModel,
                {'clientPermanentAddressId': client},
                ClientPermanentAddressModelSerializers
            ),
            'office_address': get_single_instance_data(
                ClientOfficeAddressModel,
                {'clientOfficeAddressId': client},
                ClientOfficeAddressModelSerializers
            ),
            'overseas_address': get_single_instance_data(
                ClientOverseasAddressModel,
                {'clientOverseasAddressId': client},
                ClientOverseasAddressModelSerializers
            ),
            'nominee': get_multiple_instances_data(
                ClientNomineeModel,
                {'clientNomineeId': client},
                ClientNomineeModelSerializers
            ),
            'insurance': get_multiple_instances_data(
                ClientInsuranceModel,
                {'clientInsuranceId': client},
                ClientInsuranceModelSerializers
            ),
            'medical_insurance': get_multiple_instances_data(
                ClientMedicalInsuranceModel,
                {'clientMedicalInsuranceId': client},
                ClientMedicalInsuranceModelSerializers
            ),
            'term_insurance': get_multiple_instances_data(
                ClientTermInsuranceModel,
                {'clientTermInsuranceId': client},
                ClientTermInsuranceModelSerializers
            ),
            'bank': get_multiple_instances_data(
                ClientBankModel,
                {'clientBankId': client},
                ClientBankModelSerializers
            ),
            'tax': get_single_instance_data(
                ClientTaxModel,
                {'clientTaxId': client},
                ClientTaxModelSerializers
            ),
            'attorney': get_single_instance_data(
                ClientPowerOfAttorneyModel,
                {'clientPowerOfAttorneyId': client},
                ClientPowerOfAttorneyModelSerializers
            ),
            'guardian': get_single_instance_data(
                ClientGuardianModel,
                {'clientGuardianId': client},
                ClientGuardianModelSerializers
            )
        }

    def _perform_soft_deletion(self, client):
        """
        Perform soft deletion by setting hideStatus to '1' for client and all related records
        """
        # Update hideStatus for the main client
        client.hideStatus = '1'
        client.save()

        # List of models and their foreign key fields to update
        related_models = [
            (ClientFamilyDetailModel, 'clientFamilyDetailId'),
            (ClientChildrenDetailModel, 'clientChildrenId'),
            (ClientPresentAddressModel, 'clientPresentAddressId'),
            (ClientPermanentAddressModel, 'clientPermanentAddressId'),
            (ClientOfficeAddressModel, 'clientOfficeAddressId'),
            (ClientOverseasAddressModel, 'clientOverseasAddressId'),
            (ClientNomineeModel, 'clientNomineeId'),
            (ClientInsuranceModel, 'clientInsuranceId'),
            (ClientMedicalInsuranceModel, 'clientMedicalInsuranceId'),
            (ClientTermInsuranceModel, 'clientTermInsuranceId'),
            (ClientUploadFileModel, 'clientUploadFileId'),
            (ClientBankModel, 'clientBankId'),
            (ClientTaxModel, 'clientTaxId'),
            (ClientPowerOfAttorneyModel, 'clientPowerOfAttorneyId'),
            (ClientGuardianModel, 'clientGuardianId')
        ]

        # Update hideStatus for all related models
        for model, fk_field in related_models:
            model.objects.filter(**{fk_field: client}).update(hideStatus='1')


class NavViewSet(viewsets.ModelViewSet):
    queryset = NavModel.objects.filter(hideStatus=0).order_by('-id')
    serializer_class = NavModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    @action(detail=False, methods=['GET'])
    def listing(self, request):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

        page_size = int(request.query_params.get('page_size', 100))
        search = request.query_params.get('search', '')
        cursor = request.query_params.get('cursor')

        queryset = self.get_queryset().select_related('navFundName', 'navFundName__fundAmcName')

        if search:
            queryset = queryset.filter(
                Q(navFundName__fundAmcName__amcName__icontains=search) |
                Q(navFundName__fundName__icontains=search) |
                Q(nav__icontains=search)
            )

        if cursor:
            queryset = queryset.filter(id__lte=int(cursor))

        queryset = queryset.order_by('-id')[:page_size + 1]

        results = list(queryset)
        next_cursor = None
        if len(results) > page_size:
            next_cursor = results[-1].id
            results = results[:-1]

        serializer = self.get_serializer(results, many=True)

        data = {
            'code': 1,
            'data': serializer.data,
            'message': "Retrieved Successfully",
            'next_cursor': str(next_cursor) if next_cursor else None
        }

        return Response(data)

    @action(detail=False, methods=['GET'])
    def total_count(self, request):
        search = request.query_params.get('search', '')
        queryset = self.get_queryset()

        if search:
            queryset = queryset.filter(
                Q(navFundName__fundAmcName__amcName__icontains=search) |
                Q(navFundName__fundName__icontains=search) |
                Q(nav__icontains=search)
            )

        total_count = queryset.count()
        return Response({'total_count': total_count})

    @action(detail=True, methods=['GET'])
    def list_for_update(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = NavModel.objects.get(id=pk)
                serializer = NavModelSerializers(instance)
                return Response({'code': 1, 'data': serializer.data, 'message': "Retrieved Successfully"})
            except NavModel.DoesNotExist:
                return Response({'code': 0, 'message': "NAV not found"}, status=404)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=401)

    @action(detail=True, methods=['POST'])
    @transaction.atomic
    def processing(self, request, pk=None):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

        data = request.data
        amc_id = data.get('navAmcName')
        fund_id = data.get('navFundName')
        nav = data.get('nav')
        nav_date = data.get('navDate')

        try:
            amc = AmcEntryModel.objects.get(id=amc_id)
            fund = FundModel.objects.get(id=fund_id)

            # Update fund's AMC if it has changed
            if fund.fundAmcName_id != amc.id:
                fund.fundAmcName = amc
                fund.save()

            if pk == "0":
                # Create new NAV entry
                nav_entry = NavModel.objects.create(
                    navFundName=fund,
                    nav=nav,
                    navDate=nav_date
                )
                # Log the create activity
                ActivityLogger.log_activity(
                    request=request,
                    action='CREATE',
                    entity_type='NavEntry',
                    entity_id=nav_entry.id,
                    details={'new_data': NavModelSerializers(nav_entry).data}
                )
            else:
                # Update existing NAV entry
                nav_entry = NavModel.objects.get(id=pk)
                previous_data = self.get_previous_data(nav_entry)  # Capture previous data
                nav_entry.navFundName = fund
                nav_entry.nav = nav
                nav_entry.navDate = nav_date
                nav_entry.save()
                # Log the update activity
                ActivityLogger.log_activity(
                    request=request,
                    action='UPDATE',
                    entity_type='NavEntry',
                    entity_id=pk,
                    details={'new_data': NavModelSerializers(nav_entry).data},
                    previous_data=previous_data
                )

            serializer = self.get_serializer(nav_entry)
            return Response({'code': 1, 'data': serializer.data, 'message': "Done Successfully"})

        except (AmcEntryModel.DoesNotExist, FundModel.DoesNotExist, NavModel.DoesNotExist) as e:
            return Response({'code': 0, 'message': str(e)}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'code': 0, 'message': str(e)}, status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                nav_entry = NavModel.objects.get(id=pk)
                # Capture previous data before deletion
                previous_data = self.get_previous_data(nav_entry)

                # Log the deletion with the captured data
                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='NavEntry',
                    entity_id=pk,
                    previous_data=previous_data
                )

                # Soft delete the instance
                nav_entry.hideStatus = 1
                nav_entry.save()

                response = {'code': 1, 'message': "Done Successfully"}
            except NavModel.DoesNotExist:
                response = {'code': 0, 'message': "NAV entry not found"}, 404
        else:
            response = {'code': 0, 'message': "Token is invalid"}
        return Response(response)

    @action(detail=False, methods=['POST'])
    def fetch(self, request):
        user = request.user
        if user.is_authenticated:
            date = request.data.get('date')
            start_date = request.data.get('start_date')
            end_date = request.data.get('end_date')

            try:
                if date:
                    call_command('fetch_nav_data', date=date)
                    message = f"NAV data fetched successfully for {date}"
                elif start_date and end_date:
                    call_command('fetch_nav_data', start_date=start_date, end_date=end_date)
                    message = f"Historic NAV data fetched successfully from {start_date} to {end_date}"
                else:
                    return Response({
                        'code': 0,
                        'message': "Invalid parameters. Provide either 'date' or both 'start_date' and 'end_date'."
                    }, status=status.HTTP_400_BAD_REQUEST)

                return Response({
                    'code': 1,
                    'message': message
                }, status=status.HTTP_200_OK)
            except Exception as e:
                return Response({
                    'code': 0,
                    'message': f"Error fetching NAV data: {str(e)}"
                }, status=status.HTTP_500_INTERNAL_SERVER_ERROR)
        else:
            return Response({
                'code': 0,
                'message': "Token is invalid"
            }, status=status.HTTP_401_UNAUTHORIZED)

    @action(detail=True, methods=['GET'])
    def get_nav_update_data(self, request, pk=None):
        try:
            with connection.cursor() as cursor:
                cursor.execute("SELECT * FROM get_nav_update_data(%s)", [pk])
                result = cursor.fetchone()

            if result:
                data = {
                    'navId': result[0],
                    'nav': float(result[1]),
                    'navDate': result[2].isoformat(),
                    'fundId': result[3],
                    'fundName': result[4],
                    'schemeCode': result[5],
                    'amcId': result[6],
                    'amcName': result[7]
                }
                return Response({'code': 1, 'data': data, 'message': 'NAV update data retrieved successfully'})
            else:
                return Response({'code': 0, 'message': 'NAV data not found'}, status=status.HTTP_404_NOT_FOUND)
        except Exception as e:
            return Response({'code': 0, 'message': f'Error retrieving NAV update data: {str(e)}'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)

    @action(detail=False, methods=['GET'])
    def funds_by_amc(self, request):
        amc_id = request.query_params.get('amc_id')
        if not amc_id:
            return Response({'error': 'AMC ID is required'}, status=status.HTTP_400_BAD_REQUEST)

        try:
            funds = FundModel.objects.filter(fundAmcName_id=amc_id, hideStatus=0).values('id', 'fundName')
            return Response({'code': 1, 'data': list(funds), 'message': 'Funds retrieved successfully'})
        except Exception as e:
            return Response({'code': 0, 'message': f'Error retrieving funds: {str(e)}'},
                            status=status.HTTP_500_INTERNAL_SERVER_ERROR)


class DailyEntryViewSet(viewsets.ModelViewSet):
    queryset = DailyEntryModel.objects.filter(hideStatus=0)
    serializer_class = DailyEntryModelSerializers
    permission_classes = [IsAuthenticated]

    def get_previous_data(self, instance):
        return self.get_serializer(instance).data

    def calculate_working_days(self, start_date, days):
        current_date = start_date
        working_days = 0
        while working_days < days:
            current_date += timedelta(days=1)
            if current_date.weekday() < 5:  # Monday = 0, Friday = 4
                working_days += 1
        return current_date

    @action(detail=False, methods=['GET'])
    def get_client_details(self, request):
        search_term = request.query_params.get('search_term')
        if not search_term:
            return Response({'code': 0, 'message': 'Search term is required'})

        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM get_client_details(%s)", [search_term])
            columns = [col[0] for col in cursor.description]
            client = cursor.fetchone()

        if client:
            client_data = dict(zip(columns, client))

            # Prepare the response data
            response_data = {
                'client_name': client_data['client_name'],
                'client_pan_no': client_data['client_pan_no'],
                'client_phone': client_data['client_phone'],
                'client_phone_dial_code': client_data['client_phone_dial_code'],
            }

            # Add alternate phone if it exists
            if client_data['client_alternate_phone']:
                response_data['client_alternate_phone'] = client_data['client_alternate_phone']
                response_data['client_alternate_phone_dial_code'] = client_data['client_alternate_phone_dial_code']

            return Response({
                'code': 1,
                'data': response_data,
                'message': 'Client details retrieved successfully'
            })
        else:
            return Response({'code': 0, 'message': 'Client not found'})

    @action(detail=False, methods=['GET'])
    def get_funds_by_amc(self, request):
        amc_id = request.query_params.get('amc_id')
        if not amc_id or not amc_id.isdigit():
            return Response({'code': 0, 'message': 'Valid AMC ID is required'}, status=400)

        with connection.cursor() as cursor:
            cursor.execute("SELECT * FROM get_funds_by_amc(%s)", [int(amc_id)])
            funds = [{'id': row[0], 'fundName': row[1], 'schemeCode': row[2]} for row in cursor.fetchall()]

        return Response({'code': 1, 'data': funds, 'message': 'Funds retrieved successfully'})

    @action(detail=False, methods=['GET'])
    def listing(self, request):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"}, status=status.HTTP_401_UNAUTHORIZED)

        page_size = int(request.query_params.get('page_size', 10))
        page = int(request.query_params.get('page', 1))
        search = request.query_params.get('search', '')

        queryset = self.get_queryset().select_related(
            'dailyEntryClientName',
            'dailyEntryClientPanNumber',
            'dailyEntryClientMobileNumber',
            'dailyEntryFundName',
            'dailyEntryIssueType'
        )

        if search:
            queryset = queryset.filter(
                Q(dailyEntryClientName__clientName__icontains=search) |
                Q(dailyEntryFundName__fundName__icontains=search) |
                Q(dailyEntryIssueType__issueTypeName__icontains=search) |
                Q(applicationDate__icontains=search)
            )

        total_count = queryset.count()
        total_pages = (total_count + page_size - 1) // page_size

        start = (page - 1) * page_size
        end = start + page_size

        queryset = queryset.order_by('-id')[start:end]

        serializer = self.get_serializer(queryset, many=True)

        data = {
            'code': 1,
            'data': serializer.data,
            'message': "Retrieved Successfully",
            'total_count': total_count,
            'total_pages': total_pages,
            'current_page': page
        }

        return Response(data)

    @action(detail=False, methods=['GET'])
    def total_count(self, request):
        search = request.query_params.get('search', '')
        queryset = self.get_queryset()

        if search:
            queryset = queryset.filter(
                Q(dailyEntryClientName__clientName__icontains=search) |
                Q(dailyEntryFundName__fundName__icontains=search) |
                Q(dailyEntryIssueType__issueTypeName__icontains=search) |
                Q(applicationDate__icontains=search)
            )

        total_count = queryset.count()
        return Response({'total_count': total_count})

    @action(detail=True, methods=['GET'])
    def list_for_update(self, request, pk=None):
        user = request.user
        if user.is_authenticated:
            try:
                instance = DailyEntryModel.objects.get(id=pk)
                serializer = DailyEntryModelSerializers(instance)
                return Response({'code': 1, 'data': serializer.data, 'message': "Retrieved Successfully"})
            except DailyEntryModel.DoesNotExist:
                return Response({'code': 0, 'message': "NAV not found"}, status=404)
        else:
            return Response({'code': 0, 'message': "Token is invalid"}, status=401)

    @action(detail=True, methods=['POST'])
    @transaction.atomic
    def processing(self, request, pk=None):
        user = request.user
        if not user.is_authenticated:
            return Response({'code': 0, 'message': 'Token is invalid'})

        data = request.data

        try:
            # Validate client phone country code
            client_phone_country_code = data.get('clientPhoneCountryCode')
            if not client_phone_country_code:
                return Response({'code': 0, 'message': "clientPhoneCountryCode is required"})

            try:
                country = CountryModel.objects.get(dailCode=client_phone_country_code)
            except CountryModel.DoesNotExist:
                return Response({'code': 0, 'message': f"Country with dial code {client_phone_country_code} not found"})

            # Create or update client
            client, client_created = ClientModel.objects.update_or_create(
                clientPanNo=data['dailyEntryClientPanNumber'],
                defaults={
                    'clientName': data['dailyEntryClientName'],
                    'clientPhone': data['clientMobileNumber'],
                    'clientPhoneCountryCode': country
                }
            )

            # Get previous data if updating
            previous_data = None
            if pk != "0":
                try:
                    existing_entry = DailyEntryModel.objects.get(id=pk)
                    previous_data = self.get_previous_data(existing_entry)
                except DailyEntryModel.DoesNotExist:
                    return Response({'code': 0, 'message': "Daily entry not found"}, status=404)

            # Create or get daily entry instance
            if pk == "0":
                issueDailyEntry = DailyEntryModel()
                action_type = 'CREATE'
            else:
                issueDailyEntry = existing_entry
                action_type = 'UPDATE'

            # Update daily entry fields
            issueDailyEntry.dailyEntryClientPanNumber = client
            issueDailyEntry.dailyEntryClientName = client
            issueDailyEntry.dailyEntryClientMobileNumber = client
            issueDailyEntry.dailyEntryClientCountryCode = client
            issueDailyEntry.applicationDate = datetime.strptime(data['applicationDate'], '%Y-%m-%d').date()
            issueDailyEntry.dailyEntryFundHouse = AmcEntryModel.objects.get(id=data['dailyEntryFundHouse'])
            issueDailyEntry.dailyEntryFundName = FundModel.objects.get(id=data['dailyEntryFundName'])
            issueDailyEntry.dailyEntryClientFolioNumber = data['clientFolioNumber']
            issueDailyEntry.dailyEntryAmount = data['amount']
            issueDailyEntry.dailyEntryClientChequeNumber = data['clientChequeNumber']
            issueDailyEntry.dailyEntrySipDate = datetime.strptime(data['sipDate'], '%Y-%m-%d').date() if data[
                'sipDate'] else None
            issueDailyEntry.dailyEntryStaffName = data['staffName']
            issueDailyEntry.dailyEntryTransactionAddDetails = data.get('transactionAddDetail', '')

            # Update issue type if changed
            new_issue_type = IssueTypeModel.objects.get(id=data['dailyEntryIssueType'])
            if issueDailyEntry.dailyEntryIssueType is None or issueDailyEntry.dailyEntryIssueType != new_issue_type:
                issueDailyEntry.dailyEntryIssueType = new_issue_type

            issueDailyEntry.save()

            # Calculate issue resolution date
            issue_resolution_date = self.calculate_working_days(
                issueDailyEntry.applicationDate,
                issueDailyEntry.dailyEntryIssueType.estimatedIssueDay
            )

            # Create or update associated issue
            issue, issue_created = IssueModel.objects.update_or_create(
                issueDailyEntry=issueDailyEntry,
                defaults={
                    'issueClientName': client,
                    'issueType': issueDailyEntry.dailyEntryIssueType,
                    'issueDate': issueDailyEntry.applicationDate,
                    'issueResolutionDate': issue_resolution_date,
                    'issueDescription': data.get('transactionAddDetail', ''),
                    'hideStatus': 0
                }
            )

            # Log activity
            activity_details = {
                'new_data': {
                    'client_info': {
                        'name': client.clientName,
                        'pan': client.clientPanNo,
                        'phone': client.clientPhone
                    },
                    'daily_entry_info': {
                        'fund_house': issueDailyEntry.dailyEntryFundHouse.id,
                        'fund_name': issueDailyEntry.dailyEntryFundName.id,
                        'amount': str(issueDailyEntry.dailyEntryAmount),
                        'application_date': issueDailyEntry.applicationDate.isoformat(),
                        'issue_type': issueDailyEntry.dailyEntryIssueType.id
                    },
                    'issue_info': {
                        'resolution_date': issue_resolution_date.isoformat(),
                        'description': issue.issueDescription
                    }
                },
                'transaction_type': 'create' if pk == "0" else 'update'
            }

            ActivityLogger.log_activity(
                request=request,
                action=action_type,
                entity_type='DailyEntry',
                entity_id=issueDailyEntry.id,
                details=activity_details,
                previous_data=previous_data
            )

            return Response({
                'code': 1,
                'message': 'Daily entry and issue processed successfully',
                'issueDailyEntry_id': issueDailyEntry.id,
                'issue_id': issue.id
            })

        except Exception as e:
            transaction.set_rollback(True)
            return Response({
                'code': 0,
                'message': f'Failed to process daily entry and issue: {str(e)}'
            }, status=500)

    @action(detail=True, methods=['GET'])
    def deletion(self, request, pk=None):
        if not request.user.is_authenticated:
            return Response({'code': 0, 'message': "Token is invalid"})

        try:
            with transaction.atomic():
                # Get the instance and its related data before deletion
                instance = self.get_queryset().get(id=pk)
                previous_data = self.get_previous_data(instance)

                # Get related issue before deletion
                related_issue = IssueModel.objects.filter(issueDailyEntry=instance).first()
                if related_issue:
                    related_issue.hideStatus = 1
                    related_issue.save()

                # Soft delete the daily entry
                instance.hideStatus = 1
                instance.save()

                # Log the deletion activity
                activity_details = {
                    'transaction_type': 'soft_delete',
                    'deleted_entities': {
                        'daily_entry_id': instance.id,
                        'issue_id': related_issue.id if related_issue else None
                    },
                    'deletion_time': datetime.now().isoformat()
                }

                ActivityLogger.log_activity(
                    request=request,
                    action='DELETE',
                    entity_type='DailyEntry',
                    entity_id=pk,
                    details=activity_details,
                    previous_data=previous_data
                )

                return Response({'code': 1, 'message': "Deleted Successfully"})

        except DailyEntryModel.DoesNotExist:
            return Response({
                'code': 0,
                'message': "Entry not found"
            }, status=404)
        except Exception as e:
            return Response({
                'code': 0,
                'message': f"Error processing deletion: {str(e)}"
            }, status=500)
