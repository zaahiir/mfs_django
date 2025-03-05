from django.contrib import admin
from django.urls import path, include
from apis.views import UserViewSet, EmployeeViewSet
from rest_framework_simplejwt.views import TokenObtainPairView, TokenRefreshView

from django.conf import settings
from django.conf.urls.static import static

urlpatterns = [
    path('admin/', admin.site.urls),
    path('apis/', include('apis.urls')),
    path('apis/login/', UserViewSet.as_view({'post': 'login'}), name='login'),
    path('apis/logout/', UserViewSet.as_view({'post': 'logout'}), name='logout'),
    path('apis/profile/', UserViewSet.as_view({'get': 'profile'}), name='profile'),
    path('apis/token/', TokenObtainPairView.as_view(), name='token_obtain_pair'),
    path('apis/token/refresh/', TokenRefreshView.as_view(), name='token_refresh'),
]

if settings.DEBUG:
    urlpatterns += static(settings.MEDIA_URL, document_root=settings.MEDIA_ROOT)
