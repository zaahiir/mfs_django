from django.urls import path
from rest_framework import routers
from django.conf.urls import include
from .views import *

router = routers.DefaultRouter()

router.register('user', UserViewSet, basename='user'),
router.register('userType', UserTypeViewSet, basename='userType'),
router.register('states', StateViewSet, basename='states'),
router.register('country', CountryViewSet, basename='country'),
router.register('mode', ModeViewSet, basename='mode'),
router.register('issueType', IssueTypeViewSet, basename='issueType'),
router.register('formType', FormTypeViewSet, basename='formType'),
router.register('gstType', GstTypeViewSet, basename='gstType'),
router.register('fileType', FileTypeViewSet, basename='fileType'),
router.register('gender', GenderViewSet, basename='gender'),
router.register('maritalStatus', MaritalStatusViewSet, basename='maritalStatus'),
router.register('politicallyExposedPerson', PoliticallyExposedPersonViewSet, basename='politicallyExposedPerson'),
router.register('bankName', BankNameViewSet, basename='bankName'),
router.register('relationship', RelationshipViewSet, basename='relationship'),
router.register('accountType', AccountTypeViewSet, basename='AccountType'),
router.register('accountPreference', AccountPreferenceViewSet, basename='AccountPreference'),
router.register('arnEntry', ArnEntryViewSet, basename='arnEntry'),
router.register('amcEntry', AmcEntryViewSet, basename='amcEntry'),
router.register('fund', FundViewSet, basename='fund'),
router.register('aumEntry', AumEntryViewSet, basename='aumEntry'),
router.register('commissionEntry', CommissionEntryViewSet, basename='commissionEntry'),
router.register('aumYoyGrowthEntry', AumYoyGrowthEntryViewSet, basename='aumYoyGrowthEntry'),
router.register('industryAumEntry', IndustryAumEntryViewSet, basename='industryAumEntry'),
router.register('gstEntry', GstEntryViewSet, basename='gstEntry'),
router.register('nav', NavViewSet, basename='nav'),
router.register('issue', IssueViewSet, basename='issue'),
router.register('statement', StatementViewSet, basename='statement'),
router.register('courier', CourierViewSet, basename='courier'),
router.register('courierFiles', CourierFileViewSet, basename='courierFiles'),
router.register('forms', FormsViewSet, basename='forms'),
router.register('marketing', MarketingViewSet, basename='marketing'),
router.register('task', TaskViewSet, basename='task'),
router.register('employee', EmployeeViewSet, basename='employee'),
router.register('client', ClientViewSet, basename='client'),
router.register('dailyEntry', DailyEntryViewSet, basename='dailyEntry'),


urlpatterns = [
    path('', include(router.urls)),
]