import logging
from rest_framework import serializers
from django_countries.serializers import CountryFieldMixin
from .models import *

logger = logging.getLogger(__name__)


class UserTypeModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = UserTypeModel
        fields = '__all__'


class CountryModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = CountryModel
        fields = '__all__'


class StateModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = StateModel
        fields = '__all__'


class ModeModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ModeModel
        fields = '__all__'


class IssueTypeModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = IssueTypeModel
        fields = '__all__'


class FormTypeModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = FormTypeModel
        fields = '__all__'


class GstTypeModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = GstTypeModel
        fields = '__all__'


class FileTypeModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = FileTypeModel
        fields = '__all__'


class GenderModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = GenderModel
        fields = '__all__'


class MaritalStatusModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = MaritalStatusModel
        fields = '__all__'


class PoliticallyExposedPersonModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = PoliticallyExposedPersonModel
        fields = '__all__'


class BankNameModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = BankNameModel
        fields = '__all__'


class RelationshipModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = RelationshipModel
        fields = '__all__'


class AccountTypeModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = AccountTypeModel
        fields = '__all__'


class AccountPreferenceModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = AccountPreferenceModel
        fields = '__all__'


class ArnEntryModelSerializers(serializers.ModelSerializer):
    full_mobile = serializers.SerializerMethodField()

    class Meta:
        model = ArnEntryModel
        fields = '__all__'

    def get_full_mobile(self, obj):
        if obj.arnCountryCode and obj.arnMobile:
            return f"{obj.arnCountryCode.dailCode} {obj.arnMobile}"
        return obj.arnMobile


class AmcEntryModelSerializers(CountryFieldMixin, serializers.ModelSerializer):
    amcGstType = serializers.PrimaryKeyRelatedField(queryset=GstTypeModel.objects.all())

    class Meta:
        model = AmcEntryModel
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        representation['amcGstType'] = instance.amcGstType.gstTypeName if instance.amcGstType else None
        return representation


class FundModelSerializers(serializers.ModelSerializer):
    fundAmcName = AmcEntryModelSerializers(read_only=True)
    fundAmcNameId = serializers.PrimaryKeyRelatedField(
        queryset=AmcEntryModel.objects.all(),
        source='fundAmcName',
        write_only=True
    )

    class Meta:
        model = FundModel
        fields = ['id', 'fundAmcName', 'fundAmcNameId', 'fundName', 'schemeCode', 'hideStatus']

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        representation['fundAmcName'] = instance.fundAmcName.amcName if instance.fundAmcName else None
        return representation


class AumEntryModelSerializers(serializers.ModelSerializer):
    aumArnNumber = serializers.PrimaryKeyRelatedField(queryset=ArnEntryModel.objects.all())
    aumAmcName = serializers.PrimaryKeyRelatedField(queryset=AmcEntryModel.objects.all())

    class Meta:
        model = AumEntryModel
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        representation['aumArnNumber'] = instance.aumArnNumber.arnNumber if instance.aumArnNumber else None
        representation['aumAmcName'] = instance.aumAmcName.amcName if instance.aumAmcName else None
        return representation


class CommissionEntryModelSerializers(serializers.ModelSerializer):
    commissionArnNumber = serializers.PrimaryKeyRelatedField(queryset=ArnEntryModel.objects.all())
    commissionAmcName = serializers.PrimaryKeyRelatedField(queryset=AmcEntryModel.objects.all())

    class Meta:
        model = CommissionEntryModel
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        representation[
            'commissionArnNumber'] = instance.commissionArnNumber.arnNumber if instance.commissionArnNumber else None
        representation[
            'commissionAmcName'] = instance.commissionAmcName.amcName if instance.commissionAmcName else None
        return representation


class AumYoyGrowthEntryModelSerializers(serializers.ModelSerializer):
    aumYoyGrowthAmcName = serializers.PrimaryKeyRelatedField(queryset=AmcEntryModel.objects.all())

    class Meta:
        model = AumYoyGrowthEntryModel
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        representation[
            'aumYoyGrowthAmcName'] = instance.aumYoyGrowthAmcName.amcName if instance.aumYoyGrowthAmcName else None
        return representation


class IndustryAumEntryModelSerializers(serializers.ModelSerializer):
    industryAumMode = serializers.PrimaryKeyRelatedField(queryset=ModeModel.objects.all())

    class Meta:
        model = IndustryAumEntryModel
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        representation['industryAumMode'] = instance.industryAumMode.modeName if instance.industryAumMode else None
        return representation


class GstEntryModelSerializers(serializers.ModelSerializer):
    gstAmcName = serializers.PrimaryKeyRelatedField(queryset=AmcEntryModel.objects.all())

    class Meta:
        model = GstEntryModel
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        representation[
            'gstAmcName'] = instance.gstAmcName.amcName if instance.gstAmcName else None
        return representation


class NavModelSerializers(serializers.ModelSerializer):
    navFundName = serializers.SerializerMethodField()
    amcName = serializers.SerializerMethodField()

    class Meta:
        model = NavModel
        fields = '__all__'

    def get_navFundName(self, obj):
        return obj.navFundName.fundName if obj.navFundName else None

    def get_amcName(self, obj):
        return obj.navFundName.fundAmcName.amcName if obj.navFundName and obj.navFundName.fundAmcName else None


class StatementModelSerializers(serializers.ModelSerializer):
    statementAmcName = serializers.PrimaryKeyRelatedField(queryset=AmcEntryModel.objects.all())

    class Meta:
        model = StatementModel
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        representation[
            'statementAmcName'] = instance.statementAmcName.amcName if instance.statementAmcName else None
        return representation


class CourierFileModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = CourierFileModel
        fields = ('id', 'courierFile')


class CourierModelSerializers(serializers.ModelSerializer):
    full_mobile = serializers.SerializerMethodField()
    courierClientName = serializers.PrimaryKeyRelatedField(queryset=ClientModel.objects.all())
    courierFile = serializers.ListField(
        child=serializers.FileField(
            max_length=100000,
            allow_empty_file=False,
            use_url=False,
            validators=[FileExtensionValidator(
                allowed_extensions=['pdf', 'doc', 'docx', 'jpg', 'jpeg', 'xls', 'xlsx', 'csv', 'txt']
            )]
        ),
        write_only=True,
        required=False
    )
    files = CourierFileModelSerializers(many=True, read_only=True, source='courier')

    class Meta:
        model = CourierModel
        fields = '__all__'

    def get_full_mobile(self, obj):
        if obj.courierCountryCode and obj.courierMobileNumber:
            return f"{obj.courierCountryCode.dailCode} {obj.courierMobileNumber}"
        return obj.courierMobileNumber

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        representation[
            'courierClientName'] = instance.courierClientName.clientName if instance.courierClientName else None
        return representation

    def create(self, validated_data):
        files_data = validated_data.pop('courierFile', None)
        courier = CourierModel.objects.create(**validated_data)
        if files_data:
            for file_data in files_data:
                CourierFileModel.objects.create(courier=courier, courierFile=file_data)
        return courier

    def update(self, instance, validated_data):
        files_data = validated_data.pop('courierFile', None)
        instance = super().update(instance, validated_data)
        if files_data:
            for file_data in files_data:
                CourierFileModel.objects.create(courier=instance, courierFile=file_data)
        return instance


class FormsModelSerializers(serializers.ModelSerializer):
    formsAmcName = serializers.PrimaryKeyRelatedField(queryset=AmcEntryModel.objects.all())
    formsType = serializers.PrimaryKeyRelatedField(queryset=FormTypeModel.objects.all())
    formsFile = serializers.FileField(required=False)

    class Meta:
        model = FormsModel
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        representation['formsAmcName'] = instance.formsAmcName.amcName if instance.formsAmcName else None
        representation['formsType'] = instance.formsType.formTypeName if instance.formsType else None
        if instance.formsFile and hasattr(instance.formsFile, 'url'):
            try:
                representation['formsFile'] = self.context['request'].build_absolute_uri(instance.formsFile.url)
            except (KeyError, AttributeError):
                representation['formsFile'] = instance.formsFile.name if instance.formsFile else None
        else:
            representation['formsFile'] = None
        return representation


class MarketingModelSerializers(serializers.ModelSerializer):
    marketingAmcName = serializers.PrimaryKeyRelatedField(queryset=AmcEntryModel.objects.all())
    marketingType = serializers.PrimaryKeyRelatedField(queryset=FileTypeModel.objects.all())
    marketingFile = serializers.FileField(required=False)

    class Meta:
        model = MarketingModel
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        representation[
            'marketingAmcName'] = instance.marketingAmcName.amcName if instance.marketingAmcName else None
        representation[
            'marketingType'] = instance.marketingType.fileTypeName if instance.marketingType else None
        if instance.marketingFile:
            representation['marketingFile'] = self.context['request'].build_absolute_uri(instance.marketingFile.url)
        return representation


class TaskModelSerializers(serializers.ModelSerializer):
    taskClient = serializers.PrimaryKeyRelatedField(queryset=ClientModel.objects.all())

    class Meta:
        model = TaskModel
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        representation['taskClient'] = instance.taskClient.clientName if instance.taskClient else None
        return representation


class EmployeeModelSerializers(serializers.ModelSerializer):
    employeeUserType = serializers.PrimaryKeyRelatedField(queryset=UserTypeModel.objects.all())
    employeePhotoUrl = serializers.SerializerMethodField()
    full_mobile = serializers.SerializerMethodField()

    class Meta:
        model = EmployeeModel
        fields = '__all__'

    def get_full_mobile(self, obj):
        if obj.employeeCountryCode and obj.employeePhone:
            return f"{obj.employeeCountryCode.dailCode} {obj.employeePhone}"
        return obj.employeePhone

    def get_employeePhotoUrl(self, obj):
        request = self.context.get('request')
        if obj.employeeFile:
            return request.build_absolute_uri(obj.employeeFile.url)
        return None

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        representation[
            'employeeUserType'] = instance.employeeUserType.userTypeName if instance.employeeUserType else None
        return representation


class ClientModelSerializers(serializers.ModelSerializer):
    full_mobile = serializers.SerializerMethodField()

    class Meta:
        model = ClientModel
        fields = '__all__'

    def get_full_mobile(self, obj):
        if obj.clientPhoneCountryCode and obj.clientPhone:
            return f"{obj.clientPhoneCountryCode.dailCode} {obj.clientPhone}"
        return obj.clientPhone


class ClientFamilyDetailModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientFamilyDetailModel
        fields = '__all__'


class ClientChildrenDetailModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientChildrenDetailModel
        fields = '__all__'


class ClientPresentAddressModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientPresentAddressModel
        fields = '__all__'


class ClientPermanentAddressModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientPermanentAddressModel
        fields = '__all__'


class ClientOfficeAddressModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientOfficeAddressModel
        fields = '__all__'


class ClientOverseasAddressModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientOverseasAddressModel
        fields = '__all__'


class ClientNomineeModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientNomineeModel
        fields = '__all__'


class ClientInsuranceModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientInsuranceModel
        fields = '__all__'


class ClientMedicalInsuranceModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientMedicalInsuranceModel
        fields = '__all__'


class ClientTermInsuranceModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientTermInsuranceModel
        fields = '__all__'


class ClientUploadFileModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientUploadFileModel
        fields = '__all__'

    def validate_file_field(self, value):
        if value:
            file_extension = value.name.split('.')[-1].lower()
            if file_extension not in ['jpg', 'jpeg', 'pdf']:
                raise serializers.ValidationError("Only JPG, JPEG, and PDF files are allowed.")
        return value

    # Add validation methods for each file field
    def validate_clientPaasPortSizePhoto(self, value):
        return self.validate_file_field(value)

    def validate_clientPanCardPhoto(self, value):
        return self.validate_file_field(value)

    def validate_clientAadharCard(self, value):
        return self.validate_file_field(value)

    def validate_clientDrivingLicense(self, value):
        return self.validate_file_field(value)

    def validate_clientVoterIDFrontImage(self, value):
        return self.validate_file_field(value)

    def validate_clientVoterIDBackImage(self, value):
        return self.validate_file_field(value)

    def validate_clientPassportFrontImage(self, value):
        return self.validate_file_field(value)

    def validate_clientPassportBackImage(self, value):
        return self.validate_file_field(value)

    def validate_clientForeignAddressProof(self, value):
        return self.validate_file_field(value)

    def validate_clientForeignTaxIdentificationProof(self, value):
        return self.validate_file_field(value)

    def validate_clientCancelledChequeCopy(self, value):
        return self.validate_file_field(value)

    def validate_clientBankAccountStatementOrPassbook(self, value):
        return self.validate_file_field(value)

    def validate_clientChildrenBirthCertificate(self, value):
        return self.validate_file_field(value)

    def validate_clientPowerOfAttorneyUpload(self, value):
        return self.validate_file_field(value)


class ClientBankModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientBankModel
        fields = '__all__'


class ClientTaxModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientTaxModel
        fields = '__all__'


class ClientPowerOfAttorneyModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientPowerOfAttorneyModel
        fields = '__all__'


class ClientGuardianModelSerializers(serializers.ModelSerializer):
    class Meta:
        model = ClientGuardianModel
        fields = '__all__'


class IssueModelSerializers(serializers.ModelSerializer):
    issueType = serializers.PrimaryKeyRelatedField(queryset=IssueTypeModel.objects.all())
    issueClientName = serializers.PrimaryKeyRelatedField(queryset=ClientModel.objects.all())

    class Meta:
        model = IssueModel
        fields = '__all__'

    def to_representation(self, instance):
        representation = super().to_representation(instance)
        representation['issueType'] = instance.issueType.issueTypeName if instance.issueType else None
        representation['issueClientName'] = instance.issueClientName.clientName if instance.issueClientName else None
        return representation


class DailyEntryModelSerializers(serializers.ModelSerializer):
    dailyEntryClientName = serializers.SerializerMethodField()
    dailyEntryClientPanNumber = serializers.SerializerMethodField()
    dailyEntryClientMobileNumber = serializers.SerializerMethodField()
    dailyEntryFundName = serializers.SerializerMethodField()
    dailyEntryIssueType = serializers.SerializerMethodField()
    dailyEntryClientCountryCode = serializers.SerializerMethodField()

    # dailyEntryClientCountryName = serializers.SerializerMethodField()
    # dailyEntryClientCountryDialCode = serializers.SerializerMethodField()

    class Meta:
        model = DailyEntryModel
        fields = '__all__'

    def get_dailyEntryClientName(self, obj):
        return obj.dailyEntryClientName.clientName if obj.dailyEntryClientName else None

    def get_dailyEntryClientPanNumber(self, obj):
        return obj.dailyEntryClientPanNumber.clientPanNo if obj.dailyEntryClientPanNumber else None

    def get_dailyEntryClientMobileNumber(self, obj):
        return obj.dailyEntryClientMobileNumber.clientPhone if obj.dailyEntryClientMobileNumber else None

    def get_dailyEntryFundName(self, obj):
        return obj.dailyEntryFundName.fundName if obj.dailyEntryFundName else None

    def get_dailyEntryIssueType(self, obj):
        return obj.dailyEntryIssueType.issueTypeName if obj.dailyEntryIssueType else None

    def get_dailyEntryClientCountryCode(self, obj):
        if obj.dailyEntryClientCountryCode and obj.dailyEntryClientCountryCode.clientPhoneCountryCode:
            return obj.dailyEntryClientCountryCode.clientPhoneCountryCode.dailCode
        return None

    # def get_dailyEntryClientCountryName(self, obj):
    #     if obj.dailyEntryClientCountryCode and obj.dailyEntryClientCountryCode.clientPhoneCountryCode:
    #         return obj.dailyEntryClientCountryCode.clientPhoneCountryCode.countryName
    #     return None
    #
    # def get_dailyEntryClientCountryDialCode(self, obj):
    #     if obj.dailyEntryClientCountryCode and obj.dailyEntryClientCountryCode.clientPhoneCountryCode:
    #         return obj.dailyEntryClientCountryCode.clientPhoneCountryCode.dailCode
    #     return None


class ActivityLogSerializer(serializers.ModelSerializer):
    class Meta:
        model = ActivityLog
        fields = '__all__'
