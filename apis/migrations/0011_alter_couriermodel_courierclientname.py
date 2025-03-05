# Generated by Django 5.0.7 on 2024-09-20 13:20

import django.db.models.deletion
from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('apis', '0010_issuemodel_issuedailyentry'),
    ]

    operations = [
        migrations.AlterField(
            model_name='couriermodel',
            name='courierClientName',
            field=models.ForeignKey(blank=True, null=True, on_delete=django.db.models.deletion.CASCADE, related_name='courierClientName', to='apis.clientmodel'),
        ),
    ]
