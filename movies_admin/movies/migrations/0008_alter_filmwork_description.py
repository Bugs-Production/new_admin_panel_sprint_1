# Generated by Django 4.2.11 on 2024-06-18 12:40

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("movies", "0007_alter_filmwork_creation_date"),
    ]

    operations = [
        migrations.AlterField(
            model_name="filmwork",
            name="description",
            field=models.TextField(blank=True, null=True, verbose_name="Description"),
        ),
    ]