# Generated by Django 4.2.11 on 2024-06-18 05:12

from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ("movies", "0004_alter_filmwork_options_alter_genre_options_and_more"),
    ]

    operations = [
        migrations.AlterField(
            model_name="filmwork",
            name="certificate",
            field=models.CharField(
                blank=True, max_length=512, null=True, verbose_name="Certificate"
            ),
        ),
    ]