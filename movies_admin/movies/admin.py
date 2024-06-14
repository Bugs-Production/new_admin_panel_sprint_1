from django.contrib import admin

from .models import FilmWork, Genre, GenreFilmWork, Person, PersonFilmWork


class GenreFilmWorkInline(admin.TabularInline):
    model = GenreFilmWork


class PersonFilmWorkInline(admin.TabularInline):
    model = PersonFilmWork


@admin.register(Genre)
class GenreAdmin(admin.ModelAdmin):
    list_display = ("name",)
    search_fields = ("name",)


@admin.register(FilmWork)
class FilmWorkAdmin(admin.ModelAdmin):
    inlines = (GenreFilmWorkInline, PersonFilmWorkInline)
    list_display = (
        "title",
        "type",
        "creation_date",
        "rating",
    )
    list_filter = ("type",)
    search_fields = ("title", "description", "id")


@admin.register(GenreFilmWork)
class GenreFilmWorkAdmin(admin.ModelAdmin):
    list_display = (
        "genre",
        "film_work",
    )
    list_filter = ("genre__name",)
    search_fields = ("film_work__title",)


@admin.register(PersonFilmWork)
class PersonFilmWorkAdmin(admin.ModelAdmin):
    list_display = (
        "role",
        "person",
        "film_work",
    )
    list_filter = ("role",)
    search_fields = ("person__full_name", "film_work__title", "role")


@admin.register(Person)
class PersonAdmin(admin.ModelAdmin):
    list_display = ("full_name",)
    search_fields = ("full_name",)
