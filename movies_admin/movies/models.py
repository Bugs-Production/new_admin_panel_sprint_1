import uuid

from django.core.validators import MaxValueValidator, MinValueValidator
from django.db import models
from django.utils.translation import gettext_lazy as _


class TimeStampedMixin(models.Model):
    created = models.DateTimeField(auto_now_add=True)
    modified = models.DateTimeField(auto_now=True)

    class Meta:
        abstract = True


class UUIDMixin(models.Model):
    id = models.UUIDField(primary_key=True, default=uuid.uuid4, editable=False)

    class Meta:
        abstract = True


class Genre(UUIDMixin, TimeStampedMixin):
    name = models.CharField(verbose_name=_("Name"), max_length=64)
    description = models.TextField(verbose_name=_("Description"), blank=True)

    class Meta:
        db_table = 'content"."genre'
        verbose_name = _("Genre")
        verbose_name_plural = _("Genres")

    def __str__(self):
        return self.name


class Person(UUIDMixin, TimeStampedMixin):
    full_name = models.TextField(verbose_name=_("Full Name"))

    class Meta:
        db_table = 'content"."person'
        verbose_name = _("Actor")
        verbose_name_plural = _("Actors")

    def __str__(self):
        return self.full_name


class TypeChoices(models.TextChoices):
    MOVIE = _("movie")
    TV_SHOW = _("tv_show")


class FilmWork(UUIDMixin, TimeStampedMixin):
    title = models.TextField(verbose_name=_("Title"))
    description = models.TextField(verbose_name=_("Description"), blank=True)
    creation_date = models.DateField(verbose_name=_("Creation Date"))
    rating = models.FloatField(
        verbose_name=_("Rating"),
        blank=True,
        validators=[MinValueValidator(0), MaxValueValidator(100)],
    )
    type = models.CharField(
        verbose_name=_("Type"),
        max_length=20,
        choices=TypeChoices.choices,
        default=TypeChoices.MOVIE,
    )
    certificate = models.CharField(
        verbose_name=_("Certificate"), max_length=512, blank=True
    )
    file_path = models.FileField(
        verbose_name=_("File"), blank=True, null=True, upload_to="movies/"
    )
    genres = models.ManyToManyField(Genre, through="GenreFilmWork")
    persons = models.ManyToManyField(Person, through="PersonFilmWork")

    class Meta:
        db_table = 'content"."film_work'
        verbose_name = _("Film work")
        verbose_name_plural = _("Film works")

    def __str__(self):
        return self.title


class GenreFilmWork(UUIDMixin):
    genre = models.ForeignKey("Genre", on_delete=models.CASCADE)
    film_work = models.ForeignKey("FilmWork", on_delete=models.CASCADE)
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content"."genre_film_work'
        verbose_name = _("Genre and film")
        verbose_name_plural = _("Genres and films")

    def __str__(self):
        return f"Жанр - {self.genre}, кино - {self.film_work}"


class PersonFilmWork(UUIDMixin):
    person = models.ForeignKey("Person", on_delete=models.CASCADE)
    film_work = models.ForeignKey("FilmWork", on_delete=models.CASCADE)
    role = models.TextField(verbose_name=_("Role"))
    created = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = 'content"."person_film_work'
        verbose_name = _("Actor and film work")
        verbose_name_plural = _("Actors and film works")

    def __str__(self):
        return f"Актер - {self.person}, кино - {self.film_work}"
