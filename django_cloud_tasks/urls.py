from django.urls import re_path as url
from . import views

urlpatterns = [
    url(r'^$', views.run_task, name='run_task'),
]
