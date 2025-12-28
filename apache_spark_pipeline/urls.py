from django.urls import path
from .views import (
    sync_batch,
    sync_micro,
    users,
    products,
    purchases,
)

urlpatterns = [
    path("sync/batch/", sync_batch),
    path("sync/micro/", sync_micro),

    path("graph/users/", users),
    path("graph/products/", products),
    path("graph/purchases/", purchases),
]
