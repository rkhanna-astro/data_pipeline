from django.db import models

class User(models.Model):
    name = models.CharField(max_length=255, null=False)
    email = models.EmailField()

class Product(models.Model):
    name = models.CharField(max_length=255, null=False)
    price = models.FloatField()
    description = models.TextField()

class Purchase(models.Model):
    user_id = models.OneToOneField(User, on_delete=models.CASCADE)
    product_id = models.OneToOneField(Product, on_delete=models.CASCADE)
    amount = models.FloatField()
    order_date = models.DateTimeField()
