#!/bin/bash

# Run Django server with DB init
# Not recommended in the buildup
# python manage.py makemigrations

# Migrate
python manage.py migrate

# Runs the server
python manage.py runserver 0.0.0.0:8000