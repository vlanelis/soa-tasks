import os
import datetime


JWT_SECRET_KEY = os.getenv('SECRET_KEY', os.urandom(32))
JWT_ALGO = 'HS256'
JWT_ACCESS_TOKEN_EXPIRES = datetime.timedelta(minutes=30)
JWT_REFRESH_TOKEN_EXPIRES = datetime.timedelta(days=7)
ORDER_CREATE_COOLDOWN = datetime.timedelta(minutes=0)
ORDER_UPDATE_COOLDOWN = datetime.timedelta(minutes=0)
