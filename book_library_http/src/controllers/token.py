"""
Contains operations regarding token model
"""


import uuid
from datetime import datetime, timedelta

import psycopg2
from .user import authenticate
from config import get_connection


def insert_token(*, email, password):
    """
    Inserts token in user_token table with row user_id.
    """
    conn = get_connection()
    cursor = conn.cursor()

    required_fields = [email, password]

    if not all(required_fields):
        error = str(ValueError("Email, password are required"))
        status = False
        return {"status": status, "error": error}

    if not all(isinstance(field, str) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    auth = authenticate(email=email, password=password)
    print(auth)
    if auth["status"]:
        user_id = auth["id"]
    else:
        return auth

    token = str(uuid.uuid4())
    error = None
    expiration_time = datetime.now() + timedelta(seconds=3600)

    try:
        cursor.execute(
            """INSERT INTO public.user_token (user_id,token,expiration_time)
        VALUES(%s,%s,%s) ON CONFLICT (user_id) DO UPDATE SET user_id= %s, 
        token= %s, expiration_time=%s""",
            (user_id, token, expiration_time, user_id, token, expiration_time),
        )
        conn.commit()

    except psycopg2.Error as err:
        error = str(err)
        flag = False
        return {"status": flag, "error": error}

    finally:
        conn.close()

    return {"status": True, "token": token}


def validate_token(*, token):
    """
    Validates token and returns if user is admin
    """
    conn = get_connection()
    cursor = conn.cursor()

    required_fields = [token]

    if not all(required_fields):
        error = str(ValueError("Token required"))
        status = False
        return {"status": status, "error": error}

    if not all(isinstance(field, str) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    print(token)
    current_time = datetime.now()
    print(current_time)

    cursor.execute(
        """SELECT user_id, is_admin
            FROM public.user, public.user_token 
            WHERE public.user_token.user_id = public.user.id 
            AND public.user.is_active = True 
            AND public.user_token.token = %s 
            AND public.user_token.expiration_time > %s""",
        (
            token,
            current_time,
        ),
    )

    result = cursor.fetchone()

    print(result)
    if result:
        column_values = {"user_id": result[0], "is_admin": bool(result[1])}
        return {"validated": True, "user": column_values}

    return {"validated": False, "error": "Unauthorized, please login again"}
