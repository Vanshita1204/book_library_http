"""
Contain all the operations concerning the user. Operations such as
insert, update, delete, retrieve.
"""

import json
import bcrypt
import psycopg2
from config import get_connection


def insert_user(*, name, email, password, account_num=None, upi_id=None):
    """
    This function takes name, email, password, account number, upi id
    as input and adds it to user table. Returns true if operation was successful
    and false with error if unsuccessful.
    """
    conn = get_connection()
    cursor = conn.cursor()
    required_fields = [name, email, password]

    if not all(required_fields):
        error = str(ValueError("Name, email, and password are required"))
        status = False
        return {"status": status, "error": error}

    if account_num:
        required_fields.append(account_num)

    if upi_id:
        required_fields.append(upi_id)

    if not all(isinstance(field, str) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    password = password.encode("utf-8")
    salt = bcrypt.gensalt()
    hashed_password = bcrypt.hashpw(password, salt)
    insert_query = """INSERT INTO public.user (name,email,password,
    bank_account,upi_id) VALUES(%s,%s,%s,%s,%s)"""
    records = (name, email, hashed_password, account_num, upi_id)
    error = None
    flag = False

    try:
        cursor.execute(insert_query, records)
        # commits query in the database
        conn.commit()
        flag = True

    except psycopg2.Error as err:
        error = str(err)
        flag = False

    finally:
        conn.close()

    return {"status": flag, "error": error}


def retrieve_user(*, user_id=None):
    """
    This function takes user_id .
    """

    if user_id:
        if not isinstance(user_id, int):
            error = str(ValueError("Fields with invalid data: user_id"))
            return {"status": False, "error": error}

    record = []
    conn = get_connection()
    cursor = conn.cursor()

    columns = ["id", "name", "email", "bank_account", "upi_id"]
    query = """SELECT id,name, email,bank_account, upi_id FROM
    public.user WHERE is_active=True"""

    if user_id is None:
        cursor.execute(query)

    else:
        query += """ AND id = %s"""
        record.append(user_id)
        cursor.execute(query, (user_id,))

    result = cursor.fetchall()

    if not result:
        print("User with id =", id, "does not exist")
        return {"status": False, "error": "No user with matching id found"}

    print(result)
    conn.close()
    data = []
    for row in result:
        column_values = dict(zip(columns, row))
        data.append(column_values)

    data = json.dumps(data)
    return {"status": True, "data": data}


def authenticate(*, email, password):
    """
    Authenticates user email by checking password entered
    """
    conn = get_connection()
    cursor = conn.cursor()
    record = []
    query = """ SELECT password,id FROM public.user WHERE is_active=True"""

    required_fields = ["email", "password"]
    if not all(required_fields):
        error = str(ValueError("Email, and password are required"))
        status = False
        return {"status": status, "error": error}

    if not all(isinstance(field, str) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    query += """ AND email = %s"""
    record.append(email)
    cursor.execute(query, record)
    result = cursor.fetchone()
    print(result)
    if not result:
        return {"status": False, "error": "Invalid email"}

    user_id = result[1]

    # retrieves the stored password
    stored_hash = result[0]
    stored_hash = bytes(stored_hash)
    password_bytes = password.encode("utf-8")

    if bcrypt.checkpw(password_bytes, stored_hash):
        return {"status": True, "id": user_id}

    return {"status": False, "error": "Invalid password"}


def update_password(*, email, curr_password, new_password):
    """
    Autheticates user email with current password, and updates password with new password
    """
    conn = get_connection()
    cursor = conn.cursor()

    required_fields = ["email", "curr_password", "new_password"]

    if not all(required_fields):
        error = str(ValueError("Email, and password are required"))
        status = False
        return {"status": status, "error": error}

    if not all(isinstance(field, str) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    error = None
    flag = True
    if authenticate(email=email, password=curr_password):
        password = new_password.encode("utf-8")
        salt = bcrypt.gensalt()
        hashed_password = bcrypt.hashpw(password, salt)

        try:
            cursor.execute(
                """UPDATE public.user SET
            password = %s WHERE email = %s AND is_active=True""",
                (
                    hashed_password,
                    email,
                ),
            )
            conn.commit()

        except psycopg2.Error as err:
            error = str(err)
            flag = False

        return {"status": flag, "error": error}

    return {"status": False, "error": "Incorrect email or password"}


def delete_user(*, user_id):
    """
    Authenticates user email and inactivates if authenticated
    """
    conn = get_connection()
    cursor = conn.cursor()

    error = None
    flag = True

    if not user_id:
        error = str(ValueError("Required fields missing: user_id"))
        return {"status": False, "error": error}

    if not isinstance(user_id, int):
        error = str(ValueError("Fields with invalid data: user_id"))
        return {"status": False, "error": error}

    try:
        cursor.execute(
            """UPDATE public.user SET
        is_active = False  WHERE id=%s""",
            (user_id,),
        )
        conn.commit()

    except psycopg2.Error as err:
        error = str(err)
        flag = False

    return {"status": flag, "error": error}
