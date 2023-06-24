"""
Contain all the operations concerning the transaction module. Operations such as
insert, update, delete, retrieve.
"""
import datetime
import json
import decimal
import psycopg2
from .book import retrieve_book_by_author
from config import get_connection


def insert_transaction(*, user_id, book_id, amount):
    """
    This function takes book_id, user_id, amount
    as input and adds it to user table.
    """
    conn = get_connection()
    cursor = conn.cursor()
    current_time = datetime.datetime.now()
    error = None
    flag = False

    required_fields = [user_id, book_id, amount]
    type_dict = {user_id: int, book_id: int, amount: float}

    if not all(required_fields):
        error = str(ValueError("User_id, book_id and amount are required"))
        status = False
        return {"status": status, "error": error}

    if not all(isinstance(field, type_dict[field]) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    query = """SELECT is_active FROM public.book WHERE id=%s"""
    cursor.execute(query, (book_id,))
    result = cursor.fetchone()
    if result[0] is False:
        return {"status": False, "error": "Book is inactivated"}

    for book in json.loads(retrieve_book_by_author(user_id)["data"]):
        if book_id == book["id"]:
            return {"status": False, "error": "Author cannot purchase their own book"}

    insert_query = """INSERT INTO public.transactions
    (book_id,user_id,amount,time) VALUES(%s,%s,%s,%s)"""
    records = (book_id, user_id, amount, current_time)

    try:
        cursor.execute(insert_query, records)
        conn.commit()
        flag = True

    except psycopg2.Error as err:
        error = str(err)
        flag = False

    return {"status": flag, "error": error}


def retrieve_transaction(*, user_id, book_id=None):
    """
    Retrieves transaction details according to user_id
    """
    conn = get_connection()
    cursor = conn.cursor()

    required_fields = [user_id]

    if not all(required_fields):
        error = str(ValueError("User_id required"))
        status = False
        return {"status": status, "error": error}

    if book_id:
        required_fields.append(book_id)

    if not all(isinstance(field, int) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    query = """SELECT * FROM public.transactions WHERE """
    record = []

    if user_id:
        query += """user_id = %s"""
        record.append(
            user_id,
        )

    if book_id:
        query += """AND book_id = %s"""
        record.append(book_id)

    columns = ["user_id", "book_id", "amount", "time", "id"]
    cursor.execute(query, record)
    result = cursor.fetchall()
    print(result)

    if not result:
        return {"status": False, "error": "No matching records"}

    data = []
    for row in result:
        column_values = dict(zip(columns, row))
        data.append(column_values)

    for column_values in data:
        for key, value in column_values.items():
            if isinstance(value, decimal.Decimal):
                column_values[key] = float(value)
            elif isinstance(value, datetime.datetime):
                column_values[key] = value.isoformat()

    data = json.dumps(data)
    return {"status": True, "data": data}


def retrieve_transaction_by_author(*, user_id, book_id=None):
    """
    Retrieves transaction details according to author
    """
    conn = get_connection()
    cursor = conn.cursor()

    required_fields = [user_id]

    if not all(required_fields):
        error = str(ValueError("User_id required"))
        status = False
        return {"status": status, "error": error}

    if book_id:
        required_fields.append(book_id)

    if not all(isinstance(field, int) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    query = """SELECT * FROM public.transactions, public.book WHERE
    public.transactions.book_id = public.book.id AND public.book.author_id = %s"""

    if book_id:
        query += """AND book_id = %s"""

    cursor.execute(query, required_fields)
    result = cursor.fetchall()
    columns = ["user_id", "book_id", "amount", "time", "id"]

    if not result:
        return {"status": False, "error": "No matching records"}

    data = []
    for row in result:
        column_values = dict(zip(columns, row))
        data.append(column_values)

    for column_values in data:
        for key, value in column_values.items():
            if isinstance(value, decimal.Decimal):
                column_values[key] = float(value)
            elif isinstance(value, datetime.datetime):
                column_values[key] = value.isoformat()

    data = json.dumps(data)
    return {"status": True, "data": data}
