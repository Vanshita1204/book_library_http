"""
Contain all the operations concerning the reading module. Operations such as
insert, update, delete, retrieve.
"""
import json
import psycopg2
from .transaction import retrieve_transaction
from config import get_connection


def insert_reading(*, book_id, user_id):
    """
    This function takes user id, and book id and inserts it in the reading table.
    Returns true if operation was successful and false with error if unsuccessful.
    """
    error = None
    flag = False
    conn = get_connection()
    cursor = conn.cursor()
    required_fields = [user_id, book_id]

    if not all(required_fields):
        error = str(ValueError("User_id and book_id required"))
        status = False
        return {"status": status, "error": error}

    if not all(isinstance(field, int) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    transaction = retrieve_transaction(user_id=user_id, book_id=book_id)
    if transaction["status"] is False:
        return {"status": False, "error": "Book not purchased"}

    reading = retrieve_reading(user_id=user_id, book_id=book_id)
    if reading["status"] is True:
        return reading

    insert_query = """INSERT INTO public.reading (book_id,user_id) VALUES(%s,%s)"""
    records = (book_id, user_id)
    try:
        cursor.execute(insert_query, records)
        conn.commit()
        flag = True

    except psycopg2.IntegrityError as err:
        error = str(err)
        flag = False

    except psycopg2.DatabaseError as err:
        error = str(err)
        flag = False

    finally:
        conn.close()

    return {"status": flag, "error": error}


def retrieve_reading(*, user_id, book_id=None):
    """
    This function takes user_id, book_id or both and retrieves matching columns from reading table.
    """
    conn = get_connection()
    cursor = conn.cursor()
    required_fields = [user_id]

    if book_id:
        required_fields.append(book_id)

    if not all(required_fields):
        error = str(ValueError("User_id or book_id required"))
        status = False
        return {"status": status, "error": error}

    if not all(isinstance(field, int) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    query = """SELECT * FROM public.reading WHERE """
    records = []
    if user_id:
        query += "user_id = %s "
        records.append(user_id)

    if book_id:
        if user_id:
            query += "AND "
        query += "book_id = %s "
        records.append(book_id)

    cursor.execute(query, records)
    result = cursor.fetchall()
    print(result)

    if not result:
        return {"status": False, "error": "No matching records"}

    conn.close()
    columns = ["book_id", "user_id", "id", "is_completed"]

    data = []
    for row in result:
        column_values = dict(zip(columns, row))
        data.append(column_values)

    data = json.dumps(data)

    return {"status": True, "data": data}


def retrieve_reading_by_author(*, user_id):
    """
    This function takes user_id and retrieves books published by an author.
    """
    conn = get_connection()
    cursor = conn.cursor()
    required_fields = []

    if not all(required_fields):
        error = str(ValueError("User_id or book_id required"))
        status = False
        return {"status": status, "error": error}

    if not all(isinstance(field, int) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    query = """SELECT * FROM public.reading, public.book WHERE public.reading.book_id=
    public.book.id AND public.book.author_id = %s"""
    record = user_id

    cursor.execute(query, record if isinstance(record, tuple) else (record,))
    result = cursor.fetchall()

    if not result:
        return {"status": False, "error": "No matching records"}

    print(result)

    columns = ["book_id", "user_id", "id", "is_completed"]

    data = []
    for row in result:
        column_values = dict(zip(columns, row))
        data.append(column_values)

    conn.close()

    data = json.dumps(data)
    return {"status": True, "data": data}


def book_completed(*, book_id, user_id):
    """
    This function updates a book to be completed with regards to a user.
    Returns true if operation was successful and false with error if unsuccessful.
    """
    conn = get_connection()
    cursor = conn.cursor()
    flag = True
    error = None

    required_fields = [book_id, user_id]

    if not all(required_fields):
        error = str(ValueError("User_id or book_id required"))
        status = False
        return {"status": status, "error": error}

    if not all(isinstance(field, int) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    try:
        cursor.execute(
            """UPDATE public.reading SET is_completed = True 
            WHERE book_id = %s and user_id = %s""",
            (
                book_id,
                user_id,
            ),
        )
        conn.commit()

    except psycopg2.DatabaseError as err:
        error = str(err)
        flag = False

    finally:
        conn.close()

    return {"status": flag, "error": error}
