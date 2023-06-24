"""
Contains all the operations concerning the book. Operations such as
insert, update, delete, retrieve.
"""
import decimal
import json

import psycopg2
from psycopg2 import extras

from config import get_connection


def insert_book(*, name, path, price, author_id=None, royalty=None):
    """
    Inserts name, author_id, price, royalty and path of book.
    Returns false if unsuccesssful
    """
    error = None
    flag = False
    conn = get_connection()
    cursor = conn.cursor()

    records = [name, path, price]

    required_fields = [name, path, price]
    if not all(required_fields):
        error = str(ValueError("Name, path, and price are required"))
        status = False
        return {"status": status, "error": error}

    if author_id:
        required_fields.append(author_id)

    if royalty:
        required_fields.append(royalty)

    type_dict = {name: str, path: str, price: float}
    if author_id:
        type_dict[author_id] = int

    if royalty:
        type_dict[royalty] = float

    if not all(isinstance(field, type_dict[field]) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    insert_query = """INSERT INTO public.book(name,file_path,price,
        author_id, royalty) VALUES(%s,%s,%s,%s,%s)"""

    if author_id:
        records.append(int(author_id))
        if royalty:
            records.append(float(royalty))
        else:
            records.append(None)

    else:
        records.append(None)

    try:
        cursor.execute(insert_query, records)
        conn.commit()
        flag = True

    except psycopg2.Error as err:
        error = str(err)
        flag = False

    finally:
        conn.close()

    return {"status": flag, "error": error}


def retrieve_book(user_id=None, book_id=None):
    """
    Retrieves book details according to book id
    """
    conn = get_connection()
    cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
    required_fields = []

    if user_id:
        required_fields = [user_id]

    if book_id:
        required_fields.append(book_id)

    if not all(isinstance(field, int) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}
    record = []

    if not user_id:
        query = """SELECT * FROM public.book"""

    elif book_id:
        query = """SELECT id,name,author_id,price FROM public.book WHERE id=%s"""
        record.append(book_id)

    elif user_id:
        query = """SELECT id,name,author_id,price,file_path FROM public.book
            WHERE id IN (
                SELECT book_id FROM public.transactions WHERE user_id = %s
            ) OR author_id = %s
            UNION 
            SELECT id, name, author_id, price, NULL AS file_path 
            FROM public.book 
            WHERE ((id NOT IN (
                SELECT book_id FROM public.transactions WHERE user_id = %s
            ) AND author_id != %s AND is_active = True) OR author_id IS NULL)
        """
        record.append(user_id)
        record.append(user_id)
        record.append(user_id)
        record.append(user_id)

    print(query)
    cursor.execute(query, record)
    result = cursor.fetchall()
    print(result)

    data = []

    for row in result:
        data.append(dict(row))

    for column_values in data:
        for key, value in column_values.items():
            if isinstance(value, decimal.Decimal):
                column_values[key] = float(value)

    data = json.dumps(data)
    return {"status": True, "data": data}


def retrieve_book_by_author(user_id, book_id=None):
    """
    Retrieves book details according to author
    """

    conn = get_connection()
    cursor = conn.cursor(cursor_factory=extras.RealDictCursor)
    required_fields = [user_id]
    if not all(required_fields):
        error = str(ValueError("user_id are required"))
        status = False
        return {"status": status, "error": error}

    if book_id:
        required_fields.append(book_id)

    if not all(isinstance(field, int) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    query = """SELECT * FROM public.book WHERE author_id = %s"""

    if book_id:
        query += """AND id=%s"""

    cursor.execute(query, required_fields)

    result = cursor.fetchall()
    if not result:
        return {"status": False, "error": "Book not found"}

    data = []

    for row in result:
        data.append(dict(row))

    for column_values in data:
        for key, value in column_values.items():
            if isinstance(value, decimal.Decimal):
                column_values[key] = float(value)

    data = json.dumps(data)
    return {"status": True, "data": data}


def retrieve_purchased(user_id):
    """
    Retrieves books that are purchased but not being read.
    """

    conn = get_connection()
    cursor = conn.cursor()

    required_fields = [user_id]

    if not all(required_fields):
        error = str(ValueError("User_id are required"))
        status = False
        return {"status": status, "error": error}

    if not all(isinstance(field, int) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    if user_id:
        query = """SELECT book_id FROM public.transactions WHERE user_id = %s
        EXCEPT SELECT book_id FROM public.reading WHERE user_id=%s"""
        record = (user_id, user_id)

    else:
        return {"status": False, "error": "Enter user id"}

    cursor.execute(query, record if isinstance(record, tuple) else (record,))
    result = cursor.fetchall()

    if not result:
        return {"status": False, "error": "No purchased books left to start reading"}

    book_ids = [row[0] for row in result]
    columns = ["id", "name", "author_id", "price", "file_path"]

    query = """SELECT id,name,author_id,price,file_path FROM public.book WHERE
    id = ANY(%s)"""
    cursor.execute(query, (book_ids,))
    result = cursor.fetchall()

    data = []
    for row in result:
        column_values = dict(zip(columns, row))
        data.append(column_values)

    for column_values in data:
        for key, value in column_values.items():
            if isinstance(value, decimal.Decimal):
                column_values[key] = float(value)

    data = json.dumps(data)
    return {"status": True, "data": data}


def update_book(*, book_id, royalty=None, price=None):
    """
    Update royalty to author by user input
    """
    conn = get_connection()
    cursor = conn.cursor()

    required_fields = [book_id]
    if not all(required_fields):
        error = str(ValueError("Book_id required"))
        status = False
        return {"status": status, "error": error}

    if price:
        required_fields.append(price)

    if royalty:
        required_fields.append(royalty)

    type_dict = {book_id: int}
    if price:
        type_dict[price] = float

    if royalty:
        type_dict[royalty] = float

    if not all(isinstance(field, type_dict[field]) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    query = """UPDATE public.book SET """
    record = []

    if not price and not royalty:
        return (False, "Enter price or royalty")

    if royalty:
        query += """ royalty = %s """
        record.append(royalty)

    if price and not royalty:
        if royalty:
            query += """AND"""

        query += """ price = %s"""
        record.append(price)

    query += """WHERE id = %s"""
    record.append(book_id)

    error = None
    flag = False
    try:
        cursor.execute(query, record)
        conn.commit()
        flag = True

    except AttributeError as err:
        error = str(err)
        flag = False

    except psycopg2.Error as err:
        error = str(err)
        flag = False

    return {"status": flag, "error": error}


def update_book_author(*, user_id, book_id, price):
    """Updates the price of the book with id book_id and author user_id"""
    conn = get_connection()
    cursor = conn.cursor()

    required_fields = [book_id, user_id, price]
    if not all(required_fields):
        error = str(ValueError("Name, path, and price are required"))
        status = False
        return {"status": status, "error": error}

    type_dict = {book_id: int, user_id: int, price: float}

    if not all(isinstance(field, type_dict[field]) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    query = """UPDATE public.book SET price = %s WHERE id= %s AND author_id = %s"""
    record = (price, book_id, user_id)

    error = None
    flag = False
    try:
        cursor.execute(query, record)
        conn.commit()
        flag = True

    except psycopg2.IntegrityError as err:
        error = str(err)
        flag = False

    except psycopg2.DatabaseError as err:
        error = str(err)
        flag = False

    return {"status": flag, "error": error}


def delete_book(book_id, user_id):
    """
    Deacivate book by book id
    """
    conn = get_connection()
    cursor = conn.cursor()
    required_fields = [book_id, user_id]

    if not all(required_fields):
        error = str(ValueError("Name, path, and price are required"))
        status = False
        return {"status": status, "error": error}

    if not all(isinstance(field, int) for field in required_fields):
        error = str(ValueError("Invalid datatypes"))
        status = False
        return {"status": status, "error": error}

    error = None
    flag = False

    try:
        cursor.execute(
            """UPDATE public.book SET is_active = False
        WHERE id = %s AND author_id=%s""",
            (
                book_id,
                user_id,
            ),
        )
        conn.commit()
        flag = True

    except psycopg2.IntegrityError as err:
        error = str(err)
        flag = False

    except psycopg2.DatabaseError as err:
        error = str(err)
        flag = False

    return {"status": flag, "error": error}
