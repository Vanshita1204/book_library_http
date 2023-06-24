"""
Api handling and server management
"""

from urllib.parse import urlparse, parse_qs
import json
import sys
import os


from http.server import HTTPServer, BaseHTTPRequestHandler

sys.path.append(os.path.abspath("/src"))
from controllers.token import insert_token, validate_token
from controllers.book import (
    retrieve_book,
    insert_book,
    update_book,
    delete_book,
    retrieve_book_by_author,
    retrieve_purchased,
    update_book_author,
)
from controllers.user import (
    retrieve_user,
    authenticate,
    insert_user,
    update_password,
    delete_user,
)
from controllers.reading import (
    retrieve_reading,
    insert_reading,
    retrieve_reading_by_author,
    book_completed,
)
from controllers.transaction import (
    retrieve_transaction,
    insert_transaction,
    retrieve_transaction_by_author,
)

BASE_DIR = os.getcwd()


HOST = "127.0.0.1"
PORT = 8000


class User:
    """Defines User as an object"""

    def __init__(self, user_id, user_type):
        self.user_id = user_id
        self.user_type = user_type


class APIHandle(BaseHTTPRequestHandler):
    """
    Handles all API requests in the server
    """

    def auth(self):
        token = self.headers.get("Authorization")

        if token.startswith("Bearer"):
            token = token.strip("Bearer ")
            validate = validate_token(token=token)
            if validate["validated"]:
                validated_user = validate["user"]
                self.user = User(
                    validated_user["user_id"],
                    "admin" if validated_user["is_admin"] else "user",
                )

            else:
                self.user = None

        else:
            self.user = None

    def send_error(self, code, message=None):
        """
        Sends error code and message in REST format
        """
        self.send_response(code)
        self.send_header("Content-type", "application/json")
        self.end_headers()

        if not message:
            message = self.responses.get(code)[0]
        error_response = {"error": {"code": code, "message": message}}
        self.wfile.write(json.dumps(error_response).encode("utf-8"))

    def do_GET(self):
        """
        Handles all GET requests in the server
        """
        # breakpoint()
        self.auth()

        # print(self.headers.get("User_Type"))

        response_data = {"status": True, "error": None}
        path = urlparse(self.path)
        print(path.query)

        user = self.user
        if not user:
            self.send_error(401)
            self.end_headers()

        query_params = parse_qs(path.query)
        if query_params:
            book_id = int(query_params.get("book_id", None)[0])

        get_routes_admin = {"/user": retrieve_user, "/book": retrieve_book}

        get_routes_user = {
            "/user": [retrieve_user, False],
            "/book": [retrieve_book, True],
            "/published": [retrieve_book_by_author, True],
            "/purchased": [retrieve_purchased, True],
            "/transaction/user": [retrieve_transaction, False],
            "/transaction/author": [retrieve_transaction_by_author, False],
            "/reading/user": [retrieve_reading, False],
            "/reading/author": [retrieve_reading_by_author, False],
        }
        if user.user_type == "admin":
            function = get_routes_admin.get(path.path)
            if function:
                response_data = function()
                print(response_data)

            else:
                self.send_error(404)
                self.end_headers()

        else:
            function = get_routes_user.get(path.path)

            if function[0]:
                if function[1] and query_params:
                    response_data = function[0](
                        user_id=int(user.user_id), book_id=book_id
                    )

                else:
                    response_data = function[0](user_id=int(user.user_id))
            else:
                self.send_error(404)
                self.end_headers()

        if response_data["status"] is False:
            self.send_error(404, response_data["error"])

        else:
            response = response_data["data"]
            response = json.loads(response)
            self.send_response(200)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())

    def do_POST(self):
        """
        Handles all the post requests in the server
        """

        path = urlparse(self.path)
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length).decode("utf-8")
        if post_data:
            post_data = json.loads(post_data)

        response_data = {"status": False, "error": None}

        if len(post_data) > 1:
            post_data = {**post_data}

        if path.path == "/signup":
            response_data = insert_user(**post_data)
            if response_data["status"]:
                self.send_response(204)
                self.end_headers()
            else:
                self.send_error(400, response_data["error"])
                self.end_headers()
                return

        elif path.path == "/login":
            auth = insert_token(**post_data)
            if auth["status"] is False:
                self.send_error(401, auth["error"])
                return
            else:
                print(auth["token"])
                self.send_response(200)
                self.send_header("Content-type", "application/json")
                self.end_headers()
                self.wfile.write(json.dumps(auth).encode())
                return

        self.auth()
        user = self.user

        if not user:
            self.send_error(401)
            self.end_headers()

        if path.path == "/publish":
            print(post_data)
            if user.user_type == "admin":
                response_data = insert_book(**post_data)

            else:
                response_data = insert_book(**post_data, author_id=user.user_id)

        elif path.path == "/purchase":
            response_data = insert_transaction(user_id=user.user_id, **post_data)

        elif path.path == "/reading":
            book_id = post_data["book_id"]
            response_data = insert_reading(book_id=book_id, user_id=user.user_id)

        else:
            self.send_error(404, "path not found")
            self.end_headers()

        if response_data["status"] is False:
            self.send_error(400, response_data["error"])
            self.end_headers()

        elif "data" in response_data.keys():
            self.send_response(200)
            response = response_data["data"]
            response = json.loads(response)
            self.send_header("Content-type", "application/json")
            self.end_headers()
            print(response_data)
            self.wfile.write(json.dumps(response).encode("utf-8"))

        else:
            self.send_response(204)

            self.end_headers()
            print(response_data)

    def do_PUT(self):
        """
        Handles all the PUT requests in the server.
        """
        self.auth()
        user = self.user

        if not user:
            self.send_error(401)
            self.end_headers()

        path = urlparse(self.path)
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length).decode("utf-8")
        post_data = json.loads(post_data)

        response_data = {"status": False, "error": None}
        user = self.user

        put_requests = {
            "/user": update_password,
            "/book": update_book_author,
            "/completed": book_completed,
        }

        if user.user_type == "user":
            if path.path == "/user":
                function = put_requests.get(path.path)
                response_data = function(**post_data)

            else:
                function = put_requests.get(path.path)

                if function:
                    if path.path == "/user":
                        response_data = function(user_id=user.user_id, **post_data)

                    elif path.path == "/completed":
                        response_data = function(
                            user_id=user.user_id, book_id=post_data["book_id"]
                        )

                else:
                    self.send_error(401)

        else:
            if path.path == "/book":
                response_data = update_book(**post_data)

            else:
                self.send_error(401)

        print(response_data.items())

        if response_data["status"] is False:
            self.send_error(400, response_data["error"])

        else:
            self.send_response(204)
            self.end_headers()
            print(response_data)

    def do_DELETE(self):
        """
        Handles all the DELETE requests in the server.
        """

        path = urlparse(self.path)
        content_length = int(self.headers["Content-Length"])
        post_data = self.rfile.read(content_length).decode("utf-8")
        post_data = json.loads(post_data)

        self.auth()
        user = self.user

        if not user:
            self.send_error(401)
            self.end_headers()

        if user.user_type == "user":
            if path.path == "/book":
                print(post_data)

                response_data = delete_book(
                    user_id=user.user_id, book_id=post_data["book_id"]
                )

            else:
                self.send_error(401)

        else:
            if path.path == "/user":
                response_data = delete_user(user_id=post_data["user_id"])

            else:
                self.send_error(401)

        if response_data["status"] is False:
            self.send_error(400, response_data["error"])

        else:
            self.send_response(204)
            self.end_headers()
            print(response_data)


server = HTTPServer((HOST, PORT), APIHandle)
print("serve now")
server.serve_forever()
server.server_close()
print("server close")
