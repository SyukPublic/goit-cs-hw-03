# -*- coding: utf-8 -*-

"""
HomeWork Task 2
"""

from __future__ import annotations

import enum
import argparse
from typing import Optional, Any
from bson.objectid import ObjectId

from pymongo import MongoClient
from pymongo.synchronous import database
from pymongo.server_api import ServerApi
from pymongo.errors import ConnectionFailure, OperationFailure


class MongoDB:

    def __init__(self, uri: str, dbname: str):
        self.uri = uri
        self.dbname = dbname

        self.client: Optional[MongoClient.Database] = None
        self.db: Optional[database.Database] = None

    def _is_opened(self, raise_exception: bool = False) -> bool:
        """
        Checks whether the connection to MongoDB and the database is open.

        :param raise_exception: Raise an Exception if the database connection is not open.
        :return: Connection opened flag.
        """
        if self.client is None or self.db is None:
            if raise_exception:
                raise RuntimeError("Database is not connected")
            return False
        return True

    def _open(self) -> tuple[Optional[MongoClient.Database], Optional[database.Database]]:
        """
        Connect to a MongoDB host and get a database by name.

        :return: MongoDB client and database instances
        """
        try:
            client = MongoClient(self.uri, server_api=ServerApi("1"))
            db = client.get_database(self.dbname)
            return client, db
        except ConnectionFailure:
            raise RuntimeError("Server not available")
        except OperationFailure as e:
            raise RuntimeError(f"Connection failed: {str(e)}")

    def _close(self) -> None:
        """
        Close connection to a MongoDB host
        """
        if self.client is not None:
            self.client.close()
            self.db = None
            self.client = None

    def __enter__(self) ->MongoDB:
        """
        Use self instance as ContextManager.

        :return: Self instance
        """
        if not self._is_opened():
            self.client, self.db = self._open()
        return self

    def __exit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        """
        Exit form Context
        """
        self._close()

    def __del__(self) -> None:
        """
        Checks whether the connection to MongoDB and the database is closed. Close connection if opened/
        """
        self._close()

    def open(self) -> None:
        """
        Connect to a MongoDB host and get a database by name.
        """
        if self.client is None or self.db is None:
            self.client, self.db = self._open()

    def close(self) -> None:
        """
        Close connection to a MongoDB host
        """
        self._close()

    def find(self, cat_id: str) -> Optional[dict[str, Any]]:
        """
        Read a single record from the collection by _id (as a dictionary) or None.

        :param cat_id: Cat id (_id field value).
        :return: Record dictionary
        """
        self._is_opened(raise_exception=True)
        try:
            return self.db.cats.find_one({"_id": ObjectId(cat_id)})
        except OperationFailure as e:
            raise RuntimeError(f"Operation failed: {str(e)}")

    def create_one(self, name: str, age: Optional[int], features: Optional[list[str]]) -> Optional[dict[str, Any]]:
        """
        Add a record to the collection.

        :param name: Name field value.
        :param age: Age field value.
        :param features: Features field value.
        :return: Record dictionary
        """
        self._is_opened(raise_exception=True)
        try:
            result = self.db.cats.insert_one({"name": name, "age": age, "features": features or []})
            return self.find(result.inserted_id)
        except OperationFailure as e:
            raise RuntimeError(f"Operation failed: {str(e)}")


    def read_one(self, name: str) -> Optional[dict[str, Any]]:
        """
        Read a single record from the collection by name (as a dictionary) or None.

        :param name: Name field value.
        :return: Record dictionary
        """
        self._is_opened(raise_exception=True)
        try:
            return self.db.cats.find_one({"name": name})
        except OperationFailure as e:
            raise RuntimeError(f"Operation failed: {str(e)}")

    def read_all(self) -> list[dict[str, Any]]:
        """
        Read all records from the collection (as a list of dictionary).

        :return: List of records dictionary
        """
        self._is_opened(raise_exception=True)
        try:
            return [cat for cat in self.db.cats.find({})]
        except OperationFailure as e:
            raise RuntimeError(f"Operation failed: {str(e)}")

    def update_one(self, name: str, age: Optional[int], features: Optional[list[str]]) -> Optional[dict[str, Any]]:
        """
        Update a single record in the collection by name.

        :param name: Name field value.
        :param age: Age field value.
        :param features: Features field value.
        :return: Record dictionary
        """
        self._is_opened(raise_exception=True)
        try:
            if age is not None:
                self.db.cats.update_one({"name": name}, {"$set": {"age": age}})
            if features is not None:
                self.db.cats.update_one({"name": name}, {"$addToSet": {"features": {"$each": features}}})
            return self.read_one(name)
        except OperationFailure as e:
            raise RuntimeError(f"Operation failed: {str(e)}")

    def delete_one(self, name: str) -> int:
        """
        Delete a single record from the collection by name.

        :param name: Name field value.
        :return: Number of deleted records
        """
        self._is_opened(raise_exception=True)
        try:
            result = self.db.cats.delete_one({"name": name})
            return result.deleted_count
        except OperationFailure as e:
            raise RuntimeError(f"Operation failed: {str(e)}")

    def delete_all(self) -> int:
        """
        Delete all records from the collection.

        :return: Number of deleted records
        """
        self._is_opened(raise_exception=True)
        try:
            result = self.db.cats.delete_many({})
            return result.deleted_count
        except OperationFailure as e:
            raise RuntimeError(f"Operation failed: {str(e)}")


class Actions(enum.Enum):
    create = "create"
    read = "read"
    update = "update"
    delete = "delete"

    @classmethod
    def values(cls) -> set:
        return set(map(lambda c: c.value, cls))

def cli(uri: str, dbname: str) -> None:
    try:
        parser = argparse.ArgumentParser(
            description="Cats database (MongoDB CRUD implementation)",
            epilog="Good bye!",
        )
        # parser.add_argument("action", type=str, help=f"CRUD Action [{", ".join(Actions.values())}]")
        parser.add_argument("action", type=str, choices=Actions.values(), help="CRUD Action")
        parser.add_argument("-n", "--name", type=str, help="Name of the cat")
        parser.add_argument("-a", "--age", type=int, help="Age of the cat")
        parser.add_argument("-f", "--features", type=str, help="Features of the cat", nargs="+")

        args = parser.parse_args()
        # age = args.age
        # features = args.features

        with MongoDB(uri, dbname) as db:
            match Actions(args.action):
                case Actions.create:
                    if args.name is not None:
                        print(f"Insert data about the cat \"{args.name}\" to the collection")
                        cat = db.create_one(args.name, args.age, args.features)
                        if cat:
                            print(cat)
                        else:
                            print(f"The cat \"{args.name}\" has not been added to the collection")
                    else:
                        print("Cannot add cat data without its name")
                case Actions.read:
                    if args.name is not None:
                        print(f"Read data about the cat \"{args.name}\" from the collection:")
                        cat = db.read_one(args.name)
                        if cat:
                            print(cat)
                        else:
                            print(f"The cat \"{args.name}\" not found in the collection")
                    else:
                        print("Read data about all cats in the collection:")
                        cats = db.read_all()
                        if cats:
                            for cat in cats:
                                print(cat)
                        else:
                            print("The collection is empty")
                case Actions.update:
                    if args.name is not None:
                        print(f"Update data about the cat \"{args.name}\" in the collection:")
                        cat = db.update_one(args.name, args.age, args.features)
                        if cat:
                            print(cat)
                        else:
                            print(f"The cat \"{args.name}\" not found in the collection")
                    else:
                        print("Cannot update cat data without its name")
                case Actions.delete:
                    if args.name is not None:
                        print(f"Delete data about the cat \"{args.name}\" from the collection:")
                        deleted_records_number = db.delete_one(args.name)
                        print(f"Deleted {deleted_records_number} records")
                    else:
                        print("Delete data about all cats in the collection:")
                        deleted_records_number = db.delete_all()
                        print(f"Deleted {deleted_records_number} records")
                case _:
                    print("Wrong action")

    except Exception as e:
        print(e)

    exit(0)
