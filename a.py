from pymongo import MongoClient

def main():
    # ------------------------------------------------------------------------------
    # 1. Connect to MongoDB and create/use a database and collection
    # ------------------------------------------------------------------------------
    client = MongoClient('mongodb://localhost:27017/')
    db = client.library  # Using a database named "library"
    books = db.books     # The "books" collection (created when we insert the first document)
    
    # Drop the collection if it already exists for a clean start.
    books.drop()
    print("Collection 'books' dropped (if it existed).")
    print("-" * 60)

    # ------------------------------------------------------------------------------
    # 2. Insert 5 records with fields: TITLE, DESCRIPTION, BY, URL, TAGS, LIKES
    # ------------------------------------------------------------------------------
    records = [
        {"TITLE": "mongodb", "DESCRIPTION": "Intro to MongoDB", "BY": "john",
         "URL": "http://example.com/mongodb", "TAGS": ["database", "nosql"], "LIKES": 15},
        {"TITLE": "nosql overview", "DESCRIPTION": "Overview of NoSQL databases", "BY": "mary",
         "URL": "http://example.com/nosql", "TAGS": ["nosql", "database"], "LIKES": 8},
        {"TITLE": "python basics", "DESCRIPTION": "Getting started with Python", "BY": "john",
         "URL": "http://example.com/python", "TAGS": ["programming", "python"], "LIKES": 20},
        {"TITLE": "data science", "DESCRIPTION": "Data Science using Python", "BY": "alice",
         "URL": "http://example.com/datascience", "TAGS": ["data", "science"], "LIKES": 12},
        {"TITLE": "machine learning", "DESCRIPTION": "ML fundamentals", "BY": "john",
         "URL": "http://example.com/ml", "TAGS": ["machine learning", "ai"], "LIKES": 25}
    ]
    books.insert_many(records)
    print("Inserted 5 records.")
    print("-" * 60)

    # ------------------------------------------------------------------------------
    # 3. Insert 1 more document with additional fields: username and comments
    # ------------------------------------------------------------------------------
    additional_doc = {
        "TITLE": "mongodb advanced",
        "DESCRIPTION": "Advanced topics in MongoDB",
        "BY": "john",
        "URL": "http://example.com/mongodb_advanced",
        "TAGS": ["mongodb", "advanced"],
        "LIKES": 30,
        "username": "john_doe",
        "comments": ["Great book!", "Very informative."]
    }
    books.insert_one(additional_doc)
    print("Inserted additional document with username and comments.")
    print("-" * 60)

    # ------------------------------------------------------------------------------
    # 4. Display all documents whose TITLE is 'mongodb'
    # ------------------------------------------------------------------------------
    print("Documents with TITLE 'mongodb':")
    results = books.find({"TITLE": "mongodb"})
    for doc in results:
        print(doc)
    print("-" * 60)

    # ------------------------------------------------------------------------------
    # 5. Display all documents written by 'john' OR whose TITLE is 'mongodb'
    # ------------------------------------------------------------------------------
    print("Documents where BY is 'john' OR TITLE is 'mongodb':")
    results = books.find({
        "$or": [
            {"BY": "john"},
            {"TITLE": "mongodb"}
        ]
    })
    for doc in results:
        print(doc)
    print("-" * 60)

    # ------------------------------------------------------------------------------
    # 6. Display all documents whose TITLE is 'mongodb' AND written by 'john'
    # ------------------------------------------------------------------------------
    print("Documents with TITLE 'mongodb' AND BY 'john':")
    results = books.find({"TITLE": "mongodb", "BY": "john"})
    for doc in results:
        print(doc)
    print("-" * 60)

    # ------------------------------------------------------------------------------
    # 7. Display all documents whose LIKES is greater than 10
    # ------------------------------------------------------------------------------
    print("Documents with LIKES > 10:")
    results = books.find({"LIKES": {"$gt": 10}})
    for doc in results:
        print(doc)
    print("-" * 60)

    # ------------------------------------------------------------------------------
    # 8. Display all documents whose LIKES > 100 AND (TITLE is 'mongodb' OR BY is 'john')
    # Note: With current data, none of the documents have LIKES > 100.
    # ------------------------------------------------------------------------------
    print("Documents with LIKES > 100 AND (TITLE 'mongodb' OR BY 'john'):")
    results = books.find({
        "LIKES": {"$gt": 100},
        "$or": [
            {"TITLE": "mongodb"},
            {"BY": "john"}
        ]
    })
    for doc in results:
        print(doc)
    print("-" * 60)

    # ------------------------------------------------------------------------------
    # 9. Update the TITLE of documents with TITLE 'mongodb' to 'mongodb overview'
    # ------------------------------------------------------------------------------
    update_result = books.update_many({"TITLE": "mongodb"}, {"$set": {"TITLE": "mongodb overview"}})
    print(f"Updated {update_result.modified_count} document(s) from TITLE 'mongodb' to 'mongodb overview'.")
    print("-" * 60)

    # ------------------------------------------------------------------------------
    # 10. Delete the document titled 'nosql overview'
    # ------------------------------------------------------------------------------
    delete_result = books.delete_one({"TITLE": "nosql overview"})
    print("Deleted document with TITLE 'nosql overview'.")
    print("-" * 60)

    # ------------------------------------------------------------------------------
    # 11. Display exactly two documents written by 'john'
    # ------------------------------------------------------------------------------
    print("Displaying exactly 2 documents written by 'john':")
    results = books.find({"BY": "john"}).limit(2)
    for doc in results:
        print(doc)
    print("-" * 60)

    # ------------------------------------------------------------------------------
    # 12. Display the second document published by 'john'
    # ------------------------------------------------------------------------------
    print("Displaying the second document published by 'john':")
    results = books.find({"BY": "john"}).skip(1).limit(1)
    for doc in results:
        print(doc)
    print("-" * 60)

    # ------------------------------------------------------------------------------
    # 13. Display all books in sorted order (by TITLE in ascending order)
    # ------------------------------------------------------------------------------
    print("Books sorted by TITLE (ascending):")
    results = books.find().sort("TITLE", 1)
    for doc in results:
        print(doc)
    print("-" * 60)

    # ------------------------------------------------------------------------------
    # Insert a document using the save() method
    # Note: The save() method is deprecated in newer versions of PyMongo.
    # ------------------------------------------------------------------------------
    print("Inserting a document using the save() method (if supported):")
    doc = {
        "TITLE": "new book",
        "DESCRIPTION": "A brand new book",
        "BY": "bob",
        "URL": "http://example.com/newbook",
        "TAGS": ["new"],
        "LIKES": 5
    }
    try:
        # The save() method will insert if no _id is present
        books.save(doc)
        print("Document inserted using save().")
    except Exception as e:
        print("Error using save():", e)
    print("-" * 60)

    # ------------------------------------------------------------------------------
    # MongoDB Aggregation and Indexing
    # ------------------------------------------------------------------------------

    # Aggregation 1: Find the number of books published by 'john'
    print("Aggregation 1: Number of books published by 'john':")
    pipeline = [
        {"$match": {"BY": "john"}},
        {"$count": "numberOfBooks"}
    ]
    result = list(books.aggregate(pipeline))
    print(result)
    print("-" * 60)

    # Aggregation 2: Find the minimum and maximum LIKES for books published by 'john'
    print("Aggregation 2: Minimum and maximum LIKES for books published by 'john':")
    pipeline = [
        {"$match": {"BY": "john"}},
        {"$group": {
            "_id": None,
            "minLikes": {"$min": "$LIKES"},
            "maxLikes": {"$max": "$LIKES"}
        }}
    ]
    result = list(books.aggregate(pipeline))
    print(result)
    print("-" * 60)

    # Aggregation 3: Find the average number of LIKES of the books published by 'john'
    print("Aggregation 3: Average LIKES for books published by 'john':")
    pipeline = [
        {"$match": {"BY": "john"}},
        {"$group": {
            "_id": None,
            "averageLikes": {"$avg": "$LIKES"}
        }}
    ]
    result = list(books.aggregate(pipeline))
    print(result)
    print("-" * 60)

    # Aggregation 4: Find the first and last book published by 'john' based on _id
    print("Aggregation 4: First and last book published by 'john':")
    first_book = books.find({"BY": "john"}).sort("_id", 1).limit(1)
    print("First book by john:")
    for doc in first_book:
        print(doc)
    last_book = books.find({"BY": "john"}).sort("_id", -1).limit(1)
    print("Last book by john:")
    for doc in last_book:
        print(doc)
    print("-" * 60)

    # Aggregation 5: Create an index on the "BY" field and display the query explain plan
    print("Aggregation 5: Creating an index on 'BY' field and checking query explain:")
    books.create_index("BY")
    query_plan = books.find({"BY": "john"}).explain()
    print("Query Explain Plan:")
    print(query_plan)
    print("Documents with BY 'john':")
    results = books.find({"BY": "john"})
    for doc in results:
        print(doc)
    print("-" * 60)

if __name__ == '__main__':
    main()
