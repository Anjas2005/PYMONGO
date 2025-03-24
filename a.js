// =========================
// Section 1: Working with the "books" Collection in the "library" Database
// =========================

// Switch to the "library" database (it will be created if it doesn't exist)
use library;

// 1. Create a collection named "books"
// (The collection is auto-created when you insert the first document)
db.createCollection("books");

// 2. Insert 5 records with fields: TITLE, DESCRIPTION, BY, URL, TAGS, and LIKES.
db.books.insertMany([
  { "TITLE": "mongodb", "DESCRIPTION": "Intro to MongoDB", "BY": "john", "URL": "http://example.com/mongodb", "TAGS": ["database", "nosql"], "LIKES": 15 },
  { "TITLE": "nosql overview", "DESCRIPTION": "Overview of NoSQL databases", "BY": "mary", "URL": "http://example.com/nosql", "TAGS": ["nosql", "database"], "LIKES": 8 },
  { "TITLE": "python basics", "DESCRIPTION": "Getting started with Python", "BY": "john", "URL": "http://example.com/python", "TAGS": ["programming", "python"], "LIKES": 20 },
  { "TITLE": "data science", "DESCRIPTION": "Data Science using Python", "BY": "alice", "URL": "http://example.com/datascience", "TAGS": ["data", "science"], "LIKES": 12 },
  { "TITLE": "machine learning", "DESCRIPTION": "ML fundamentals", "BY": "john", "URL": "http://example.com/ml", "TAGS": ["machine learning", "ai"], "LIKES": 25 }
]);

// 3. Insert 1 more document in the collection with additional fields of username and comments.
db.books.insertOne({
  "TITLE": "mongodb advanced",
  "DESCRIPTION": "Advanced topics in MongoDB",
  "BY": "john",
  "URL": "http://example.com/mongodb_advanced",
  "TAGS": ["mongodb", "advanced"],
  "LIKES": 30,
  "username": "john_doe",
  "comments": ["Great book!", "Very informative."]
});

// 4. Display all the documents whose title is 'mongodb'.
print("Documents with TITLE 'mongodb':");
db.books.find({ "TITLE": "mongodb" }).forEach(printjson);

// 5. Display all the documents written by 'john' or whose title is 'mongodb'.
print("\nDocuments written by 'john' or with TITLE 'mongodb':");
db.books.find({ $or: [ { "BY": "john" }, { "TITLE": "mongodb" } ] }).forEach(printjson);

// 6. Display all the documents whose title is 'mongodb' and written by 'john'.
print("\nDocuments with TITLE 'mongodb' and written by 'john':");
db.books.find({ "TITLE": "mongodb", "BY": "john" }).forEach(printjson);

// 7. Display all the documents whose LIKES is greater than 10.
print("\nDocuments with LIKES greater than 10:");
db.books.find({ "LIKES": { $gt: 10 } }).forEach(printjson);

// 8. Display all the documents whose LIKES is greater than 100 and whose title is either 'mongodb' or written by 'john'.
// (Note: With the current data, none have LIKES > 100)
print("\nDocuments with LIKES > 100 and (TITLE 'mongodb' or BY 'john'):");
db.books.find({ "LIKES": { $gt: 100 }, $or: [ { "TITLE": "mongodb" }, { "BY": "john" } ] }).forEach(printjson);

// 9. Update the title of the 'mongodb' document to 'mongodb overview'.
db.books.updateMany({ "TITLE": "mongodb" }, { $set: { "TITLE": "mongodb overview" } });
print("\nAfter updating TITLE 'mongodb' to 'mongodb overview':");
db.books.find({ "TITLE": "mongodb overview" }).forEach(printjson);

// 10. Delete the document titled 'nosql overview'.
db.books.deleteOne({ "TITLE": "nosql overview" });
print("\nAfter deleting document with TITLE 'nosql overview':");
db.books.find().forEach(printjson);

// 11. Display exactly two documents written by 'john'.
print("\nExactly two documents written by 'john':");
db.books.find({ "BY": "john" }).limit(2).forEach(printjson);

// 12. Display the second document published by 'john'.
print("\nThe second document published by 'john':");
db.books.find({ "BY": "john" }).skip(1).limit(1).forEach(printjson);

// 13. Display all the books in sorted order (by TITLE in ascending order).
print("\nAll books sorted by TITLE (ascending):");
db.books.find().sort({ "TITLE": 1 }).forEach(printjson);

// Insert a document using the save() method.
// Note: The save() method is deprecated in newer versions but still works for demonstration.
var newDoc = {
  "TITLE": "new book",
  "DESCRIPTION": "A brand new book",
  "BY": "bob",
  "URL": "http://example.com/newbook",
  "TAGS": ["new"],
  "LIKES": 5
};
db.books.save(newDoc);
print("\nAfter inserting a new document using save():");
db.books.find({ "TITLE": "new book" }).forEach(printjson);

// -------------------------
// MongoDB Aggregation and Indexing
// -------------------------

// 1. Find the number of books published by 'john'.
print("\nNumber of books published by 'john':");
db.books.aggregate([
  { $match: { "BY": "john" } },
  { $count: "numberOfBooks" }
]).forEach(printjson);

// 2. Find the books with the minimum and maximum likes published by 'john'.
print("\nMinimum and Maximum LIKES for books published by 'john':");
db.books.aggregate([
  { $match: { "BY": "john" } },
  { $group: { _id: null, minLikes: { $min: "$LIKES" }, maxLikes: { $max: "$LIKES" } } }
]).forEach(printjson);

// 3. Find the average number of likes of the books published by 'john'.
print("\nAverage LIKES for books published by 'john':");
db.books.aggregate([
  { $match: { "BY": "john" } },
  { $group: { _id: null, averageLikes: { $avg: "$LIKES" } } }
]).forEach(printjson);

// 4. Find the first and last book published by 'john'.
// Using the _id field (which is time-based) for ordering.
print("\nFirst book by 'john':");
db.books.find({ "BY": "john" }).sort({ _id: 1 }).limit(1).forEach(printjson);
print("\nLast book by 'john':");
db.books.find({ "BY": "john" }).sort({ _id: -1 }).limit(1).forEach(printjson);

// 5. Create an index on the author name (BY field) and check index usage while displaying books published by 'john'.
db.books.createIndex({ "BY": 1 });
print("\nQuery plan for finding books by 'john' (showing index usage):");
var queryPlan = db.books.find({ "BY": "john" }).explain("executionStats");
printjson(queryPlan);
print("\nBooks published by 'john':");
db.books.find({ "BY": "john" }).forEach(printjson);


// =========================
// Section 2: Working with a "class1" Collection in the "wisdom-acadamy" Database
// =========================

// Switch to the "wisdom-acadamy" database (it will be created if it doesn't exist)
use wisdom-acadamy;

// Create the collection "class1" (auto-created on first insert)
db.createCollection("class1");

// Insert a document (student record) into the "class1" collection.
print("\nInsert a student document into class1:");
var studentInfo = {
  "Name": "Drake",
  "section": "1",
  "maths_marks": 50,
  "sst_marks": 59
};
var studentResult = db.class1.insertOne(studentInfo);
print("Student with ID " + studentResult.insertedId + " has been created.");

// Read one document from "class1" where section is "1".
print("\nRead one document from class1 where section is '1':");
var oneStudent = db.class1.findOne({ "section": "1" });
printjson(oneStudent);

// Read all documents from "class1" where section is "1".
print("\nRead all documents from class1 where section is '1':");
db.class1.find({ "section": "1" }).forEach(printjson);

// Update documents: Increment the section field by 100 where section equals "1".
// (Note: The section field in our inserted document is a string; adjust your query if needed.)
print("\nUpdate documents in class1 where section equals '1':");
db.class1.updateMany({ "section": "1" }, { $inc: { "section": 100 } });
db.class1.find().forEach(printjson);

// Delete documents where section equals 3 (if any).
print("\nDelete documents in class1 where section equals 3:");
var deleteResult = db.class1.deleteMany({ "section": 3 });
printjson(deleteResult);
