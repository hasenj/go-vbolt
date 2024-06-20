# Introduction

VBolt is a data persistance and retrieval system.

It's thin layer on top of [BoltDB][bolt], that turns it from a raw byte buffer
based key-value store, into a robust storage engine that provides indexing
capabilities.

It's only a few hundred lines of code, compared to bolt itself, which is about
4000 lines (excluding comments, whitespace, and tests).

VBolt relies on [VPack][vpack] for the serialization scheme, which itself is
also merely a few hundred lines of code.

[bolt]: https://pkg.go.dev/github.com/boltdb/bolt
[vpack]: https://pkg.go.dev/go.hasen.dev/vpack

# Building blocks

Here's a list of basic concepts you need to understand to start using this
package:

- Database
- Transaction
- Binary Serialization
- Bucket
  - one-to-one mapping
  - key -> object
- Index
  - bidirectional many-to-many map
  - term <-> target

## Database & Transactions

These concepts are carried over from [BoltDB][bolt] as-is. The database is a single
file, which you open during the initialization of your application.

When you want to perform a read operation, you create a Read Only Transaction.
If you want to update data, you create a Read Write Transaction.

Write transactions are serialized, so you can only open one at a time.

You **MUST** close the transaction when you are done with it. This includes
read-only transactions. Don't have long running transactions.

## Binary Serializtion

Binary serialization is provided via [VPack][vpack]. Refer to its documentation.
Essentially, a serialization function takes a pointer to the object you want to
serialize/deserialize, and a pointer to a buffer. You serialize the fields of
the object in order. The same function serves both as a serialization AND
deserialization.

## Bucket

A bucket is a one-to-one mapping that's persisted to storage. The key is
typically an integer or a uuid, and the value is an arbitrary object.

To use a bucket, you first have to declare it

```go
var Info vbolt.Info // define once


// declare the buckets your application wants to use


// int => User
// maps user id to user
var UserBucket = vbolt.Bucket(&Info, "user", vpack.FInt, SerializeUser)

// int => []byte
// maps userid to bcrypt hashed password
var PasswordBucket = vbolt.Bucket(&Info, "passwords", vpack.FInt, vpack.ByteSlice)

// string => int
// maps username to userid
var UsernameBucket = vbolt.Bucket(&Info, "username", vpack.StringZ, vpack.Int)

// declare as many buckets as you want ...

```

You declare your buckets by calling `vbolt.Bucket`. The return value is
`*volt.BucketInfo`, which will be used for reading from the bucket and writing
to it.

Bucket declarations are bound to a `vbolt.Info` object for introspection
purposes.

Unlike a regular `map` in Go, a bucket is not _just_ about mapping a key to
value; it's primarily about persisting data to disk, so the `key` and `value`
parameters are not given as _types_, but as _serialization functions_.

Since the underlying storage is a B-Tree, you might want to give some consideration regarding which serialization function you want to use for the key.

If you want to be able to iterate on the items in the bucket in order, use the
`vpack.FInt` serialization function, because it stores integers as 8 bytes (in
big endian), guaranteeing that the order of keys in the bucket matches the
numeric order of the ids.

Similarly, if you want to iterate on a bucket in order where the key is a
string, use `vpack.StringZ` instead of `vpack.String`. The former uses c-style
null terminated encoding for the strings, making it suitable for sorted
iteartion, while the later places the length of the string (in varint encoding)
before the string, which while useful when encoding a string field as part of
an object, does not have any particular advantage when used as the serialization
function for the key of a bucket.

### Reading

```go
func Read[K comparable, T any](tx *Tx, info *BucketInfo[K, T], id K, item *T) bool
```

To read from a bucket, you need a transaction.

The api is designed to take a pointer to the variable that will be filled with
the data read. The return value is a boolean indicating success or failure.

Example:

```go
// assume tx and userId are given
var user User
vbolt.Read(tx, UserBucket, userId, &user)
```

We provide helper functions to read a list of object ids into a slice or a map:

```go
func ReadSlice[K comparable, T any](tx *Tx, info *BucketInfo[K, T], ids []K, list *[]T) int
func ReadSliceToMap[K comparable, T any](tx *Tx, info *BucketInfo[K, T], ids []K, itemsMap map[K]T) int
```

The return value is the number of objects loaded, which maybe smaller than the
number ids given.

This is useful to load related objects. For example, suppose you have an
`Article` object and it belongs to a list of `Categories`. How do you model this
relationship? Well it's simple: the Article struct has a `CategoryIds []int`
field, and after you load the article, you can load the categories:

```go
var article Article
var categories []Category
vbolt.Read(tx, ArticleBucket, articleId, &article)
vbolt.ReadSlice(tx, CategoryBucket, article.CategoryIds, &categories)
```

To get the list of articles in the given category, you use the index, which is
explained in the next section.

### Writing

To write to a bucket, you need a read-write transaction.

```go
func Write[K comparable, T any](tx *Tx, info *BucketInfo[K, T], id K, item *T)
```

If `id` is the zero value for its type, nothing will be written. =

Example usage:

```go
vbolt.Write(tx, UserBucket, userId, &user)
vbolt.Write(tx, UserNameBucket, user.Name, &userId)
vbolt.Write(tx, PasswordBucket, userId, &passwdHash)
```

NOTE: if writing fails, the Write function will panic.

It will not return an error. It will panic.

Why not return errors? Because there's nothing you can do in the error recovery
other than abort the operation and report to the user that an error has
occurred.

A write error here is not much different than trying to access data behind a
null pointer, or access an element outside array bounds.

They really _are_ exceptions, and it's not wise to litter your code with checks
against those errors.

Instead, you should write the code so that such "errors" never happen.


All the conditions under which a write error occurs (other than OS level I/O
errors) are avoidable programmer mistakes:

- The transaction is not writable
- The bucket name is invalid
- The key is invalid (too long in bytes)
- Serialization fails

Your serialization functions should never fail. You should never use a read-only
transaction to write data. Your keys should never be invalid.

The only reasonable course of action is to recover from the panic at a central
top level function, report the error to the user, and let the program continue
normally (waiting for user requests to do other operations).

### Sequential Ids

If the key for your bucket is an int, you can leverage this function:

```go
func NextIntId[K, T any](tx *Tx, info *BucketInfo[K, T]) int
```

It's wrapper around BoltDB's `bucket.NextSequence` but it panics on failure
instead of returning an error.

Example:

```go
userId := vbolt.NextIntId(tx, UserBucket)
```

## Index

An index is a bidirectional multi-map, or a many-to-many map. The expected
usecase for an index is to map a term to a target.

"Term" refers to a query term, while a target is usually an object id.

Conceptually you have a bunch of [target, term] pairs that are maintained such
that you can quickly find the list of targets given a term, or the list of terms
given a target.

Think of an index on the back of a book. You have some key words and the pages
they appear in. The word is the _term_, while the page number is the _target_.

```go
// int => int
// term: keyword id, target: object id
var KeywordsIndex = vbolt.Index(&Info, "keywords-idx", vpack.StringZ, vpack.FInt)
```

How would you build such an index? A simple way is to iterate on the pages in
the book. For each _page_, you can collect all the important _words_ that appear
on it, and update the index by setting the "terms" that point to the "target"
(page number) on the index.

This is the API we provide to update the index: set target terms. Given an
index, and given a target, set the terms for it.

Now, it gets a little bit more complicated.

### Target Priority

Each [term, target] pair has a _priority_ to sort the targets relative to the
term. If you don't need a priority to sort the targets by, then you can ignore
this feature, but if you do want targets to be sortable, you need to give the
matter some consideration.

You need to come up with a scheme to produce a _priority_ number for each term,
_without_ considering anything outside the target object.

In the keyword <-> page number example (for the book's index section), we need a
scheme that computes the priority _without_ having to check all the other pages
that the term appears on.

A simple but effective scheme in this case would be to give more weight to the
term if it appears in a header (or a subheader) on the page, and less weight if
it appears in the footnotes.

We can count how many times the word appears, and multiply each occurance by a
weight assosiated with the type of block it appears in. Example weights:

- Header: 20
- Subheader: 10
- Regular text: 5
- Footnotes: 1

Why this scheme? Well consider some term T that appears in a header on page 100
but merely in the footnotes on page 10. Assuming people looking in the index
want to learn more about topic represented by the term T, it makes sense for us
to point them to page 100 first, before we tell them about page 10.

Now, in the current version of VBolt, the priority is stored as FUint16, meaning
it's 16bit integer sorted in ascending order. So the lower priority will appear
first. We can convert a weight to a priority by first capping the weight at some
ceiling value, say, 1000, and then subtract the weight from that ceiling value to
produce the priority.

The key is composed of the "term" bytes, followed by the priority bytes,
followed by the target bytes. You can visualize it like this:

    [B | T | T | T | T | T | T | P | P | K | K | K | K | K]

Where `B` is a special byte value, `T` and `K` are flexible because they are
used provided, but P at this point is fixed to uint16 in big endian encoding.
Because of this limitation, the priority number needs to be chosen so that the
sort order is meaningful when priorities are ordered ascendingly.

A more robust system (perhaps we can do this in a future update) is to use a
serialization function for the priority as well, and provide little endian
serialization functions in the VPack library so that you can sort numbers in
descending order.

### Index APIs

### Set Target Terms

```go
func SetTargetTerms[K, T comparable](tx *Tx, info *IndexInfo[K, T], target K, terms map[T]uint16)
```

This sets the terms that should map to the given target. If there are already
targets in the index that map to this object but are not present in the request
set of terms here, they will be removed. If a term already exists but has a
different priority, the priority will be updated. If a term is new, it will be
added.

The `terms` parameter is a map because it maps the term to its priority.

If all the terms have the same priority (i.e. the priority depends on the target
itself and not on the term), you can use this helper function:

```go
func SetTargetTermsUniform[K, T comparable](tx *Tx, info *IndexInfo[K, T], target K, terms []T, priority uint16)
```

and if you don't care about priorities at all, you can use:

```go
func SetTargetTermsPlain[K, T comparable](tx *Tx, info *IndexInfo[K, T], target K, terms []T)
```

Example usage:

```go
// collect the terms-priority mapping somehow from the text on the current page
terms := collectPageKeywordPriorities(page.Content)
// update the keywords index
vbolt.SetTargetTerms(tx, KeywordsIndex, page.Number, terms)
```

### Iterate targets for a term

Given a term, we want to read the targets ordered by priority, with the option
of starting at an offset, or a specific key, and the option of limiting the
number of targets loaded

```go
type Window struct {
	StartByte []byte
	Offset    int
	Limit     int
}

func ReadTermTargets[K, T comparable](tx *Tx, info *IndexInfo[K, T], term T, targets *[]K, window Window) []byte
```

If we expect the term to not have many targets and we can load all of them at
once, then we don't have to worry about the window. However, the reason we do
have the concept of an index in the first place is that we expect the list of
targets to be large, so we need some method of controlling its pagination.

The simplest way is using an offset, limit pair. But a more robust way is to use
a `start key`. Because the Index relies on the B-Tree implementation, and because
the keys are sorted such that all targets for a term appear in sequence in the
B-Tree, we can have faster iteration by remember the literal byte key we stopped
at in the previous pagination step.

Note that if the supplied window.StartByte is not valid for the given term, the
function will exit early and nothing will be added to `targets`.

The return value from ReadTermTargets is a `[]byte` representing the starting
point for the iteration cursor if we want to load the next page quickly.

In the context of a web application, this `[]byte` can be encoded to base64 to
be sent to the client as an opaque "cursor".

Example usage:

```go
// assuming query.Cursor represents the query parameter "cursor" from the http request
var startKey = base64.RawURLEncoding.DecodeString(query.Cursor)
var window = volt.Window{ StartKey: startKey, Limit: 10 }
var pageNumbers = make([]int, 0, 10)
var nextKey = vbolt.ReadTermTargets(tx, KeywordsIndex, query.Term, &pageNumbers, window)
var nextCursor = base64.RawURLEncoding.EncodeToString(nextKey)

// load pages via pageNumbers
var pages []Page
vbolt.ReadSlice(tx, PagesBucket, pageNumbers, &pages)
```

### Counts

The index automatically keeps track of "counts": how many targets are currently
assigned to a specific term

```go
func ReadTermCount[K, T comparable](tx *Tx, info *IndexInfo[K, T], term *T, count *int) bool
```

Example usage (continuing with the keywords example)

```go
var pagesCount int
vbolt.ReadTermCount(tx, KeywordsIndex, query.Term, &pagesCount)
```

### Notes

You should not use indices to store "source of truth" kind of data. The data you
store in the index should be deriveable from the source object.

The next section should clarify this point.

# Modelling relationships

## Modelling one to many relationships

Suppose an instance of type A refers to many instance of type B, such that it's
not many-to-many. That is, if some instance `b0` of type B is referred to by an
instance `a0` of A, then there's no other instance of A that also refers to
`b0`.

How do we model such a thing in VBolt?

You have a few choices, but the most obvious are:

- Directly embed all B instances inside the A instace.

  This is the obvious choice when B is always only ever loaded as a part of A
  and doesn't realy exist as an independent unit.

  ```go
  type A struct {
      Id int
      ...
      BList []B
  }

  type B struct {
      ...
  }
  ```

- Store each type in a separate bucket, use a field to implement one side of the
  relationship, and an index to implement the other

  We have two choices here.

  - A refers to Bs, with an optional index to get from B to A

    ```go
    type A struct {
        Id int
        ...
        B_Ids []int
    }

    type B struct {
        Id int
        ...
    }
    var A_Bucket = vbolt.Bucket(&info, "a", vpack.FInt, Serialize_A)
    var B_Bucket = vbolt.Bucket(&info, "b", vpack.FInt, Serialize_B)
    var BtoA = vbolt.Index(&info, "b=>a", vpack.FInt, vpack.FInt)

    // when updating A
    vbolt.SetTargetTermsPlain(tx, BtoA, A.Id, A.B_Ids)

    // when load the A associated with a B
    var aId int
    vbolt.ReadTermTargetSingle(tx, BtoA, b0.Id, &aId)
    var a0 A
    vbolt.Read(tx, A_Bucket, aId, &a0)
    ```

  - Bs refers to A, with an index to get from A to Bs
    ```go
    type A struct {
        Id int
        ...
    }

    type B struct {
        Id int
        A_Id int
        ...
    }

    var A_Bucket = vbolt.Bucket(&info, "a", vpack.FInt, Serialize_A)
    var B_Bucket = vbolt.Bucket(&info, "b", vpack.FInt, Serialize_B)
    var AtoB = vbolt.Index(&info, "b=>b", vpack.FInt, vpack.FInt)

    // when updating B
    vbolt.SetTargetSingleTerm(tx, AtoB, B.Id, B.A_Id)

    // when loading Bs from A
    var window vbolt.Window
    var bIds []int
    vbolt.ReadTermTargets(tx, AtoB, a0.Id, &bIds, window)
    var bList []B
    vbolt.ReadSlice(tx, B_Bucket, bIds, &bList)
    ```

## Modelling many to many relationships

For many to many relationships, both A and B refer to each other without limit,
however, usually one of them will have fewer references than the other. For
example, imagine a blog website: each article belongs to one or more categories,
but there are far more articles than there are categories, and while an article
might belong to three categories, a category can have hundreds or thousands of
articles. So it makes sense to store the categoryIds on the article and use the
index to get from the category to the articles.

Your first job is to identify which object id constitutes the term to the index,
and which constitutes the target, based on the criteria explained above.

In this case, the Article is the target and the Category is the term.

```go
type Category struct {
    Id int
    Name string
    ...
}

type Article struct {
    Id name
    ...
    CategoryIds []int
}

var ArticleBucket = vbolt.Bucket(&info, "article", vpack.FInt, SerializeArticle)
var CategoryBucket = vbolt.Bucket(&info, "category", vpack.FInt, SerializeArticle)
var CategoryArticles = vbolt.Index(&info, "cat=>art", vpack.FInt, vpack.FInt)

// when saving an article
vbolt.Write(tx, ArticleBucket, article.Id, &article)
vbolt.SetTargetTermsPlain(tx, CategoryArticles, article.Id, article.CategoryIds)

// when reading categories from article
var categories []Category
vbolt.ReadSlice(tx, CategoryBucket, article.CategoryIds, &categories)

// when reading articles from category
var window = vbolt.Window { ... }
var articleIds []int
vbolt.ReadTermTargets(tx, CategoryArticles, category.Id, &articleIds, window)

var articles []Article
vbolt.ReadSlice(tx, ArticleBucket, articleIds, &articles)
```

# Pre-Computation

## Index is for acceleration, not a source of truth

Notice that we only use the index as an acceleration structure. We never store
the source data on the index.

For example, to model the relationships between articles and categories, we
stored the `categoryIds` on the Article. In theory, we can retrive all articles
ids associated with a category id by iterate all articles and checking the
`categoryIds` field, but that would be very inefficient, so we use the index to
store the reverse relationship.

```go
vbolt.SetTargetTermsPlain(tx, CategoryArticles, article.Id, article.CategoryIds)
```

What this line does is essentially facilitate the pre-computation of the list of
article ids associated with category ids.

If the index data is gone for some reason, we can rebuild it by iterating on all
articles and calling `SetTargetTerms`

## Summary Objects

Suppose you're viewing the paginated view of the articles included in a certain
category, and the list contains, say, 30 articles per page.

You probably don't want to load the entire article each time and send it over
the network to the client. They probably won't even read them. It's a lot of
wasted resources for both the server, the network bandwidth, and the client.

Instead you want some kind of `ArticleSummary` object, that just contains the
title, the subtitle, a short summary, and the publication date.

Should you produce the article summary after you load the object? Remember in
the sample code above, we did this:

```go
var articles []Article
vbolt.ReadSlice(tx, ArticleBucket, articleIds, &articles)
```

We could call a function to summerize each article at this point.

```go
var summaries []ArticleSummary
for _, article := range articles {
    summaries = append(summaries, SummerizeArticle(article))
}
```

This would limit the wasted resource for the network and the client, but not for
the server: the server now has even more work, first load the entire article,
then produce a summary for it.

It would be much better if the summary was pre-computed, and we just loaded the
summary.

```go
// type and bucket definitions
type ArticleSummary struct {
    Id int // same as article id
    ....
}

var ArticleSummaryBucket = vbolt.Bucket(&info, "article_summary", vpack.FInt, SerializeArticleSummary)

// when saving an article
var summary = SummerizeArticle(article)
vbolt.Write(tx, ArticleSummaryBucket, article.Id, &summary)

// when loading category articles:
var summaries []ArticleSummary
vbolt.ReadSlice(tx, ArticleSummaryBucket, articleIds, &summaries)
```

## Pre-Compute everything

So much of the resource waste and performance problems in the relational
paradigm come from the desire to normalize all data and the obsession with the
querying capabilities of the SQL database engine.

The truth is, while the flexibility of SQL is useful when inspecting the data as
a user, they are not that useful when operating on the data as a program.

SQL was initially designed as a UI, not as an API.

Which brings me to why I even created this package in the first place.

# Motivation & Philosophy

I created this package because I wanted a storage engine that does not suffer
from impedance mismatch between the way application code thinks about data and
the way the database thinks about it.

As a bonus, I also wanted a system that does not do a lot of needless paper
shuffling busy work.

Consider what happens when you use an ORM to load a simple flat object from an
SQL based relational database.

- The ORM layer constructs an SQL string
- The SQL string is sent over to the database engine (potentially over network,
  unless using SQLite)
- The database engine parses the SQL string and decides how to execute it
- The database engine fetches data from its storage
- The database engine formats data as a set of rows with named and/or ordered
  columns
- The database sends the data over back to your application code in a table
  format
- The ORM layer combines the table formatted data into a struct/object in your
  target language
- (optional) You convert the object from the ORM format into your own
  application format

Almost all of the above work is pointless busy work that does not need to
happen. Here's what I actually want to happen:

- I tell the storage layer the address of the object I want to load
- The storage layer finds the serialized version of the object
- The data is deserialized into the format used by my application code

Now consider what has to happen if your data is not simply flat, and does not
neatly fit into the format of a table with columns. Consider a structure like
the following:

```go

type A struct {
    ....
    BItems []B
    X_Ids  []uint64

}

type B struct {
    ....
    CItems []C
    Y_Ids  []uint64
}

type C struct {
    .....
    Z_Ids []uint64
}

```


If you want to handle this kind of data layout through a relational database,
you have to write additional code, often very complicated, to map your data back
and forth into and from relational tables.

What's worse, you have to do this mapping three times:

- Reading data
- Updating data
- Inserting data

Because the shape of the query for each type of operation is totally different.
Insert statements are totally different from update statements, and totally
different from select statements.

Now, you might say that your ORM library handles it for you, but does it really?
I've seen quite a number of ORMs and I don't think any of them handles nested
objects really well beyond one level, even in dynamically typed languages.

Even if some ORM can do it, this is still a lot of pointless work that
relational databases force your program to do, and a lot of complexity in the
ORM code that frankly should not need to exist.

Now it gets worse: what if the nested data (structs B and C in the example
above) are almost never loaded as independent units of data by themselves ever?
They are always loaded as part of their container object A, and are always
updated along with their container object A. Ideally you want to bundle the
whole object together even in the storage system, but relational databases don't
let you do that.

---------------

We want data storage and retrieval to have a proper programmatic interface
(API), not a textual query language.

We want to have complete control over how we store data: whether we bundle all
nested objects together with their parent or spread them apart.

We want querying and indexing to be explicit. There are only two types of
queries:

- Loading objects from a bucket by id(s)
- Iterating over matches from an index by a query term

To make the index queryable, we build and populate it explicitly.

Although the automatic indexing of SQL sounds appealing, the lack of control,
and the imposition of object-relational impedance mismatch, combine to negate
(nay, eradicate) all the supposed gains.

This notion of the database engine as "magic" that automatically figures out how
to perform queries efficiently is a source of many performance problems. So many
dev teams in the industry waste countless hours fire fighting performance
problems that arise from such mis-use of the database.
