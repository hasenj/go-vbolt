# Introduction

VBolt makes it simple to store, retrive, and query data with a regular
programming interface. No textual query language, no need for ORM, no impedence
mismatch. You just call functions to load and query data.

This means, for the vast majority of programs, you do not need to differentiate
between your "domain models" and your "persistance models". They're one and the
same.

VBolt lets you store and query your domain models

This vastly simplifies the programming model and shortens the time to get things
done.

Here's how you do it:

- Define your data models
- Define how they are to be serialized
    - Thanks to VPack, this is very simple and robust
- Define *Buckets* and *Indexes*

That's it.

You can:

- Use buckets to load and store objects.
- Use indexes to query objects

A bucket is essentially a persisted map (key -> value), while the index is a
persisted bi-directional multi-map (term <-> target) that you define explicitly.

Let's see a few practical example

# Example: group chats

A group chat is a place for people to post messages that only they can see.

- Threads can have multiple participants
- User can participate in multiple threads (groups)
- Users can post new messags to threads
- Users can read messages in threads
- Users can be notified when a thread has new messages since their last visit

Here's a simplified data model


```go

type User struct {
	vpack.UUID

	Handle      string
	DisplayName string
	Picture     string // relative path;
}

type Thread struct {
	vpack.UUID

	Participants  []vpack.UUID // list of user ids
	StartedAt     time.Time
	LatestMessage vpack.UUID // used to quickly detect the presense of unread messages
}

type Message struct {
	vpack.UUID

	SenderId  vpack.UUID // reference to User
	ThreadId  vpack.UUID
	Timestamp time.Time

	Content string
}

type LastRead struct {
	UserId   vpack.UUID
	ThreadId vpack.UUID
	LastRead vpack.UUID // last message in this thread the user knows about
	ReadAt   time.Time
}

```

Here's a possible definition for Buckets and Indexes

```go
```

Let's implement some use cases.

```go

```
