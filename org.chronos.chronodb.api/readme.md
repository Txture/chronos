# ![ChronoGraph](../markdownSources/chronoDB.png)

ChronoDB is a key-value store with support for content versioning and secondary indexing.

For a conceptual overview, please see our [article in Software and Systems Modeling](https://link.springer.com/content/pdf/10.1007/s10270-019-00725-0.pdf). If you want to dig right into the code and start using ChronoGraph, read on.

> **Fair Warning:** ChronoDB is a very low-level API, and many responsibilities are left to the developer using it. For most use cases, you'll want to use [ChronoGraph](../org.chronos.chronograph/readme.md) instead. Only use ChronoDB directly if you are sure that you _really_ want a key-value interface.

# Getting Started

First of all, you need to include ChronoDB in your JDK project. You can use your favourite dependency
management tool.

You need two artifacts:

- `org.chronos.chronodb.api` contains the Java API and the reference in-memory backend.
- `org.chronos.chronodb.exodus` contains the default persistent backend implementation.

## Gradle

```gradle
dependencies {
  implementation 'com.github.martinhaeusler:org.chronos.chronodb.api:1.0.0'
  implementation 'com.github.martinhaeusler:org.chronos.chronodb.exodus:1.0.0'
}
```

... or, with the Gradle Kotlin DSL:

```kotlin
implementation("com.github.martinhaeusler:org.chronos.chronodb.api:1.0.0")
implementation("com.github.martinhaeusler:org.chronos.chronodb.exodus:1.0.0")
```

## Maven

```xml
<dependency>
  <groupId>com.github.martinhaeusler</groupId>
  <artifactId>org.chronos.chronodb.api</artifactId>
  <version>1.0.0</version>
</dependency>
<dependency>
  <groupId>com.github.martinhaeusler</groupId>
  <artifactId>org.chronos.chronodb.exodus</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Building from Source

If you would rather build everything from source, simply run:

```sh
./gradlew build
```

... from the **root** directory to run a standard gradle build of all artifacts.

## Creating a new ChronoDB instance

There are multiple ways to create a ChronoDB instance. The default way is via a builder syntax:

```java
ChronoDB chronoDB = ChronoDB.FACTORY.create()
                        // indicate the backend you would like to use
                        .database(ExodusChronoDB.Companion.getBUILDER())
                        // indicate the directory where your data is located
                        .onFile("/path/to/your/database/folder")
                        // you can call further configuration methods here
                        .build()
```

If you would rather have an in-memory backend:

```java
ChronoDB chronoDB = ChronoDB.FACTORY.create()
                        .database(InMemoryChronoDB.BUILDER)
                        // you can call further configuration methods here
                        .build()
```

Alternatively, you can create a ChronoDB instance from a `java.util.Properties` object (which you can either create directly, or load e.g. from a `*.properties` file):

```java
Properties properties = new Properties();
properties.setProperty(ChronoDBConfiguration.STORAGE_BACKEND, "exodus");
properties.setProperty(ExodusChronoDBConfiguration.WORK_DIR, "/path/to/your/database/folder");
ChronoDB chronoDB = ChronoDB.FACTORY.create()
                        .fromConfiguration()
                        .fromProperties(properties)
                        // any configuration method called here will OVERRIDE the properties
                        .build();
```

For more configuration options, please refer to the configuration classes mentioned in the code sample above, or consult your code completion on the builder.

> **Important note:** ChronoDB instances implement `AutoCloseable` and must be closed explicitly by the programmer by calling `.close()` on them!

## General Usage

The `ChronoDB` and `ChronoDBTransaction` are the primary interfaces you will be working with. `ChronoDB` is the "root" interface that grants access to all other features, while `ChronoDBTransaction` allows you to read data from the store and write data to it.

```java
// create a ChronoDB instance as outlined above
ChronoDB db = ...;

// insert some data into the database
ChronoDBTransaction tx = db.tx();
tx.put("My Key", "My Value");
tx.commit();

// read the value back
ChronoDBTransaction tx2 = db.tx();
String value = tx2.get("My Key");
System.out.println(value); // will print "My Value"

// once you are done with the database...
db.close();
```

> **ATTENTION:** All queries in ChronoDB return the data **as-is** from the database. If you modify a key-value pair in the transaction, and query the database before your change is committed, it will **not** be visible in the queries (even though it's the same transaction). **This is intended behaviour**. Those semantics are different from the way e.g. ChronoGraph handles transactions: transient changes are visible to queries within the same transaction in ChronoGraph.

## Keyspaces

To keep the contents organized, ChronoDB offers the concept of **keyspaces**. Formally, a keyspace is a named collection of key-value-pairs with unique keys. You can think of each keyspace as a folder in your file system; within it, each file name must be unique. Another analogy for keyspaces are SQL tables; within each table, the primary key must be unique.

To write data to a keyspace, or read data from a keyspace, simply specify its name as the **first** parameter in `transaction.get(...)` and `transaction.put(...)` methods:

```java
ChronoDB db = ...;

ChronoDBTransaction tx = db.tx();
// keyspaces are created on-the-fly. We can simply pretend
// that they exist when inserting data.
tx.put("Math", "pi", 3.1415);
tx.put("Math", "one", 1);
tx.put("Phone Numbers", "John Doe", "123-456-789");
tx.put("Phone Numbers", "Jane Doe", "987-654-321");
tx.commit();

ChronoDBTransaction tx2 = db.tx();
System.out.println(tx.get("Math", "pi")); // prints "3.1415"
System.out.println(tx.get("pi")); // prints "" (value is null; we forgot the keyspace!)
System.out.println(tx.get("Phone Numbers", "John Doe")); // prints "123-456-789"
```

## Supported Value Types

ChronoDB supports **arbitrary** values as long as they are serializable and deserializable with [Kryo](https://github.com/EsotericSoftware/kryo). In most cases, this means:

- All primitive types (e.g. `int`, `long`, `double`...), their wrappers (`Integer`, `Long`, `Double`) and Strings are supported
- `Array`s of those types are supported
- `List`s, `Set`s and `Map`s of those types are supported
- A custom class is supported if:
  - It has a default (no-argument) constructor (may be `private`)
  - It only references fields which are of a supported type

**BIG WORD OF CAUTION:** Kryo performs reasonably well on custom types. However, keep in mind that your format is **set in stone**. There are no migration paths in case your classes change. Also, you will not be able to load your data from persistence again if the class you used to serialize it is no longer on your classpath, or its fields have changed. If you expect/require major format changes, please consider using only primitives and strings. A common way to store data in a way that does not depend on kryo is by first serializing it to a string (e.g. JSON or XML). The price to pay of course is a larger footprint on disk and the added performance cost of the conversion.

## Secondary Indices & Queries

ChronoDB offers secondary indices on your data. Since ChronoDB assumes very little about the internal structure of your values, you need to tell ChronoDB how to extract an indexable value from your data. You do this by implementing an `Indexer`. To be precise, one of `StringIndexer`, `LongIndexer` and `DoubleIndexer`, like so:

```java
public class PersonFirstNameIndexer implements StringIndexer {

    @Override
    public boolean canIndex(final Object object) {
        // we can index only person objects
        return object instanceof Person;
    }

    @Override
    public Set<String> getIndexValues(final Object object) {
        Person person = (Person) object;
        return Collections.singleton(person.getFirstName());
    }

    // Indexers MUST implement hashCode() and equals()!
    // If your indexer contains fields, you have to include them!

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public boolean equals(final Object obj) {
       return super.equals(obj);
    }

}
```

You can then add your custom indexer to the `IndexManager` to activate it:

```java
ChronoDB db = ...;
IndexManager indexManager = db.getIndexManager();

indexManager.addIndexer("firstName", new PersonFirstNameIndexer());
// ... add more indexers if you want...

// after all indexers have been added, rebuild the index
indexManager.reindexAll();
```

> **WARNING:** Index manager operations are _management operations_. They are **not** thread-safe, **not** subject to transactional safety and should not be invoked during regular database operation! It is highly recommended to set up the indices on application startup, and to keep them the same after that.

> **IMPORTANT:** For any given value object, there must be **at most one indexer per index name** where `canIndex(...)` returns `true`. If there are multiple indexers for the same object and index name, an exception will be thrown.

Once we have built a secondary index, we can run a **query** on it:

```java
ChronoDB db = ...;

ChronoDBTransaction tx = db.tx();
Set<QualifiedKey> allJohns = tx.find()
    .inKeyspace("persons")
    .where("firstName").containsIgnoreCase("john")
    .getKeysAsSet();

// qualified keys are simply combinations of keyspaces and keys.
for(QualifiedKey qKey : allJohns){
    Person person = tx.get(qKey.getKeyspace(), qKey.getKey());
}
```

The ChronoDB query language is simple but powerful. Keep in mind that you can **only** query a property **if it has an index**. Non-indexed properties cannot be queried, because ChronoDB has no way of knowing how to extract the expected index value from your custom objects.

## Querying Historical Data

The primary use case for querying historical data in ChronoDB is to get a **view** on past states of the data. This is achieved by passing the desired date and time into the `.tx(...)` method as an argument. All data delivered by that transaction will reflect the database state at that point in time.

```java
ChronoDB db = ...;

long yesterday = System.currentTimeMills() - TimeUnit.DAYS.toMillis(1);
ChronoDBTransaction txYesterday = db.tx(yesterday);
// this will retrieve John Doe in the state it had yesterday
Person john = txYesterday.get("persons", "john.doe");

// secondary index queries are also supported, and will return yesterday's values:
Set<QualifiedKey> allOfYesterdaysJohns = tx.find()
    .inKeyspace("persons")
    .where("firstName").containsIgnoreCase("john")
    .getKeysAsSet();
```

You can also use the various overloads of `tx.getCommitTimestampsBetween`, `tx.getCommitMetadataBetween` and related methods to retrieve the individual commit timestamps.

## Branches

ChronoDB supports not only versioning, but also **branching**. You can think of branches the same way they are used in version control systems for source code (e.g. Git, SVN): they are "alternative realities", and once a branch has been created, changes on its parent will not be visible on the child branch, and vice versa. Branches can be nested arbitrarily deep, however expect a slight performance impact for each nesting level.

There is one branch which always exists by default and cannot be deleted. That branch is named `master`.

```java
ChronoDB db = ...;
BranchManager branchManager = db.getBranchManager();

// add some data to the master branch
ChronoDBTransaction masterTx = db.tx();
masterTx.put("hello", "world");
masterTx.put("foo", "bar");
masterTx.commit();

// create "my-branch" as child of "master"
branchManager.createBranch("my-branch");

// add some data to "my-branch"
ChronoDBTransaction myBranchTx = db.tx("my-branch");
myBranchTx.put("hello", "chronos");
myBranchTx.put("fizz", "buzz");
myBranchTx.commit();

// our changes are not visible on "master"...
ChronoDBTransaction masterTx2 = db.tx();
System.out.println(masterTx2.get("hello")); // prints "world"
System.out.println(masterTx2.get("fizz")); // prints "" (value is null)

// ... but they are visible on our branch
ChronoDBTransaction myBranchTx2 = db.tx("my-branch");
System.out.println(myBranchTx2.get("hello")); // prints "chronos"
System.out.println(myBranchTx2.get("fizz")); // prints "buzz"
// note that we can also see what was on master before the branch was created:
System.out.println(myBranchTx2.get("foo")); // prints "bar"
```
