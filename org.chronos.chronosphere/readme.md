# ![ChronoDB](../markdownSources/chronoSphere.png)

ChronoSphere is a [Model Repository](https://www.researchgate.net/profile/Juri_Rocco/publication/275637197_Collaborative_Repositories_in_Model-Driven_Engineering_Software_Technology/links/5540a98c0cf2320416ed0fd2.pdf) for models in the [Ecore](https://wiki.eclipse.org/Ecore) format defined in the [Eclipse Modeling Framework](http://www.eclipse.org/modeling/emf/) (EMF).

For a conceptual overview, please see our [article in Software and Systems Modeling](https://link.springer.com/content/pdf/10.1007/s10270-019-00725-0.pdf). If you want to dig right into the code and start using ChronoGraph, read on.

## Core Features

- Efficient storage of Ecore models with up to several 100,000 elements.
- Model versioning with commit-granularity.
- Lightweight Branching
- Model indexing and model-level query support.
- Developer-friendly API
- Dependency management via Maven / Gradle. No Eclipse or OSGi environment needed.
- Full support for dynamic Ecore (no source code generation required).
- Based on [ChronoGraph](../org.chronos.chronograph/readme.md), a versioned graph database.

## Getting Started

First of all, you need to include ChronoSphere in your JDK project. You can use your favourite dependency
management tool; all dependencies are provided at Maven Central.

You need four artifacts:

- `org.chronos.chronodb.api` contains the ChronoDB Java API and the reference in-memory backend.
- `org.chronos.chronodb.exodus` contains the default persistent backend implementation.
- `org.chronos.chronograph` contains the graph abstraction layer.
- `org.chronos.chronosphere` contains the actual EMF repository.

### Gradle

```gradle
dependencies {
  implementation 'com.github.martinhaeusler:org.chronos.chronodb.api:1.0.0'
  implementation 'com.github.martinhaeusler:org.chronos.chronodb.exodus:1.0.0'
  implementation 'com.github.martinhaeusler:org.chronos.chronograph:1.0.0'
  implementation 'com.github.martinhaeusler:org.chronos:chronosphere:1.0.0'
}
```

... or, with the Gradle Kotlin DSL:

```kotlin
implementation("com.github.martinhaeusler:org.chronos.chronodb.api:1.0.0")
implementation("com.github.martinhaeusler:org.chronos.chronodb.exodus:1.0.0")
implementation("com.github.martinhaeusler:org.chronos.chronograph:1.0.0")
implementation("com.github.martinhaeusler:org.chronos.chronosphere:1.0.0")
```

### Maven

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
<dependency>
  <groupId>com.github.martinhaeusler</groupId>
  <artifactId>org.chronos.chronograph</artifactId>
  <version>1.0.0</version>
</dependency>
<dependency>
  <groupId>com.github.martinhaeusler</groupId>
  <artifactId>org.chronos.chronosphere</artifactId>
  <version>1.0.0</version>
</dependency>
```

## Building from Source

If you would rather build everything from source, simply run:

```sh
./gradlew build
```

... from the **root** directory to run a standard gradle build of all artifacts.

## Starting a new instance

ChronoSphere employs an API design which we like to call a _Forward API_. It is designed to make the best possible use of code completion in an IDE, such as Eclipse, IntelliJ IDEA, Netbeans, or others. The key concept is to start with a simple object, and the rest of the API unfolds via code completion.

Let's create a new instance of ChronoSphere. Following the Forward API principle, you start simple - with the `ChronoSphere` interface. Code completion will reveal the static `FACTORY` field. From there, it's a fluent builder pattern:

```java
    ChronoSphere repository = ChronoSphere.FACTORY.create().inMemoryRepository().build();

    // remember to close this repository once you are done working with it.
    repository.close();
```

Note that the builder pattern employed above has many other options, and several different backends are supported. For a list of supported backends, please check the code completion.

After starting up a ChronoSphere instance, you should check the registered `EPackage`s:

```java
    Set<EPackage> ePackages = sphere.getEPackageManager().getRegisteredEPackages();
    if(ePackages.isEmpty()){
        // no EPackages are registered, add the EPackage(s) you want to work with.
        sphere.getEPackageManager().registerOrUpdateEPackage(myEPackage);
    }
```

Please note that **no code generation is required** when working with ChronoSphere. The **preferred** way of interacting with Ecore in ChronoSphere is to use the _Reflective API_ (e.g. `eObject.eGet(...)` and `eObject.eSet(...)`).

## Transactions

In order to perform actual work on a ChronoSphere instance, you need to make use of `Transaction`s. A transaction is a unit of work that will be executed on the repository according to the [ACID](https://en.wikipedia.org/wiki/ACID) properties. You also profit from the highest isolation level ("serializable", a.k.a. _snapshot isolation_), which means that parallel transactions will never interfere with each other in unpredictable ways.

To open a transaction on a ChronoSphere instance, call the `tx()` method, like so:

```java
    ChronoSphere repository = ...;
    try(ChronoSphereTransaction tx = repository.tx()){
        // perform work here
    }
```

The example above makes use of Java's [try-with-resources](https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html) statement. It _automatically_ closes the transaction for you.

> **Attention**: When a transaction is **closed** by the `try-with-resources` statement, any changes will be **rolled back** (i.e. undone), unless you call `tx.commit()` before reaching the end of the `try` block.

Let's add some actual EObjects to our repository:

```java
    ChronoSphere repository = ...;
    try(ChronoSphereTransaction tx = repository.tx()){
        EObject eObject = createSomeEObject();
        tx.attach(eObject);
        tx.commit();
    }
```

In the method `createSomeEObject` above, you can create virtually any `EObject` of your choice. As long as it is a syntactically valid `EObject` and adheres to the Ecore contract, it will work with ChronoSphere. Afterwards, we call `attach` in order to add this `EObject` to our repository. There are several overloads for `attach`, e.g. one that accepts `Iterable`s of `EObject`s for your convenience, in case that you want to add multiple elements at once. **Don't forget to call `commmit` to save your changes!**

## Queries

ChronoSphere comes with its own query framework, which we call _EQuery_. EQuery is a flexible, **traversal-based** query language that is **embedded into Java** as an [internal DSL](https://martinfowler.com/bliki/InternalDslStyle.html). So, in essence, it's just Java, so you should not have any issues with its syntax. Let's start simple:

```java
    ChronoSphere repository = ...;
    try(ChronoSphereTransaction tx = repository.tx()){
        // fetch the Ecore metadata we need from our registerd EPackage
        EClass person = EMFUtils.getEClassBySimpleName(tx.getEPackages(), "Person");
        EAttribute firstName = EMFUtils.getEAttribute(person,"firstName");
        // run the query
        Set<EObject> johns = tx.find().startingFromEObjectsWith(firstName, "John").toSet();
        System.out.println("Found " + johns.size() + " Person(s) with 'firstName' equal to 'John'");
    }
```

The EQuery language is very varied. The documentation is still a "todo", but you can use the code completion of your IDE to explore the possibilities.

# Frequently Asked Questions

## What about generated code (Ecore Genmodels)?

We do not explicitly support them. These classes **may** work with ChronoSphere, but we do not provide dedicated support. The reason is that ChronoSphere supports _metamodel evolution_ and _versioning_ at the same time. If you evolve your metamodel, and travel back in time, your new generated classes will not match the old metamodel anymore. Your code will break. Use dynamic Ecore instead.
