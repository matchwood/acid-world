## Acid-world
[acid-state](https://github.com/acid-state/acid-state) is a great package, but it misses some useful features. Acid-world is a further exploration of the design space in the direction of greater flexibility and usability (it is like a 'world' of different acid 'states'). In particular, acid-world takes advantage of the development of type level programming in Haskell in the last few years: most of the code in this package was simply impossible when acid-state was released. 

Like acid-state, the main persistence model used in acid-world is event logging combined with checkpoints. This package is currently at the proof of concept stage, and is not suitable for use in production. 

### Features vs acid-state

#### Multiple state segments
Rather than providing a `State s` style environment with `s` defined as a single root type, acid-world allows you to work with multiple different types. This is achieved using type level lists and type level strings. Each type you want to use in your state is simply declared as a `Segment`, eg
    
    instance Segment "Users" where
      type SegmentS "Users" = HashMap Int User

You can then write queries and updates that specifically operate on that segment, using the `HasSegment` class, eg 
    
    -- `ss` is a type level list of available segments, like ["Users", "Addresses", "Comments"]
    insertUser :: (HasSegment ss "Users") => Int -> User -> AWUpdate ss User 
    insertUser k u = do 
      users <- askSegment (Proxy :: Proxy "Users")
      putSegment (Proxy :: Proxy "Users") (insert k u users)
      pure u

This allows for much better separation of concerns than acid-state, as update and query functions only need to know about the segments that concern them.

It also allows for something that is simply impossible in acid-state - libraries can define state segments and events of their own, and you can add them into your own acid-world. 

#### Multiple serialisation options
Acid world is structured to allow for multiple possible serialisation strategies and multiple possible backends. Serialisers and backends have to be using the same intermediate types (`ByteString` for example) to be used together. At present the main file system backend can be used with three different serialisers: CBOR, SafeCopy and JSON. The JSON serialiser produces utf-8 encoded files, so you can open them up and edit them by hand if so desired.

#### Composable update events
Acid state models every event to as a specific data type (produced by Template Haskell). Acid world has a single type class for events, and a single container. Instances look like
    
    instance Eventable "insertUser" where 
      type EventArgs "insertUser" = '[Int, User] -- arguments this event takes
      type EventResult "insertUser" = User -- the result of the event
      type EventSegments "insertUser" = ["Users"] -- the segments this event needs to be able to run 
      runEvent _ = toRunEvent {- provided by the package -} insertUser {- can be any function of the type Int -> User -> AWUpdate ss User -}

Events themselves are simply a container that wraps up the event name (which ties it to the `Eventable` class) and a heterogenous list of the arguments.

This allows for the definition of a type, `EventC`, that composes events together in a type safe way, and the whole composed event can be run atomically.

    [..] type EventArgs "insertAddress" = '[Text, Text, User] [..]

    composedEvent :: EventC '["insertAddress", "insertUser"]
    composedEvent = mkEvent (Proxy :: Proxy "insertAddress") "State College" "Pennsylvania" :<< mkEvent (Proxy :: Proxy "insertUser") 1 (User "Haskell" "Curry")
   

#### User specified invariant checking as part of atomic updates
Despite our best efforts, Haskell's type system cannot always capture all the invariants we would like to express. Acid world provides for a single invariant function for every state segment, of the form `a -> Maybe Text` where a `Just Text` is interpreted as an error message. You can, eg, restrict the number of users with `if HM.size a > 100 then Just "Only 100 users allowed!" else Nothing` . Invariants are guaranteed to be run every time a segment changes, prior to final persistence of the event. Future development may allow for a way to define invariants on parts of some segments (eg. the specific `(k,v)` change made to a HashMap).

#### Database backends (eg Postgres)
Some people like the idea of acid-state, but for whatever reason are required to use a database. Acid-world allows for this. In the current (rudimentary) postgresql backend events are stored in a single events table, while checkpoints are stored in their own tables, one for each segment.

#### Alternative persistence strategy for data (keyed maps) that won't fit into memory 
Perhaps the biggest pain point of `acid-state` is that it requires all data to be in memory all of the time. If you want transparent access to a Haskell type then this is effectively unavoidable. Various attempts have been made to provide an alternative to acid-state here, and this package includes another one (currently named, rather badly, `CacheState`). At present the implementation is fairly basic, but the idea uses a similar strategy to the rest of acid-world - state is separated out into segments, and updates and queries can operate on the segments that are relevant to them. 

The big difference with the rest of acid-world, though, is that the persistence is not event based. The strategy is more like that used by [VCache](http://hackage.haskell.org/package/vcache) (which sadly is not maintained) but a lot less sophisticated (at present at least). CacheState supports keyed maps (and single values, but a lot of the benefit is lost there). The idea is to provide an easy way to persist of set of table like structures (currently HashMap, and more interestingly, [IxSet] (https://hackage.haskell.org/package/ixset-typed). The spine (and with `IxSet`, the indexes) are kept in memory while the values themselves are not (depending on caching policy). This allows you to leverage, eg, the excellent indexing features of `IxSet` while addressing a much larger data set than could be held in memory. The current implementation uses LMDB as a persistence layer, but different backends (anything that can act as a key value store) could be implemented for this as well.

After thinking about this a fair amount I'm not sure that any solution can do much better than this and still have any flavour of `acid-state` to it. If you aren't keeping any part of the data structure in memory then you pretty much just have a deserialisation wrapper around something like LMDB, or a database (if you want indexing). But I might be completely wrong - all suggestions and thoughts are welcome!

### Improvements vs acid-state

#### 'Constant' memory usage
Due to the way acid-state deserialises it can sometimes use massive amounts of memory when restoring state (I once encountered a situation where it used something like 30GB of memory to restore a state of around 1GB). Acid world attempts to avoid this by deserialising in streams (using conduit), and therefore should never use much more memory than the state itself.

#### No Template Haskell
The most complicated user land class in acid-world is `Eventable`, which is really just a deconstructed function type definition and constraint. Template Haskell could be used to generate instances of Eventable, but it is not really necessary. Also, acid-world has no equivalent of the `makeAcidic` function - the design does not require a single place where everything is defined, aside from at a type level (the acid world container is parameterised with type level lists of every event and every segment you want to use).

#### Simpler file structure for persistence
A simple quality of life improvement - acid-world stores a single events log and single file for each segment in a checkpoint. When a new checkpoint occurs a new, dated, folder is created containing the previous state. So you have something like
 
    - current
      events.log.json
      - checkpoint 
        Users.json
        Addresses.json
    - archive
      - 2018-08-04_11-22-44-238800_UTC
        events.log.json
        - checkpoint
          Users.json
          Address.json

Every folder is a complete, restorable snapshot of the state.

#### Partial write detection and correction
Acid-world tags each event with a UUID, and writes out the UUID of the last persisted event to a separate file. It can therefore detect if a deserialisation error is the result of a partial write, and correct for this automatically (this feature is still under development).

### Future features 

#### Automatic checkpointing
With various strategies (time / file size / number of events). 

#### Historic event querying (replays etc)
All events are tagged with a UUID and a UTCTime. It would therefore be relatively easy to provide an api for reconstructing state up to a certain point in time, or to exclude specific events, or ranges of events, safely.

#### Event notifications
Events have type `Event (n :: Symbol)`. It will be easy enough to provide users with a way of registering pre and post event handlers of the type `Event n -> IO ()` or similar, specialised to individual events (so `Event "insertUser" -> IO ()`) that will be fired every time the event is issued.


### The state of this code

Rather than copying and pasting swathes of acid-state, this package has been pretty much written from scratch. The focus has been on producing a proof of concept, mainly on the type level (handling heteregenous lists with lots of class constraints and type synonyms is a bit finicky). Not much fine tuning has been done yet on exception handling / memory usage / performance, but the general structure of the package was designed from the start to put an emphasis on memory usage and performance: "\"premature optimization is the root of all evil\" is the root of all evil" as they say.

