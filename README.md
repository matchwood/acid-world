## Acid-world
[acid-state](https://github.com/acid-state/acid-state) is a great package, but it misses some useful features. Acid-world is designed to be a further exploration of the design space in the direction of greater flexibility and usability. Like acid-state, the main persistence model used in acid-world is event logging combined with checkpoints.

### Features

#### Multiple state segments

#### Multiple serialisation options

#### Composable (sorta) update events

#### User specified invariant checking as part of atomic updates

#### Database backends (eg Postgres)

#### Alternative persistence strategy for data that won't fit into memory (keyed maps)

### Improvements

#### 'Constant' memory usage

#### No Template Haskell

#### Simpler file structure for persistence

### Probable additional features

#### Historic event querying (replays etc)

#### Event notifications


### State of this code

Rather than copying and pasting swathes of acid-state, this package has been pretty much written from scratch. The focus has been on producing a proof of concept, mainly on the type level (handling heteregenous lists with lots of class constraints and type synonyms is a bit finicky). I have therefore not focused much on exception handling / memory usage / performance.

