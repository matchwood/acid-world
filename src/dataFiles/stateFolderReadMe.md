## State folder structure

Inside the state folder there will be one `current` folder, and possible one `previous`. The presence of a `previous` folder indicates that the last attempt at checkpointing did not complete. `acid-world` will automatically sort this out when it is next succesfully started. 

By default, the `current` (or the `current` + `previous`) folders contain the most recent checkpoint and any events that have been persisted since that checkpoint.

All previous checkpoints and event logs are contained in the `archive` folder. Each individual folder in the `archive` folder contains a valid possible state that can be used to restore from. The folders are named with the full UTC date and time of their creation. To restore from an archived state simply rename / remove the existing `current` (or `current` + `previous`) folders, and copy the archive folder you are interested in to `[statePath]/current`. 

All of the folders in the `archive` folder are not necessary for restoring the current state, but if you are interested in querying historial event logs then you need to keep them around. If you are not interested in historical event logs then they can safely be deleted or removed.