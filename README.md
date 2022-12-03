# clj-tikv

Clojure bindings for [TiKV](https://tikv.org).

This is a thin wrapper for the [tikv-java-client](https://github.com/tikv/client-java).

### Developing

In order to develop or use the lib locally you need to have a TiKV instance running.
This can either be achieved by using TiKV's own installer `tiup`.
Follow the instructions at [https://tikv.org/docs/5.1/concepts/tikv-in-5-minutes/]().
The second option is to user docker-compose. The instructions for this
can be found at [https://tikv.org/docs/3.0/tasks/try/docker-stack/]()
or you just go to the dev-tikv subdirectory and call
```
$ cd dev-tikv
$ docker-compose up
```
This should suffice to let you connect to a TiKV instance on `localhost:2379`.

To run the setup inside a docker swarm component run
```
$ docker stack deploy --compose-file stack.yml tikv
```

### Projects using clj-tikv

## License
