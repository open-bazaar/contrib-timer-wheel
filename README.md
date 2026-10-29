# bazaar-contrib-timer-wheel

Provide the `TIMER` service for scheduling callbacks.

```sh
bcn contrib add timer-wheel --enable
```

The corresponding entry in `config.toml`:

```toml
version = "5"

[[contribs]]
package = "bazaar-contrib-timer-wheel==0.3.*"
[[contribs.enabled]]
entry = "timer-wheel"
```
