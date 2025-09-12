# DiceDB - Inifinite Cache

## Building the Module

1. Make sure you have gcc installed
2. Update the path in Makefile and make it point to directory in DiceDB where valkeymodule.h is present
3. Run the build command:
```bash
make
```

This will create `dicedb-infcache.so` shared library file.

## Loading the Module

Start DiceDB server with the module

```bash
dicedb-server --loadmodule <path to dicedb-infcache.so>
```
